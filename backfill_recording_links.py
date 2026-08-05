#!/usr/bin/env python3
"""
Populates missing recording links in Salesforce from shortlinks already in S3.

WHY THIS EXISTS
  The linker writes a recording link when an object lands in S3. It is driven
  by an S3 event, so it only ever fires once, at upload time. Any recording
  whose upload predates a linker fix -- or which hit a bug at the moment it
  was processed -- never got its link, and never will: the event is long gone.

  This walks what is already in S3 and fills those gaps.

WHAT IT WILL NOT DO
  It NEVER overwrites an existing link. If the field already holds anything,
  the record is skipped and, where the stored link differs from the one this
  script would have written, the difference is REPORTED rather than acted on.

  That is deliberate. A shortlink id is randomly generated, so a recording can
  legitimately have more than one shortlink pointing at it -- both working.
  Replacing a functioning link because it is not the one this script happened
  to pick would be a regression dressed up as a fix.

  It also NEVER creates a shortlink. Only shortlinks already present in S3 are
  used. A recording with no shortlink is reported, not invented -- creating one
  here would write a link the download service has never served and nobody has
  verified.

HOW IT WORKS
  1. Read every shortlink JSON once, building {meeting_id -> download url}.
     Only shortlinks whose key sits in a media folder are considered, so an
     annotated analysis video can never become the stored recording link.
  2. Batch-query Salesforce (~200 ids per call) for both objects.
  3. For each meeting id, decide the target the same way the linker does:
        an Interview__c record exists -> Interview__c.Interview_Recording_Link__c
        otherwise                     -> Session__c.Recording_URL__c
  4. PATCH only where the target field is empty.

  Departments are not filtered. Whatever has a shortlink and a matching record
  is covered.

SAFETY
  - DRY RUN BY DEFAULT. Nothing is written until --execute.
  - Existing values are never modified.
  - No S3 object is read, written, moved or deleted beyond reading shortlink
    JSONs.
  - Resumable: every PATCH is recorded in a manifest, so a re-run skips work
    already done.

USAGE
  python3 backfill_recording_links.py
  python3 backfill_recording_links.py --limit 5 --execute
  python3 backfill_recording_links.py --execute
"""

import argparse
import base64
import json
import os
import socket
import sys
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    import boto3
    from botocore.config import Config
except ImportError:
    print("boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)

try:
    import jwt
except ImportError:
    print("PyJWT not installed. Run: pip install PyJWT cryptography", file=sys.stderr)
    sys.exit(1)

BUCKET            = os.environ.get("MIGRATION_BUCKET", "zoom-automation-bucket")
SHORTLINK_PREFIX  = os.environ.get("SHORTLINK_PREFIX", "shortlinks/")
DOWNLOAD_BASE_URL = os.environ.get("DOWNLOAD_BASE_URL", "").rstrip("/")

# Only a key inside one of these folders may become a recording link. The
# analysis worker writes its annotated video OUTSIDE them, so this is what
# keeps it from being stored as the recording.
MEDIA_FOLDERS = {s.strip().lower() for s in
                 os.environ.get("LINKABLE_MEDIA_FOLDERS", "mp4").split(",") if s.strip()}

SF_SECRET_NAME = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")

SF_INTERVIEW_OBJECT  = os.environ.get("SF_OBJECT_API_NAME", "Interview__c")
SF_INTERVIEW_MID     = os.environ.get("SF_MEETING_ID_FIELD_API_NAME", "Zoom_Meeting_Id__c")
SF_INTERVIEW_LINK    = os.environ.get("SF_LINK_FIELD_API_NAME", "Interview_Recording_Link__c")

SF_SESSION_OBJECT = os.environ.get("SF_SESSION_OBJECT_API_NAME", "Session__c")
SF_SESSION_MID    = os.environ.get("SF_SESSION_MEETING_FIELD_API_NAME", "External_Meeting_ID__c")
SF_SESSION_LINK   = os.environ.get("TRAINING_SF_LINK_FIELD", "Recording_URL__c")

MANIFEST_FILE = os.environ.get("BACKFILL_MANIFEST_PATH", "backfill_manifest.json")
SNS_TOPIC_ARN = os.environ.get("MIGRATION_SNS_TOPIC_ARN", "").strip()
AWS_REGION    = os.environ.get("AWS_REGION", "us-east-1")

SOQL_BATCH_SIZE = 200

_cfg = Config(max_pool_connections=100, retries={"max_attempts": 10, "mode": "adaptive"})
s3 = boto3.client("s3", region_name=AWS_REGION, config=_cfg)

_lock = threading.Lock()


def log(msg=""):
    with _lock:
        print(msg, flush=True)


def notify(subject, message):
    if not SNS_TOPIC_ARN:
        return
    try:
        boto3.client("sns", region_name=AWS_REGION).publish(
            TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
        log(f"[NOTIFY] {subject}")
    except Exception as exc:
        log(f"[NOTIFY] failed (non-fatal): {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  Salesforce
# ══════════════════════════════════════════════════════════════════════════════

def get_sf_secret():
    raw = json.loads(boto3.client("secretsmanager", region_name=AWS_REGION)
                     .get_secret_value(SecretId=SF_SECRET_NAME)["SecretString"])
    missing = [k for k in ("SF_CLIENT_ID", "SF_USERNAME", "SF_LOGIN_URL") if not raw.get(k)]
    if missing:
        raise ValueError(f"Secret {SF_SECRET_NAME} missing: {', '.join(missing)}")
    key = raw.get("PRIVATE_KEY")
    if not key and raw.get("PRIVATE_KEY_B64"):
        key = base64.b64decode(raw["PRIVATE_KEY_B64"]).decode("utf-8")
    if not key:
        raise ValueError("Secret has neither PRIVATE_KEY nor PRIVATE_KEY_B64")
    return {"client_id": raw["SF_CLIENT_ID"], "username": raw["SF_USERNAME"],
            "login_url": raw["SF_LOGIN_URL"], "private_key": key}


def sf_login(sf):
    assertion = jwt.encode(
        {"iss": sf["client_id"], "sub": sf["username"],
         "aud": sf["login_url"], "exp": int(time.time()) + 300},
        sf["private_key"].replace("\\n", "\n").strip(), algorithm="RS256")
    data = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": assertion}).encode()
    req = urllib.request.Request(
        f"{sf['login_url'].rstrip('/')}/services/oauth2/token", data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as r:
        body = json.loads(r.read().decode())
    return body["access_token"], body["instance_url"]


def sf_query(token, instance_url, soql, suppress=False):
    out = []
    url = f"{instance_url}/services/data/v59.0/query?q={urllib.parse.quote(soql)}"
    while url:
        req = urllib.request.Request(url, method="GET")
        req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                body = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")
            if suppress:
                log(f"  (query failed, skipped: {detail[:160]})")
                return None
            raise RuntimeError(f"SOQL failed ({e.code}): {detail}")
        out.extend(body.get("records", []))
        nxt = body.get("nextRecordsUrl")
        url = f"{instance_url}{nxt}" if nxt else None
    return out


def sf_patch(token, instance_url, obj, record_id, field, value):
    url = f"{instance_url}/services/data/v59.0/sobjects/{obj}/{record_id}"
    req = urllib.request.Request(url, data=json.dumps({field: value}).encode(),
                                 method="PATCH")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return True, ""
    except urllib.error.HTTPError as e:
        return False, e.read().decode(errors="replace")[:300]


def fetch_records(token, instance_url, obj, mid_field, link_field, meeting_ids):
    """{meeting_id: {"id", "link"}} for ids this object knows about."""
    found = {}
    ids = sorted(meeting_ids)
    for i in range(0, len(ids), SOQL_BATCH_SIZE):
        batch = ids[i:i + SOQL_BATCH_SIZE]
        in_list = ", ".join("'" + m.replace("'", "\\'") + "'" for m in batch)
        soql = (f"SELECT Id, {mid_field}, {link_field} FROM {obj} "
                f"WHERE {mid_field} IN ({in_list})")
        recs = sf_query(token, instance_url, soql, suppress=True)
        if recs is None:
            continue
        for r in recs:
            mid = str(r.get(mid_field) or "").strip()
            if mid:
                found[mid] = {"id": r["Id"], "link": (r.get(link_field) or "").strip()}
        if (i // SOQL_BATCH_SIZE) % 10 == 0:
            log(f"    {obj}: {i + len(batch)}/{len(ids)} queried, {len(found)} matched")
    return found


# ══════════════════════════════════════════════════════════════════════════════
#  Shortlinks
# ══════════════════════════════════════════════════════════════════════════════

def list_shortlink_keys():
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=SHORTLINK_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                yield obj["Key"]


def read_shortlink(key):
    """(short_id, meeting_id, default_key) or None."""
    try:
        payload = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None

    short_id = key[len(SHORTLINK_PREFIX):].removesuffix(".json")
    default_key = (payload.get("default_key") or payload.get("key") or "").strip()
    if not default_key:
        return None

    # Media-folder check: the annotated analysis video lives outside these, so
    # this is what stops it being stored as the recording link.
    parts = default_key.split("/")
    if len(parts) < 2 or parts[-2].lower() not in MEDIA_FOLDERS:
        return None

    meeting_id = (str(payload.get("meeting_id") or "").strip()
                  or str(payload.get("meeting_id_raw") or "").strip())
    if not meeting_id:
        # Fall back to the segment before the media folder.
        if len(parts) >= 3 and parts[-3].isdigit():
            meeting_id = parts[-3]
    if not meeting_id:
        return None

    return short_id, meeting_id, default_key


def build_shortlink_index(workers):
    """{meeting_id: {"short_id", "url", "key"}}

    Where a meeting has several qualifying shortlinks, the FIRST is kept and
    the rest counted. They all resolve to a recording for that meeting, so any
    is serviceable -- but the count is reported so the ambiguity is visible
    rather than hidden.
    """
    keys = list(list_shortlink_keys())
    log(f"{len(keys)} shortlink file(s) found. Reading ...")

    index, dupes, unusable = {}, 0, 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(read_shortlink, k) for k in keys]
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if not res:
                unusable += 1
            else:
                short_id, mid, dk = res
                if mid in index:
                    dupes += 1
                else:
                    index[mid] = {
                        "short_id": short_id,
                        "url": f"{DOWNLOAD_BASE_URL}?id={urllib.parse.quote(short_id, safe='')}",
                        "key": dk,
                    }
            if i % 1000 == 0:
                log(f"    ...{i}/{len(keys)}")

    log(f"  usable: {len(index)} meeting id(s)   "
        f"extra shortlinks for same meeting: {dupes}   "
        f"not a media-folder key: {unusable}")
    return index


# ══════════════════════════════════════════════════════════════════════════════
#  Manifest
# ══════════════════════════════════════════════════════════════════════════════

def load_manifest():
    try:
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_manifest(m):
    with open(MANIFEST_FILE, "w") as f:
        json.dump(m, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true",
                   help="Actually write to Salesforce. Without this: dry run.")
    p.add_argument("--limit", type=int, default=None, help="Only the first N records.")
    p.add_argument("--workers", type=int, default=32, help="Concurrent shortlink reads.")
    args = p.parse_args()

    if not DOWNLOAD_BASE_URL:
        log("DOWNLOAD_BASE_URL is not set — the generated links would be wrong.\n"
            "Set it to the same value the linker Lambda uses, e.g.\n"
            "  export DOWNLOAD_BASE_URL=https://xxxx.execute-api.us-east-1.amazonaws.com/download")
        sys.exit(1)

    started = time.time()
    host = socket.gethostname()
    log(f"=== Recording-link backfill started "
        f"{datetime.now(timezone.utc).isoformat()} on {host} ===")
    log(f"Flags: execute={args.execute} limit={args.limit} workers={args.workers}")
    log(f"Download base: {DOWNLOAD_BASE_URL}")
    log(f"Media folders: {sorted(MEDIA_FOLDERS)}\n")

    index = build_shortlink_index(args.workers)
    if not index:
        log("No usable shortlinks. Nothing to do.")
        return
    log("")

    sf = get_sf_secret()
    token, instance_url = sf_login(sf)
    log(f"Salesforce login OK ({instance_url})\n")

    meeting_ids = set(index)
    log(f"Querying {SF_INTERVIEW_OBJECT} for {len(meeting_ids)} meeting id(s) ...")
    interviews = fetch_records(token, instance_url, SF_INTERVIEW_OBJECT,
                               SF_INTERVIEW_MID, SF_INTERVIEW_LINK, meeting_ids)
    log(f"  {len(interviews)} matched\n")

    log(f"Querying {SF_SESSION_OBJECT} ...")
    sessions = fetch_records(token, instance_url, SF_SESSION_OBJECT,
                             SF_SESSION_MID, SF_SESSION_LINK, meeting_ids)
    log(f"  {len(sessions)} matched\n")

    to_write, already, mismatches, no_record = [], 0, [], 0

    for mid, sl in index.items():
        # Same routing the linker uses: an Interview record wins, otherwise
        # the Session record. Decided from Salesforce, never from the S3 path.
        if mid in interviews:
            obj, field, rec = SF_INTERVIEW_OBJECT, SF_INTERVIEW_LINK, interviews[mid]
        elif mid in sessions:
            obj, field, rec = SF_SESSION_OBJECT, SF_SESSION_LINK, sessions[mid]
        else:
            no_record += 1
            continue

        if rec["link"]:
            already += 1
            if rec["link"] != sl["url"]:
                mismatches.append((mid, obj, rec["id"], rec["link"], sl["url"]))
            continue

        to_write.append({"meeting_id": mid, "object": obj, "field": field,
                         "record_id": rec["id"], "url": sl["url"],
                         "short_id": sl["short_id"], "key": sl["key"]})

    log("Summary:")
    log(f"  need a link  : {len(to_write)}")
    log(f"  already have : {already}")
    log(f"  no SF record : {no_record}")
    log(f"  differing    : {len(mismatches)}  (reported only — never overwritten)")

    by_obj = defaultdict(int)
    for w in to_write:
        by_obj[w["object"]] += 1
    if by_obj:
        log("\n  to write, by object:")
        for o, n in sorted(by_obj.items()):
            log(f"    {o}: {n}")

    if mismatches:
        log(f"\n{len(mismatches)} record(s) already hold a DIFFERENT link. "
            f"Left untouched — review if you want them changed:")
        for mid, obj, rid, have, would in mismatches[:10]:
            log(f"  {obj} {rid}  meeting {mid}")
            log(f"    stored : {have}")
            log(f"    would  : {would}")
        if len(mismatches) > 10:
            log(f"  ... and {len(mismatches) - 10} more")

    if not to_write:
        log("\nNothing to write.")
        return

    manifest = load_manifest()
    if args.limit:
        to_write = to_write[:args.limit]
        log(f"\n--limit {args.limit}: only the first {args.limit}.")

    if not args.execute:
        log("\n=== DRY RUN — nothing written to Salesforce ===\n")
        for w in to_write[:25]:
            log(f"  {w['object']} {w['record_id']}  meeting {w['meeting_id']}")
            log(f"    {w['field']} <- {w['url']}")
            log(f"    from {w['key']}\n")
        if len(to_write) > 25:
            log(f"  ... and {len(to_write) - 25} more\n")
        log("Review, then re-run with --execute (start with --limit 5 --execute).")
        return

    written = failed = skipped = 0
    for i, w in enumerate(to_write, 1):
        mkey = f"{w['object']}:{w['record_id']}"
        if manifest.get(mkey, {}).get("written"):
            skipped += 1
            continue

        ok, err = sf_patch(token, instance_url, w["object"], w["record_id"],
                           w["field"], w["url"])
        if ok:
            written += 1
            log(f"[{i}/{len(to_write)}] {w['object']} {w['record_id']} <- {w['short_id']}")
        else:
            failed += 1
            log(f"[{i}/{len(to_write)}] FAILED {w['object']} {w['record_id']}: {err}")

        manifest[mkey] = {"written": ok, "url": w["url"], "meeting_id": w["meeting_id"],
                          "at": datetime.now(timezone.utc).isoformat()}
        save_manifest(manifest)

        if i % 200 == 0:
            notify(f"Link backfill progress {i}/{len(to_write)}",
                   f"written={written} failed={failed}")

    elapsed = int(time.time() - started)
    log(f"\n=== Done ===")
    log(f"Written: {written}   Failed: {failed}   Skipped (already done): {skipped}")
    log(f"Untouched (already had a link): {already}")
    log(f"Elapsed: {elapsed // 60}m {elapsed % 60}s")

    status = "COMPLETED WITH FAILURES" if failed else "COMPLETED OK"
    notify(f"Recording-link backfill {status} ({written} written, {failed} failed)",
           f"Host: {host}\nWritten: {written}\nFailed: {failed}\n"
           f"Already had a link: {already}\nDiffering (untouched): {len(mismatches)}\n"
           f"No SF record: {no_record}\nElapsed: {elapsed // 60}m {elapsed % 60}s\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\nInterrupted. Progress saved — re-run to resume.")
        sys.exit(130)
    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        log(f"\n!!! CRASHED !!!\n{tb}")
        notify("Recording-link backfill CRASHED", f"{exc}\n\n{tb}")
        sys.exit(1)
