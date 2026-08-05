#!/usr/bin/env python3
"""
Restructures Interview-Success into type folders.

    BEFORE
      Interview-Success/{Host}/{Y}/{M}/{Cand}/{Company}/{Date}/{Round}/{MID}/...

    AFTER
      Interview-Success/Interview/{Host}/{Y}/{M}/{Cand}/{Company}/{Date}/{Round}/{MID}/...
      Interview-Success/Internal-Interview/{Host}/{Y}/{M}/{Cand}/{Date}/{Time}/{MID}/...
      Interview-Success/{Other-Purpose}/{Host}/{Y}/{M}/{Cand}/{Date}/{Time}/{MID}/...

HOW ROUTING IS DECIDED
  Salesforce is authoritative, exactly as in the Lambda:

      Session__c.Purpose__c == "Internal Interview"  -> Internal-Interview
      Session__c.Purpose__c == anything else         -> slugified Purpose
      no Session__c record at all                    -> Interview

  Meeting ids are batched ~200 per SOQL call, so ~7,000 sessions cost ~35
  queries rather than 7,000.

  The PATH is used as a cross-check, never as the decision. An old internal
  interview is recognisable by its Unknown_Company/Unknown_Round placeholders,
  so if Salesforce and the path disagree the script logs it loudly and follows
  Salesforce. Disagreements are worth seeing -- they mean either a genuine
  interview stored with missing data, or an internal one that somehow got real
  company/round values.

THE TIME SEGMENT
  Internal interviews move to the generic layout, which needs a Time folder the
  old path does not contain. It is derived from Session__c.Actual_Start__c,
  falling back to Start_Time_IST__c -- the same source the Lambda uses, so a
  migrated session and a newly recorded one produce the same shape.

  If neither field is populated the session is SKIPPED, not guessed. A wrong
  timestamp buried in a path is worse than a session left in place, because
  nothing about it looks wrong afterwards.

CONCURRENCY
  Object copies run in a thread pool. S3 copy_object is server-side, so the
  script spends nearly all its time waiting on network rather than using CPU --
  which is why threads help and a bigger instance does not. 32 is a sensible
  default; much beyond 64 S3 begins throttling and backoff makes it slower.

SAFETY  -- same model as every prior migration
  - copy -> verify (exact byte size per object) -> delete, in that order
  - without --move nothing is ever deleted
  - DRY RUN BY DEFAULT
  - own manifest, written after every session, so it is resumable
  - already-migrated sessions (already under a type folder) are never rescanned

USAGE
  python3 migrate_interview_success.py
  python3 migrate_interview_success.py --limit 5 --execute --move
  nohup python3 -u migrate_interview_success.py --execute --move \\
        --workers 32 --stop-instance-when-done > ~/is_migration.log 2>&1 &
"""

import argparse
import base64
import json
import os
import re
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
    from botocore.exceptions import ClientError
except ImportError:
    print("boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)

try:
    import jwt
except ImportError:
    print("PyJWT not installed. Run: pip install PyJWT cryptography", file=sys.stderr)
    sys.exit(1)

BUCKET     = os.environ.get("MIGRATION_BUCKET", "zoom-automation-bucket")
DEPARTMENT = "Interview-Success/"

TYPE_ACTUAL    = "Interview"
TYPE_INTERNAL  = "Internal-Interview"
INTERNAL_PURPOSE = "Internal Interview"

# Never rescan these -- they are this script's own output.
KNOWN_TYPE_FOLDERS = {TYPE_ACTUAL, TYPE_INTERNAL}

SF_SECRET_NAME    = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
SF_SESSION_OBJECT = os.environ.get("SF_SESSION_OBJECT_API_NAME", "Session__c")
SF_MEETING_FIELD  = os.environ.get("SF_SESSION_MEETING_FIELD_API_NAME",
                                   "External_Meeting_ID__c")
SF_PURPOSE_FIELD  = os.environ.get("SF_SESSION_PURPOSE_FIELD", "Purpose__c")
SF_ACTUAL_START   = os.environ.get("SF_SESSION_ACTUAL_START_FIELD", "Actual_Start__c")
SF_START_IST      = os.environ.get("SF_SESSION_START_IST_FIELD", "Start_Time_IST__c")

MANIFEST_FILE = os.environ.get("IS_MANIFEST_PATH", "interview_success_manifest.json")
SNS_TOPIC_ARN = os.environ.get("MIGRATION_SNS_TOPIC_ARN", "").strip()
AWS_REGION    = os.environ.get("AWS_REGION", "us-east-1")

SOQL_BATCH_SIZE = 200
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Generous pool so threads are not fighting over connections.
_cfg = Config(max_pool_connections=100, retries={"max_attempts": 10, "mode": "adaptive"})
s3  = boto3.client("s3",  region_name=AWS_REGION, config=_cfg)
ec2 = boto3.client("ec2", region_name=AWS_REGION)

_log_lock = threading.Lock()


def log(msg: str = ""):
    """Thread-safe, always flushed -- otherwise interleaved worker output
    corrupts lines and `tail -f` on a nohup log looks frozen."""
    with _log_lock:
        print(msg, flush=True)


def notify(subject: str, message: str):
    if not SNS_TOPIC_ARN:
        return
    try:
        boto3.client("sns", region_name=AWS_REGION).publish(
            TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
        log(f"[NOTIFY] {subject}")
    except Exception as exc:
        log(f"[NOTIFY] SNS publish failed (non-fatal): {exc}")


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


def sf_query_all(token, instance_url, soql):
    out = []
    url = f"{instance_url}/services/data/v59.0/query?q={urllib.parse.quote(soql)}"
    while url:
        req = urllib.request.Request(url, method="GET")
        req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                body = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"SOQL failed ({e.code}): {e.read().decode(errors='replace')}")
        out.extend(body.get("records", []))
        nxt = body.get("nextRecordsUrl")
        url = f"{instance_url}{nxt}" if nxt else None
    return out


def fetch_session_data(meeting_ids):
    """{meeting_id: {purpose, actual_start, start_ist}} for ids Salesforce knows.

    Batched ~200 per query: ~7,000 sessions cost ~35 calls, not 7,000.
    Ids absent from the result simply have no Session record, which is itself
    the signal for 'actual interview'.
    """
    sf = get_sf_secret()
    token, instance_url = sf_login(sf)
    log(f"Salesforce login OK ({instance_url})")

    ids = sorted(meeting_ids)
    found = {}
    for i in range(0, len(ids), SOQL_BATCH_SIZE):
        batch = ids[i:i + SOQL_BATCH_SIZE]
        in_list = ", ".join("'" + m.replace("'", "\\'") + "'" for m in batch)
        soql = (f"SELECT {SF_MEETING_FIELD}, {SF_PURPOSE_FIELD}, "
                f"{SF_ACTUAL_START}, {SF_START_IST} "
                f"FROM {SF_SESSION_OBJECT} WHERE {SF_MEETING_FIELD} IN ({in_list})")
        for rec in sf_query_all(token, instance_url, soql):
            mid = str(rec.get(SF_MEETING_FIELD) or "").strip()
            if mid:
                found[mid] = {
                    "purpose":      (rec.get(SF_PURPOSE_FIELD) or "").strip(),
                    "actual_start": rec.get(SF_ACTUAL_START),
                    "start_ist":    rec.get(SF_START_IST),
                }
        log(f"  SF batch {i // SOQL_BATCH_SIZE + 1}: "
            f"{len(batch)} queried, {len(found)} matched so far")
    return found


# ══════════════════════════════════════════════════════════════════════════════
#  Naming
# ══════════════════════════════════════════════════════════════════════════════

def slugify_purpose(purpose):
    """Matches the Lambda's slugifier, so a Purpose folder created here and one
    created by a live recording are byte-identical."""
    slug = re.sub(r"[^A-Za-z0-9]+", "-", (purpose or "").strip()).strip("-")
    if not slug:
        return "Other"
    return "-".join(w[:1].upper() + w[1:] for w in slug.split("-") if w)


def build_time_folder(sf_row):
    """Time-H-MM-AM-IST from Salesforce, matching the Lambda's format.

    Returns None when neither timestamp is populated -- the caller then SKIPS
    the session rather than inventing a time. An invented timestamp is
    invisible once written; a skipped session is obvious and fixable.
    """
    raw = sf_row.get("actual_start") or sf_row.get("start_ist")
    if not raw:
        return None
    try:
        txt = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        from datetime import timedelta
        ist = dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
        hour12 = ist.hour % 12 or 12
        ampm = "AM" if ist.hour < 12 else "PM"
        return f"Time-{hour12}-{ist.minute:02d}-{ampm}-IST"
    except Exception as exc:
        log(f"    could not parse timestamp {raw!r}: {exc}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  Planning
# ══════════════════════════════════════════════════════════════════════════════

def list_all_objects(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def scan_sessions():
    """Group every object under the department by its session prefix.

    A session is identified by the segment AFTER the date -- either a round
    (old actual-interview layout) or a Time folder. Anything already sitting
    under a known type folder is skipped, so re-running is safe.
    """
    sessions = defaultdict(lambda: {"objects": [], "bytes": 0})
    skipped_migrated = 0

    for key, size in list_all_objects(DEPARTMENT):
        rel = key[len(DEPARTMENT):]
        parts = rel.split("/")
        if not parts or not parts[0]:
            continue

        if parts[0] in KNOWN_TYPE_FOLDERS:
            skipped_migrated += 1
            continue

        date_idx = next((i for i, p in enumerate(parts) if DATE_RE.match(p)), None)
        if date_idx is None or date_idx + 2 >= len(parts):
            continue

        session_prefix = DEPARTMENT + "/".join(parts[: date_idx + 3]) + "/"
        sessions[session_prefix]["objects"].append((key, size))
        sessions[session_prefix]["bytes"] += size

    if skipped_migrated:
        log(f"  ({skipped_migrated} object(s) already under a type folder — skipped)")
    return sessions


def parse_session_prefix(prefix):
    """{host, year, month, candidate, company, date, round, meeting_id}"""
    parts = prefix[len(DEPARTMENT):].rstrip("/").split("/")
    date_idx = next((i for i, p in enumerate(parts) if DATE_RE.match(p)), None)
    if date_idx is None or date_idx < 4:
        return None
    return {
        "host":       parts[date_idx - 4],
        "year":       parts[date_idx - 3],
        "month":      parts[date_idx - 2],
        "candidate":  parts[date_idx - 1],
        "company":    parts[date_idx - 1],   # placeholder, corrected below
        "date":       parts[date_idx],
        "round":      parts[date_idx + 1] if len(parts) > date_idx + 1 else "",
        "meeting_id": parts[date_idx + 2] if len(parts) > date_idx + 2 else "",
        "parts":      parts,
        "date_idx":   date_idx,
    }


def parse_full(prefix):
    """Old actual-interview layout, 8 segments:
    {Host}/{Y}/{M}/{Cand}/{Company}/{Date}/{Round}/{MID}
    """
    parts = prefix[len(DEPARTMENT):].rstrip("/").split("/")
    date_idx = next((i for i, p in enumerate(parts) if DATE_RE.match(p)), None)
    if date_idx is None or date_idx < 5 or date_idx + 2 >= len(parts):
        return None
    return {
        "host":       parts[date_idx - 5],
        "year":       parts[date_idx - 4],
        "month":      parts[date_idx - 3],
        "candidate":  parts[date_idx - 2],
        "company":    parts[date_idx - 1],
        "date":       parts[date_idx],
        "round":      parts[date_idx + 1],
        "meeting_id": parts[date_idx + 2],
    }


def plan(sessions, sf_data):
    """Decide each session's destination. Salesforce decides; the path is only
    a cross-check."""
    jobs, skipped, mismatches = [], [], []

    for prefix, info in sessions.items():
        p = parse_full(prefix)
        if not p:
            skipped.append((prefix, "unrecognised layout"))
            continue

        mid = p["meeting_id"]
        sf  = sf_data.get(mid)
        looks_internal = (p["company"] == "Unknown_Company"
                          or p["round"] == "Unknown_Round")

        if sf is None:
            type_folder, reason = TYPE_ACTUAL, "no-session-record"
            if looks_internal:
                mismatches.append(
                    (prefix, "path has Unknown_* placeholders but Salesforce has "
                             "no Session record — treating as actual interview"))
        elif sf["purpose"].strip().lower() == INTERNAL_PURPOSE.lower():
            type_folder, reason = TYPE_INTERNAL, "purpose-internal-interview"
            if not looks_internal:
                mismatches.append(
                    (prefix, f"Salesforce says Internal Interview but path has real "
                             f"company/round ({p['company']}/{p['round']})"))
        elif sf["purpose"]:
            type_folder = slugify_purpose(sf["purpose"])
            reason = f"purpose-{sf['purpose']}"
        else:
            type_folder, reason = TYPE_ACTUAL, "session-exists-blank-purpose"

        if type_folder == TYPE_ACTUAL:
            new_prefix = (f"{DEPARTMENT}{TYPE_ACTUAL}/{p['host']}/{p['year']}/"
                          f"{p['month']}/{p['candidate']}/{p['company']}/"
                          f"{p['date']}/{p['round']}/{p['meeting_id']}/")
        else:
            time_folder = build_time_folder(sf) if sf else None
            if not time_folder:
                skipped.append((prefix, f"{type_folder}: no usable Salesforce "
                                        f"timestamp — cannot build Time segment"))
                continue
            new_prefix = (f"{DEPARTMENT}{type_folder}/{p['host']}/{p['year']}/"
                          f"{p['month']}/{p['candidate']}/{p['date']}/"
                          f"{time_folder}/{p['meeting_id']}/")

        jobs.append({
            "old_prefix": prefix, "new_prefix": new_prefix,
            "objects": info["objects"], "total_bytes": info["bytes"],
            "type_folder": type_folder, "reason": reason,
            "meeting_id": mid,
        })

    return jobs, skipped, mismatches


# ══════════════════════════════════════════════════════════════════════════════
#  Copy / verify / delete
# ══════════════════════════════════════════════════════════════════════════════

def copy_one(old_key, new_key, size):
    """Copy a single object and verify its exact byte size. Runs in a worker."""
    try:
        s3.copy_object(Bucket=BUCKET,
                       CopySource={"Bucket": BUCKET, "Key": old_key},
                       Key=new_key)
    except Exception as exc:
        return False, f"COPY FAILED {old_key}: {exc}"
    try:
        got = s3.head_object(Bucket=BUCKET, Key=new_key)["ContentLength"]
        if got != size:
            return False, f"SIZE MISMATCH {new_key}: expected {size}, got {got}"
    except Exception as exc:
        return False, f"VERIFY FAILED {new_key}: {exc}"
    return True, ""


def migrate_session(job, workers):
    """Copy every object in one session concurrently, then verify all succeeded.

    Returns (ok, errors). The caller deletes originals only when ok is True --
    a partial copy therefore leaves the source completely untouched.
    """
    old_prefix, new_prefix = job["old_prefix"], job["new_prefix"]
    errors = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(copy_one, k, new_prefix + k[len(old_prefix):], sz): k
            for k, sz in job["objects"]
        }
        for fut in as_completed(futures):
            ok, err = fut.result()
            if not ok:
                errors.append(err)

    return (not errors), errors


def delete_objects(keys):
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        try:
            resp = s3.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
            if resp.get("Errors"):
                log(f"    DELETE errors: {resp['Errors']}")
                return False
        except Exception as exc:
            log(f"    DELETE FAILED: {exc}")
            return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  Manifest
# ══════════════════════════════════════════════════════════════════════════════

_manifest_lock = threading.Lock()


def load_manifest():
    try:
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_manifest(m):
    with _manifest_lock:
        with open(MANIFEST_FILE, "w") as f:
            json.dump(m, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
#  Instance shutdown
# ══════════════════════════════════════════════════════════════════════════════

def this_instance_id():
    """IMDSv2 -- your instances require it."""
    try:
        tok_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT")
        tok_req.add_header("X-aws-ec2-metadata-token-ttl-seconds", "60")
        with urllib.request.urlopen(tok_req, timeout=3) as r:
            token = r.read().decode()
        idr = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/instance-id")
        idr.add_header("X-aws-ec2-metadata-token", token)
        with urllib.request.urlopen(idr, timeout=3) as r:
            return r.read().decode()
    except Exception as exc:
        log(f"Could not read instance id from metadata: {exc}")
        return None


def stop_this_instance():
    """Stop (not terminate) this instance. At $4+/hour an unattended run that
    finishes at 3am should not keep billing until someone notices."""
    iid = this_instance_id()
    if not iid:
        log("Auto-stop requested but instance id unavailable — NOT stopping.")
        notify("Migration done but AUTO-STOP FAILED",
               "Could not determine the instance id. STOP IT MANUALLY.")
        return
    log(f"Stopping instance {iid} ...")
    notify("Migration complete — stopping instance",
           f"Interview-Success migration finished. Stopping {iid} now.")
    try:
        ec2.stop_instances(InstanceIds=[iid])
        log(f"Stop requested for {iid}.")
    except Exception as exc:
        log(f"AUTO-STOP FAILED: {exc}")
        notify("Migration done but AUTO-STOP FAILED",
               f"Instance {iid} could not be stopped: {exc}\n\nSTOP IT MANUALLY.")


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true",
                   help="Actually copy. Without this: dry run, nothing touched.")
    p.add_argument("--move", action="store_true",
                   help="Delete originals after every object in that session verifies.")
    p.add_argument("--limit", type=int, default=None, help="Only the first N sessions.")
    p.add_argument("--workers", type=int, default=32,
                   help="Concurrent object copies (default 32). Beyond ~64 S3 "
                        "throttles and backoff makes it slower, not faster.")
    p.add_argument("--stop-instance-when-done", action="store_true",
                   help="Stop this EC2 instance on completion. Requires "
                        "ec2:StopInstances in the instance role.")
    args = p.parse_args()

    started = time.time()
    host = socket.gethostname()
    log(f"=== Interview-Success migration started "
        f"{datetime.now(timezone.utc).isoformat()} on {host} ===")
    log(f"Flags: execute={args.execute} move={args.move} "
        f"limit={args.limit} workers={args.workers}")
    log(f"Manifest: {os.path.abspath(MANIFEST_FILE)}")
    log(f"SNS: {'ON' if SNS_TOPIC_ARN else 'OFF'}\n")

    notify("Interview-Success migration STARTED",
           f"Host: {host}\nexecute={args.execute} move={args.move} "
           f"workers={args.workers}\n")

    log("Scanning S3 ...")
    sessions = scan_sessions()
    log(f"{len(sessions)} session(s) found.\n")
    if not sessions:
        log("Nothing to do.")
        return

    meeting_ids = set()
    for prefix in sessions:
        pp = parse_full(prefix)
        if pp and pp["meeting_id"]:
            meeting_ids.add(pp["meeting_id"])
    log(f"Querying Salesforce for {len(meeting_ids)} meeting id(s) "
        f"in batches of {SOQL_BATCH_SIZE} ...")

    try:
        sf_data = fetch_session_data(meeting_ids)
    except Exception as exc:
        log(f"\nSALESFORCE LOOKUP FAILED — stopping before touching any S3 data.\n{exc}")
        notify("Interview-Success migration FAILED (Salesforce)",
               f"{exc}\n\nNo S3 data was touched.")
        sys.exit(1)
    log(f"Salesforce returned {len(sf_data)} matching Session record(s).\n")

    jobs, skipped, mismatches = plan(sessions, sf_data)

    by_type = defaultdict(int)
    for j in jobs:
        by_type[j["type_folder"]] += 1
    log("Destination breakdown:")
    for t, n in sorted(by_type.items()):
        log(f"  {t}: {n} session(s)")

    if mismatches:
        log(f"\n{len(mismatches)} PATH/SALESFORCE MISMATCH(ES) "
            f"— following Salesforce:")
        for pre, why in mismatches[:20]:
            log(f"  {pre}\n    {why}")
        if len(mismatches) > 20:
            log(f"  ... and {len(mismatches) - 20} more")

    if skipped:
        log(f"\n{len(skipped)} session(s) SKIPPED:")
        for pre, why in skipped[:20]:
            log(f"  {pre}\n    {why}")
        if len(skipped) > 20:
            log(f"  ... and {len(skipped) - 20} more")

    total_objects = sum(len(j["objects"]) for j in jobs)
    total_bytes = sum(j["total_bytes"] for j in jobs)
    log(f"\n{len(jobs)} session(s), {total_objects} object(s), {total_bytes:,} bytes\n")

    manifest = load_manifest()
    if args.limit:
        jobs = jobs[:args.limit]
        log(f"--limit {args.limit}: only the first {args.limit} session(s).\n")

    if not args.execute:
        log("=== DRY RUN — nothing copied or deleted ===\n")
        for j in jobs[:40]:
            log(f"  {j['old_prefix']}\n    -> {j['new_prefix']}")
            log(f"    ({len(j['objects'])} objects, {j['total_bytes']:,} bytes, "
                f"{j['type_folder']}, {j['reason']})\n")
        if len(jobs) > 40:
            log(f"  ... and {len(jobs) - 40} more session(s)\n")
        log("Review the plan, then re-run with --execute "
            "(start with --limit 5 --execute --move).")
        return

    done = failed = deleted = skipped_done = 0
    for idx, job in enumerate(jobs, 1):
        prior = manifest.get(job["old_prefix"], {})
        if prior.get("verified") and (not args.move or prior.get("deleted")):
            skipped_done += 1
            continue

        log(f"[{idx}/{len(jobs)}] {job['type_folder']}: {job['old_prefix']}"
            f"\n        -> {job['new_prefix']}  ({len(job['objects'])} objects)")
        ok, errors = migrate_session(job, args.workers)

        did_delete = False
        if ok:
            done += 1
            log("        verified OK")
            if args.move:
                did_delete = delete_objects([k for k, _ in job["objects"]])
                if did_delete:
                    deleted += 1
                    log("        originals deleted")
                else:
                    log("        WARNING: copies verified and safe, but deleting "
                        "the originals failed. Nothing lost — safe to re-run.")
        else:
            failed += 1
            for e in errors[:5]:
                log(f"        {e}")
            log("        NOT deleting — originals untouched.")

        manifest[job["old_prefix"]] = {
            "new_prefix": job["new_prefix"], "verified": ok, "deleted": did_delete,
            "type_folder": job["type_folder"], "reason": job["reason"],
            "objects": len(job["objects"]), "bytes": job["total_bytes"],
            "meeting_id": job["meeting_id"],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        save_manifest(manifest)

        if idx % 250 == 0:
            notify(f"Interview-Success migration progress {idx}/{len(jobs)}",
                   f"done={done} failed={failed} deleted={deleted}")

    elapsed = int(time.time() - started)
    h, rem = divmod(elapsed, 3600)
    m, s = divmod(rem, 60)
    el = f"{h}h {m}m {s}s"

    log(f"\n=== Done ===")
    log(f"Migrated: {done}   Deleted: {deleted}   Failed: {failed}   "
        f"Skipped (already done): {skipped_done}")
    log(f"Planning skips: {len(skipped)}   Mismatches logged: {len(mismatches)}")
    log(f"Elapsed: {el}")

    status = "COMPLETED WITH FAILURES" if failed else "COMPLETED OK"
    notify(f"Interview-Success migration {status} ({done} migrated, {failed} failed)",
           f"Host: {host}\nStatus: {status}\nMigrated: {done}\nDeleted: {deleted}\n"
           f"Failed: {failed}\nSkipped (already done): {skipped_done}\n"
           f"Planning skips: {len(skipped)}\nMismatches: {len(mismatches)}\n"
           f"Elapsed: {el}\nManifest: {os.path.abspath(MANIFEST_FILE)}\n")

    if args.stop_instance_when_done:
        stop_this_instance()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\nInterrupted. Progress saved — re-run to resume.")
        sys.exit(130)
    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        log(f"\n!!! MIGRATION CRASHED !!!\n{tb}")
        notify("Interview-Success migration CRASHED",
               f"Host: {socket.gethostname()}\n\n{exc}\n\n{tb}\n\n"
               "Data is safe: originals are deleted only after every object in "
               "that session verifies, and the manifest is written per session.\n"
               "NOTE: the instance was NOT stopped — stop it manually.")
        sys.exit(1)
