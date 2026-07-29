#!/usr/bin/env python3
"""
Moves Interview Readiness sessions into Training/Interview-Readiness/.

WHY THIS EXISTS
  The main migration could only tell "advance" folders apart from everything
  else, because that was the only marker frozen into the old folder names.
  Interview Readiness sessions left no such marker, so they landed in
  Training/Resume-Based/ (and a few in Advanced/ or Other/). Salesforce DOES
  know which sessions they are, so this pass asks Salesforce and corrects them.

HOW IT DECIDES  -- this is the whole safety model, read it
  1. It queries Salesforce ITSELF (same JWT credentials the Lambda uses) for
     every Session whose Program is the target program:

         SELECT External_Meeting_ID__c FROM Session__c
         WHERE Program_Version__r.Program__r.Name = '<--program>'

  2. It scans S3 and moves a session ONLY if that session's meeting ID came
     back in step 1.

  Nothing else is considered. A session Salesforce did not return is physically
  incapable of being moved -- so Resume-Based and Advanced data cannot be
  disturbed. If the Salesforce query fails, the script stops before touching
  any S3 data at all.

SAFETY MODEL  -- identical to the main migration
  - Copy first, then VERIFY (object count + total bytes), and only then delete
    the old copy -- and only if you passed --move.
  - Without --move nothing is ever deleted; both copies are kept.
  - DRY RUN BY DEFAULT. Nothing happens until you pass --execute.
  - Idempotent and resumable via its own manifest (separate from the main
    migration's manifest, so the two can never interfere).
  - Training/Interview-Readiness/ is never scanned, so anything already in the
    right place is left alone.

WHAT IT SCANS
  Training/Resume-Based/    Training/Advanced/    Training/Other/
  Nothing else in the bucket is read or touched.

WHAT IT MOVES for a matched meeting ID
  1. The session folder and everything inside it:
       Training/<Type>/<trainer>/<y>/<m>/<candidate>/<date>/<time>/<meetingID>/...
  2. Its date-level merged result, one level higher:
       Training/<Type>/<trainer>/<y>/<m>/<candidate>/<date>/session-result-<meetingID>.json
  Both carry the same meeting ID, so they move together -- neither is orphaned.

PREREQUISITES
  pip install boto3 PyJWT cryptography
  IAM on the instance role:
    - S3 Get/Put/List on Training/*  (+ Delete if using --move)
    - secretsmanager:GetSecretValue on the Salesforce JWT secret   <-- NEW
  Optional: export MIGRATION_SNS_TOPIC_ARN=arn:aws:sns:...

USAGE (in order)
  python3 reclassify_interview_readiness.py
  python3 reclassify_interview_readiness.py --limit 5 --execute --move
  python3 reclassify_interview_readiness.py --execute --move

  Long run, survives disconnect:
    nohup python3 -u reclassify_interview_readiness.py --execute --move \
          > ~/Migration/reclassify.log 2>&1 &

  Reusable for other program types later:
    python3 reclassify_interview_readiness.py --program "Technical 1-1 Training" \
            --target-type-folder "Technical-1-1"
"""

import argparse
import base64
import json
import os
import re
import socket
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone

try:
    import boto3
except ImportError:
    print("boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)

try:
    import jwt
except ImportError:
    print("PyJWT not installed. Run: pip install PyJWT cryptography", file=sys.stderr)
    sys.exit(1)

BUCKET = "zoom-automation-bucket"

# Where Interview Readiness sessions might currently be sitting. The target
# type is deliberately absent -- already-correct data must never be reprocessed.
SOURCE_TYPE_PREFIXES = [
    "Training/Resume-Based/",
    "Training/Advanced/",
    "Training/Other/",
]

# ── Salesforce config (same secret the Lambda uses) ──────────────────────────
SF_SECRET_NAME    = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
SF_SESSION_OBJECT = os.environ.get("SF_SESSION_OBJECT_API_NAME", "Session__c")
SF_MEETING_FIELD  = os.environ.get("SF_SESSION_MEETING_FIELD_API_NAME",
                                   "External_Meeting_ID__c")
SF_PROGRAM_PATH   = os.environ.get("SF_SESSION_PROGRAM_RELATION_DIRECT",
                                   "Program_Version__r.Program__r.Name")

MANIFEST_FILE = os.environ.get("RECLASSIFY_MANIFEST_PATH", "reclassify_manifest.json")
SNS_TOPIC_ARN = os.environ.get("MIGRATION_SNS_TOPIC_ARN", "").strip()
AWS_REGION    = os.environ.get("AWS_REGION", "us-east-1")

SESSION_DEPTH = 7   # trainer/year/month/candidate/date/time/meetingID
DATE_DEPTH    = 6   # trainer/year/month/candidate/date/FILENAME

SESSION_RESULT_RE = re.compile(r"session-result-(\d+)\.json$", re.IGNORECASE)

s3 = boto3.client("s3")


def log(msg: str = ""):
    """print() that always flushes -- otherwise `tail -f` on a nohup log looks
    frozen for minutes at a time."""
    print(msg, flush=True)


def notify(subject: str, message: str):
    if not SNS_TOPIC_ARN:
        return
    try:
        boto3.client("sns", region_name=AWS_REGION).publish(
            TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
        log(f"[NOTIFY] SNS sent: {subject}")
    except Exception as exc:
        log(f"[NOTIFY] SNS publish failed (non-fatal): {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  Salesforce -- JWT bearer flow, mirrors the Lambda exactly
# ══════════════════════════════════════════════════════════════════════════════

def get_sf_secret():
    resp = boto3.client("secretsmanager", region_name=AWS_REGION) \
                .get_secret_value(SecretId=SF_SECRET_NAME)
    raw = json.loads(resp["SecretString"])

    missing = [k for k in ("SF_CLIENT_ID", "SF_USERNAME", "SF_LOGIN_URL")
               if not raw.get(k)]
    if missing:
        raise ValueError(f"Secret {SF_SECRET_NAME} is missing: {', '.join(missing)}")

    private_key = raw.get("PRIVATE_KEY")
    if not private_key and raw.get("PRIVATE_KEY_B64"):
        private_key = base64.b64decode(raw["PRIVATE_KEY_B64"]).decode("utf-8")
    if not private_key:
        raise ValueError("Secret has neither PRIVATE_KEY nor PRIVATE_KEY_B64")

    return {
        "client_id":   raw["SF_CLIENT_ID"],
        "username":    raw["SF_USERNAME"],
        "login_url":   raw["SF_LOGIN_URL"],
        "private_key": private_key,
    }


def sf_login(sf: dict):
    payload = {
        "iss": sf["client_id"],
        "sub": sf["username"],
        "aud": sf["login_url"],
        "exp": int(time.time()) + 300,
    }
    assertion = jwt.encode(payload,
                           sf["private_key"].replace("\\n", "\n").strip(),
                           algorithm="RS256")

    data = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion":  assertion,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{sf['login_url'].rstrip('/')}/services/oauth2/token",
        data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return body["access_token"], body["instance_url"]


def sf_fetch_meeting_ids(program_name: str) -> set:
    """Every meeting ID whose Session's Program matches program_name.
    Follows nextRecordsUrl so result sets larger than one page are complete."""
    sf = get_sf_secret()
    token, instance_url = sf_login(sf)
    log(f"Salesforce login OK ({instance_url})")

    safe = program_name.replace("\\", "\\\\").replace("'", "\\'")
    soql = (f"SELECT {SF_MEETING_FIELD} FROM {SF_SESSION_OBJECT} "
            f"WHERE {SF_PROGRAM_PATH} = '{safe}'")
    log(f"SOQL: {soql}")

    ids = set()
    url = f"{instance_url}/services/data/v59.0/query?q={urllib.parse.quote(soql)}"
    pages = 0

    while url:
        req = urllib.request.Request(url, method="GET")
        req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Salesforce query failed ({e.code}): {detail}")

        pages += 1
        for rec in body.get("records", []):
            value = str(rec.get(SF_MEETING_FIELD) or "").strip()
            if value:
                ids.add(value)

        nxt = body.get("nextRecordsUrl")
        url = f"{instance_url}{nxt}" if nxt else None

    log(f"Salesforce returned {len(ids)} meeting ID(s) across {pages} page(s)")
    return ids


# ══════════════════════════════════════════════════════════════════════════════
#  S3 planning
# ══════════════════════════════════════════════════════════════════════════════

def list_all_objects(prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def plan_moves(target_ids: set, target_prefix: str):
    """A job is created ONLY when a meeting ID from Salesforce appears in the
    path. Everything else is ignored entirely."""
    session_jobs = defaultdict(lambda: {"objects": [], "total_bytes": 0,
                                        "new_prefix": "", "source_type": ""})
    file_jobs = []

    for src_prefix in SOURCE_TYPE_PREFIXES:
        if src_prefix == target_prefix:
            continue                       # never reprocess already-correct data
        type_label = src_prefix.rstrip("/").split("/")[-1]

        for key, size in list_all_objects(src_prefix):
            relative = key[len(src_prefix):]
            parts = relative.split("/")

            # session folder: .../<date>/<time>/<meetingID>/<files...>
            if len(parts) > SESSION_DEPTH:
                if parts[SESSION_DEPTH - 1] not in target_ids:
                    continue
                old_prefix = src_prefix + "/".join(parts[:SESSION_DEPTH]) + "/"
                job = session_jobs[old_prefix]
                job["objects"].append((key, size))
                job["total_bytes"] += size
                job["new_prefix"]  = target_prefix + "/".join(parts[:SESSION_DEPTH]) + "/"
                job["source_type"] = type_label
                continue

            # date-level file: .../<date>/session-result-<meetingID>.json
            if len(parts) == DATE_DEPTH:
                m = SESSION_RESULT_RE.search(parts[-1])
                if not m or m.group(1) not in target_ids:
                    continue
                file_jobs.append({
                    "kind":        "file",
                    "old_prefix":  key,
                    "new_prefix":  target_prefix + relative,
                    "objects":     [(key, size)],
                    "total_bytes": size,
                    "source_type": type_label,
                })

    jobs = [{
        "kind":        "session",
        "old_prefix":  old,
        "new_prefix":  info["new_prefix"],
        "objects":     info["objects"],
        "total_bytes": info["total_bytes"],
        "source_type": info["source_type"],
    } for old, info in session_jobs.items()]
    jobs.extend(file_jobs)
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
#  Copy / verify / delete
# ══════════════════════════════════════════════════════════════════════════════

def copy_and_verify_session(job: dict) -> bool:
    old_prefix, new_prefix = job["old_prefix"], job["new_prefix"]
    for old_key, _ in job["objects"]:
        new_key = new_prefix + old_key[len(old_prefix):]
        try:
            s3.copy_object(Bucket=BUCKET,
                           CopySource={"Bucket": BUCKET, "Key": old_key},
                           Key=new_key)
        except Exception as exc:
            log(f"    COPY FAILED: {old_key} -> {new_key}: {exc}")

    new_objects = list(list_all_objects(new_prefix))
    new_count, new_bytes = len(new_objects), sum(s for _, s in new_objects)
    ok = (new_count == len(job["objects"])) and (new_bytes == job["total_bytes"])
    if not ok:
        log(f"    VERIFY MISMATCH: {old_prefix}\n"
            f"      old: {len(job['objects'])} objects / {job['total_bytes']} bytes\n"
            f"      new: {new_count} objects / {new_bytes} bytes")
    return ok


def copy_and_verify_file(job: dict) -> bool:
    old_key, new_key = job["objects"][0][0], job["new_prefix"]
    try:
        s3.copy_object(Bucket=BUCKET,
                       CopySource={"Bucket": BUCKET, "Key": old_key},
                       Key=new_key)
    except Exception as exc:
        log(f"    COPY FAILED: {old_key} -> {new_key}: {exc}")
        return False
    try:
        head = s3.head_object(Bucket=BUCKET, Key=new_key)
        if head["ContentLength"] != job["total_bytes"]:
            log(f"    VERIFY MISMATCH (file): {old_key}\n"
                f"      old: {job['total_bytes']} bytes\n"
                f"      new: {head['ContentLength']} bytes")
            return False
    except Exception as exc:
        log(f"    VERIFY FAILED (file) {new_key}: {exc}")
        return False
    return True


def delete_old_objects(job: dict) -> bool:
    """Only ever called after that job already verified. Batched at 1000,
    which is S3's own limit for delete_objects."""
    keys = [k for k, _ in job["objects"]]
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
            log(f"    DELETE FAILED for batch starting {batch[0]}: {exc}")
            return False
    return True


# ── Manifest ─────────────────────────────────────────────────────────────────

def load_manifest() -> dict:
    try:
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_manifest(m: dict):
    with open(MANIFEST_FILE, "w") as f:
        json.dump(m, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--program", default="Interview Readiness Training",
                   help="Salesforce Program name to match. Must be EXACT.")
    p.add_argument("--target-type-folder", default="Interview-Readiness",
                   help="S3 type folder to move matches into.")
    p.add_argument("--execute", action="store_true",
                   help="Actually copy. Without this: dry run, nothing touched.")
    p.add_argument("--move", action="store_true",
                   help="Delete the old copy after its new copy VERIFIES. Needs --execute.")
    p.add_argument("--limit", type=int, default=None,
                   help="Only process the first N -- for a small real test batch.")
    args = p.parse_args()

    target_prefix = f"Training/{args.target_type_folder.strip('/')}/"

    started = time.time()
    host = socket.gethostname()
    log(f"=== Reclassify run started {datetime.now(timezone.utc).isoformat()} on {host} ===")
    log(f"Program:  {args.program!r}")
    log(f"Target:   {target_prefix}")
    log(f"Flags:    execute={args.execute} move={args.move} limit={args.limit}")
    log(f"Manifest: {os.path.abspath(MANIFEST_FILE)}")
    log(f"SNS:      {'ON' if SNS_TOPIC_ARN else 'OFF'}\n")

    # --- Salesforce first. If this fails we stop before touching anything. ---
    try:
        target_ids = sf_fetch_meeting_ids(args.program)
    except Exception as exc:
        log(f"\nSALESFORCE LOOKUP FAILED -- stopping before touching any S3 data.\n{exc}")
        notify("Reclassify FAILED (Salesforce)",
               f"Could not fetch meeting IDs on {host}.\n\n{exc}\n\nNo S3 data was touched.")
        sys.exit(1)

    if not target_ids:
        log(f"\nSalesforce returned 0 sessions for program {args.program!r}.\n"
            f"Nothing to do. Check the program name matches Salesforce exactly.")
        return
    log("")

    log("Scanning Resume-Based / Advanced / Other for those IDs...")
    jobs = plan_moves(target_ids, target_prefix)
    log(f"Found {len(jobs)} item(s) to move.\n")

    by_src = defaultdict(int)
    for j in jobs:
        by_src[f"{j['source_type']} ({j['kind']})"] += 1
    if by_src:
        log("Breakdown by current location:")
        for k, v in sorted(by_src.items()):
            log(f"  {k}: {v}")

    matched = set()
    for j in jobs:
        m = (re.search(r"/(\d+)/?$", j["old_prefix"].rstrip("/"))
             or SESSION_RESULT_RE.search(j["old_prefix"]))
        if m:
            matched.add(m.group(1))
    log(f"\n{len(matched)} of {len(target_ids)} Salesforce IDs matched something in S3.")
    if len(matched) < len(target_ids):
        log(f"{len(target_ids) - len(matched)} ID(s) matched nothing -- expected if those\n"
            f"sessions were never recorded, or are already in {target_prefix}")
    log("")

    manifest = load_manifest()
    if args.limit:
        jobs = jobs[:args.limit]
        log(f"--limit {args.limit}: processing only the first {args.limit}.\n")

    if not args.execute:
        log("=== DRY RUN — nothing will be copied or deleted ===\n")
        for j in jobs:
            log(f"  {j['old_prefix']}")
            log(f"    -> {j['new_prefix']}")
            log(f"    ({len(j['objects'])} objects, {j['total_bytes']:,} bytes, "
                f"from {j['source_type']})")
            if args.move:
                log("    --move set: the OLD copy above WOULD BE DELETED once verified")
            log("")
        log("Nothing changed. Review the plan, then re-run with --execute\n"
            "(start with --limit 5 --execute --move).")
        return

    moved = deleted = failed = skipped = 0
    for job in jobs:
        prior = manifest.get(job["old_prefix"], {})
        if prior.get("verified") and (not args.move or prior.get("old_deleted")):
            skipped += 1
            continue

        log(f"Moving: {job['old_prefix']}\n     -> {job['new_prefix']}")
        ok = (copy_and_verify_file(job) if job["kind"] == "file"
              else copy_and_verify_session(job))

        old_deleted = False
        if ok:
            moved += 1
            log("    verified OK")
            if args.move:
                old_deleted = delete_old_objects(job)
                if old_deleted:
                    deleted += 1
                    log("    old copy deleted")
                else:
                    log("    WARNING: new copy verified and safe, but deleting the old "
                        "copy failed. Old data untouched -- safe to re-run.")
        else:
            failed += 1

        manifest[job["old_prefix"]] = {
            "new_prefix":   job["new_prefix"],
            "verified":     ok,
            "old_deleted":  old_deleted,
            "object_count": len(job["objects"]),
            "total_bytes":  job["total_bytes"],
            "source_type":  job["source_type"],
            "program":      args.program,
            "checked_at":   datetime.now(timezone.utc).isoformat(),
        }
        save_manifest(manifest)

    elapsed = int(time.time() - started)
    h, rem = divmod(elapsed, 3600)
    mnt, sec = divmod(rem, 60)
    el = f"{h}h {mnt}m {sec}s"

    log(f"\n=== Done ===")
    log(f"Moved+verified: {moved}   Old copies deleted: {deleted}   "
        f"Failed: {failed}   Skipped (already done): {skipped}")
    log(f"Elapsed: {el}")
    if failed:
        log(f"\n{failed} item(s) failed -- their OLD data is untouched and safe.\n"
            "Re-run the same command; completed items are skipped automatically.")

    status = "COMPLETED WITH FAILURES" if failed else "COMPLETED OK"
    notify(f"Reclassify {status} ({moved} moved, {failed} failed)",
           f"Reclassify finished on {host}.\n\nProgram: {args.program}\n"
           f"Target:  {target_prefix}\nStatus:  {status}\n"
           f"Moved+verified: {moved}\nOld deleted: {deleted}\nFailed: {failed}\n"
           f"Skipped: {skipped}\nElapsed: {el}\n"
           f"Manifest: {os.path.abspath(MANIFEST_FILE)}\n")
    log(f"\n=== Reclassify run ended {datetime.now(timezone.utc).isoformat()} ===")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\nInterrupted. Progress is saved -- re-run to resume.")
        sys.exit(130)
    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        log(f"\n!!! RECLASSIFY CRASHED !!!\n{tb}")
        notify("Reclassify CRASHED",
               f"Crashed on {socket.gethostname()}.\n\nError: {exc}\n\n{tb}\n\n"
               "Data is safe: an old copy is only ever deleted after its new copy\n"
               "byte-verifies, and the manifest is written after every item.\n")
        sys.exit(1)
