#!/usr/bin/env python3
"""
Migrates Zoom cloud recordings into S3, using the processor Lambda's OWN
path-building code.

WHY IT IMPORTS THE LAMBDA INSTEAD OF REIMPLEMENTING IT
  The S3 path is not a simple template. It varies per department, and is built
  from Salesforce lookups, participant lists, topic parsing, fuzzy trainer
  matching and program classification. Rewriting that here would mean two
  implementations drifting apart, and a recording placed at a plausible but
  wrong path is far worse than one not placed at all -- it looks correct until
  someone goes looking for it.

  So this imports the Lambda source and calls its real functions:
      normalize_department()   resolve_storage_info()   build_base_prefix()

  Put lambda_function.py (the DEPLOYED processor) beside this script. If it is
  missing or has drifted from production, the paths will be wrong -- so the
  script refuses to run without it rather than guessing.

WHAT IT DOES
  1. Lists every cloud recording via the Zoom API.
  2. Reads each host's Zoom Department -- the sole input deciding the top-level
     folder.
  3. SKIPS anything whose department is blank or unrecognised. Those are left
     on Zoom untouched, and their hosts are listed at the end so the accounts
     can be tagged. Inventing a folder for them would create work to undo.
  4. SKIPS anything whose meeting id already has media in S3. The cleanup
     service deletes from Zoom only after confirming S3, so most of what is on
     Zoom is already stored -- this is what stops the script re-downloading
     tens of GB it does not need.
  5. Downloads the rest from Zoom and uploads to S3 at the Lambda-format path.

WHAT IT NEVER DOES
  - Never deletes from Zoom. Read-only against Zoom, always.
  - Never overwrites an existing S3 object.
  - Never writes to Salesforce. The S3 upload triggers the linker Lambda, which
    creates the shortlink and writes the link -- exactly as for a live
    recording. Doing it here would duplicate that and risk disagreeing with it.

REQUIRES
  .env beside this script, ZOOM values only:
      Account_Id, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET
  Do NOT put AWS keys there -- the instance role already provides AWS access,
  and static keys would override it with different permissions.

  pip install boto3 python-dotenv requests
  Zoom scopes: recording:read:admin, user:read:admin

USAGE
  python3 zoom_to_s3_migration.py
  python3 zoom_to_s3_migration.py --limit 2 --execute
  nohup python3 -u zoom_to_s3_migration.py --execute --workers 8 \\
        > ~/zoom_migration.log 2>&1 &
"""

import argparse
import base64
import importlib.util
import json
import os
import re
import socket
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

try:
    import boto3
    import requests
    from botocore.config import Config
    from dotenv import load_dotenv
except ImportError as exc:
    print(f"Missing dependency: {exc}\nRun: pip install boto3 python-dotenv requests",
          file=sys.stderr)
    sys.exit(1)

load_dotenv()

ZOOM_ACCOUNT_ID    = os.environ.get("Account_Id") or os.environ.get("ZOOM_ACCOUNT_ID")
ZOOM_CLIENT_ID     = os.environ.get("ZOOM_CLIENT_ID")
ZOOM_CLIENT_SECRET = os.environ.get("ZOOM_CLIENT_SECRET")
S3_BUCKET          = os.environ.get("S3_BUCKET_NAME", "zoom-automation-bucket")
AWS_REGION         = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SNS_TOPIC_ARN      = os.environ.get("MIGRATION_SNS_TOPIC_ARN", "").strip()
LAMBDA_PATH        = os.environ.get("PROCESSOR_LAMBDA_PATH", "lambda_function.py")
# Must match the processor Lambda's own ZOOM_SECRET_NAME -- those are the
# Server-to-Server credentials production authenticates with every day.
ZOOM_SECRET_NAME   = os.environ.get("ZOOM_SECRET_NAME", "zoom/general-oauth")
MANIFEST_FILE      = os.environ.get("ZOOM_MIGRATION_MANIFEST", "zoom_migration_manifest.json")

_cfg = Config(max_pool_connections=64, retries={"max_attempts": 10, "mode": "adaptive"})
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
#  Import the Lambda's own path logic
# ══════════════════════════════════════════════════════════════════════════════

def load_processor():
    """Import the deployed processor so its real path functions are used.

    Its module-level code reads env vars that are only needed at runtime, so
    placeholders are set purely to let the import succeed. Nothing here invokes
    the Lambda -- only its pure path-building functions are called.
    """
    if not os.path.exists(LAMBDA_PATH):
        log(f"ERROR: {LAMBDA_PATH} not found.\n"
            f"Place the DEPLOYED zoom-recording-processor source beside this "
            f"script (or set PROCESSOR_LAMBDA_PATH). Without it the S3 paths "
            f"would have to be guessed, and a wrong path is worse than no "
            f"migration at all.")
        sys.exit(1)

    # The Lambda reads these at import time. ZOOM_SECRET_NAME must be the REAL
    # secret name -- get_zoom_secret() uses it directly, so a placeholder here
    # produces "not authorized ... on resource: placeholder", which reads like
    # an IAM problem when it is actually a wrong name being requested.
    os.environ.setdefault("ZOOM_SECRET_NAME", ZOOM_SECRET_NAME)
    os.environ.setdefault("S3_BUCKET_NAME", S3_BUCKET)

    spec = importlib.util.spec_from_file_location("processor", LAMBDA_PATH)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:
        log(f"ERROR importing {LAMBDA_PATH}: {exc}")
        sys.exit(1)

    for fn in ("normalize_department", "resolve_storage_info", "build_base_prefix",
               "sanitize_name", "month_folder_name", "build_time_folder_ist"):
        if not hasattr(mod, fn):
            log(f"ERROR: {LAMBDA_PATH} has no {fn}() — is this really the "
                f"processor Lambda?")
            sys.exit(1)
    return mod


# ══════════════════════════════════════════════════════════════════════════════
#  Zoom
# ══════════════════════════════════════════════════════════════════════════════

def zoom_token(proc):
    """Authenticate to Zoom using the LAMBDA'S OWN credential path.

    WHY NOT .env
      The Server-to-Server credentials the processor uses live in Secrets
      Manager under ZOOM_SECRET_NAME. Those are known-good -- production
      authenticates with them continuously.

      Credentials pasted into .env are easy to get wrong: a Zoom account
      typically has several apps, and only the Server-to-Server OAuth one
      supports the account_credentials grant. An OAuth (user-authorised) app's
      credentials look identical but fail with "The application doesn't support
      account_credential" -- which is exactly what happened on the first run.

      Reading the secret removes that whole class of mistake, and keeps
      credentials off disk.

    FALLBACK
      If the secret cannot be read (usually a missing IAM permission), .env is
      tried -- but only if all three values are present there.
    """
    try:
        secret_obj = proc.get_zoom_secret()
        token = proc.get_s2s_access_token(secret_obj)
        log(f"  authenticated via Secrets Manager ({proc.ZOOM_SECRET_NAME})")
        return token
    except Exception as exc:
        log(f"  Secrets Manager auth failed: {exc}")

    if not all([ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET]):
        log("\nERROR: could not authenticate to Zoom.\n"
            f"  Reading {getattr(proc, 'ZOOM_SECRET_NAME', 'the Zoom secret')} "
            f"failed, and .env does not have all three fallback values.\n\n"
            "  Most likely the instance role lacks secretsmanager:GetSecretValue\n"
            "  on that secret. Add it, or put the SERVER-TO-SERVER OAuth app's\n"
            "  credentials in .env (an OAuth app's will not work).")
        sys.exit(1)

    log("  falling back to .env credentials")
    basic = base64.b64encode(f"{ZOOM_CLIENT_ID}:{ZOOM_CLIENT_SECRET}".encode()).decode()
    r = requests.post("https://zoom.us/oauth/token",
                      params={"grant_type": "account_credentials",
                              "account_id": ZOOM_ACCOUNT_ID},
                      headers={"Authorization": f"Basic {basic}"}, timeout=30)
    if r.status_code != 200:
        log(f"\nZoom auth failed ({r.status_code}): {r.text}\n"
            "  If this says 'doesn't support account_credential', those are an\n"
            "  OAuth app's credentials. Only a Server-to-Server OAuth app works.")
        sys.exit(1)
    return r.json()["access_token"]


def zoom_get(token, path, params=None):
    for attempt in (1, 2, 3):
        r = requests.get(f"https://api.zoom.us/v2{path}",
                         headers={"Authorization": f"Bearer {token}"},
                         params=params or {}, timeout=60)
        if r.status_code == 429:
            time.sleep(2 * attempt)
            continue
        if r.status_code != 200:
            raise RuntimeError(f"GET {path} -> {r.status_code}: {r.text[:250]}")
        return r.json()
    raise RuntimeError(f"GET {path} rate limited after retries")


def list_users(token):
    users, npt = [], None
    while True:
        params = {"page_size": 300, "status": "active"}
        if npt:
            params["next_page_token"] = npt
        data = zoom_get(token, "/users", params)
        users += data.get("users", [])
        npt = data.get("next_page_token")
        if not npt:
            break
    return users


def list_recordings(token, user_id, frm, to):
    """Zoom caps each recordings query at ~1 month, so longer ranges are walked
    month by month."""
    out, cursor = [], frm
    while cursor < to:
        chunk_end = min(cursor + timedelta(days=29), to)
        npt = None
        while True:
            params = {"from": cursor.strftime("%Y-%m-%d"),
                      "to": chunk_end.strftime("%Y-%m-%d"), "page_size": 300}
            if npt:
                params["next_page_token"] = npt
            try:
                data = zoom_get(token, f"/users/{user_id}/recordings", params)
            except RuntimeError as exc:
                if "404" in str(exc):
                    return out
                raise
            out += data.get("meetings", [])
            npt = data.get("next_page_token")
            if not npt:
                break
        cursor = chunk_end + timedelta(days=1)
    return out


def zoom_participants(token, meeting_uuid):
    """Participants for a past meeting. Best-effort: the processor uses these
    for candidate resolution, but Zoom drops them for older meetings and an
    empty list is handled gracefully downstream."""
    try:
        enc = requests.utils.quote(requests.utils.quote(meeting_uuid, safe=""), safe="")
        data = zoom_get(token, f"/past_meetings/{enc}/participants",
                        {"page_size": 300})
        return data.get("participants", [])
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  S3
# ══════════════════════════════════════════════════════════════════════════════

def s3_existing_meeting_ids():
    found = set()
    pat = re.compile(r"/(\d{9,11})/(MP4|M4A|mp4|m4a)/")
    paginator = s3.get_paginator("list_objects_v2")
    scanned = 0
    for page in paginator.paginate(Bucket=S3_BUCKET):
        for obj in page.get("Contents", []):
            scanned += 1
            m = pat.search(obj["Key"])
            if m:
                found.add(m.group(1))
        if scanned % 100000 < 1000:
            log(f"    ...scanned {scanned} objects, {len(found)} meeting id(s)")
    return found


def s3_object_exists(key):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False


FILE_TYPE_FOLDER = {"MP4": "MP4", "M4A": "M4A",
                    "TRANSCRIPT": "TRANSCRIPT", "CHAT": "CHAT",
                    "CC": "CC", "TIMELINE": "TIMELINE"}


def upload_one(token, base_prefix, rec_file):
    """Download one file from Zoom and put it in S3. Returns (ok, note)."""
    ftype = (rec_file.get("file_type") or "").upper()
    folder = FILE_TYPE_FOLDER.get(ftype)
    if not folder:
        return True, f"skipped file_type={ftype}"

    ext = (rec_file.get("file_extension") or ftype).lower()
    key = f"{base_prefix}{folder}/{rec_file.get('id')}.{ext}"

    if s3_object_exists(key):
        return True, "already present"

    url = rec_file.get("download_url")
    if not url:
        return False, "no download_url"

    try:
        with requests.get(url, headers={"Authorization": f"Bearer {token}"},
                          stream=True, timeout=900) as resp:
            if resp.status_code != 200:
                return False, f"download {resp.status_code}"
            s3.upload_fileobj(resp.raw, S3_BUCKET, key)
    except Exception as exc:
        return False, f"transfer failed: {exc}"

    expected = rec_file.get("file_size")
    if expected:
        try:
            got = s3.head_object(Bucket=S3_BUCKET, Key=key)["ContentLength"]
            if got != expected:
                return False, f"size mismatch: zoom {expected}, s3 {got}"
        except Exception as exc:
            return False, f"verify failed: {exc}"
    return True, key


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
    with _lock:
        with open(MANIFEST_FILE, "w") as f:
            json.dump(m, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true",
                   help="Actually download and upload. Without this: dry run.")
    p.add_argument("--limit", type=int, default=None, help="Only the first N meetings.")
    p.add_argument("--workers", type=int, default=8,
                   help="Concurrent file transfers (default 8). These stream "
                        "through this machine, unlike S3-to-S3 copies, so more "
                        "workers means more bandwidth contention, not less time.")
    p.add_argument("--days", type=int, default=365, help="How far back on Zoom.")
    p.add_argument("--cleanup-zoom", action="store_true",
                   help="After a meeting's files are uploaded AND verified, hand "
                        "it to the existing zoom-recording-cleaner queue so the "
                        "copy on Zoom is removed. Off by default -- deletion is "
                        "the one irreversible step here, so it must be asked for. "
                        "Nothing is deleted by this script directly; the cleaner "
                        "re-verifies S3 before removing anything.")
    args = p.parse_args()

    started = time.time()
    host = socket.gethostname()
    log(f"=== Zoom -> S3 migration started {datetime.now(timezone.utc).isoformat()} "
        f"on {host} ===")
    log(f"Flags: execute={args.execute} limit={args.limit} workers={args.workers} "
        f"days={args.days}")
    log(f"Bucket: {S3_BUCKET}\n")

    log(f"[1/5] Importing path logic from {LAMBDA_PATH} ...")
    proc = load_processor()
    log("  OK — using the Lambda's own build_base_prefix()")

    if args.cleanup_zoom:
        # enqueue_delete_job() no-ops when these are unset -- by design, so a
        # missing env var can never cause a failed delete. But here that would
        # mean --cleanup-zoom silently doing nothing while reporting success,
        # so it is checked up front instead.
        if not getattr(proc, "DELETE_QUEUE_URL", ""):
            log("\nERROR: --cleanup-zoom requires DELETE_QUEUE_URL.\n"
                "  Without it enqueue_delete_job() silently does nothing and the\n"
                "  Zoom copies would never be removed, while the run reported\n"
                "  success. Set it to the same queue the Lambda uses:\n"
                "    export DELETE_QUEUE_URL=<zoom-recording-cleaner queue url>")
            sys.exit(1)
        if not getattr(proc, "ZOOM_DELETE_ENABLED", False):
            log("\nERROR: --cleanup-zoom requires ZOOM_DELETE_ENABLED=1.")
            sys.exit(1)
        log("  cleanup queue configured — verified uploads will be queued for "
            "Zoom deletion")
    log("")

    log("[2/5] Scanning S3 for meetings already stored ...")
    in_s3 = s3_existing_meeting_ids()
    log(f"  {len(in_s3)} meeting id(s) already in S3\n")

    log("[3/5] Zoom auth + users ...")
    token = zoom_token(proc)
    users = list_users(token)
    log(f"  {len(users)} active user(s)\n")

    log(f"[4/5] Listing cloud recordings (last {args.days} days) ...")
    to = datetime.now(timezone.utc).date()
    frm = to - timedelta(days=args.days)

    todo, skip_in_s3, skip_no_dept, skip_bad_dept = [], [], [], []

    for i, u in enumerate(users, 1):
        uid, email = u.get("id"), (u.get("email") or "")
        raw_dept = (u.get("dept") or "").strip()
        dept = proc.normalize_department(raw_dept)

        try:
            meetings = list_recordings(token, uid, frm, to)
        except Exception as exc:
            log(f"    (could not list {email}: {exc})")
            continue

        for m in meetings:
            mid = str(m.get("id") or "").strip()
            entry = {"meeting_id": mid, "uuid": m.get("uuid"),
                     "topic": m.get("topic") or "", "start_time": m.get("start_time"),
                     "host_id": uid, "host_email": email, "host_name":
                         ((u.get("first_name") or "") + " " + (u.get("last_name") or "")).strip(),
                     "raw_dept": raw_dept, "department": dept,
                     "files": m.get("recording_files") or [],
                     "size": sum(f.get("file_size", 0) for f in (m.get("recording_files") or []))}

            if mid in in_s3:
                skip_in_s3.append(entry)
            elif not raw_dept:
                skip_no_dept.append(entry)
            elif not dept:
                skip_bad_dept.append(entry)
            else:
                todo.append(entry)

        if i % 20 == 0:
            log(f"    ...{i}/{len(users)} users")

    def gb(rows):
        return sum(r["size"] for r in rows) / (1024 ** 3)

    log(f"\n[5/5] Scope")
    log("=" * 74)
    log(f"  MIGRATABLE             : {len(todo):5d}  ({gb(todo):.1f} GB)")
    log(f"  already in S3          : {len(skip_in_s3):5d}  ({gb(skip_in_s3):.1f} GB)")
    log(f"  host has NO department : {len(skip_no_dept):5d}  ({gb(skip_no_dept):.1f} GB)")
    log(f"  department UNRECOGNISED: {len(skip_bad_dept):5d}  ({gb(skip_bad_dept):.1f} GB)")
    log("=" * 74)

    if todo:
        log("\n  migratable by department:")
        for d, c in Counter(r["department"] for r in todo).most_common():
            log(f"    {c:5d}  {d}")

    if skip_no_dept:
        log("\n  hosts with NO department — tag these in Zoom Admin, then re-run:")
        for e, c in Counter(r["host_email"] for r in skip_no_dept).most_common():
            log(f"    {c:5d}  {e}")

    if skip_bad_dept:
        log("\n  department values not in the Lambda's mapping:")
        for d, c in Counter(r["raw_dept"] for r in skip_bad_dept).most_common():
            log(f"    {c:5d}  {d!r}")

    if not todo:
        log("\nNothing to migrate.")
        return

    manifest = load_manifest()
    if args.limit:
        todo = todo[:args.limit]
        log(f"\n--limit {args.limit}: only the first {args.limit} meeting(s).")

    if not args.execute:
        log("\n=== DRY RUN — nothing downloaded or uploaded ===\n")
        for e in todo[:10]:
            parts = zoom_participants(token, e["uuid"])
            try:
                prefix = build_prefix_for(proc, e, parts)
            except Exception as exc:
                prefix = f"(could not build path: {exc})"
            log(f"  {e['host_email']}  [{e['department']}]  {e['topic'][:50]}")
            log(f"    meeting {e['meeting_id']}  {len(e['files'])} file(s)  "
                f"{e['size'] / (1024**2):.0f} MB")
            log(f"    -> {prefix}\n")
        if len(todo) > 10:
            log(f"  ... and {len(todo) - 10} more\n")
        if args.cleanup_zoom:
            log("--cleanup-zoom is set: each meeting above WOULD be queued for "
                "Zoom deletion after its files upload and verify.\n")
        log("Review the paths above, then re-run with --execute "
            "(start with --limit 2 --execute).")
        return

    notify("Zoom -> S3 migration STARTED",
           f"{len(todo)} meeting(s), {gb(todo):.1f} GB, host {host}")

    done = failed = skipped = cleaned = 0
    failures = []

    for idx, e in enumerate(todo, 1):
        if manifest.get(e["meeting_id"], {}).get("ok"):
            skipped += 1
            continue

        parts = zoom_participants(token, e["uuid"])
        try:
            prefix = build_prefix_for(proc, e, parts)
        except Exception as exc:
            failed += 1
            failures.append({**{k: e[k] for k in ("meeting_id", "host_email", "topic")},
                             "error": f"path build failed: {exc}"})
            log(f"[{idx}/{len(todo)}] FAILED path build {e['meeting_id']}: {exc}")
            continue

        log(f"[{idx}/{len(todo)}] {e['department']}  {e['meeting_id']}  "
            f"{len(e['files'])} file(s)  {e['size'] / (1024**2):.0f} MB")
        log(f"        -> {prefix}")

        errs = []
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futs = [pool.submit(upload_one, token, prefix, f) for f in e["files"]]
            for fut in as_completed(futs):
                ok, note = fut.result()
                if not ok:
                    errs.append(note)

        if errs:
            failed += 1
            failures.append({**{k: e[k] for k in ("meeting_id", "host_email", "topic")},
                             "error": "; ".join(errs[:3])})
            log(f"        FAILED: {errs[0]}")
        else:
            done += 1
            log("        uploaded and verified")

            if args.cleanup_zoom:
                # Only reached when EVERY file for this meeting uploaded and
                # its byte size matched what Zoom reported. A partial upload
                # never gets here, so a meeting can never be removed from Zoom
                # while any of its files are missing from S3.
                #
                # The cleaner is handed the prefix we actually used, so it
                # cannot look in the wrong folder -- and it re-verifies S3
                # itself before deleting. Two independent checks before
                # anything irreversible happens.
                try:
                    proc.enqueue_delete_job(
                        meeting_id=e["meeting_id"],
                        meeting_uuid=e["uuid"],
                        host_id=e["host_id"],
                        base_prefix=prefix,
                        department_folder=e["department"],
                        start_time=e["start_time"],
                        event_type="zoom_to_s3_migration",
                    )
                    cleaned += 1
                    log("        queued for Zoom cleanup")
                except Exception as exc:
                    log(f"        WARNING: cleanup enqueue failed (non-fatal, "
                        f"recording is safe in S3): {exc}")

        manifest[e["meeting_id"]] = {"ok": not errs, "prefix": prefix,
                                     "cleanup_queued": bool(args.cleanup_zoom and not errs),
                                     "at": datetime.now(timezone.utc).isoformat()}
        save_manifest(manifest)

        if idx % 25 == 0:
            notify(f"Zoom -> S3 progress {idx}/{len(todo)}",
                   f"done={done} failed={failed}")

    elapsed = int(time.time() - started)
    log(f"\n=== Done ===")
    log(f"Migrated: {done}   Failed: {failed}   Skipped (already done): {skipped}")
    if args.cleanup_zoom:
        log(f"Queued for Zoom cleanup: {cleaned}")
    log(f"Elapsed: {elapsed // 60}m {elapsed % 60}s")

    if failures:
        log(f"\nFAILED ({len(failures)}):")
        for f in failures:
            log(f"  {f['meeting_id']}  {f['host_email']}\n    {f['error']}")
        with open("zoom_migration_failures.json", "w") as fh:
            json.dump(failures, fh, indent=2)
        log(f"\n  detail -> {os.path.abspath('zoom_migration_failures.json')}")

    if args.cleanup_zoom:
        log(f"\nNOTE: {cleaned} meeting(s) were QUEUED for Zoom cleanup. This "
            "script never deletes directly -- zoom-recording-cleaner re-verifies "
            "S3 and removes the Zoom copy. Anything that failed upload was NOT "
            "queued and remains on Zoom.")
    else:
        log("\nNOTE: nothing was deleted from Zoom (--cleanup-zoom not set).")
    log("The S3 uploads will have triggered the linker Lambda, which creates "
        "shortlinks and writes the Salesforce links on its own.")

    notify(f"Zoom -> S3 migration {'COMPLETED WITH FAILURES' if failed else 'COMPLETED OK'}",
           f"Migrated: {done}\nFailed: {failed}\nSkipped: {skipped}\n"
           f"Elapsed: {elapsed // 60}m {elapsed % 60}s\n")


def build_prefix_for(proc, entry, participants):
    """Call the Lambda's own resolve_storage_info() + build_base_prefix().

    TRAINING NEEDS SALESFORCE
      For the Training department the folder after Training/ is the PROGRAM,
      which only Salesforce knows. Passing None would put every training
      recording under the catch-all instead of Resume-Based / Advanced /
      Interview-Readiness / Retraining -- correct-looking but wrong, and it
      would need another migration to undo.

      So the same lookups the Lambda performs are performed here, using the
      Lambda's own functions. If Salesforce cannot answer, program_folder stays
      None and the Lambda's own fallback applies -- identical to what a live
      recording would get in the same situation.

    INTERVIEW-SUCCESS NEEDS SALESFORCE TOO
      Its type folder (Interview / Internal-Interview / a slugified purpose) is
      resolved inside resolve_storage_info() via its own Salesforce calls, so
      nothing extra is needed here.
    """
    start = entry["start_time"] or ""
    dept = entry["department"]

    # A short or missing start_time produces UnknownYear/UnknownMonth/
    # Time-Unknown-IST. The Lambda tolerates that for a live recording because
    # something is better than nothing, but here it would bury a recording in a
    # folder nobody would ever look in -- and if --cleanup-zoom is on, the Zoom
    # copy would then be deleted. Refuse instead; the meeting stays on Zoom and
    # is reported.
    if len(start) < 10:
        raise ValueError(f"unusable start_time {start!r} — would produce an "
                         f"Unknown date path")

    host_name = proc.sanitize_name(entry["host_name"] or
                                   entry["host_email"].split("@")[0] or "Unknown_Host")

    program_name = program_folder = trainer_sf = session_type = None
    if dept == "Training":
        # UNPACK BY LENGTH, NOT BY A FIXED COUNT.
        #
        # This function's return width has grown as fields were added --
        # session_type was appended most recently. Unpacking a fixed number of
        # values means a future addition silently raises "too many values to
        # unpack", the exception is swallowed, program_name stays None, and
        # EVERY training recording lands in Training/Other/ while Salesforce
        # was answering correctly all along.
        #
        # That is not hypothetical: it is exactly what the dry run caught.
        try:
            res = proc.lookup_training_day_from_sf(entry["meeting_id"])
            if res:
                program_name = res[3] if len(res) > 3 else None
                trainer_sf   = res[4] if len(res) > 4 else None
                session_type = res[7] if len(res) > 7 else None
        except Exception as exc:
            log(f"        (Salesforce lookup failed for {entry['meeting_id']}: {exc})")

        if program_name:
            try:
                program_folder, _reason = proc.resolve_program_folder(program_name)
            except Exception:
                program_folder = proc.program_folder_name(program_name)

    info = proc.resolve_storage_info(
        department_folder=dept,
        topic=entry["topic"],
        participants=participants,
        host_email=entry["host_email"],
        meeting_id=entry["meeting_id"],
        host_name=host_name,
        program_name=program_name,
        program_folder=program_folder,
        trainer_name_sf=trainer_sf,
        session_type=session_type,
    )

    return proc.build_base_prefix(
        department_folder=dept,
        host_name=info.get("canonical_host_name") or host_name,
        year=start[:4] if len(start) >= 4 else "UnknownYear",
        month=proc.month_folder_name(start),
        candidate_name=info.get("candidate_name") or "Unknown_Candidate",
        date_only=start[:10] if len(start) >= 10 else "UnknownDate",
        time_folder=proc.build_time_folder_ist(start),
        company_name=info.get("company_name"),
        round_name=info.get("round_name"),
        meeting_id=entry["meeting_id"],
        trainer_name=info.get("trainer_name"),
        hr_person_name=info.get("hr_person_name"),
        canonical_host_name=info.get("canonical_host_name"),
        program_folder=info.get("program_folder") or program_folder,
        interview_type=info.get("interview_type"),
    )


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
        notify("Zoom -> S3 migration CRASHED", f"{exc}\n\n{tb}")
        sys.exit(1)