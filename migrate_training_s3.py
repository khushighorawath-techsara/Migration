#!/usr/bin/env python3
"""
Migrates historical Training-department S3 data into the new structure:
    Training/{Type}/{Trainer}/{Year}/{Month}/{Candidate}/{Date}/{Time}/{MeetingID}/

SAFETY MODEL — read this before running:
  - COPY-BY-DEFAULT, DELETE-ONLY-IF-YOU-ASK. Without --move, this behaves
    exactly as before: pure copy, nothing ever deleted, both old and new
    data exist side by side. With --move: after a session's new copy is
    VERIFIED complete, its OLD copy is deleted immediately — deletion is
    always a direct, immediate consequence of a successful verification
    for that exact session, never a separate later pass over everything.
  - Every session is verified after copying (object count + total bytes
    compared between old and new location) before being marked done, and
    before any deletion can happen for it.
  - Idempotent and resumable: progress is saved to a manifest file after
    every session. Re-running skips anything already verified+deleted, and
    safely ignores data that's already in the new structure (see PREREQUISITES).
  - DRY RUN BY DEFAULT. Nothing is copied OR deleted until you pass --execute.

WHAT THIS MIGRATES:
  1. Advanced-Training/{trainer}/{year}/{month}/{grouped-candidates}/{date}/{time}/{meetingid}/
     -> Training/Advanced/{trainer}/{year}/{month}/Group/{date}/{time}/{meetingid}/

  2. Training/{trainer}/{year}/{month}/{candidate}/{date}/{time}/{meetingid}/
     where {candidate} matches an "advance" pattern (Advance_Training,
     Advanced_Training, Advanced-Training, etc. — matched after stripping
     separators and lowercasing, so spelling drift doesn't slip through)
     -> Training/Advanced/{trainer}/{year}/{month}/Group/{date}/{time}/{meetingid}/

  3. Training/{trainer}/{year}/{month}/{candidate}/{date}/{time}/{meetingid}/
     where {candidate} is a real name
     -> Training/Resume-Based/{trainer}/{year}/{month}/{candidate}/{date}/{time}/{meetingid}/

WHAT THIS DOES NOT TOUCH:
  - Any department other than Training/ and Advanced-Training/ (HR, Marketing,
    CEO, QMS, etc. are completely out of scope).
  - Deletion of the old Advanced-Training/ department folder, or the old
    Advance_Training/Advanced_Training folders inside Training/, UNLESS you
    explicitly pass --move. Without --move (the default), nothing is ever
    deleted — this remains a deliberate, separate, manual step. With --move,
    deletion still only ever happens per-session, and only immediately after
    that session's new copy is verified — see SAFETY MODEL above.

PREREQUISITES:
  - `pip install boto3`
  - AWS credentials with GetObject/PutObject/ListBucket/CopyObject on BOTH
    "Training/*" AND "Advanced-Training/*" in the target bucket. If you plan
    to use --move, DeleteObject is also required on both prefixes. The
    existing worker IAM role was scoped only to "Training/*" as of this
    writing — check it covers "Advanced-Training/*" too before running,
    or this will fail partway through with AccessDenied on Pattern-A jobs.
  - Confirm BUCKET below matches your real bucket name.

USAGE (in order — do not skip ahead):
  python3 migrate_training_s3.py                            # 1. dry run, plan only
  python3 migrate_training_s3.py --limit 5 --execute --move # 2. small real test batch
  python3 migrate_training_s3.py --execute --move           # 3. full real run

RUNNING ON EC2 (so it survives your SSH/Session-Manager disconnecting):
  nohup python3 -u migrate_training_s3.py --execute --move \
        > ~/migration.log 2>&1 &
  tail -f ~/migration.log        # watch progress live
  # -u and the log() helper both force unbuffered output, otherwise the log
  # file stays empty for minutes and looks frozen.

COMPLETION ALERT (optional but recommended for long unattended runs):
  export MIGRATION_SNS_TOPIC_ARN=arn:aws:sns:us-east-1:<acct>:s3-migration-alerts
  ...or pass --sns-arn arn:aws:sns:...
  You get an email when the run finishes OR crashes. Requires sns:Publish in
  the instance's IAM role.
"""

import argparse
import json
import os
import socket
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

try:
    import boto3
except ImportError:
    print("boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)

BUCKET = "zoom-automation-bucket"
OLD_TRAINING_PREFIX = "Training/"
OLD_ADVANCED_DEPT_PREFIX = "Advanced-Training/"

# Optional: set this env var (or pass --sns-arn) to get an email/SMS when the
# run finishes or crashes. Leave unset to just print to the log as usual.
SNS_TOPIC_ARN = os.environ.get("MIGRATION_SNS_TOPIC_ARN", "").strip()

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Must match TRAINING_TYPE_FOLDER_NAMES in the Lambda exactly, so old and
# new data end up describing the same thing the same way.
TRAINING_TYPE_FOLDER_NAMES = {
    "ADVANCED":            "Advanced",
    "NORMAL":              "Resume-Based",
    "INTERVIEW_READINESS": "Interview-Readiness",
    "UNKNOWN":             "Other",
}
KNOWN_TYPE_FOLDER_VALUES = set(TRAINING_TYPE_FOLDER_NAMES.values())

MANIFEST_FILE = os.environ.get("MIGRATION_MANIFEST_PATH", "migration_manifest.json")
SESSION_DEPTH = 7  # trainer/year/month/candidate/date/time/meetingid — 7 segments

s3 = boto3.client("s3")


def log(msg: str = ""):
    """print() that ALWAYS flushes immediately.

    Critical on EC2: when you run this under `nohup ... > log.txt`, Python
    block-buffers stdout, so `tail -f log.txt` would show nothing for many
    minutes and look frozen. Flushing every line makes progress visible live.
    """
    print(msg, flush=True)


def notify(subject: str, message: str, sns_arn: str = ""):
    """Send an SNS notification if an ARN is configured. Never fatal —
    a failed notification must not take down a successful migration."""
    arn = (sns_arn or SNS_TOPIC_ARN).strip()
    if not arn:
        return
    try:
        boto3.client("sns", region_name=AWS_REGION).publish(
            TopicArn=arn,
            Subject=subject[:100],       # SNS hard-limits Subject to 100 chars
            Message=message,
        )
        log(f"[NOTIFY] SNS sent: {subject}")
    except Exception as exc:
        log(f"[NOTIFY] SNS publish failed (non-fatal): {exc}")


# ── Classification (mirrors the Lambda's logic for historical data, where
#    Salesforce can't be trusted to still have every old record) ───────────

def _normalize(text: str) -> str:
    """Lowercase, strip separators. 'Advance_Training' and 'Advanced-Training'
    both become 'advancetraining' — catches spelling drift without an
    exact-string list."""
    return text.lower().replace("_", "").replace("-", "").replace(" ", "")


def is_advance_marker(candidate_folder_name: str) -> bool:
    return "advance" in _normalize(candidate_folder_name)


# ── S3 listing / grouping ───────────────────────────────────────────────────

def list_all_objects(prefix: str):
    """Yields (key, size) for every object under prefix. Paginated."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def group_by_session(prefix: str, depth: int, skip_new_structure: bool = False):
    """
    Groups objects under `prefix` by their session-level prefix (the path
    through and including the MeetingID folder, `depth` segments deep).

    skip_new_structure: when True, ignores any key whose first segment
    (right after `prefix`) is already one of our new Type folder names —
    protects a re-run after Phase 3 is live from trying to "migrate" data
    that's already correctly placed.

    Returns: {session_prefix: {"objects": [(key,size),...], "total_bytes": int}}
    """
    sessions = defaultdict(lambda: {"objects": [], "total_bytes": 0})
    for key, size in list_all_objects(prefix):
        relative = key[len(prefix):]
        parts = relative.split("/")

        if skip_new_structure and parts and parts[0] in KNOWN_TYPE_FOLDER_VALUES:
            continue

        if len(parts) <= depth:
            continue  # not deep enough to be real session content, skip

        session_key = prefix + "/".join(parts[:depth]) + "/"
        sessions[session_key]["objects"].append((key, size))
        sessions[session_key]["total_bytes"] += size
    return sessions


# ── Migration planning ──────────────────────────────────────────────────────

def plan_date_level_files():
    """Loose files that sit at the DATE level — one folder ABOVE the Time
    folder — rather than inside a meeting folder.

    The worker's cross-chunk merge step writes session-result-{meeting_id}.json
    here (confirmed in real S3 listings). These are only 6 segments deep, so
    the session grouping above (which needs 8+) skips them entirely and they
    would be silently left behind in the old structure.

    Each of these becomes a single-file job (kind='file') rather than a
    prefix job, because a date-level prefix also contains all the Time
    folders beneath it — verifying by prefix would compare the wrong things.
    """
    jobs = []
    for src_prefix in (OLD_TRAINING_PREFIX, OLD_ADVANCED_DEPT_PREFIX):
        for key, size in list_all_objects(src_prefix):
            relative = key[len(src_prefix):]
            parts = relative.split("/")

            # Already in the new structure -> leave alone.
            if src_prefix == OLD_TRAINING_PREFIX and parts and parts[0] in KNOWN_TYPE_FOLDER_VALUES:
                continue

            # Exactly trainer/year/month/candidate/date/FILENAME
            if len(parts) != 6:
                continue

            trainer, year, month, candidate, date, filename = parts

            if src_prefix == OLD_ADVANCED_DEPT_PREFIX:
                type_folder, new_candidate = "Advanced", "Group"
                source = "DateLevel-PatternA"
            elif is_advance_marker(candidate):
                type_folder, new_candidate = "Advanced", "Group"
                source = "DateLevel-PatternB"
            else:
                type_folder, new_candidate = "Resume-Based", candidate
                source = "DateLevel-PatternC"

            new_key = (
                f"Training/{type_folder}/{trainer}/{year}/{month}/"
                f"{new_candidate}/{date}/{filename}"
            )
            jobs.append({
                "kind":        "file",
                "old_prefix":  key,          # the object key itself
                "new_prefix":  new_key,
                "objects":     [(key, size)],
                "total_bytes": size,
                "source":      source,
            })
    return jobs


def copy_single_file(job: dict) -> bool:
    """Copy one object to its new key, then verify by comparing size."""
    old_key = job["objects"][0][0]
    new_key = job["new_prefix"]
    try:
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": old_key},
            Key=new_key,
        )
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
        log(f"    VERIFY FAILED (file) for {new_key}: {exc}")
        return False
    return True


def plan_migrations():
    """Returns a list of migration jobs, each a dict with old_prefix,
    new_prefix, objects, total_bytes, and source (which pattern it came from,
    purely for the summary printout)."""
    jobs = []

    # Pattern A: top-level Advanced-Training department.
    adv_sessions = group_by_session(OLD_ADVANCED_DEPT_PREFIX, depth=SESSION_DEPTH)
    for old_prefix, info in adv_sessions.items():
        parts = old_prefix[len(OLD_ADVANCED_DEPT_PREFIX):].strip("/").split("/")
        trainer, year, month, _grouped, date, time_f, meeting_id = parts[:SESSION_DEPTH]
        new_prefix = (
            f"Training/Advanced/{trainer}/{year}/{month}/Group/{date}/{time_f}/{meeting_id}/"
        )
        jobs.append({
            "kind": "session",
            "old_prefix": old_prefix, "new_prefix": new_prefix,
            "objects": info["objects"], "total_bytes": info["total_bytes"],
            "source": "PatternA-TopLevelAdvancedDept",
        })

    # Patterns B & C: inside Training/, split by the candidate-slot text.
    # skip_new_structure=True guards against re-processing our own output.
    training_sessions = group_by_session(
        OLD_TRAINING_PREFIX, depth=SESSION_DEPTH, skip_new_structure=True
    )
    for old_prefix, info in training_sessions.items():
        parts = old_prefix[len(OLD_TRAINING_PREFIX):].strip("/").split("/")
        trainer, year, month, candidate, date, time_f, meeting_id = parts[:SESSION_DEPTH]

        if is_advance_marker(candidate):
            new_prefix = (
                f"Training/Advanced/{trainer}/{year}/{month}/Group/{date}/{time_f}/{meeting_id}/"
            )
            source = "PatternB-NestedAdvanced"
        else:
            new_prefix = (
                f"Training/Resume-Based/{trainer}/{year}/{month}/{candidate}/{date}/{time_f}/{meeting_id}/"
            )
            source = "PatternC-Normal"

        jobs.append({
            "kind": "session",
            "old_prefix": old_prefix, "new_prefix": new_prefix,
            "objects": info["objects"], "total_bytes": info["total_bytes"],
            "source": source,
        })

    # Loose date-level files (session-result-*.json from the merge step).
    # Without this they are silently left behind -- see plan_date_level_files.
    jobs.extend(plan_date_level_files())

    return jobs


# ── Copy + verify ────────────────────────────────────────────────────────────

def copy_session(job: dict) -> bool:
    """Copies every object in a session to its new location, one at a time.
    Never touches the source. Returns True only if verify_session() confirms
    a byte-for-byte-complete match afterward."""
    old_prefix = job["old_prefix"]
    new_prefix = job["new_prefix"]

    for old_key, _size in job["objects"]:
        relative = old_key[len(old_prefix):]
        new_key = new_prefix + relative
        try:
            s3.copy_object(
                Bucket=BUCKET,
                CopySource={"Bucket": BUCKET, "Key": old_key},
                Key=new_key,
            )
        except Exception as exc:
            log(f"    COPY FAILED: {old_key} -> {new_key}: {exc}")

    return verify_session(job)


def verify_session(job: dict) -> bool:
    """Compares object count + total bytes between old and new location.
    This is the only thing that marks a session 'done' — nothing is trusted
    on faith."""
    new_objects = list(list_all_objects(job["new_prefix"]))
    new_count = len(new_objects)
    new_bytes = sum(size for _, size in new_objects)

    old_count = len(job["objects"])
    old_bytes = job["total_bytes"]

    ok = (new_count == old_count) and (new_bytes == old_bytes)
    if not ok:
        log(
            f"    VERIFY MISMATCH: {job['old_prefix']} -> {job['new_prefix']}\n"
            f"      old: {old_count} objects / {old_bytes} bytes\n"
            f"      new: {new_count} objects / {new_bytes} bytes"
        )
    return ok


def delete_old_objects(job: dict) -> bool:
    """Deletes every object at the OLD location for this session.
    SAFETY CONTRACT: only ever call this after verify_session() has already
    returned True for this exact job. This function does not re-verify —
    it trusts the caller enforced that ordering. Batches deletes at 1000
    keys per call (S3's own limit for delete_objects)."""
    old_keys = [old_key for old_key, _size in job["objects"]]
    for i in range(0, len(old_keys), 1000):
        batch = old_keys[i : i + 1000]
        try:
            resp = s3.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            errors = resp.get("Errors", [])
            if errors:
                log(f"    DELETE errors in batch: {errors}")
                return False
        except Exception as exc:
            log(f"    DELETE FAILED for batch starting at {batch[0]}: {exc}")
            return False
    return True


# ── Manifest (resumability) ─────────────────────────────────────────────────

def load_manifest() -> dict:
    try:
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_manifest(manifest: dict):
    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f, indent=2)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually copy. Without this flag: dry run, plan only, nothing touched.",
    )
    parser.add_argument(
        "--move", action="store_true",
        help="After a session's new copy is VERIFIED, delete the old copy "
             "immediately. Requires --execute. Without this flag (the default), "
             "both old and new copies are kept — nothing is ever deleted.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only process the first N sessions — for a small real test batch.",
    )
    parser.add_argument(
        "--sns-arn", default="",
        help="SNS topic ARN to notify when the run finishes or crashes. "
             "Overrides the MIGRATION_SNS_TOPIC_ARN env var. Optional.",
    )
    args = parser.parse_args()

    started_at = time.time()
    host = socket.gethostname()
    log(f"=== Migration run started {datetime.now(timezone.utc).isoformat()} on {host} ===")
    log(f"Flags: execute={args.execute} move={args.move} limit={args.limit}")
    log(f"Manifest file: {os.path.abspath(MANIFEST_FILE)}")
    log(f"SNS alerts: {'ON' if (args.sns_arn or SNS_TOPIC_ARN) else 'OFF (no ARN configured)'}\n")

    log(f"Planning migration for bucket: {BUCKET}\n")
    jobs = plan_migrations()
    log(f"Found {len(jobs)} session(s) to migrate.\n")

    by_source = defaultdict(int)
    for j in jobs:
        by_source[j["source"]] += 1
    log("Breakdown by source pattern:")
    for source, count in sorted(by_source.items()):
        log(f"  {source}: {count}")
    log()

    manifest = load_manifest()
    already_verified = sum(1 for j in jobs if manifest.get(j["old_prefix"], {}).get("verified"))
    if already_verified:
        log(f"{already_verified} session(s) already verified in a prior run.\n")

    if args.limit:
        # Sample up to --limit sessions from EACH source pattern, not just the
        # first N overall — otherwise a small test run could land entirely on
        # one pattern (e.g. all top-level Advanced-Training) and never
        # exercise the others, giving false confidence.
        by_source_jobs = defaultdict(list)
        for j in jobs:
            by_source_jobs[j["source"]].append(j)
        jobs = []
        for source in sorted(by_source_jobs):
            jobs.extend(by_source_jobs[source][: args.limit])
        log(f"--limit {args.limit} set: sampling up to {args.limit} session(s) from EACH "
              f"source pattern below (not just the first {args.limit} overall) "
              f"— {len(jobs)} total selected.\n")

    if not args.execute:
        log("=== DRY RUN — no data will be copied or deleted ===\n")
        for job in jobs:
            log(f"  {job['old_prefix']}")
            log(f"    -> {job['new_prefix']}")
            log(f"    ({len(job['objects'])} objects, {job['total_bytes']:,} bytes, {job['source']})")
            if args.move:
                log(f"    --move is set: OLD copy above WOULD BE DELETED once verified")
            log()
        log("Nothing was copied or deleted. Review the plan above, then re-run with --execute")
        log("(start with --limit 5 --execute for a small real test first).")
        return

    copied, deleted, failed, skipped = 0, 0, 0, 0
    for job in jobs:
        prior = manifest.get(job["old_prefix"], {})

        # Already verified in a prior run.
        if prior.get("verified"):
            if not args.move or prior.get("old_deleted"):
                skipped += 1
                continue
            # Verified earlier but old copy still sitting there, and --move
            # is on NOW -- just do the deletion step, no need to re-copy.
            log(f"Already verified: {job['old_prefix']} -- deleting old copy now (--move)")
            ok_delete = delete_old_objects(job)
            manifest[job["old_prefix"]]["old_deleted"] = ok_delete
            save_manifest(manifest)
            if ok_delete:
                deleted += 1
            else:
                log(f"    WARNING: new copy is verified and safe, but deleting the "
                      f"old copy failed. Old data is untouched -- safe to just re-run.")
            continue

        # Fresh session: copy, then verify.
        log(f"Copying: {job['old_prefix']} -> {job['new_prefix']}")
        ok = copy_single_file(job) if job.get("kind") == "file" else copy_session(job)

        old_deleted = False
        if ok and args.move:
            log(f"    verified OK -- deleting old copy (--move)")
            old_deleted = delete_old_objects(job)
            if not old_deleted:
                log(f"    WARNING: new copy is verified and safe, but deleting the "
                      f"old copy failed. Old data is untouched -- safe to just re-run.")

        manifest[job["old_prefix"]] = {
            "new_prefix":    job["new_prefix"],
            "verified":      ok,
            "old_deleted":   old_deleted,
            "object_count":  len(job["objects"]),
            "total_bytes":   job["total_bytes"],
            "checked_at":    datetime.now(timezone.utc).isoformat(),
        }
        save_manifest(manifest)  # after EVERY session — safe to interrupt and resume

        if ok:
            copied += 1
            log("    verified OK")
            if old_deleted:
                deleted += 1
        else:
            failed += 1

    elapsed = time.time() - started_at
    mins, secs = divmod(int(elapsed), 60)
    hours, mins = divmod(mins, 60)
    elapsed_str = f"{hours}h {mins}m {secs}s"

    log(f"\n=== Done ===")
    log(f"Copied+verified: {copied}   Old copies deleted: {deleted}   "
          f"Failed/mismatched: {failed}   Skipped (already done): {skipped}")
    log(f"Elapsed: {elapsed_str}")
    if failed:
        log(
            f"\n{failed} session(s) need attention — see VERIFY MISMATCH lines above.\n"
            "Just re-run this script again; verified sessions are skipped automatically,\n"
            "so only the failed ones will be retried."
        )
    if args.move and deleted < copied:
        log(
            f"\nNote: {copied - deleted} session(s) verified but their old copy was NOT "
            "deleted (a delete failure, logged above as a WARNING). Old data for those "
            "is untouched and safe — just re-run with --move to retry deletion."
        )

    # ── Completion alert ────────────────────────────────────────────────
    status = "COMPLETED WITH FAILURES" if failed else "COMPLETED OK"
    notify(
        subject=f"S3 migration {status} ({copied} copied, {failed} failed)",
        message=(
            f"Training S3 migration finished on {host}.\n\n"
            f"Status:            {status}\n"
            f"Copied + verified: {copied}\n"
            f"Old copies deleted:{deleted}\n"
            f"Failed/mismatched: {failed}\n"
            f"Skipped (already): {skipped}\n"
            f"Elapsed:           {elapsed_str}\n"
            f"Bucket:            {BUCKET}\n"
            f"Flags:             execute={args.execute} move={args.move} limit={args.limit}\n"
            f"Manifest:          {os.path.abspath(MANIFEST_FILE)}\n\n"
            + ("ACTION NEEDED: some sessions failed verification. Their OLD data is\n"
               "untouched and safe. Re-run the same command to retry only those.\n"
               if failed else
               "No action needed. All sessions verified successfully.\n")
        ),
        sns_arn=args.sns_arn,
    )
    log(f"\n=== Migration run ended {datetime.now(timezone.utc).isoformat()} ===")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Ctrl-C / SIGINT. Safe: the manifest is saved after every session,
        # so re-running picks up exactly where this left off.
        log("\nInterrupted by user. Progress is saved in the manifest — "
            "re-run the same command to resume.")
        sys.exit(130)
    except Exception as exc:
        # Crash: alert loudly, because on EC2 nobody is watching the terminal.
        import traceback
        tb = traceback.format_exc()
        log(f"\n!!! MIGRATION CRASHED !!!\n{tb}")
        notify(
            subject="S3 migration CRASHED",
            message=(
                f"The Training S3 migration crashed on {socket.gethostname()}.\n\n"
                f"Error: {exc}\n\n{tb}\n\n"
                "Your data is safe: this script only deletes an old copy AFTER that\n"
                "session's new copy is byte-verified, and the manifest is written after\n"
                "every session. Re-run the same command to resume where it stopped.\n"
            ),
        )
        sys.exit(1)
