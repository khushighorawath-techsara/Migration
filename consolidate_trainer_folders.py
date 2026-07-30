#!/usr/bin/env python3
"""
Merges lowercase trainer folders into their capitalized twins.

WHY
  find_canonical_folder() used to scan Training/ looking for trainer names, but
  after the Type restructure that level holds Advanced/, Resume-Based/, Other/,
  Interview-Readiness/. Every trainer was logged as `new_entry`, so whatever
  casing the source produced got used verbatim -- creating both
  `ved_sharma/` and `Ved_Sharma/`. The Lambda is fixed (logs now show
  `case_corrected`), so this is a one-time cleanup of what already exists.

HOW IT DECIDES
  A hardcoded MERGES list, below. Nothing is discovered or guessed -- only the
  exact pairs listed are touched, and only ever lowercase -> Capitalized.

WHY PER-OBJECT VERIFICATION (different from the earlier migrations)
  The earlier scripts compared whole-prefix object counts and byte totals.
  That does not work here: the destination folder ALREADY contains hundreds or
  thousands of unrelated objects, so a prefix total tells us nothing about
  whether OUR objects arrived. Each object is therefore copied and then
  head_object'd individually, comparing exact byte size.

COLLISION HANDLING
  If a destination key already exists:
    - same size  -> treat as already-merged; the source copy is safe to delete
    - diff size  -> COLLISION. Neither side is touched, and it is reported.
  This is the case where the same meeting somehow exists under both casings
  with different content. Blindly overwriting could destroy the newer file.

SAFETY MODEL  -- same as the earlier migrations
  - Copy -> verify -> delete, in that order, per object.
  - Without --move nothing is ever deleted.
  - DRY RUN BY DEFAULT. Nothing happens until you pass --execute.
  - Own manifest file, so it cannot interfere with the earlier migrations.
  - Resumable: re-running skips whatever is already done.

USAGE
  python3 consolidate_trainer_folders.py
  python3 consolidate_trainer_folders.py --limit 1 --execute --move
  python3 consolidate_trainer_folders.py --execute --move
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
    from botocore.exceptions import ClientError
except ImportError:
    print("boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)

BUCKET = "zoom-automation-bucket"

# (type folder, lowercase trainer, Capitalized trainer)
MERGES = [
    ("Advanced",            "dev_purohit",      "Dev_Purohit"),
    ("Advanced",            "maithilya_patle",  "Maithilya_Patle"),
    ("Advanced",            "sneha_chaudhary",  "Sneha_Chaudhary"),
    ("Resume-Based",        "divya_prajapati",  "Divya_Prajapati"),
    ("Resume-Based",        "ronak_chaudhary",  "Ronak_Chaudhary"),
    ("Resume-Based",        "ved_sharma",       "Ved_Sharma"),
    ("Interview-Readiness", "mudit_singh",      "Mudit_Singh"),
    ("Interview-Readiness", "naghma_akhtar",    "Naghma_Akhtar"),
    ("Interview-Readiness", "twinkal_gandhi",   "Twinkal_Gandhi"),
    ("Interview-Readiness", "ved_sharma",       "Ved_Sharma"),
]

# Every type folder that can contain trainer folders. --auto scans all of them.
TYPE_FOLDERS = ["Advanced", "Resume-Based", "Interview-Readiness",
                "Retraining", "Other"]

MANIFEST_FILE = os.environ.get("CONSOLIDATE_MANIFEST_PATH", "consolidate_manifest.json")
SNS_TOPIC_ARN = os.environ.get("MIGRATION_SNS_TOPIC_ARN", "").strip()
AWS_REGION    = os.environ.get("AWS_REGION", "us-east-1")

s3 = boto3.client("s3")


def log(msg: str = ""):
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


def list_all_objects(prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def head_size(key: str):
    """Byte size of an object, or None if it does not exist."""
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def list_trainer_folders(type_folder: str):
    """Immediate child folder names under Training/<type>/ (not recursive)."""
    prefix = f"Training/{type_folder}/"
    names = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"][len(prefix):].rstrip("/")
            if name:
                names.append(name)
    return names


def detect_merges():
    """
    Find trainer folders that differ ONLY by case, across every type folder.

    Safe by construction: two genuinely different trainers cannot have names
    that are identical once lowercased, so a collision here is always the same
    person. Anything with no case-variant twin is left alone -- this MERGES
    duplicates, it never RENAMES a folder that stands on its own.

    Winner = the one containing uppercase. If a group has zero or more than one
    such variant, it is ambiguous and skipped with a warning rather than guessed.
    """
    found, ambiguous = [], []
    for tf in TYPE_FOLDERS:
        try:
            names = list_trainer_folders(tf)
        except Exception as exc:
            log(f"  (could not list Training/{tf}/: {exc})")
            continue
        if not names:
            continue

        groups = defaultdict(list)
        for n in names:
            groups[n.lower()].append(n)

        for key, variants in sorted(groups.items()):
            if len(variants) < 2:
                continue
            capitalized = [v for v in variants if v != v.lower()]
            if len(capitalized) != 1:
                ambiguous.append((tf, variants))
                continue
            winner = capitalized[0]
            for loser in variants:
                if loser != winner:
                    found.append((tf, loser, winner))

    if ambiguous:
        log("\n  AMBIGUOUS — skipped, resolve by hand:")
        for tf, variants in ambiguous:
            log(f"    Training/{tf}/  ->  {variants}")
    return found


def plan_merge(type_folder: str, lower: str, upper: str):
    """Every object under the lowercase folder, with its destination key."""
    src_prefix = f"Training/{type_folder}/{lower}/"
    dst_prefix = f"Training/{type_folder}/{upper}/"

    items = []
    for key, size in list_all_objects(src_prefix):
        dst_key = dst_prefix + key[len(src_prefix):]
        items.append({"src": key, "dst": dst_key, "size": size})
    return {
        "pair":        f"{type_folder}/{lower} -> {type_folder}/{upper}",
        "src_prefix":  src_prefix,
        "dst_prefix":  dst_prefix,
        "items":       items,
        "total_bytes": sum(i["size"] for i in items),
    }


def process_object(item: dict) -> str:
    """
    Returns one of: 'copied' | 'already_present' | 'collision' | 'failed'
    Never deletes -- the caller does that, and only for verified objects.
    """
    src, dst, size = item["src"], item["dst"], item["size"]

    existing = head_size(dst)
    if existing is not None:
        if existing == size:
            return "already_present"
        log(f"    COLLISION: {dst}\n"
            f"      source {size} bytes vs destination {existing} bytes -- SKIPPED")
        return "collision"

    try:
        s3.copy_object(Bucket=BUCKET,
                       CopySource={"Bucket": BUCKET, "Key": src},
                       Key=dst)
    except Exception as exc:
        log(f"    COPY FAILED: {src} -> {dst}: {exc}")
        return "failed"

    verified = head_size(dst)
    if verified != size:
        log(f"    VERIFY MISMATCH: {dst} (expected {size}, got {verified})")
        return "failed"
    return "copied"


def delete_keys(keys: list) -> bool:
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
            log(f"    DELETE FAILED starting {batch[0]}: {exc}")
            return False
    return True


def load_manifest() -> dict:
    try:
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_manifest(m: dict):
    with open(MANIFEST_FILE, "w") as f:
        json.dump(m, f, indent=2)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true",
                   help="Actually copy. Without this: dry run, nothing touched.")
    p.add_argument("--move", action="store_true",
                   help="Delete the lowercase copy after every object verifies.")
    p.add_argument("--limit", type=int, default=None,
                   help="Only process the first N merge pairs.")
    p.add_argument("--auto", action="store_true",
                   help="Detect case-only duplicate trainer folders across ALL "
                        "type folders instead of using the hardcoded MERGES list. "
                        "Use this after any reclassification, which copies paths "
                        "verbatim and can therefore carry casing variants into a "
                        "new type folder.")
    args = p.parse_args()

    started = time.time()
    host = socket.gethostname()
    log(f"=== Consolidate run started {datetime.now(timezone.utc).isoformat()} on {host} ===")
    log(f"Flags:    execute={args.execute} move={args.move} limit={args.limit}")
    log(f"Manifest: {os.path.abspath(MANIFEST_FILE)}")
    log(f"SNS:      {'ON' if SNS_TOPIC_ARN else 'OFF'}\n")

    if args.auto:
        log("Auto-detecting case-only duplicate trainer folders...")
        merges = detect_merges()
        if not merges:
            log("\nNo casing duplicates found in any type folder. Nothing to do.")
            return
        log(f"\nDetected {len(merges)} duplicate pair(s):")
        for t, lo, up in merges:
            log(f"  Training/{t}/{lo}  ->  Training/{t}/{up}")
        log("")
    else:
        merges = MERGES

    log("Planning...")
    plans = [plan_merge(t, lo, up) for t, lo, up in merges]
    plans = [pl for pl in plans if pl["items"]]

    total_objects = sum(len(pl["items"]) for pl in plans)
    total_bytes   = sum(pl["total_bytes"] for pl in plans)
    log(f"{len(plans)} merge pair(s), {total_objects} object(s), {total_bytes:,} bytes\n")

    for pl in plans:
        log(f"  {pl['pair']}: {len(pl['items'])} objects, {pl['total_bytes']:,} bytes")
    log("")

    manifest = load_manifest()
    if args.limit:
        plans = plans[:args.limit]
        log(f"--limit {args.limit}: processing only the first {args.limit} pair(s).\n")

    if not args.execute:
        log("=== DRY RUN — nothing will be copied or deleted ===\n")
        for pl in plans:
            log(f"  {pl['pair']}")
            for it in pl["items"][:3]:
                log(f"    {it['src']}\n      -> {it['dst']}")
            if len(pl["items"]) > 3:
                log(f"    ... and {len(pl['items']) - 3} more")
            if args.move:
                log("    --move set: the lowercase copies WOULD BE DELETED once verified")
            log("")
        log("Nothing changed. Review, then re-run with --execute\n"
            "(start with --limit 1 --execute --move).")
        return

    tot_copied = tot_present = tot_collision = tot_failed = tot_deleted = 0

    for pl in plans:
        if manifest.get(pl["pair"], {}).get("done"):
            log(f"Skipping (already done): {pl['pair']}")
            continue

        log(f"\nMerging: {pl['pair']}  ({len(pl['items'])} objects)")
        copied = present = collision = failed = 0
        deletable = []

        for it in pl["items"]:
            outcome = process_object(it)
            if outcome == "copied":
                copied += 1
                deletable.append(it["src"])
            elif outcome == "already_present":
                present += 1
                deletable.append(it["src"])   # identical copy already at destination
            elif outcome == "collision":
                collision += 1
            else:
                failed += 1

        log(f"    copied={copied} already_present={present} "
            f"collision={collision} failed={failed}")

        deleted_ok = False
        if args.move and not failed and not collision and deletable:
            deleted_ok = delete_keys(deletable)
            if deleted_ok:
                tot_deleted += len(deletable)
                log(f"    lowercase folder emptied ({len(deletable)} objects deleted)")
            else:
                log("    WARNING: copies are verified and safe, but deleting the "
                    "lowercase originals failed. Nothing lost -- safe to re-run.")
        elif args.move and (failed or collision):
            log("    NOT deleting: this pair had failures or collisions. "
                "Both copies are intact -- resolve, then re-run.")

        tot_copied += copied; tot_present += present
        tot_collision += collision; tot_failed += failed

        manifest[pl["pair"]] = {
            "done":          (failed == 0 and collision == 0
                              and (deleted_ok or not args.move)),
            "objects":       len(pl["items"]),
            "copied":        copied,
            "already_present": present,
            "collision":     collision,
            "failed":        failed,
            "deleted":       deleted_ok,
            "checked_at":    datetime.now(timezone.utc).isoformat(),
        }
        save_manifest(manifest)

    elapsed = int(time.time() - started)
    mins, secs = divmod(elapsed, 60)

    log(f"\n=== Done ===")
    log(f"Copied: {tot_copied}   Already present: {tot_present}   "
        f"Collisions: {tot_collision}   Failed: {tot_failed}   "
        f"Lowercase objects deleted: {tot_deleted}")
    log(f"Elapsed: {mins}m {secs}s")

    if tot_collision:
        log(f"\n{tot_collision} collision(s): the same key exists under BOTH casings "
            f"with DIFFERENT content.\nNeither copy was touched. Inspect those "
            f"manually before re-running.")
    if tot_failed:
        log(f"\n{tot_failed} object(s) failed. Nothing was deleted for those pairs. "
            f"Safe to re-run.")

    status = ("COMPLETED WITH ISSUES" if (tot_failed or tot_collision)
              else "COMPLETED OK")
    notify(f"Trainer folder consolidation {status}",
           f"Finished on {host}.\n\nStatus: {status}\nCopied: {tot_copied}\n"
           f"Already present: {tot_present}\nCollisions: {tot_collision}\n"
           f"Failed: {tot_failed}\nDeleted: {tot_deleted}\n"
           f"Elapsed: {mins}m {secs}s\nManifest: {os.path.abspath(MANIFEST_FILE)}\n")
    log(f"\n=== Consolidate run ended {datetime.now(timezone.utc).isoformat()} ===")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\nInterrupted. Progress saved -- re-run to resume.")
        sys.exit(130)
    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        log(f"\n!!! CONSOLIDATE CRASHED !!!\n{tb}")
        notify("Trainer folder consolidation CRASHED",
               f"Crashed on {socket.gethostname()}.\n\n{exc}\n\n{tb}\n\n"
               "Data is safe: originals are only deleted after every object in "
               "that pair verifies.\n")
        sys.exit(1)