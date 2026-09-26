#!/usr/bin/env python3
"""
Finds Interview-Success meeting folders that hold anything beyond the normal
recording files -- analysis output, temp files, or any stray folder.

READ-ONLY. Lists S3 and prints. Nothing is created, moved or deleted.

EXPECTED inside a meeting folder:
    MP4/  M4A/  TRANSCRIPT/  CHAT/  participants.json

Anything else is reported: result.json, report.html, analysis-video.mp4,
proof/, training-temp.json, cc/, or a folder nobody has seen before.
Date-level session-result-*.json is reported separately -- it sits OUTSIDE
the meeting folder, one level up, which is how the first migration missed it.

Run on the migration EC2:
    export AWS_DEFAULT_REGION=us-east-1
    python3 find_analysis_folders.py                    # summary + samples
    python3 find_analysis_folders.py --all              # every affected prefix
    python3 find_analysis_folders.py --prefix Training/ # scan elsewhere
"""

import argparse
import re
from collections import defaultdict

import boto3

BUCKET = "zoom-automation-bucket"

EXPECTED_DIRS = {"MP4", "M4A", "TRANSCRIPT", "CHAT"}
EXPECTED_FILES = {"participants.json"}

MEETING_RE = re.compile(r"/(\d{9,11})/")
SESSION_RESULT_RE = re.compile(r"/session-result-\d{9,11}\.json$")

s3 = boto3.client("s3")


def split_at_meeting(key):
    """'A/B/94551815856/MP4/v.mp4' -> ('A/B/94551815856', 'MP4/v.mp4').

    Uses the LAST meeting-id-shaped segment, so a numeric folder earlier in
    the path cannot hijack the split. Returns (None, None) if there is none.
    """
    padded = "/" + key
    matches = list(MEETING_RE.finditer(padded))
    if not matches:
        return None, None
    end = matches[-1].end() - 1          # index in `key`, just past ".../{mid}/"
    return key[:end - 1], key[end:]      # prefix without trailing slash, remainder


def list_keys(prefix):
    pager = s3.get_paginator("list_objects_v2")
    for page in pager.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="Interview-Success/")
    ap.add_argument("--all", action="store_true", help="print every prefix, not just samples")
    ap.add_argument("--samples", type=int, default=5)
    args = ap.parse_args()

    extras = defaultdict(set)       # meeting prefix -> unexpected entries
    by_kind = defaultdict(list)     # entry name    -> meeting prefixes
    date_level = []
    meetings = set()
    total = 0

    for key, _size in list_keys(args.prefix):
        total += 1

        if SESSION_RESULT_RE.search("/" + key):
            date_level.append(key)
            continue

        meeting_prefix, rest = split_at_meeting(key)
        if meeting_prefix is None:
            continue

        meetings.add(meeting_prefix)
        if not rest:
            continue

        head, _, tail = rest.partition("/")
        if tail:                                   # it is a directory
            if head not in EXPECTED_DIRS:
                extras[meeting_prefix].add(head + "/")
        else:                                      # it is a file at the top level
            if head not in EXPECTED_FILES:
                extras[meeting_prefix].add(head)

    for prefix, items in extras.items():
        for item in items:
            by_kind[item].append(prefix)

    print(f"\nscanned                    : s3://{BUCKET}/{args.prefix}")
    print(f"objects                    : {total:,}")
    print(f"meeting folders            : {len(meetings):,}")
    print(f"folders with extra content : {len(extras):,}\n")

    if not by_kind and not date_level:
        print("Nothing beyond the expected recording files. Clean.")
        return

    if by_kind:
        print("=" * 74)
        print("EXTRA ENTRIES, by what was found")
        print("=" * 74)
        for item, prefixes in sorted(by_kind.items(), key=lambda kv: -len(kv[1])):
            print(f"\n{item}   ({len(prefixes):,} meeting folder(s))")
            shown = prefixes if args.all else prefixes[: args.samples]
            for p in sorted(shown):
                print(f"    s3://{BUCKET}/{p}/")
            if not args.all and len(prefixes) > len(shown):
                print(f"    ... and {len(prefixes) - len(shown):,} more  (--all)")

    if date_level:
        print("\n" + "=" * 74)
        print(f"DATE-LEVEL session-result-*.json   ({len(date_level):,})")
        print("(one level ABOVE the meeting folder)")
        print("=" * 74)
        shown = date_level if args.all else date_level[: args.samples]
        for k in sorted(shown):
            print(f"    s3://{BUCKET}/{k}")
        if not args.all and len(date_level) > len(shown):
            print(f"    ... and {len(date_level) - len(shown):,} more  (--all)")

    if extras:
        sample = sorted(extras)[0]
        print("\n" + "=" * 74)
        print("FULL CONTENTS OF ONE AFFECTED FOLDER")
        print(f"s3://{BUCKET}/{sample}/")
        print("=" * 74)
        for key, size in list_keys(sample + "/"):
            print(f"    {key[len(sample) + 1:]:<55} {size:>13,} B")
    print()


if __name__ == "__main__":
    main()
