#!/usr/bin/env python3
"""
READ-ONLY step 2: prove each matched recording really is that practice mock.

For every mock (IS side and IR side) it gathers candidate folders:
  - the folders verify_on_ec2.py already found (by meeting id or by name), and
  - for sides with nothing found: every folder on the mock date (+/-1) whose host or candidate matches.
Then it reads participants.json in each candidate folder (who joined, when) so we can check
candidate, host and time, and whether the IS and IR sessions overlap.
The 20 analysed files not tied to a mock are read the same way.

On the EC2 (same folder as verify_on_ec2.py):
  python3 -u confirm_on_ec2.py verify_report.csv analysed_files_report.csv
Output: candidates.csv (one row per mock x side x folder). Writes nothing to S3/Zoom/Salesforce.
"""
import os, sys, re, csv, json, collections, datetime as dt
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
import boto3
from concurrent.futures import ThreadPoolExecutor
from verify_on_ec2 import BUCKET, index_bucket, gap

IST = dt.timedelta(hours=5, minutes=30)

def norm(s): return re.sub(r"[^a-z]", "", (s or "").lower())
STOP = {"nil", "not", "applicable", "none", "zoom", "user", "the", "and", "iphone", "android", "notetaker", "otter"}
def toks(s): return [t for t in re.split(r"[^a-z]+", (s or "").lower()) if len(t) >= 3 and t not in STOP]

def same(a, b):
    """Loose name match: a token (4+ letters) of one appears inside the other, either way."""
    na, nb = norm(a), norm(b)
    if not na or not nb: return False
    return any(len(t) >= 4 and t in nb for t in toks(a)) or any(len(t) >= 4 and t in na for t in toks(b))

def time_folder(path):
    m = re.search(r"/Time-(\d{1,2})-(\d{2})-(AM|PM)-IST/", path)
    if not m: return ""
    return f"{int(m.group(1)) % 12 + (12 if m.group(3) == 'PM' else 0):02d}:{m.group(2)}"

def split_folders(s):
    return [p.split(" [")[0].strip() for p in (s or "").split(" | ") if p.strip()]

def read_participants(s3, folder):
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=folder + "participants.json")["Body"].read())
    except Exception:
        return None
    lst = d if isinstance(d, list) else next((v for v in d.values() if isinstance(v, list)), []) if isinstance(d, dict) else []
    people, joins, leaves = [], [], []
    for p in lst:
        if not isinstance(p, dict): continue
        nm = p.get("name") or p.get("user_name") or p.get("display_name") or ""
        em = p.get("user_email") or p.get("email") or ""
        people.append(f"{nm}<{em}>" if em else nm)
        for k, bucket in (("join_time", joins), ("leave_time", leaves)):
            v = p.get(k)
            if v:
                try: bucket.append(dt.datetime.strptime(v[:19], "%Y-%m-%dT%H:%M:%S") + IST)
                except Exception: pass
    uniq = list(dict.fromkeys(x for x in people if x))
    return {"names": " ; ".join(uniq), "n": len(uniq),
            "first_join": min(joins).strftime("%Y-%m-%d %H:%M") if joins else "",
            "last_leave": max(leaves).strftime("%Y-%m-%d %H:%M") if leaves else ""}

def main():
    mocks = list(csv.DictReader(open(sys.argv[1])))
    files = list(csv.DictReader(open(sys.argv[2]))) if len(sys.argv) > 2 else []
    folders, _ = index_bucket()
    by_date = collections.defaultdict(list)
    for p, f in folders.items():
        if f["date"]: by_date[f["date"]].append(p)

    def near_dates(d):
        if not d: return []
        x = dt.date.fromisoformat(d)
        return [(x + dt.timedelta(days=k)).isoformat() for k in (-1, 0, 1)]

    jobs = []   # (mock_no, side, folder, how)
    for m in mocks:
        if "did not join" in m["flags"]: continue
        for side, person, roots in (("is", m["is_person"], ("Interview-Success/",)),
                                     ("ir", m["ir_person"], ("Training/", "Interview-Success/Internal-Interview/"))):
            found = [(p, "id") for p in split_folders(m[f"s3_{side}"])] + \
                    [(p, "name") for p in split_folders(m[f"s3_{side}_by_name"])]
            if side == "is":
                found += [(p, "id-other-date") for p in split_folders(m["s3_is_other_dates"])]
            if not found:
                for d in near_dates(m["mock_date"]):
                    for p in by_date.get(d, []):
                        f = folders[p]
                        if p.startswith(roots) and (same(m["candidate"], f["candidate"]) or same(person, f["host"])):
                            found.append((p, "search"))
            for p, how in dict.fromkeys(found):
                jobs.append((m["mock_no"], side, p, how))
    for f in files:
        if not f["mock_no"] or f["mock_no"].startswith("REVIEW"):
            for p in split_folders(f["s3_folder"]):
                jobs.append((f["file"], "analysed", p, "analysed-file"))

    uniq = list(dict.fromkeys(j[2] for j in jobs))
    print(f"reading participants.json in {len(uniq)} folders ...", flush=True)
    s3 = boto3.client("s3")
    with ThreadPoolExecutor(32) as ex:
        parts = dict(zip(uniq, ex.map(lambda p: read_participants(s3, p), uniq)))

    rows = []
    for mock_no, side, p, how in jobs:
        f = folders.get(p, {})
        pt = parts.get(p) or {}
        rows.append({"mock_no": mock_no, "side": side, "how": how, "folder": p,
                     "f_host": f.get("host", ""), "f_candidate": f.get("candidate", ""), "f_date": f.get("date", ""),
                     "f_time": time_folder(p), "mp4": len(f.get("mp4", [])), "analysis_files": f.get("analysis", 0),
                     "has_participants": "y" if parts.get(p) else "n", "participants": pt.get("names", ""),
                     "n_participants": pt.get("n", ""), "first_join_ist": pt.get("first_join", ""),
                     "last_leave_ist": pt.get("last_leave", "")})
    with open("candidates.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    c = collections.Counter((r["side"], r["how"]) for r in rows)
    print("\ncandidate folders:")
    for k, n in sorted(c.items()): print(f"  {k[0]:9s} {k[1]:14s} {n}")
    print(f"with participants.json: {sum(1 for r in rows if r['has_participants'] == 'y')} of {len(rows)}")
    print("\nwritten: candidates.csv")

if __name__ == "__main__":
    main()
