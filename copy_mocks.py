#!/usr/bin/env python3
"""
IR mock backfill: copy each mock's IS recording into Interview-Success/Internal-Interview/.

  python3 -u copy_mocks.py copy_plan.csv                     # DRY RUN (default): checks only, writes nothing to S3
  python3 -u copy_mocks.py copy_plan.csv --links --only O25  # make ONE missing IR shortlink, open it to test
  python3 -u copy_mocks.py copy_plan.csv --links             # make the rest (safe before the SF load)
  python3 -u copy_mocks.py copy_plan.csv --run --sf-loaded   # real copy; only after the Salesforce load is done

Dry run checks every row and writes copy_check.csv:
  OK              safe to copy
  ALREADY_COPIED  target already complete (re-runs skip it)
  HOLD_CLASH      same candidate has a real-company interview folder overlapping in time -> not copied
  OK_OWN_LINK     two of our mocks share one meeting id: copied, and this script makes the shortlink itself
                  (those Sessions are loaded without the meeting id, so the linker can't mix them up)
  HOLD_REUSED_ID  the meeting id also appears in recordings on OTHER days (personal room) -> not copied,
                  because the linker would put the link on every record with that id
  MISSING_SOURCE  source folder has no mp4
  ZOOM_FOUND / ZOOM_NOT_FOUND   for Zoom pulls
  IR_LINK_NEEDED  IR recording without a shortlink (handled in a later step)
It also saves shortlink_samples.json (3 existing shortlinks, to mirror their format later).

Real run (--run --sf-loaded): copies OK rows only (and pulls ZOOM_FOUND rows). Originals are never touched,
analysis/ folders are never copied, non-mp4 files go first and the mp4 last, so the linker sees a complete folder.
Writes copy_log.csv. Safe to re-run: finished targets are skipped.
"""
import os, sys, re, csv, json, time, datetime as dt, collections, urllib.request, urllib.parse
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
import boto3
from concurrent.futures import ThreadPoolExecutor
from verify_on_ec2 import BUCKET, index_bucket, zget, zoom_token

IST = dt.timedelta(hours=5, minutes=30)
MONTH = ["January","February","March","April","May","June","July","August","September","October","November","December"]
s3 = boto3.client("s3")

# ---------- names ----------
STOP = {"nil","not","applicable","none","zoom","user","the","and","iphone","android","notetaker","otter"}
COMMON = {"kumar","reddy","venkata","venkat","naga","krishna","raju","patel","singh","shah","sharma","mohammed","mohammad",
          "mohd","chandra","lakshmi","priya","sri","sree","shri","devi","rani","babu","naidu","prasad","gupta","jain","khan","yadav","verma"}
def norm(s): return re.sub(r"[^a-z]", "", (s or "").lower())
def toks(s): return [t for t in re.split(r"[^a-z]+", (s or "").lower()) if len(t) >= 3 and t not in STOP]
def same(a, b):
    na, nb = norm(a), norm(b)
    if not na or not nb: return False
    def hits(x, other):
        h = [t for t in toks(x) if len(t) >= 4 and t in other]
        return any(t not in COMMON for t in h) or len(h) >= 2
    return hits(a, nb) or hits(b, na)

def _parts(s): return [t for t in re.split(r"[^a-z]+", (s or "").lower()) if len(t) >= 3 and t not in STOP | {"na"}]
def strong_same(a, b):
    """Same person for the clash check: 2 shared name parts, one name inside the other, or the same surname (6+ letters)."""
    ta, tb = _parts(a), _parts(b)
    if not ta or not tb: return False
    if len(set(ta) & set(tb)) >= 2: return True
    na, nb = "".join(ta), "".join(tb)
    if len(min(na, nb, key=len)) >= 8 and (na in nb or nb in na): return True
    return any(len(x) >= 6 and x in set(tb) for x in (ta[-1],)) or any(len(x) >= 6 and x in set(ta) for x in (tb[-1],))

def actual_interview_folder(p):
    s = p.split("/")
    if len(s) > 1 and s[1] in ("Training", "Internal-Interview"): return False      # IS-team internal sessions
    if "Unknown_Company" in p or "/Mock/" in p or "/Time-" in p: return False         # no Interview record behind it
    return p.startswith("Interview-Success/")

def time_folder(t):
    h, m = t.hour, t.minute
    return f"Time-{h % 12 or 12}-{m:02d}-{'AM' if h < 12 else 'PM'}-IST"

def shift_date(t):            # folders use the shift date: 00:00-05:59 IST belongs to the previous day
    return (t - dt.timedelta(days=1)).date() if t.hour < 6 else t.date()

def participants(folder):
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=folder + "participants.json")["Body"].read())
    except Exception:
        return None
    lst = d if isinstance(d, list) else next((v for v in d.values() if isinstance(v, list)), []) if isinstance(d, dict) else []
    names, js, ls = [], [], []
    for p in lst:
        if not isinstance(p, dict): continue
        names.append(p.get("name") or p.get("user_name") or "")
        for k, acc in (("join_time", js), ("leave_time", ls)):
            if p.get(k):
                try: acc.append(dt.datetime.strptime(p[k][:19], "%Y-%m-%dT%H:%M:%S") + IST)
                except Exception: pass
    return {"names": names, "start": min(js) if js else None, "end": max(ls) if ls else None}

def list_keys(prefix):
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return out

def mock_dt(row):
    if not row["mock_date"] or not re.match(r"\d\d:\d\d", row["start_ist"] or ""): return None
    return dt.datetime.fromisoformat(row["mock_date"]).replace(hour=int(row["start_ist"][:2]), minute=int(row["start_ist"][3:5]))

# ---------- Zoom (read; download only in --run) ----------
def zoom_find(mid, when):
    inst = zget(f"/past_meetings/{mid}/instances")
    if "_error" in inst or not when: return None
    best = None
    for it in inst.get("meetings", []):
        t = dt.datetime.strptime(it["start_time"], "%Y-%m-%dT%H:%M:%SZ") + IST
        g = min(abs((t - w).total_seconds()) for w in ([when] + ([when + dt.timedelta(days=1)] if when.hour < 6 else []))) / 60
        if g <= 90 and (best is None or g < best[0]): best = (g, it, t)
    if not best: return None
    uuid = best[1]["uuid"]
    enc = urllib.parse.quote(urllib.parse.quote(uuid, safe=""), safe="") if uuid.startswith("/") or "//" in uuid else urllib.parse.quote(uuid, safe="")
    rec = zget(f"/meetings/{enc}/recordings")
    files = [f for f in rec.get("recording_files", []) if f.get("download_url")] if "_error" not in rec else []
    return {"start": best[2], "files": files} if files else None

BASE_URL = os.environ.get("DOWNLOAD_BASE_URL", "https://yqygg7wc5g.execute-api.us-east-1.amazonaws.com/download")

def new_id():
    import secrets, string
    abc = string.ascii_letters + string.digits
    while True:
        i = "".join(secrets.choice(abc) for _ in range(8))
        try: s3.head_object(Bucket=BUCKET, Key=f"shortlinks/{i}.json")
        except Exception: return i

def make_shortlink(folder, department):
    """Same format (version 2) as the shortlinks the linker writes. Returns (url, mp4 key)."""
    mp4s = [(k, sz) for k, sz in list_keys(folder) if k.lower().endswith(".mp4") and "/analysis/" not in k]
    if not mp4s: return "", ""
    key = max(mp4s, key=lambda x: x[1])[0]
    p = [x for x in folder.split("/") if x]
    di = next((i for i in range(len(p) - 1, -1, -1) if re.match(r"^\d{4}-\d{2}-\d{2}$", p[i])), None)
    yi = next((i for i in range(len(p)) if re.match(r"^\d{4}$", p[i])), None)
    cand_raw = p[yi + 2] if yi is not None and yi + 2 < len(p) else ""
    host = p[yi - 1] if yi else ""
    body = {"version": 2, "bucket": BUCKET, "prefix": folder, "default_key": key,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(), "filename": key.rsplit("/", 1)[-1],
            "candidate": cand_raw.replace("_", " "), "candidate_raw": cand_raw, "company_raw": "",
            "trainer_raw": host if department == "Training" else "", "meeting_id": p[-1], "meeting_id_raw": p[-1],
            "interview_date": p[di] if di is not None else "", "round_raw": "", "department": department,
            "created_by": "ir-mock-backfill"}
    i = new_id()
    s3.put_object(Bucket=BUCKET, Key=f"shortlinks/{i}.json", Body=json.dumps(body).encode(), ContentType="application/json")
    return f"{BASE_URL}?id={i}", key

EXT = {"MP4": "mp4", "M4A": "m4a", "TRANSCRIPT": "vtt", "CC": "vtt", "CHAT": "txt", "TIMELINE": "json"}

def main():
    plan = list(csv.DictReader(open(sys.argv[1])))
    run = "--run" in sys.argv
    if "--links" in sys.argv:
        links = []
        only = sys.argv[sys.argv.index("--only") + 1] if "--only" in sys.argv else None   # test one mock first
        for r in plan:
            if r["action"] != "IR_LINK" or (only and r["mock_key"] != only): continue
            url, key = make_shortlink(r["source"], "Training" if r["source"].startswith("Training/") else "Interview-Success")
            links.append({"mock_key": r["mock_key"], "side": "IR", "url": url, "mp4_key": key})
            print(f"  {r['mock_key']} IR -> {url or 'NO MP4'}", flush=True)
        with open("ir_links.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["mock_key", "side", "url", "mp4_key"]); w.writeheader(); w.writerows(links)
        print(f"\nIR shortlinks made: {sum(1 for l in links if l['url'])} of {len(links)} -> ir_links.csv")
        return
    if run and "--sf-loaded" not in sys.argv:
        sys.exit("Real run needs --sf-loaded: the Salesforce Sessions must exist first, or the linker has nowhere to put the links.")
    print("MODE:", "REAL RUN" if run else "DRY RUN (nothing is written to S3)", flush=True)

    folders, sl_keys = index_bucket()
    by_mid = collections.defaultdict(list)
    for p, f in folders.items(): by_mid[f["meeting_id"]].append(p)
    by_date = collections.defaultdict(list)
    for p, f in folders.items():
        if f["date"]: by_date[f["date"]].append(p)
    plan_folders = {r["source"] for r in plan} | {r["target"] for r in plan}

    # three existing shortlinks, to copy their format later
    samples = []
    for k in sl_keys[:200]:
        try: samples.append({"key": k, "body": json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())})
        except Exception: pass
        if len(samples) >= 3: break
    json.dump(samples, open("shortlink_samples.json", "w"), indent=2, default=str)

    out, st = [], collections.Counter()
    for r in plan:
        res = dict(r); res["check"] = ""; res["detail"] = ""
        md = mock_dt(r)
        if r["action"] == "IR_LINK":
            res["check"] = "IR_LINK_NEEDED"
        elif r["action"] == "COPY_IS":
            src, tgt, mid = r["source"], r["target"], r["meeting_id"]
            f = folders.get(src)
            if not f or not f["mp4"]:
                res["check"] = "MISSING_SOURCE"
            else:
                tkeys = list_keys(tgt)
                skeys = [k for k, _ in list_keys(src) if "/analysis/" not in k]
                if tkeys and len(tkeys) >= len(skeys):
                    res["check"] = "ALREADY_COPIED"
                else:
                    others = [p for p in by_mid.get(mid, []) if p not in (src, tgt)
                              and not (folders[p]["date"] and abs((dt.date.fromisoformat(folders[p]["date"]) - dt.date.fromisoformat(f["date"])).days) <= 1)]
                    shared_in_plan = [x["mock_key"] for x in plan if x["action"] == "COPY_IS" and x["meeting_id"] == mid and x["mock_key"] != r["mock_key"]]
                    # a real-company interview folder for the same candidate, overlapping in time
                    clash = []
                    def win(pt, fallback):
                        st_ = (pt or {}).get("start") or fallback
                        if not st_: return None, None
                        return st_, (pt or {}).get("end") or st_ + dt.timedelta(minutes=60)
                    a1, a2 = win(participants(src), md)
                    d0 = f["date"]
                    for dd in [d0] + [str(dt.date.fromisoformat(d0) + dt.timedelta(days=k)) for k in (-1, 1)]:
                        for p in by_date.get(dd, []):
                            g = folders[p]
                            if p == src or not actual_interview_folder(p): continue
                            if not strong_same(r["candidate"], g["candidate"]): continue
                            b1, b2 = win(participants(p), None)
                            if a1 and b1 and min(a2, b2) > max(a1, b1):
                                clash.append(p)
                    if clash:
                        res["check"] = "HOLD_CLASH"; res["detail"] = " | ".join(clash[:3])
                    elif others:
                        res["check"] = "HOLD_REUSED_ID"; res["detail"] = f"{len(others)} other folder(s): " + " | ".join(others[:2])
                    elif shared_in_plan:
                        res["check"] = "OK_OWN_LINK"; res["detail"] = f"{len(skeys)} files; same meeting id as {','.join(shared_in_plan)}"
                    else:
                        res["check"] = "OK"; res["detail"] = f"{len(skeys)} files"
        elif r["action"] == "PULL_IS_FROM_ZOOM":
            z = zoom_find(r["meeting_id"], md) if md else None
            if not z:
                res["check"] = "ZOOM_NOT_FOUND"
            else:
                t = z["start"]; sd = shift_date(t)
                res["target"] = r["target"].replace("<YYYY>", str(sd.year)).replace("<Month>", MONTH[sd.month - 1]) \
                                           .replace("<date>", sd.isoformat()).replace("<Time>", time_folder(t))
                res["check"] = "ZOOM_FOUND"; res["detail"] = f"{len(z['files'])} files, started {t:%Y-%m-%d %H:%M} IST"
                res["_zoom"] = z
            time.sleep(0.2)
        st[res["check"]] += 1
        out.append(res)

    if run:
        log, own = [], []
        for res in out:
            if res["check"] in ("OK", "OK_OWN_LINK"):
                keys = [k for k, _ in list_keys(res["source"]) if "/analysis/" not in k]
                keys.sort(key=lambda k: k.lower().endswith(".mp4"))           # mp4 last
                for k in keys:
                    nk = res["target"] + k[len(res["source"]):]
                    s3.copy({"Bucket": BUCKET, "Key": k}, BUCKET, nk)
                    log.append({"mock_key": res["mock_key"], "from": k, "to": nk})
                print(f"  copied {res['mock_key']}: {len(keys)} files", flush=True)
                if res["check"] == "OK_OWN_LINK":
                    url, _ = make_shortlink(res["target"], "Interview-Success")
                    own.append({"mock_key": res["mock_key"], "side": "IS", "url": url})
            elif res["check"] == "ZOOM_FOUND":
                tok = zoom_token()
                files = sorted(res["_zoom"]["files"], key=lambda f: f.get("file_type") == "MP4")   # mp4 last
                for fz in files:
                    ft = fz.get("file_type", "OTHER")
                    nk = f"{res['target']}{ft}/{fz.get('id', 'file')}.{EXT.get(ft, (fz.get('file_extension') or 'bin').lower())}"
                    req = urllib.request.Request(fz["download_url"], headers={"Authorization": "Bearer " + tok})
                    with urllib.request.urlopen(req, timeout=600) as resp:
                        s3.upload_fileobj(resp, BUCKET, nk)
                    log.append({"mock_key": res["mock_key"], "from": "zoom:" + ft, "to": nk})
                print(f"  pulled {res['mock_key']} from Zoom: {len(files)} files", flush=True)
        with open("copy_log.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["mock_key", "from", "to"]); w.writeheader(); w.writerows(log)
        print(f"copied/pulled {len(log)} objects -> copy_log.csv")
        if own:
            with open("is_own_links.csv", "w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=["mock_key", "side", "url"]); w.writeheader(); w.writerows(own)
            print(f"own IS shortlinks: {len(own)} -> is_own_links.csv")

    for res in out: res.pop("_zoom", None)
    with open("copy_check.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)
    print("\nCHECKS:")
    for k, n in sorted(st.items()): print(f"  {k:16s} {n}")
    print("\nwritten: copy_check.csv, shortlink_samples.json" + (", copy_log.csv" if run else ""))

if __name__ == "__main__":
    main()
