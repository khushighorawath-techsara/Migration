#!/usr/bin/env python3
"""
READ-ONLY check of every IR mock against S3 (and optionally Zoom). Writes only local CSV files.

On the s3-migration-runner EC2:
  source ~/venv/bin/activate
  python3 -u verify_on_ec2.py ir_mocks_all.csv analysed_files.csv          # S3 only
  python3 -u verify_on_ec2.py ir_mocks_all.csv analysed_files.csv --zoom --links   # + Zoom + existing shortlinks
Outputs: verify_report.csv (one row per mock), analysed_files_report.csv, layout.txt

Homes: IS session -> Interview-Success/Internal-Interview/ (IS department)
       IR session -> Training/Interview-Readiness/        (IR department, where the IR host's recordings already go)
"""
import sys, re, csv, json, time, base64, datetime as dt, collections, urllib.request, urllib.parse, urllib.error
import boto3
from concurrent.futures import ThreadPoolExecutor

BUCKET = "zoom-automation-bucket"
SKIP_TOP = ("shortlinks/", "velocity", "meeting-index/")
IS_HOME = "Interview-Success/Internal-Interview/"
IR_HOME = "Training/Interview-Readiness/"
MID_RE = re.compile(r"^\d{9,11}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def toks(s):
    s = (s or "").lower().replace("n/a", " ")
    return {t for t in re.split(r"[^a-z]+", s) if len(t) >= 3 and t not in ("nil", "not", "applicable", "none", "ms")}

def name_in(name, text):
    want, have = toks(name), toks(text)
    return bool(want) and len(want & have) >= min(2, len(want))

def parse_path(pref):
    """Folder fields, counted back from the meeting id: .../<Host>/<Year>/<Month>/<Candidate>/[<Company>/]<Date>/[<Round or Time>/]<MeetingID>/"""
    p = [x for x in pref.split("/") if x]
    out = {"dept": p[0] if p else "", "type": "", "host": "", "candidate": "", "company": "", "date": "", "round_or_time": ""}
    if len(p) > 1 and p[0] == "Interview-Success" and p[1] in ("Interview", "Internal-Interview"): out["type"] = p[1]
    elif len(p) > 1 and p[0] == "Training": out["type"] = p[1]
    di = next((i for i in range(len(p) - 1, -1, -1) if DATE_RE.match(p[i])), None)
    if di is not None:
        out["date"] = p[di]
        if di + 1 < len(p) - 1: out["round_or_time"] = p[di + 1]
        yi = next((i for i in range(di) if re.match(r"^\d{4}$", p[i])), None)
        if yi is not None:
            out["host"] = p[yi - 1] if yi >= 1 else ""
            mid_part = p[yi + 2:di]                     # after Year/Month
            if mid_part: out["candidate"] = mid_part[0]
            if len(mid_part) > 1: out["company"] = mid_part[1]
    return out

def index_bucket():
    s3 = boto3.client("s3")
    folders, n = {}, 0
    samples = {IS_HOME: [], IR_HOME: []}
    tops, shortlink_keys = collections.Counter(), []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for o in page.get("Contents", []):
            k = o["Key"]; n += 1
            for h in samples:
                if k.startswith(h) and len(samples[h]) < 60: samples[h].append(k)
            if k.startswith("shortlinks/") and k.endswith(".json"): shortlink_keys.append(k)
            if k.startswith(SKIP_TOP): continue
            tops["/".join(k.split("/")[:2])] += 1
            parts = k.split("/")
            for i in range(len(parts) - 1):
                if MID_RE.match(parts[i]):
                    pref = "/".join(parts[:i + 1]) + "/"
                    f = folders.get(pref)
                    if f is None:
                        f = folders[pref] = {"meeting_id": parts[i], "mp4": [], "analysis": 0, **parse_path(pref)}
                    rest = "/".join(parts[i + 1:]).lower()
                    if rest.endswith(".mp4") and "analysis" not in rest: f["mp4"].append(k)
                    if rest.startswith("analysis/"): f["analysis"] += 1
                    break
        if n % 100000 < 1000 and n > 1000: print(f"  listed {n} objects ...", flush=True)
    print(f"listed {n} objects, {len(folders)} meeting folders", flush=True)
    with open("layout.txt", "w") as fh:
        for h, ks in samples.items():
            c = sum(1 for p in folders if p.startswith(h))
            fh.write(f"=== {h}  meeting folders: {c}\n" + "\n".join(ks) + "\n\n")
            print(f"{h} meeting folders: {c}")
        fh.write("=== objects per top folder\n" + "\n".join(f"{n:8d}  {t}" for t, n in tops.most_common()) + "\n")
    return folders, shortlink_keys

def index_shortlinks(keys):
    """Existing shortlinks: mp4 key -> shortlink id. Read only."""
    s3, out = boto3.client("s3"), {}
    def one(k):
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
        except Exception:
            return k, []
        found = []
        def walk(v):
            if isinstance(v, dict): [walk(x) for x in v.values()]
            elif isinstance(v, list): [walk(x) for x in v]
            elif isinstance(v, str) and v.lower().endswith(".mp4"): found.append(v.split(BUCKET + "/")[-1].lstrip("/"))
        walk(d)
        return k, found
    print(f"reading {len(keys)} shortlinks ...", flush=True)
    with ThreadPoolExecutor(32) as ex:
        for k, found in ex.map(one, keys):
            for mp4 in found: out[urllib.parse.unquote(mp4)] = k.rsplit("/", 1)[-1][:-5]
    print(f"shortlinks pointing at an mp4: {len(out)}", flush=True)
    return out

def gap(a, b):
    try: return abs((dt.date.fromisoformat(a) - dt.date.fromisoformat(b)).days)
    except Exception: return 99

# ---------------- Zoom, read only ----------------
_tok = {"v": None, "exp": 0}
def zoom_token():
    if _tok["v"] and time.time() < _tok["exp"] - 60: return _tok["v"]
    sec = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId="zoom/general-oauth")["SecretString"])
    g = lambda *ks: next((sec[k] for k in ks if k in sec), None)
    acct = g("account_id", "ZOOM_ACCOUNT_ID", "accountId")
    cid, csec = g("client_id", "ZOOM_CLIENT_ID", "clientId"), g("client_secret", "ZOOM_CLIENT_SECRET", "clientSecret")
    req = urllib.request.Request("https://zoom.us/oauth/token?" + urllib.parse.urlencode(
        {"grant_type": "account_credentials", "account_id": acct}), method="POST",
        headers={"Authorization": "Basic " + base64.b64encode(f"{cid}:{csec}".encode()).decode()})
    d = json.load(urllib.request.urlopen(req, timeout=30))
    _tok.update(v=d["access_token"], exp=time.time() + int(d.get("expires_in", 3600)))
    return _tok["v"]

def zget(path):
    req = urllib.request.Request("https://api.zoom.us/v2" + path, headers={"Authorization": "Bearer " + zoom_token()})
    try: return json.load(urllib.request.urlopen(req, timeout=30))
    except urllib.error.HTTPError as e:
        if e.code == 429: time.sleep(2); return zget(path)
        return {"_error": e.code}

def zoom_check(mid, day, start):
    inst = zget(f"/past_meetings/{mid}/instances")
    if "_error" in inst: return f"zoom error {inst['_error']}", ""
    want = None
    if day and start:
        h, m = map(int, start.split(":")); want = dt.datetime.fromisoformat(day).replace(hour=h, minute=m)
    best = None
    for it in inst.get("meetings", []):
        t = dt.datetime.strptime(it["start_time"], "%Y-%m-%dT%H:%M:%SZ") + dt.timedelta(hours=5, minutes=30)
        g = abs((t - want).total_seconds()) / 60 if want else 0
        if want and g > 36 * 60: continue
        if best is None or g < best[0]: best = (g, it, t)
    if not best: return "no meeting near that date in Zoom", ""
    g, it, t = best
    uuid = it["uuid"]
    enc = urllib.parse.quote(urllib.parse.quote(uuid, safe=""), safe="") if uuid.startswith("/") or "//" in uuid \
        else urllib.parse.quote(uuid, safe="")
    rec = zget(f"/meetings/{enc}/recordings")
    files = rec.get("recording_files", []) if "_error" not in rec else []
    return ("recording in Zoom" if files else "meeting found, no recording in Zoom"), t.strftime("%Y-%m-%d %H:%M")

def fmt(lst):
    return " | ".join(f"{p} [mp4={len(f['mp4'])} analysis={'y' if f['analysis'] else 'n'}]" for p, f in lst[:3])

def main():
    mocks = list(csv.DictReader(open(sys.argv[1])))
    files = list(csv.DictReader(open(sys.argv[2]))) if len(sys.argv) > 2 and not sys.argv[2].startswith("--") else []
    use_zoom = "--zoom" in sys.argv
    folders, sl_keys = index_bucket()
    links = index_shortlinks(sl_keys) if "--links" in sys.argv else {}
    def link_of(lst):
        for p, f in lst:
            for k in f["mp4"]:
                if k in links: return links[k]
        return ""
    def where(lst, home):
        if not lst: return ""
        return "HOME" if lst[0][0].startswith(home) else (lst[0][1]["dept"] + "/" + lst[0][1]["type"]).rstrip("/")
    by_mid = collections.defaultdict(list)
    for p, f in folders.items(): by_mid[f["meeting_id"]].append((p, f))

    def find(mid, day):
        hits = by_mid.get(mid, []) if mid else []
        near = [(p, f) for p, f in hits if not day or not f["date"] or gap(f["date"], day) <= 1]
        return near, [x for x in hits if x not in near]

    rows, verdicts = [], collections.Counter()
    for m in mocks:
        r = dict(m); d = m["mock_date"]
        is_near, is_far = find(m["is_meeting_id"], d)
        ir_near, ir_far = find(m["ir_meeting_id"], d)
        is_name, ir_name = [], []
        if d and not is_near:
            is_name = [(p, f) for p, f in folders.items() if f["date"] and gap(f["date"], d) <= 1
                       and name_in(m["candidate"], f["candidate"]) and name_in(m["is_person"], f["host"])]
        if d and not ir_near:
            ir_name = [(p, f) for p, f in folders.items() if f["date"] and gap(f["date"], d) <= 1
                       and name_in(m["candidate"], f["candidate"]) and name_in(m["ir_person"], f["host"])]
        r.update(s3_is=fmt(is_near), s3_is_other_dates=fmt(is_far), s3_is_by_name=fmt(is_name),
                 s3_ir=fmt(ir_near), s3_ir_by_name=fmt(ir_name),
                 is_now_in=where(is_near or is_name, IS_HOME), ir_now_in=where(ir_near or ir_name, IR_HOME),
                 is_shortlink=link_of(is_near or is_name), ir_shortlink=link_of(ir_near or ir_name))
        # HOME = already in its department folder (IS -> Internal-Interview, IR -> Interview-Readiness)
        if "did not join" in m["flags"]: v = "NO_SHOW"
        elif is_near: v = "IS_IN_S3" if len(is_near) == 1 else "IS_IN_S3_SEVERAL"
        elif is_name: v = "IS_IN_S3_BY_NAME"
        elif m["is_meeting_id"]: v = "IS_NOT_IN_S3"
        else: v = "IS_NO_ID_NOT_FOUND"
        if use_zoom and m["is_meeting_id"] and v == "IS_NOT_IN_S3":
            z, t = zoom_check(m["is_meeting_id"], d, m["start_ist"]); r["zoom_is"] = f"{z} {t}".strip()
            if z == "recording in Zoom": v = "IS_ONLY_IN_ZOOM"
            time.sleep(0.2)
        ir = "IR_IN_S3" if ir_near else ("IR_BY_NAME" if ir_name else ("IR_NO_ID" if not m["ir_meeting_id"] else "IR_NOT_IN_S3"))
        if use_zoom and m["ir_meeting_id"] and not ir_near:
            z, t = zoom_check(m["ir_meeting_id"], d, m["start_ist"]); r["zoom_ir"] = f"{z} {t}".strip(); time.sleep(0.2)
            if ir == "IR_NOT_IN_S3" and z == "recording in Zoom": ir = "IR_ONLY_IN_ZOOM"
        r["ir_status"] = ir
        r["verdict"] = v; verdicts[v] += 1; verdicts["ir: " + r["ir_status"]] += 1
        if r["is_now_in"]: verdicts["is now in: " + r["is_now_in"]] += 1
        if r["ir_now_in"]: verdicts["ir now in: " + r["ir_now_in"]] += 1
        if links: verdicts["is has shortlink: " + ("yes" if r["is_shortlink"] else "no")] += 1
        if links: verdicts["ir has shortlink: " + ("yes" if r["ir_shortlink"] else "no")] += 1
        rows.append(r)
    cols = list(rows[0].keys())
    for c in ("zoom_is", "zoom_ir"):
        if c not in cols: cols.append(c)
    with open("verify_report.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols); w.writeheader(); w.writerows(rows)

    # analysed files: where is the analysed recording, and does it point at a mock?
    frows = []
    for f in files:
        near, far = find(f["meeting_id"], f["file_date"])
        r = dict(f); r["s3_folder"] = fmt(near or far)
        if near:
            pf = near[0][1]
            r.update(s3_host=pf["host"], s3_candidate=pf["candidate"], s3_date=pf["date"], s3_dept=pf["dept"] + "/" + pf["type"])
            if not f["mock_no"]:
                cands = [m["mock_no"] for m in mocks if m["mock_date"] and gap(m["mock_date"], pf["date"]) <= 1
                         and name_in(m["candidate"], pf["candidate"])]
                r["suggested_mock"] = ",".join(cands)
        frows.append(r)
    if frows:
        with open("analysed_files_report.csv", "w", newline="") as fh:
            keys = list(dict.fromkeys(k for r in frows for k in r))
            w = csv.DictWriter(fh, fieldnames=keys); w.writeheader(); w.writerows(frows)
        print(f"analysed files: {sum(1 for r in frows if r.get('s3_host'))} of {len(frows)} found in S3")
    print("\nVERDICTS:")
    for k, n in sorted(verdicts.items()): print(f"  {k:28s} {n}")
    print("\nwritten: verify_report.csv, analysed_files_report.csv, layout.txt")

if __name__ == "__main__":
    main()
