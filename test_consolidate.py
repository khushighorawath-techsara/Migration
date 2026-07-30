import os
os.environ["AWS_DEFAULT_REGION"]="us-east-1"
import consolidate_trainer_folders as C

FAKE = [
 ("Training/Resume-Based/ved_sharma/2026/July/Cand_A/2026-07-13/Time-7PM/99254436941/MP4/v.mp4", 5000),
 ("Training/Resume-Based/ved_sharma/2026/July/Cand_A/2026-07-13/session-result-99254436941.json", 900),
 # must NOT be touched — different trainer
 ("Training/Resume-Based/Ved_Sharma/2026/July/Cand_B/2026-07-20/Time-1PM/94000000000/MP4/v.mp4", 400),
 ("Training/Advanced/dev_purohit/2026/June/Grp/2026-06-10/Time-2AM/41067/MP4/v.mp4", 700),
]
C.list_all_objects = lambda p: ((k,s) for k,s in FAKE if k.startswith(p))

pl = C.plan_merge("Resume-Based","ved_sharma","Ved_Sharma")
print(f"\n{pl['pair']} -> {len(pl['items'])} objects")
for i in pl["items"]:
    print(f"  {i['src']}\n    -> {i['dst']}")
    assert i["dst"].startswith("Training/Resume-Based/Ved_Sharma/")
    assert "/ved_sharma/" not in i["dst"]
assert len(pl["items"]) == 2, "should only pick up the 2 lowercase objects"

pl2 = C.plan_merge("Advanced","dev_purohit","Dev_Purohit")
assert len(pl2["items"]) == 1
assert pl2["items"][0]["dst"] == "Training/Advanced/Dev_Purohit/2026/June/Grp/2026-06-10/Time-2AM/41067/MP4/v.mp4"
print(f"\n{pl2['pair']} -> {len(pl2['items'])} object  OK")

print("\n" + "="*60)
print("PASSED:")
print("  - only lowercase objects selected")
print("  - capitalized folder's own files left alone")
print("  - inner path preserved exactly, only trainer segment changed")
print("="*60)
