"""Proves the reclassify script moves ONLY the sessions Salesforce named."""
import os
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
import reclassify_interview_readiness as R

# Pretend Salesforce returned these two. The third ID is NOT IR (control).
SF_IDS = {"99254436941", "94721979494"}
NOT_IR = "96185229447"
TARGET = "Training/Interview-Readiness/"

FAKE = [
    # IR session sitting in Resume-Based -> MOVE
    ("Training/Resume-Based/Ved_Sharma/2026/July/AVINASH_G/2026-07-13/Time-7-00-PM-IST/99254436941/MP4/v.mp4", 5000),
    ("Training/Resume-Based/Ved_Sharma/2026/July/AVINASH_G/2026-07-13/Time-7-00-PM-IST/99254436941/report.html", 200),
    # its date-level result -> MOVE too
    ("Training/Resume-Based/Ved_Sharma/2026/July/AVINASH_G/2026-07-13/session-result-99254436941.json", 900),
    # IR session that ended up in Other -> MOVE
    ("Training/Other/sneha_chaudhary/2026/July/Deepthi_M/2026-07-21/Time-3-00-PM-IST/94721979494/MP4/v.mp4", 700),
    # NOT IR, in Resume-Based -> MUST BE LEFT ALONE
    ("Training/Resume-Based/Anarkali_P/2026/April/Akhil_S/2026-04-17/Time-12-51-AM-IST/96185229447/MP4/v.mp4", 400),
    ("Training/Resume-Based/Anarkali_P/2026/April/Akhil_S/2026-04-17/session-result-96185229447.json", 100),
    # already correct -> MUST NOT be reprocessed
    ("Training/Interview-Readiness/Ved_Sharma/2026/July/X/2026-07-20/Time-1-00-PM-IST/99254436941/MP4/v.mp4", 300),
]

R.list_all_objects = lambda prefix: ((k, s) for k, s in FAKE if k.startswith(prefix))
jobs = R.plan_moves(SF_IDS, TARGET)

print(f"\nPlanned {len(jobs)} move(s):\n")
for j in jobs:
    print(f"  [{j['kind']} from {j['source_type']}]")
    print(f"    OLD: {j['old_prefix']}")
    print(f"    NEW: {j['new_prefix']}\n")

olds = [j["old_prefix"] for j in jobs]
assert len(jobs) == 3, f"expected 3 (2 sessions + 1 date file), got {len(jobs)}"
assert any("99254436941/" in o and o.startswith("Training/Resume-Based/") for o in olds)
assert any("94721979494/" in o and o.startswith("Training/Other/") for o in olds)
assert any("session-result-99254436941.json" in o for o in olds)
assert not any(NOT_IR in o for o in olds), "NON-IR SESSION PICKED UP -- UNSAFE!"
assert not any(o.startswith(TARGET) for o in olds), "already-correct data reprocessed!"
for j in jobs:
    assert j["new_prefix"].startswith(TARGET), "wrong destination"

print("="*68)
print("ALL CHECKS PASSED:")
print("  - IR session in Resume-Based        -> moved")
print("  - IR session in Other               -> moved")
print("  - IR date-level session-result      -> moved")
print("  - NON-IR session in Resume-Based    -> LEFT ALONE (control)")
print("  - already in Interview-Readiness    -> not reprocessed")
print("="*68)
