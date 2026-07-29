"""
Validates migrate_training_s3.py's planning logic against the EXACT real
paths seen in the screenshots, with no AWS connection at all — just checking
that the parsing/classification/new-path logic produces the right answer on
real-shaped data before this ever touches production.
"""
import migrate_training_s3 as m

# Real object keys, taken directly from the screenshots (Images 1-8),
# trimmed to a representative few files per session rather than everything.
FAKE_OBJECTS = [
    # Pattern A: top-level Advanced-Training department (Images 5 & 6)
    ("Advanced-Training/Sneha_Chaudhary/2026/July/"
     "563-bhanu_Varshini-Khaja_Faizan-Mani-Mohammed_Farhan_Wajid-Mohammed_Obaid_Ahmed-"
     "Pavithran_Gnanasekaran-Ruthura_Meedimale-Vidya_Nomula/2026-07-01/Time-12-55-AM-IST/"
     "97609808470/MP4/rec.mp4", 500_000),
    ("Advanced-Training/Sneha_Chaudhary/2026/July/"
     "563-bhanu_Varshini-Khaja_Faizan-Mani-Mohammed_Farhan_Wajid-Mohammed_Obaid_Ahmed-"
     "Pavithran_Gnanasekaran-Ruthura_Meedimale-Vidya_Nomula/2026-07-01/Time-12-55-AM-IST/"
     "97609808470/participants.json", 755),

    # Pattern B: nested "Advanced_Training" inside Training/ (Image 3, sneha_chaudhary)
    ("Training/sneha_chaudhary/2026/July/Advanced_Training/2026-07-21/Time-7-28-PM-IST/"
     "98970301816/report.html", 27_900),
    ("Training/sneha_chaudhary/2026/July/Advanced_Training/2026-07-21/Time-7-28-PM-IST/"
     "98970301816/result.json", 44_800),

    # Pattern B: nested "Advance_Training" — the OTHER spelling (Image 1, Dev_Purohit)
    ("Training/Dev_Purohit/2026/July/Advance_Training/2026-07-10/Time-5-00-PM-IST/"
     "11112222/MP4/rec.mp4", 400_000),

    # Pattern C: genuine Normal training (Image 2, Naveen_Reddy_Velmala)
    ("Training/sneha_chaudhary/2026/July/Naveen_Reddy_Velmala/2026-07-14/Time-7-33-PM-IST/"
     "94554430792/report.html", 28_200),
    ("Training/sneha_chaudhary/2026/July/Naveen_Reddy_Velmala/2026-07-14/Time-7-33-PM-IST/"
     "94554430792/result.json", 80_100),

    # Already-migrated NEW-structure data (simulating a re-run after Phase 3 is live) —
    # must NOT be picked up again.
    ("Training/Resume-Based/sneha_chaudhary/2026/August/SomeCandidate/2026-08-05/"
     "Time-2-00-PM-IST/55556666/report.html", 20_000),
]


def fake_list_all_objects(prefix):
    for key, size in FAKE_OBJECTS:
        if key.startswith(prefix):
            yield key, size


m.list_all_objects = fake_list_all_objects  # monkey-patch, no real S3 call

jobs = m.plan_migrations()

print(f"Planned {len(jobs)} job(s):\n")
for j in jobs:
    print(f"  [{j['source']}]")
    print(f"    OLD: {j['old_prefix']}")
    print(f"    NEW: {j['new_prefix']}")
    print(f"    objects: {len(j['objects'])}, bytes: {j['total_bytes']}\n")

# ── Assertions ───────────────────────────────────────────────────────────
by_source = {j["old_prefix"]: j for j in jobs}

assert len(jobs) == 4, f"expected 4 jobs (PatternA, 2x PatternB variants, PatternC) — the already-migrated data must be excluded, got {len(jobs)}"

pattern_a = [j for j in jobs if j["source"] == "PatternA-TopLevelAdvancedDept"][0]
assert pattern_a["new_prefix"] == "Training/Advanced/Sneha_Chaudhary/2026/July/Group/2026-07-01/Time-12-55-AM-IST/97609808470/", \
    f"Pattern A new_prefix wrong: {pattern_a['new_prefix']}"

pattern_b_variant1 = [j for j in jobs if "sneha_chaudhary" in j["old_prefix"] and j["source"] == "PatternB-NestedAdvanced"][0]
assert pattern_b_variant1["new_prefix"] == "Training/Advanced/sneha_chaudhary/2026/July/Group/2026-07-21/Time-7-28-PM-IST/98970301816/", \
    f"Pattern B (Advanced_Training spelling) new_prefix wrong: {pattern_b_variant1['new_prefix']}"

pattern_b_variant2 = [j for j in jobs if "Dev_Purohit" in j["old_prefix"]][0]
assert pattern_b_variant2["source"] == "PatternB-NestedAdvanced", \
    f"'Advance_Training' (no d) spelling was not caught: classified as {pattern_b_variant2['source']}"
assert pattern_b_variant2["new_prefix"] == "Training/Advanced/Dev_Purohit/2026/July/Group/2026-07-10/Time-5-00-PM-IST/11112222/", \
    f"Pattern B (Advance_Training spelling) new_prefix wrong: {pattern_b_variant2['new_prefix']}"

pattern_c = [j for j in jobs if j["source"] == "PatternC-Normal"][0]
assert pattern_c["new_prefix"] == "Training/Resume-Based/sneha_chaudhary/2026/July/Naveen_Reddy_Velmala/2026-07-14/Time-7-33-PM-IST/94554430792/", \
    f"Pattern C new_prefix wrong: {pattern_c['new_prefix']}"

already_migrated_keys = [j for j in jobs if "Resume-Based" in j["old_prefix"] or "/August/" in j["old_prefix"]]
assert not already_migrated_keys, "already-migrated new-structure data was incorrectly picked up for re-migration!"

print("ALL ASSERTIONS PASSED:")
print("  - Pattern A (top-level Advanced-Training dept) -> correct new path")
print("  - Pattern B 'Advanced_Training' spelling -> correctly caught as Advanced")
print("  - Pattern B 'Advance_Training' spelling (no d) -> correctly caught as Advanced")
print("  - Pattern C (real candidate name) -> correctly classified as Resume-Based")
print("  - Already-migrated new-structure data -> correctly excluded from re-migration")
