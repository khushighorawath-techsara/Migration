"""
Proves which files the migration actually picks up, using the EXACT file
lists from a real analyzed session and a real non-analyzed session.
No AWS calls — the S3 listing is stubbed out.
"""
import migrate_training_s3 as m

SESSION = "Training/Dev_Purohit/2026/July/Some_Candidate/2026-07-14/Time-7-33-PM-IST/94554430792/"
DATE_LEVEL = "Training/Dev_Purohit/2026/July/Some_Candidate/2026-07-14/"

FAKE_OBJECTS = [
    # ---- analyzed session: every file type listed ----
    (SESSION + "analysis-video.mp4", 55_600_000),
    (SESSION + "CHAT/chat.txt", 1_200),
    (SESSION + "M4A/audio.m4a", 804_668),
    (SESSION + "MP4/video.mp4", 4_044_929),
    (SESSION + "proof/scripted_deception/quote.txt", 900),
    (SESSION + "proof/scripted_deception/frame_0610.jpg", 120_000),
    (SESSION + "proof/section_coverage/coverage_report.txt", 3_400),
    (SESSION + "report.html", 28_200),
    (SESSION + "result.json", 80_100),
    (SESSION + "training-temp.json", 491),
    (SESSION + "TRANSCRIPT/transcript.vtt", 44_000),
    # ---- extra file types seen on non-analyzed sessions ----
    (SESSION + "cc/captions.vtt", 12_000),
    (SESSION + "participants.json", 755),
    (SESSION + "zoom-cleanup.json", 2_300),
    (SESSION + "TIMELINE/timeline.json", 204_143),
    # ---- a deeply nested file, to check depth isn't a problem ----
    (SESSION + "proof/a/b/c/d/deep_evidence.jpg", 5_000),
    # ---- THE EDGE CASE: merged session result lives at the DATE level,
    #      one level ABOVE the Time folder (seen in real S3 screenshots) ----
    (DATE_LEVEL + "session-result-94554430792.json", 16_900),
]


def fake_list_all_objects(prefix):
    for key, size in FAKE_OBJECTS:
        if key.startswith(prefix):
            yield key, size


m.list_all_objects = fake_list_all_objects

jobs = m.plan_migrations()

print(f"Planned {len(jobs)} job(s)\n")

copied_keys = set()
for j in jobs:
    print(f"OLD: {j['old_prefix']}")
    print(f"NEW: {j['new_prefix']}")
    print(f"objects: {len(j['objects'])}\n")
    for old_key, _ in j["objects"]:
        copied_keys.add(old_key)
        relative = old_key[len(j["old_prefix"]):]
        print(f"   COPIED -> {j['new_prefix']}{relative}")

all_keys = {k for k, _ in FAKE_OBJECTS}
missed = all_keys - copied_keys

print("\n" + "=" * 70)
if missed:
    print(f"!!! {len(missed)} FILE(S) NOT COPIED BY THE MIGRATION !!!")
    for k in sorted(missed):
        print(f"   MISSED -> {k}")
else:
    print("All files copied.")
print("=" * 70)
