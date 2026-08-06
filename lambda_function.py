import os
import json
import base64
import time
import math
import re  # ── used by training day parser + program folder slugify
from urllib.parse import quote, urlencode
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher, get_close_matches

import boto3
import requests

TOKEN_URL = "https://zoom.us/oauth/token"
API_BASE  = "https://api.zoom.us/v2"

secrets = boto3.client("secretsmanager")
s3      = boto3.client("s3")
sqs     = boto3.client("sqs")   # analysis job enqueue
ec2     = boto3.client("ec2")   # wake the analysis worker

# ── Zoom env vars ──────────────────────────────────────────────────────────────
ZOOM_SECRET_NAME    = os.environ["ZOOM_SECRET_NAME"]
S3_BUCKET_NAME      = os.environ["S3_BUCKET_NAME"]
COMPANY_EMAIL_DOMAIN = os.environ.get("COMPANY_EMAIL_DOMAIN", "techsarasolutions.com").lower()

# ── Salesforce env vars ────────────────────────────────────────────────────────
SF_SECRET_NAME            = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
SF_OBJECT_API_NAME        = os.environ.get("SF_OBJECT_API_NAME", "Interview__c")
SF_MEETING_ID_FIELD       = os.environ.get("SF_MEETING_ID_FIELD_API_NAME", "Zoom_Meeting_Id__c")
SF_ROUND_FIELD            = os.environ.get("SF_ROUND_FIELD_API_NAME", "Round_Info__c")

# ── Training analysis env vars ─────────────────────────────────────────────────
ANALYSIS_QUEUE_URL            = os.environ.get("ANALYSIS_QUEUE_URL")
WORKER_INSTANCE_ID            = os.environ.get("WORKER_INSTANCE_ID", "").strip()
SF_SESSION_OBJECT             = os.environ.get("SF_SESSION_OBJECT_API_NAME", "Session__c")
SF_SESSION_MEETING_FIELD      = os.environ.get("SF_SESSION_MEETING_FIELD_API_NAME", "External_Meeting_ID__c")
SF_SESSION_STEP_RELATION      = os.environ.get("SF_SESSION_STEP_RELATION", "Candidate_Training_Step__r.Name")
SF_SESSION_CANDIDATE_RELATION = os.environ.get("SF_SESSION_CANDIDATE_RELATION", "Candidate__r.Name")
SF_SESSION_PROGRAM_RELATION        = os.environ.get("SF_SESSION_PROGRAM_RELATION", "Candidate_Training__r.Program__r.Name")
SF_SESSION_PROGRAM_RELATION_DIRECT = os.environ.get("SF_SESSION_PROGRAM_RELATION_DIRECT", "Program_Version__r.Program__r.Name")
SF_SESSION_SEQUENCE_RELATION  = os.environ.get("SF_SESSION_SEQUENCE_RELATION", "Candidate_Training_Step__r.Sequence__c")
SF_SESSION_TRAINER_RELATION   = os.environ.get("SF_SESSION_TRAINER_RELATION", "Candidate_Training_Step__r.Assigned_Trainer_Name__c")
SF_SESSION_HOST_USER_FIELD    = os.environ.get("SF_SESSION_HOST_USER_FIELD", "Host_User__c")
SF_SESSION_PURPOSE_FIELD      = os.environ.get("SF_SESSION_PURPOSE_FIELD", "Purpose__c")

# Program_Version__c.Session_Type__c — picklist: "Single Session" / "Group Session".
# This is the AUTHORITATIVE answer to "is this a batch class?", because it is a
# property of the program version itself, set once when the version is created.
# It does not care how many people happened to show up on a given day.
#
# The real relationship path is THREE hops:
#     Session__c
#       -> Candidate_Training__r      (lookup to Candidate_Training__c)
#            -> Program_Version__r    (lookup to Program_Version__c)
#                 -> Session_Type__c
#
# SF_SESSION_TYPE_RELATION_ALT is tried second, in case a given org also has a
# direct Program_Version__c lookup on Session__c. Set it to "" to disable.
SF_SESSION_TYPE_RELATION      = os.environ.get(
    "SF_SESSION_TYPE_RELATION", "Candidate_Training__r.Program_Version__r.Session_Type__c"
)
SF_SESSION_TYPE_RELATION_ALT  = os.environ.get(
    "SF_SESSION_TYPE_RELATION_ALT", "Program_Version__r.Session_Type__c"
)


# ══════════════════════════════════════════════════════════════════════════════
#  PROGRAM FOLDERS  (the part that used to need a code change per program)
#
#  OLD behaviour: a hardcoded PROGRAM_TYPE_MAP decided the folder. A program
#  that wasn't in that dict became UNKNOWN and its recordings were dumped into
#  Training/Other/ until somebody edited this file and redeployed.
#
#  NEW behaviour: the Salesforce Program NAME *is* the folder name. Nothing to
#  edit when a new program (e.g. "Advanced Python") goes live. The folder is
#  created lazily, on the FIRST recording that belongs to that program.
#
#  This affects the S3 PATH ONLY. Which programs get graded, and which use the
#  group/batch path, are still decided by PROGRAM_TYPE_MAP exactly as before.
# ══════════════════════════════════════════════════════════════════════════════

# Where the program list is read from. Only Name is needed.
# NOTE: this list is used ONLY by the Zoom-topic fallback (see
# program_name_from_topic). On the normal path Salesforce tells us the program
# directly from Session__c, so no extra query is made.
SF_PROGRAM_OBJECT     = os.environ.get("SF_PROGRAM_OBJECT_API_NAME", "Program__c")
SF_PROGRAM_NAME_FIELD = os.environ.get("SF_PROGRAM_NAME_FIELD", "Name")
# Optional SOQL WHERE clause, e.g. "IsActive__c = true". Empty = every program.
SF_PROGRAM_WHERE      = os.environ.get("SF_PROGRAM_WHERE", "").strip()

# How long a warm container reuses the Salesforce program list before
# re-querying. 3600 = one SF query per hour per container, and only on the
# fallback path.
PROGRAM_CACHE_TTL_SEC = int(os.environ.get("PROGRAM_CACHE_TTL_SEC", "3600"))

# Folder used when Salesforce gives us no program at all (permission problem,
# missing link, SF outage) AND the Zoom topic doesn't match a known program.
UNKNOWN_PROGRAM_FOLDER = os.environ.get("UNKNOWN_PROGRAM_FOLDER", "Other")


def _parse_pair_env(raw: str, defaults: dict) -> dict:
    """
    Parse "Name=Folder|Name=Folder" into a dict, layered on top of `defaults`.
    Pipe-separated (not comma) because program names may contain commas.
    A malformed chunk is skipped, never raises.
    """
    out = dict(defaults)
    for chunk in (raw or "").split("|"):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        key, value = key.strip(), value.strip()
        if key and value:
            out[key] = value
    return out


# ── Folder-name pins for the programs that already have folders in S3 ─────────
# Without these, "Advanced AI/ML Training" would slugify to
# "Advanced-AI-ML-Training" and fork away from the existing "Advanced/" folder.
# These four lines exist purely to protect data already sitting in the bucket.
# Override/extend from the environment with PROGRAM_FOLDER_ALIASES, e.g.
#   Advanced AI/ML Training=Advanced|Resume Based Training=Resume-Based
_DEFAULT_PROGRAM_FOLDER_ALIASES = {
    "Advanced AI/ML Training":      "Advanced",
    "Resume Based Training":        "Resume-Based",
    "Interview Readiness Training": "Interview-Readiness",
    "Retraining Program":           "Retraining",
}
PROGRAM_FOLDER_ALIASES = _parse_pair_env(
    os.environ.get("PROGRAM_FOLDER_ALIASES"),
    _DEFAULT_PROGRAM_FOLDER_ALIASES,
)
_ALIAS_LOOKUP = {k.strip().lower(): v for k, v in PROGRAM_FOLDER_ALIASES.items()}

# Shadow mode by default: every session is classified and logged, but nothing
# is actually skipped until this is explicitly set to "true" in the Lambda's
# environment. (Currently "true" in production.)
ENFORCE_TRAINING_TYPE_FILTER = os.environ.get(
    "ENFORCE_TRAINING_TYPE_FILTER", "false"
).strip().lower() == "true"


# ══════════════════════════════════════════════════════════════════════════════
#  ANALYSIS / GROUP-SESSION CLASSIFICATION  — UNCHANGED FROM PRODUCTION
#
#  This map still decides two things, exactly as it does today:
#    1. whether a session is sent to the grading worker
#       (NORMAL + RETRAINING only, when ENFORCE_TRAINING_TYPE_FILTER=true)
#    2. whether a session uses the group/batch path (ADVANCED only)
#
#  It NO LONGER decides the S3 folder — program_folder_name() does that now.
#  A program that isn't listed here (e.g. "Advanced Python") gets its own
#  folder automatically but is NOT graded and is NOT treated as a group
#  session, which is precisely how production behaves today.
#
#  Add a line here when you're ready to start grading a new program.
# ══════════════════════════════════════════════════════════════════════════════
PROGRAM_TYPE_MAP = {
    "Advanced AI/ML Training":      "ADVANCED",
    "Resume Based Training":        "NORMAL",
    "Interview Readiness Training": "INTERVIEW_READINESS",
    "Retraining Program":           "RETRAINING",
}


def classify_training_type(program_name: str) -> str:
    """'NORMAL' / 'ADVANCED' / 'UNKNOWN'. Never raises; unmapped is safe-by-default."""
    if not program_name:
        return "UNKNOWN"
    return PROGRAM_TYPE_MAP.get(program_name.strip(), "UNKNOWN")


# Legacy code -> folder. No longer used to build paths (program_folder_name()
# does that), kept so an old training_type value read out of an existing
# training-temp.json can still be mapped back to its folder by hand.
TRAINING_TYPE_FOLDER_NAMES = {
    "ADVANCED":            "Advanced",
    "NORMAL":              "Resume-Based",
    "INTERVIEW_READINESS": "Interview-Readiness",
    "RETRAINING":          "Retraining",
    "UNKNOWN":             UNKNOWN_PROGRAM_FOLDER,
}


def program_folder_name(program_name: str) -> str:
    """
    THE canonical S3 folder name for a Salesforce program.

    Deterministic, NOT fuzzy — two different programs can never collapse into
    each other the way two spellings of a trainer name do. "Advanced" and
    "Advanced-Python" stay separate folders forever.

        "Advanced Python"          -> "Advanced-Python"
        "advanced   python"        -> "Advanced-Python"   (casing/spacing collapse)
        "Advanced AI/ML Training"  -> "Advanced"          (pinned alias)
        None / ""                  -> "Other"
    """
    if not program_name:
        return UNKNOWN_PROGRAM_FOLDER

    raw = " ".join(str(program_name).split())

    pinned = _ALIAS_LOOKUP.get(raw.lower())
    if pinned:
        return pinned

    slug = re.sub(r"[^A-Za-z0-9]+", "-", raw).strip("-")
    if not slug:
        return UNKNOWN_PROGRAM_FOLDER

    return "-".join(w[:1].upper() + w[1:] for w in slug.split("-") if w)


def resolve_program_folder(program_name: str) -> tuple[str, str]:
    """
    THE folder a program's recordings go into, reconciled against the folders
    that already exist under Training/.

    Matching is EXACT, with ONE exception: letter case.

        computed "Advanced-Python", existing "Advanced-Python"  -> reuse  (exact)
        computed "ADVANCED-PYTHON", existing "Advanced-Python"  -> reuse  (case_corrected)
        computed "Advanced-Python", existing "Advanced"         -> NEW folder

    So a program whose name was typed in different casing in Salesforce can
    never create a second folder — the folder already in S3 always wins.

    There is deliberately NO fuzzy matching here. "Advanced" and
    "Advanced-Python" are ~88% similar as strings, which is above the trainer
    fuzzy threshold; difflib would merge them and silently file an entire
    program's recordings under the wrong program. People's names get fuzzy
    matching, program names never do.

    Returns (folder_name, reason).
    """
    computed = program_folder_name(program_name)
    if computed == UNKNOWN_PROGRAM_FOLDER:
        return computed, "unknown_program"

    try:
        existing = list_known_folders("Training/")
    except Exception as exc:
        print(f"Could not list Training/ to reconcile program folder: {exc}")
        return computed, "listing_failed"

    for folder in existing:
        if folder.lower() == computed.lower():
            if folder == computed:
                return folder, "exact"
            print(f"Program folder case-corrected: '{computed}' -> '{folder}' "
                  f"(existing folder wins, no duplicate created)")
            return folder, "case_corrected"

    return computed, "new_folder"


def should_analyze_program(program_name: str) -> bool:
    """
    UNCHANGED PRODUCTION RULE: only NORMAL and RETRAINING programs are graded,
    and only when ENFORCE_TRAINING_TYPE_FILTER is on. Anything unmapped
    (including a brand new program) is stored but not graded.
    """
    if not ENFORCE_TRAINING_TYPE_FILTER:
        return True
    return classify_training_type(program_name) in ("NORMAL", "RETRAINING")


# ── Group detection ───────────────────────────────────────────────────────────
#   GROUP_MIN_CANDIDATES  - how many DISTINCT EXTERNAL candidates make a
#                           session a group, used ONLY as the last-resort
#                           fallback when Salesforce tells us nothing.
#   GROUP_COUNT_FALLBACK  - set 0 to switch that head-count fallback off, in
#                           which case an unknown session uses the normal
#                           one-candidate path.
GROUP_MIN_CANDIDATES = int(os.environ.get("GROUP_MIN_CANDIDATES", "2"))
GROUP_COUNT_FALLBACK = os.environ.get(
    "GROUP_COUNT_FALLBACK", "1"
).strip().lower() in {"1", "true", "yes", "y"}


def normalize_session_type(raw) -> str | None:
    """
    Turn Program_Version__c.Session_Type__c into 'GROUP' / 'SINGLE' / None.

    Tolerant of picklist label drift: matches on the meaningful word rather
    than the exact string, so "Group Session", "group", "Group_Session" and
    "GROUP SESSION" all read as GROUP. An unrecognised value returns None,
    which means "Salesforce did not tell us" — the caller then falls back
    rather than guessing from a value it does not understand.
    """
    if not raw:
        return None

    text = " ".join(str(raw).replace("_", " ").replace("-", " ").lower().split())
    if not text:
        return None

    if "group" in text or "batch" in text:
        return "GROUP"
    if "single" in text or "individual" in text or "one on one" in text or "1 1" in text:
        return "SINGLE"

    print(f"[SESSION-TYPE-UNKNOWN] Session_Type__c={raw!r} is not a value this "
          f"Lambda recognises — falling back to the other signals")
    return None


def is_group_session(program_name, participants, host_email,
                     session_type=None) -> tuple[bool, str]:
    """
    Decide whether this session uses the group/batch path ("Group" candidate
    segment + participants.json). Returns (is_group, reason).

    Signals in priority order:

      1. PROGRAM_TYPE_MAP — for the four programs already listed there, the map
         still wins. This is deliberate: it guarantees the existing programs
         behave EXACTLY as they do in production today, so deploying this
         cannot move or reshape anything already in S3. If Salesforce disagrees
         with the map, that is logged loudly so you can verify the data and
         then delete the map entry when you are happy to let Salesforce lead.

      2. Program_Version__c.Session_Type__c — the real answer for every other
         program, including brand new ones. It is a property of the program
         version, not of who turned up, so Day 1 and Day 5 always agree.

      3. Distinct external candidate head count — last resort, only if
         Salesforce gave us nothing (permission problem, missing link, or the
         picklist is empty on that program version).
    """
    tt        = classify_training_type(program_name)
    sf_type   = normalize_session_type(session_type)

    # 1. Legacy map wins for the programs it knows, to protect production.
    if tt != "UNKNOWN":
        mapped_group = (tt == "ADVANCED")
        if sf_type is not None and (sf_type == "GROUP") != mapped_group:
            print(f"[SESSION-TYPE-MISMATCH] program={program_name!r}: "
                  f"PROGRAM_TYPE_MAP says {'GROUP' if mapped_group else 'SINGLE'} but "
                  f"Salesforce Session_Type__c says {sf_type}. Using the map. "
                  f"Remove {program_name!r} from PROGRAM_TYPE_MAP to let Salesforce decide.")
        return mapped_group, f"program_map({tt})"

    # 2. Salesforce is the authority for everything else.
    if sf_type == "GROUP":
        return True, "salesforce_session_type(Group Session)"
    if sf_type == "SINGLE":
        return False, "salesforce_session_type(Single Session)"

    # 3. Nothing from Salesforce — fall back to counting real candidates.
    if not GROUP_COUNT_FALLBACK:
        return False, "no_session_type_and_count_fallback_disabled"

    distinct = len(pick_all_candidates(participants, host_email))
    if distinct >= GROUP_MIN_CANDIDATES:
        print(f"[GROUP-BY-COUNT] program={program_name!r} has no Session_Type__c and is "
              f"not in PROGRAM_TYPE_MAP; {distinct} distinct candidates >= "
              f"{GROUP_MIN_CANDIDATES} -> GROUP path. Set Session_Type__c on the "
              f"Program Version to make this decision stable.")
        return True, f"candidate_count({distinct})"

    print(f"[GROUP-BY-COUNT] program={program_name!r} has no Session_Type__c and is "
          f"not in PROGRAM_TYPE_MAP; only {distinct} distinct candidate(s) -> single path. "
          f"Set Session_Type__c on the Program Version to make this decision stable.")
    return False, f"candidate_count({distinct})"


# ── Live Salesforce program list (cached) ─────────────────────────────────────
_PROGRAM_CACHE = {"names": [], "timestamp": 0.0}


def list_sf_programs(force: bool = False) -> list:
    """
    Every program Name in Salesforce. Cached for PROGRAM_CACHE_TTL_SEC.
    On any failure returns the last good list (possibly empty) — never raises,
    because a Salesforce hiccup must not stop a recording from being uploaded.
    """
    now = time.time()
    if (
        not force
        and _PROGRAM_CACHE["names"]
        and (now - _PROGRAM_CACHE["timestamp"]) < PROGRAM_CACHE_TTL_SEC
    ):
        return _PROGRAM_CACHE["names"]

    try:
        sf_secret = get_sf_secret()
        access_token, instance_url = get_sf_access_token(sf_secret)

        soql = f"SELECT {SF_PROGRAM_NAME_FIELD} FROM {SF_PROGRAM_OBJECT}"
        if SF_PROGRAM_WHERE:
            soql += f" WHERE {SF_PROGRAM_WHERE}"
        soql += " LIMIT 2000"

        names   = []
        url     = f"{instance_url}/services/data/v59.0/query"
        params  = {"q": soql}
        headers = {"Authorization": f"Bearer {access_token}"}

        while url:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code >= 400:
                print(f"SF program list failed {resp.status_code}: {resp.text}")
                return _PROGRAM_CACHE["names"]

            data = resp.json()
            for rec in data.get("records", []):
                value = rec.get(SF_PROGRAM_NAME_FIELD)
                if value:
                    names.append(str(value).strip())

            next_url = data.get("nextRecordsUrl")
            url      = f"{instance_url}{next_url}" if next_url else None
            params   = None   # nextRecordsUrl already carries the query

        # Dedupe, keep first-seen order
        seen, unique = set(), []
        for n in names:
            key = n.lower()
            if key not in seen:
                seen.add(key)
                unique.append(n)

        _PROGRAM_CACHE["names"]     = unique
        _PROGRAM_CACHE["timestamp"] = now
        print(f"SF program list refreshed: {len(unique)} programs -> {unique}")
        return unique

    except Exception as exc:
        print(f"SF program list errored ({exc}) — using previous cache")
        return _PROGRAM_CACHE["names"]


def program_name_from_topic(topic: str):
    """
    FALLBACK ONLY — used when Salesforce gives us no program name.

    Matches the LIVE Salesforce program list (not a frozen dict) against the
    Zoom topic text, so a brand new program is covered here automatically too.
    Falls back to the alias keys if the SF list is unavailable.

    Conservative: needs the full program name to appear in the topic. If two
    programs match and one is not simply a longer version of the other, it
    returns None rather than guessing.
    """
    if not topic:
        return None

    haystack = " ".join(str(topic).lower().split())
    known    = list_sf_programs() or list(PROGRAM_FOLDER_ALIASES.keys())

    hits = {n for n in known if " ".join(n.lower().split()) in haystack}
    if not hits:
        return None
    if len(hits) == 1:
        return hits.pop()

    # Overlapping names, e.g. "Advanced Python" and "Advanced Python Training".
    # Take the longest ONLY if every other hit is contained inside it.
    best   = max(hits, key=len)
    others = [h for h in hits if h != best]
    if all(o.lower() in best.lower() for o in others):
        return best

    print(f"Topic matched multiple unrelated programs {sorted(hits)} — not guessing")
    return None


# ── Lazy program-folder creation ──────────────────────────────────────────────
# There is deliberately NO proactive folder sync. A program folder appears in
# S3 the moment the FIRST recording for that program is processed: the base
# prefix placeholder written in process_recording_event() creates
# Training/{Program}/ on the way to writing the meeting folder.
#
# ensure_training_program_folder() below makes that explicit rather than relying
# on the deep prefix alone, so the program folder is visible in the S3 console
# even before the meeting subfolders finish uploading.


def ensure_training_program_folder(program_folder: str, folder_reason: str) -> None:
    """
    Write the Training/{Program}/ placeholder for a program whose folder does
    not exist yet. Called on the first recording of a new program.

    Does nothing when the folder already exists (reason 'exact' or
    'case_corrected'), so no duplicate and no wasted PUT. Never raises — a
    failure here must not stop the recording from being uploaded.
    """
    if not program_folder:
        return
    if folder_reason in ("exact", "case_corrected"):
        return   # folder is already there, possibly under different casing

    key = f"Training/{program_folder}/"
    try:
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=b"")
        print(f"NEW PROGRAM FOLDER created on first recording: s3://{S3_BUCKET_NAME}/{key}")
        # Drop the cached Training/ listing so the new folder is visible to the
        # very next resolve_program_folder() call in this same container.
        _FOLDER_CACHE.pop("Training/", None)
    except Exception as exc:
        print(f"Failed creating program folder {key}: {exc}")


def titlecase_folder_name(name: str) -> str:
    """
    THE canonical trainer-folder spelling.

    Trainer names arrive from three sources with different conventions:
    Salesforce and the Zoom topic yield capitalized names, the host-email
    fallback yields lowercase. Forcing every one of them through this helper
    BEFORE the folder lookup means all three converge on one spelling, so a
    casing variant (ved_sharma vs Ved_Sharma) can no longer be created.

    Must match titlecase_folder_name() in consolidate_trainer_folders.py.
    """
    return "_".join(p[:1].upper() + p[1:] for p in name.split("_") if p)


# ══════════════════════════════════════════════════════════════════════════════
# ── Zoom cloud cleanup (verified delete) ──────────────────────────────────────
#   DELETE_QUEUE_URL     - SQS queue consumed by zoom-recording-cleaner
#   ZOOM_DELETE_ENABLED  - master kill switch. Set 0 and no cleanup job is ever
#                          queued. Uploads keep working. Jobs already queued still
#                          drain (set DELETE_FROM_ZOOM=0 on the cleaner to stop those).
#   DELETE_DELAY_SECONDS - SQS DelaySeconds on the cleanup job. 900 = SQS max.
#                          Buys Zoom time to finish generating the transcript before
#                          the cleaner first looks at the meeting.
# ══════════════════════════════════════════════════════════════════════════════
DELETE_QUEUE_URL     = os.environ.get("DELETE_QUEUE_URL", "").strip()
ZOOM_DELETE_ENABLED  = os.environ.get("ZOOM_DELETE_ENABLED", "1").strip().lower() in {"1", "true", "yes", "y"}
DELETE_DELAY_SECONDS = int(os.environ.get("DELETE_DELAY_SECONDS", "900"))

# ── Fuzzy folder-matching env vars  (used by Training & HR) ───────────────────
#   FOLDER_FUZZY_THRESHOLD  - similarity threshold (0.0-1.0). Default 0.88.
#                             Higher = stricter (fewer auto-corrections).
#   FOLDER_CACHE_TTL_SEC    - seconds to cache the existing-folder list.
#                             Default 600 (10 min).
FOLDER_FUZZY_THRESHOLD = float(os.environ.get("FOLDER_FUZZY_THRESHOLD",
                                              os.environ.get("TRAINER_FUZZY_THRESHOLD", "0.88")))
FOLDER_CACHE_TTL_SEC   = int(os.environ.get("FOLDER_CACHE_TTL_SEC",
                                            os.environ.get("TRAINER_CACHE_TTL_SEC", "600")))

# ── Group-session env vars ────────────────────────────────────────────────────
#   ADV_TRAINING_GENERIC_HOSTS   - comma-separated shared/host account emails.
#                                  When one of these hosts the meeting, the real
#                                  trainer is resolved from the participant list
#                                  (first @company-domain participant).
#   ADV_TRAINING_MAX_FOLDER_LEN  - max chars for the grouped candidates folder.
#                                  Overflow becomes "-and_N_more". Full list is
#                                  always written to participants.json.
ADV_TRAINING_GENERIC_HOSTS = {
    e.strip().lower()
    for e in os.environ.get(
        "ADV_TRAINING_GENERIC_HOSTS",
        "advance.training@techsarasolutions.com",
    ).split(",")
    if e.strip()
}
ADV_TRAINING_MAX_FOLDER_LEN = int(os.environ.get("ADV_TRAINING_MAX_FOLDER_LEN", "200"))

# ──────────────────────────────────────────────────────────────────────────────
ALLOWED_DEPARTMENTS = {
    "training":             "Training",

    "interview-success":    "Interview-Success",
    "interview success":    "Interview-Success",
    "interview_success":    "Interview-Success",

    "customer-success":     "Customer-Success",
    "customer success":     "Customer-Success",
    "customer_success":     "Customer-Success",

    "marketing":            "Marketing",

    "coo":                  "COO",
    "ceo":                  "CEO",

    "executive assistant":  "Executive-Assistant",
    "executive-assistant":  "Executive-Assistant",
    "executive_assistant":  "Executive-Assistant",

    "techsphere":           "Techsphere",
    "tech sphere":          "Techsphere",
    "tech-sphere":          "Techsphere",
    "tech_sphere":          "Techsphere",

    # HR department
    "hr":                   "HR",
    "h.r.":                 "HR",
    "h r":                  "HR",
    "human resources":      "HR",
    "human-resources":      "HR",
    "human_resources":      "HR",
    "humanresources":       "HR",

    # QMS department
    "qms":                          "QMS",
    "q.m.s":                        "QMS",
    "q.m.s.":                       "QMS",
    "q m s":                        "QMS",
    "quality management":           "QMS",
    "quality management system":    "QMS",
    "quality-management-system":    "QMS",
    "quality_management_system":    "QMS",

    # Business-Development department
    "business-development":  "Business-Development",
    "business development":  "Business-Development",
    "business_development":  "Business-Development",
    "businessdevelopment":   "Business-Development",
    "bd":                    "Business-Development",
    "b.d.":                  "Business-Development",
    "biz dev":               "Business-Development",
    "bizdev":                "Business-Development",

    # NEW — Accountant department
    "accountant":            "Accountant",
    "accountants":           "Accountant",
    "accounts":              "Accountant",
    "accounting":            "Accountant",

    # NEW — Operations-Manager department
    "operations-manager":    "Operations-Manager",
    "operations manager":    "Operations-Manager",
    "operations_manager":    "Operations-Manager",
    "operationsmanager":     "Operations-Manager",
    "operations":            "Operations-Manager",
    "operation manager":     "Operations-Manager",
    "ops manager":           "Operations-Manager",
    "ops":                   "Operations-Manager",
}

# NOTE: the cleaner runs with UPLOAD_FILE_TYPES=ALL, so it self-heals CC / TIMELINE /
# SUMMARY files that this list skips. If you would rather grab them up front, add
# them here — it changes nothing else.
ALLOWED_FILE_TYPES = {"MP4", "M4A", "TRANSCRIPT", "CHAT"}

# Month folder uses real names (January, February, ...) instead of Month-N
MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def month_folder_name(start_time: str) -> str:
    """
    Return the month-folder name from a Zoom start_time like "2026-05-19T...".
    Returns 'January' .. 'December' or 'UnknownMonth' on parse failure.
    """
    if not start_time or len(start_time) < 7:
        return "UnknownMonth"
    try:
        n = int(start_time[5:7])
        if 1 <= n <= 12:
            return MONTH_NAMES[n - 1]
    except Exception:
        pass
    return "UnknownMonth"


# ══════════════════════════════════════════════════════════════════════════════
#  Salesforce JWT helpers  (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def get_sf_secret():
    resp = secrets.get_secret_value(SecretId=SF_SECRET_NAME)
    return json.loads(resp["SecretString"])


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _build_jwt_assertion(sf_secret: dict) -> str:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as asym_padding

    client_id   = sf_secret["SF_CLIENT_ID"]
    username    = sf_secret["SF_USERNAME"]
    login_url   = sf_secret.get("SF_LOGIN_URL", "https://login.salesforce.com")
    pem_b64     = sf_secret["PRIVATE_KEY_B64"]

    pem_bytes   = base64.b64decode(pem_b64)
    private_key = serialization.load_pem_private_key(pem_bytes, password=None)

    header  = {"alg": "RS256"}
    payload = {
        "iss": client_id,
        "sub": username,
        "aud": login_url,
        "exp": math.floor(time.time()) + 300,
    }

    header_enc    = _b64url_encode(json.dumps(header,  separators=(",", ":")).encode())
    payload_enc   = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{header_enc}.{payload_enc}".encode()

    signature = private_key.sign(signing_input, asym_padding.PKCS1v15(), hashes.SHA256())
    sig_enc   = _b64url_encode(signature)

    return f"{header_enc}.{payload_enc}.{sig_enc}"


def get_sf_access_token(sf_secret: dict) -> tuple[str, str]:
    login_url = sf_secret.get("SF_LOGIN_URL", "https://login.salesforce.com")
    assertion = _build_jwt_assertion(sf_secret)

    resp = requests.post(
        f"{login_url}/services/oauth2/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion":  assertion,
        },
        timeout=30,
    )
    print(f"SF token status: {resp.status_code}")
    resp.raise_for_status()

    tok          = resp.json()
    access_token = tok.get("access_token")
    instance_url = tok.get("instance_url")
    if not access_token or not instance_url:
        raise RuntimeError(f"SF token response missing fields: {tok}")

    return access_token, instance_url


INTERVIEW_TYPE_FOLDER_ACTUAL   = os.environ.get("INTERVIEW_TYPE_FOLDER_ACTUAL", "Interview")
INTERVIEW_TYPE_FOLDER_INTERNAL = os.environ.get("INTERVIEW_TYPE_FOLDER_INTERNAL", "Internal-Interview")
INTERNAL_INTERVIEW_PURPOSE     = os.environ.get("INTERNAL_INTERVIEW_PURPOSE", "Internal Interview")


def slugify_purpose(purpose: str) -> str:
    """Turn a Session Purpose__c value into a safe S3 folder name.

    Mirrors the program slugifier, so a Purpose we have never seen still gets
    its own correctly-named folder instead of silently piling into Other/.
    """
    slug = re.sub(r"[^A-Za-z0-9]+", "-", (purpose or "").strip()).strip("-")
    if not slug:
        return "Other"
    return "-".join(w[:1].upper() + w[1:] for w in slug.split("-") if w)


def lookup_session_purpose(meeting_id) -> str | None:
    """Purpose__c on the Session__c matching this meeting id, or None.

    ONLY called for Interview-Success recordings where the Interview__c lookup
    already came back empty. That ordering is deliberate:

      * Actual interviews are the priority case and are confirmed FIRST, by
        Interview__c. They never trigger this extra call and their behaviour
        is completely unchanged.
      * An internal interview has no Interview__c record, so it reaches here
        and is identified positively by its Session Purpose, instead of being
        guessed at from a topic string or dumped under Unknown_Company.

    Returns None on any failure, which routes the recording to the actual
    interview folder -- i.e. today's behaviour. A Salesforce outage cannot
    change where actual interviews land.
    """
    try:
        sf_secret = get_sf_secret()
        access_token, instance_url = get_sf_access_token(sf_secret)

        soql = (
            f"SELECT {SF_SESSION_PURPOSE_FIELD} "
            f"FROM {SF_SESSION_OBJECT} "
            f"WHERE {SF_SESSION_MEETING_FIELD} = '{meeting_id}' "
            f"LIMIT 1"
        )
        resp = requests.get(
            f"{instance_url}/services/data/v59.0/query",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": soql},
            timeout=30,
        )
        if resp.status_code >= 400:
            print(f"SF purpose lookup HTTP {resp.status_code}: {resp.text}")
            return None

        records = resp.json().get("records", [])
        if not records:
            print(f"No {SF_SESSION_OBJECT} for meeting_id={meeting_id} "
                  f"-> treating as actual interview")
            return None

        purpose = (records[0].get(SF_SESSION_PURPOSE_FIELD) or "").strip()
        print(f"SF purpose lookup: meeting_id={meeting_id} Purpose__c={purpose!r}")
        return purpose or None

    except Exception as exc:
        print(f"SF purpose lookup failed (meeting_id={meeting_id}): {exc} "
              f"-> treating as actual interview")
        return None


def resolve_interview_type_folder(meeting_id, sf_round):
    """Which type folder an Interview-Success recording belongs in.

    sf_round truthy  -> Interview__c confirmed it, actual interview, no extra call
    Purpose internal -> Internal-Interview
    Purpose other    -> slugified Purpose, its own folder
    anything else    -> Interview (no Session, blank Purpose, or query failed)
    """
    if sf_round:
        return INTERVIEW_TYPE_FOLDER_ACTUAL, "interview__c-match"

    purpose = lookup_session_purpose(meeting_id)
    if not purpose:
        return INTERVIEW_TYPE_FOLDER_ACTUAL, "no-session-or-lookup-failed"

    if purpose.strip().lower() == INTERNAL_INTERVIEW_PURPOSE.strip().lower():
        return INTERVIEW_TYPE_FOLDER_INTERNAL, "purpose-internal-interview"

    return slugify_purpose(purpose), f"purpose-{purpose}"


def lookup_round_from_sf(meeting_id) -> str | None:
    try:
        sf_secret    = get_sf_secret()
        access_token, instance_url = get_sf_access_token(sf_secret)

        soql = (
            f"SELECT {SF_ROUND_FIELD} "
            f"FROM {SF_OBJECT_API_NAME} "
            f"WHERE {SF_MEETING_ID_FIELD} = '{meeting_id}' "
            f"LIMIT 1"
        )
        query_url = f"{instance_url}/services/data/v59.0/query"
        resp = requests.get(
            query_url,
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": soql},
            timeout=30,
        )
        print(f"SF query status: {resp.status_code}")
        resp.raise_for_status()

        records = resp.json().get("records", [])
        if not records:
            print(f"No Salesforce record found for meeting_id={meeting_id}")
            return None

        raw_round = records[0].get(SF_ROUND_FIELD)
        if not raw_round:
            print(f"Salesforce record found but {SF_ROUND_FIELD} is empty for meeting_id={meeting_id}")
            return None

        round_name = sanitize_name(str(raw_round))
        print(f"Salesforce round lookup success: meeting_id={meeting_id} -> {round_name}")
        return round_name

    except Exception as exc:
        print(f"Salesforce round lookup failed (meeting_id={meeting_id}): {exc}")
        return None


# ── Training Day lookup (Session__c) ──────────────────────────────────────────

def _drill(record: dict, field_path: str):
    """Walk a SOQL relationship path like 'Candidate_Training_Step__r.Name' through nested dicts."""
    cur = record
    for part in field_path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def lookup_training_day_from_sf(meeting_id):
    """
    Resolve the training Day for this Zoom meeting from Salesforce.
    Session__c.<meeting field> == meeting_id  ->  Candidate_Training_Step__r
    Prefers the numeric Sequence__c field when present (clean, immune to
    title-text edits); falls back to regex-parsing "Day N" out of the
    free-text step Name only if Sequence__c is missing or unparseable.
    Also pulls Assigned_Trainer_Name__c (preferred trainer-name source over
    parsing the Zoom host email), Host_User__c (raw Employee Id, logged for
    now), and Purpose__c (logged as a mismatch if populated and not "Training").
    Also pulls Program_Version__r.Session_Type__c ("Single Session" /
    "Group Session"), which is the authoritative answer to whether this is a
    batch class. Queried in ISOLATION so a missing field permission cannot
    take the rest of the lookup down with it.
    Returns (day_number, step_name, candidate_name, program_name,
             trainer_name_sf, host_user_id, purpose_value, session_type) —
    all str|None except day_number which is int|None.
    Reuses the existing JWT auth helpers.
    """
    try:
        sf_secret = get_sf_secret()
        access_token, instance_url = get_sf_access_token(sf_secret)

        def _run(soql, label):
            """Run one SOQL. Returns the first record dict, or None if the
            query failed. NEVER raises -- callers decide what a failure means."""
            try:
                resp = requests.get(
                    f"{instance_url}/services/data/v59.0/query",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params={"q": soql},
                    timeout=30,
                )
                if resp.status_code >= 400:
                    print(f"SF query [{label}] status {resp.status_code}: {resp.text}")
                    return None
                recs = resp.json().get("records", [])
                if not recs:
                    print(f"SF query [{label}] returned 0 records")
                    return None
                return recs[0]
            except Exception as exc:
                print(f"SF query [{label}] errored: {exc}")
                return None

        # ── Query 1: CORE fields only ────────────────────────────────────
        # Deliberately excludes the Program relationships. SOQL is
        # all-or-nothing: one field the integration user cannot read fails
        # the WHOLE query. Keeping the program lookups out of here means a
        # program permission problem can no longer wipe out the day number,
        # trainer name, candidate and purpose as well.
        core_soql = (
            f"SELECT {SF_SESSION_STEP_RELATION}, {SF_SESSION_CANDIDATE_RELATION}, "
            f"{SF_SESSION_SEQUENCE_RELATION}, {SF_SESSION_TRAINER_RELATION}, "
            f"{SF_SESSION_HOST_USER_FIELD}, {SF_SESSION_PURPOSE_FIELD} "
            f"FROM {SF_SESSION_OBJECT} "
            f"WHERE {SF_SESSION_MEETING_FIELD} = '{meeting_id}' "
            f"LIMIT 1"
        )
        rec = _run(core_soql, "core")
        if rec is None:
            print(f"SF core lookup failed for {SF_SESSION_MEETING_FIELD}={meeting_id}")
            return None, None, None, None, None, None, None, None

        step_name       = _drill(rec, SF_SESSION_STEP_RELATION)
        candidate       = _drill(rec, SF_SESSION_CANDIDATE_RELATION)
        sequence_raw    = _drill(rec, SF_SESSION_SEQUENCE_RELATION)
        trainer_name_sf = _drill(rec, SF_SESSION_TRAINER_RELATION)
        host_user_id    = _drill(rec, SF_SESSION_HOST_USER_FIELD)
        purpose_value   = _drill(rec, SF_SESSION_PURPOSE_FIELD)

        # ── Query 2 & 3: the two Program paths, each ISOLATED ────────────
        # Tried separately so that one broken/unreadable relationship does
        # not take the other down with it.
        program_name_direct = None
        program_name_via_ct = None

        for relation, label, setter in (
            (SF_SESSION_PROGRAM_RELATION_DIRECT, "program-direct", "direct"),
            (SF_SESSION_PROGRAM_RELATION,        "program-via-ct", "via_ct"),
        ):
            if not relation:
                continue   # set the env var to "" to skip a path that doesn't exist
            prec = _run(
                f"SELECT {relation} FROM {SF_SESSION_OBJECT} "
                f"WHERE {SF_SESSION_MEETING_FIELD} = '{meeting_id}' LIMIT 1",
                label,
            )
            if prec is not None:
                value = _drill(prec, relation)
                if setter == "direct":
                    program_name_direct = value
                else:
                    program_name_via_ct = value
                if value:
                    break   # got a usable program name, no need for the other path

        program_name = program_name_direct or program_name_via_ct

        # ── Query 4 (+5): Session_Type__c, each path ISOLATED ────────────
        # Kept out of the queries above on purpose. If the integration user
        # cannot read Session_Type__c (a newer field is very likely to be
        # missing from the permission set at first), SOQL fails the WHOLE
        # query. Isolating it means a missing permission costs us only the
        # group/single hint — the program name, day, trainer and candidate
        # all still resolve, and the code falls back to the head count.
        #
        # Primary path is the real schema:
        #   Session__c -> Candidate_Training__r -> Program_Version__r -> Session_Type__c
        # The ALT direct path is tried only if the primary returns nothing,
        # and each is a separate query so a bad relationship name on one can
        # never kill the other.
        session_type        = None
        session_type_source = None

        for relation, label in (
            (SF_SESSION_TYPE_RELATION,     "session-type-via-ct"),
            (SF_SESSION_TYPE_RELATION_ALT, "session-type-direct"),
        ):
            if not relation:
                continue
            strec = _run(
                f"SELECT {relation} FROM {SF_SESSION_OBJECT} "
                f"WHERE {SF_SESSION_MEETING_FIELD} = '{meeting_id}' LIMIT 1",
                label,
            )
            if strec is not None:
                value = _drill(strec, relation)
                if value:
                    session_type        = value
                    session_type_source = relation
                    break   # got it, don't bother with the other path

        if not session_type:
            print(f"Session_Type__c empty or unreadable for meeting_id={meeting_id} "
                  f"— group/single will fall back to the other signals")

        day_number = None
        day_source = None
        if sequence_raw is not None:
            try:
                day_number = int(round(float(sequence_raw)))
                day_source = "Sequence__c"
            except (TypeError, ValueError):
                day_number = None  # fall through to regex below

        if day_number is None and step_name:
            m = re.search(r"day\s*0*(\d+)", step_name, re.IGNORECASE)
            if m:
                day_number = int(m.group(1))
                day_source = "regex-on-Name"

        if purpose_value and purpose_value != "Training":
            print(f"[SF-PURPOSE-MISMATCH] meeting={meeting_id} Purpose__c={purpose_value!r} (expected 'Training')")

        print(f"SF training-day lookup ok: meeting_id={meeting_id} day={day_number} "
              f"(source={day_source}) step={step_name!r} "
              f"program={program_name!r} (direct={program_name_direct!r} via_ct={program_name_via_ct!r}) "
              f"trainer_sf={trainer_name_sf!r} host_user_id={host_user_id!r} purpose={purpose_value!r} "
              f"session_type={session_type!r} (via={session_type_source!r})")
        return (day_number, step_name, candidate, program_name, trainer_name_sf,
                host_user_id, purpose_value, session_type)

    except Exception as exc:
        print(f"SF training-day lookup failed (meeting_id={meeting_id}): {exc}")
        return None, None, None, None, None, None, None, None


# ══════════════════════════════════════════════════════════════════════════════
#  Zoom helpers  (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def get_zoom_secret():
    resp = secrets.get_secret_value(SecretId=ZOOM_SECRET_NAME)
    secret_obj = json.loads(resp["SecretString"])
    missing = [k for k in ("account_id", "client_id", "client_secret") if not secret_obj.get(k)]
    if missing:
        raise RuntimeError(f"Missing keys in {ZOOM_SECRET_NAME}: {missing}")
    return secret_obj


def basic_auth_header(client_id, client_secret):
    raw = f"{client_id}:{client_secret}".encode()
    return "Basic " + base64.b64encode(raw).decode()


def get_s2s_access_token(secret_obj):
    r = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": basic_auth_header(secret_obj["client_id"], secret_obj["client_secret"]),
            "Content-Type":  "application/x-www-form-urlencoded",
        },
        params={"grant_type": "account_credentials", "account_id": secret_obj["account_id"]},
        timeout=60,
    )
    print(f"Zoom token status: {r.status_code}")
    r.raise_for_status()
    tok = r.json()
    if not tok.get("access_token"):
        raise RuntimeError(f"Zoom token response missing access_token: {tok}")
    return tok["access_token"]


def zoom_get(access_token, path):
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=60)
    print(f"GET {url} -> {r.status_code}")
    r.raise_for_status()
    return r.json()


# ══════════════════════════════════════════════════════════════════════════════
#  Utility helpers
# ══════════════════════════════════════════════════════════════════════════════

def sanitize_name(name: str) -> str:
    if not name:
        return "Unknown"
    cleaned = "".join(c if c.isalnum() or c in (" ", "-", "_") else "_" for c in str(name)).strip()
    return cleaned.replace(" ", "_") or "Unknown"


def normalize_department(raw_dept: str):
    return ALLOWED_DEPARTMENTS.get((raw_dept or "").strip().lower())


def pick_candidate(participants, host_email):
    host_email = (host_email or "").strip().lower()

    for p in participants:
        email = (p.get("user_email") or p.get("email") or "").strip().lower()
        if email and email != host_email and not email.endswith("@" + COMPANY_EMAIL_DOMAIN):
            return sanitize_name(p.get("name") or email.split("@")[0])

    for p in participants:
        email = (p.get("user_email") or p.get("email") or "").strip().lower()
        name  = (p.get("name") or "").strip()
        if email != host_email and name:
            return sanitize_name(name)

    if host_email:
        return sanitize_name(host_email.split("@")[0])

    return "Unknown_Candidate"


# ── Group-session helpers ─────────────────────────────────────────────────────

def resolve_adv_training_host(participants, host_email, host_name):
    """
    Group-training meetings are often hosted by a shared account
    (advance.training@...). The real trainer joins as a participant with
    their own @techsarasolutions.com email — that person becomes the Host
    folder. If a real person hosted the meeting directly, keep them.
    """
    he = (host_email or "").strip().lower()

    # A real person hosted it (not the shared account) -> keep Zoom host
    if he and he not in ADV_TRAINING_GENERIC_HOSTS:
        return host_name

    # Shared account hosted -> find the internal trainer among participants
    for p in participants:
        email = (p.get("user_email") or p.get("email") or "").strip().lower()
        if (
            email
            and email.endswith("@" + COMPANY_EMAIL_DOMAIN)
            and email not in ADV_TRAINING_GENERIC_HOSTS
        ):
            return sanitize_name((p.get("name") or email.split("@")[0]).strip())

    return host_name  # nobody internal joined — fall back to shared account name


def pick_all_candidates(participants, host_email):
    """
    ALL external (non @techsarasolutions.com) participants, deduped by
    email/name, sorted by name so BOTH webhook events (video + transcript)
    build the exact same folder. Returns [{"name":..., "email":...}, ...].
    """
    host_email = (host_email or "").strip().lower()
    seen, out = set(), []

    for p in participants:
        email = (p.get("user_email") or p.get("email") or "").strip().lower()
        name  = (p.get("name") or "").strip()

        if email == host_email:
            continue
        if email and email.endswith("@" + COMPANY_EMAIL_DOMAIN):
            continue                      # internal staff are not candidates

        clean = sanitize_name(name or (email.split("@")[0] if email else ""))
        if clean == "Unknown":
            continue

        key = email or clean.lower()
        if key in seen:
            continue                      # same person re-joined
        seen.add(key)
        out.append({"name": clean, "email": email})

    out.sort(key=lambda c: c["name"].lower())
    return out


def build_attendance_record(participants, host_email):
    """
    EVERY person who joined, classified -- not just external candidates.

    pick_all_candidates() deliberately filters to externals because it feeds
    the FOLDER NAME. This is different: it is the attendance record, so it
    keeps everyone and labels them instead of dropping them.

    Why this exists: a session's folder can only carry one candidate name, so
    when several people attend, the others were previously unrecoverable --
    there was no record of who else was there. Written for every session now,
    1:1 or group, so attendance is always answerable from S3 alone.

    role is one of: "host" | "internal" | "candidate"
    """
    host_email_l = (host_email or "").strip().lower()
    seen, out = set(), []

    for p in participants:
        email = (p.get("user_email") or p.get("email") or "").strip().lower()
        name  = (p.get("name") or "").strip()

        clean = sanitize_name(name or (email.split("@")[0] if email else ""))
        if clean == "Unknown" and not email:
            continue                      # nothing identifiable at all

        key = email or clean.lower()
        if key in seen:
            continue                      # same person re-joined
        seen.add(key)

        if email and email == host_email_l:
            role = "host"
        elif email and email.endswith("@" + COMPANY_EMAIL_DOMAIN):
            role = "internal"
        else:
            role = "candidate"

        entry = {"name": clean, "email": email, "role": role}
        for k_src, k_dst in (("join_time", "join_time"),
                             ("leave_time", "leave_time"),
                             ("duration", "duration_seconds")):
            if p.get(k_src) is not None:
                entry[k_dst] = p[k_src]
        out.append(entry)

    # host first, then internal, then candidates -- each alphabetically
    order = {"host": 0, "internal": 1, "candidate": 2}
    out.sort(key=lambda e: (order.get(e["role"], 3), e["name"].lower()))
    return out


def build_group_candidate_folder(candidates, max_len=None):
    """
    One folder segment for the whole group:
        Amit_Verma-Priya_Patel-Rahul_Sharma-and_17_more
    Capped because S3 keys max out at 1024 bytes — 20 full names would
    blow past it. Full list always lands in participants.json.
    """
    if max_len is None:
        max_len = ADV_TRAINING_MAX_FOLDER_LEN

    names = [c["name"] for c in candidates]
    if not names:
        return "Unknown_Candidates"

    folder, used = names[0][:max_len], 1
    for n in names[1:]:
        nxt = f"{folder}-{n}"
        if len(nxt) > max_len:
            break
        folder, used = nxt, used + 1

    remaining = len(names) - used
    if remaining > 0:
        folder = f"{folder}-and_{remaining}_more"
    return folder


def parse_interview_success_topic(topic: str):
    raw_topic = (topic or "").strip()
    if not raw_topic:
        return None

    parts = [p.strip() for p in raw_topic.split("<>") if p.strip()]
    if len(parts) < 3:
        return None

    return {
        "candidate_name": sanitize_name(parts[0]),
        "company_name":   sanitize_name(parts[1]),
        "round_name":     sanitize_name(parts[-1]),
        "parsed_ok":      True,
    }


def parse_training_topic(topic: str):
    """
    Expected format:  <Candidate Name> <> <Trainer Name> <> Training
    Returns dict with candidate_name, trainer_name  OR  None.
    """
    raw_topic = (topic or "").strip()
    if not raw_topic:
        return None

    parts = [p.strip() for p in raw_topic.split("<>") if p.strip()]

    if len(parts) < 3:
        print(f"Training topic parse: expected 3 parts, got {len(parts)} - topic={raw_topic!r}")
        return None

    if parts[-1].strip().lower() != "training":
        print(f"Training topic parse: last part is not 'Training' - topic={raw_topic!r}")
        return None

    candidate_name = sanitize_name(parts[0])
    trainer_name   = sanitize_name(parts[1])

    if candidate_name == "Unknown" or trainer_name == "Unknown":
        print(f"Training topic parse: empty candidate or trainer - topic={raw_topic!r}")
        return None

    print(f"Training topic parse success: candidate={candidate_name}, trainer={trainer_name}")
    return {
        "candidate_name": candidate_name,
        "trainer_name":   trainer_name,
        "parsed_ok":      True,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  GENERIC fuzzy folder-name matcher  (used for PEOPLE names only)
#
#  Caches a map of prefix -> list-of-existing-folder-names, so the Lambda can
#  canonicalise any incoming person name (trainer, HR rep, etc.) against
#  folders already in S3. Typo-tolerant via difflib.
#
#  NOTE: program folders deliberately do NOT go through this. A program folder
#  is exact (program_folder_name), so "Advanced" and "Advanced-Python" can
#  never be fuzzy-merged into one another.
#
#  Cache lifetime: FOLDER_CACHE_TTL_SEC. Lambda containers stay warm for
#  ~5-15 min, so this typically costs one S3 LIST per cold start, per prefix.
# ══════════════════════════════════════════════════════════════════════════════

_FOLDER_CACHE: dict = {}   # {prefix: {"folders": [...], "timestamp": float}}


def list_known_folders(dept_prefix: str) -> list:
    """
    Return the list of existing folder names directly under {dept_prefix}.
    Cached per prefix for FOLDER_CACHE_TTL_SEC seconds.

    Example:  list_known_folders("Training/")                  -> ["Advanced", "Advanced-Python", ...]
    Example:  list_known_folders("Training/Advanced-Python/")  -> ["Ved_Sharma", ...]
    """
    if not dept_prefix.endswith("/"):
        dept_prefix = dept_prefix + "/"

    now = time.time()
    entry = _FOLDER_CACHE.get(dept_prefix)
    if entry and (now - entry["timestamp"]) < FOLDER_CACHE_TTL_SEC:
        return entry["folders"]

    try:
        folders = set()
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=S3_BUCKET_NAME,
            Prefix=dept_prefix,
            Delimiter="/",
        ):
            for cp in page.get("CommonPrefixes", []):
                prefix = cp.get("Prefix", "")
                name   = prefix[len(dept_prefix):].rstrip("/")
                if name:
                    folders.add(name)

        sorted_folders = sorted(folders)
        _FOLDER_CACHE[dept_prefix] = {
            "folders":   sorted_folders,
            "timestamp": now,
        }
        print(f"Folder cache refreshed for {dept_prefix}: {len(sorted_folders)} entries")
        return sorted_folders
    except Exception as exc:
        print(f"Failed to list folders under {dept_prefix}: {exc} - using previous cache")
        return entry["folders"] if entry else []


def find_canonical_folder(
    name_raw: str,
    dept_prefix: str,
    threshold: float = None,
) -> tuple[str, str]:
    """
    Look up the canonical folder name for `name_raw` under `dept_prefix`.

    Returns (canonical_name, match_reason).
    match_reason is one of:
      'exact'              - exact case match against an existing folder
      'case_corrected'     - same name, different casing -> canonical wins
      'fuzzy_matched(...)' - close fuzzy match (typo-tolerant)
      'new_entry'          - no good match found, will create a new folder
      'empty_input'        - no input name
      'no_known_entries'   - no folders yet under dept_prefix (first ever)
    """
    if threshold is None:
        threshold = FOLDER_FUZZY_THRESHOLD

    if not name_raw:
        return name_raw, "empty_input"

    known = list_known_folders(dept_prefix)
    if not known:
        return name_raw, "no_known_entries"

    normalized = name_raw.lower().strip()

    # 1. Exact case-insensitive match
    for canonical in known:
        if canonical.lower() == normalized:
            if canonical == name_raw:
                return canonical, "exact"
            return canonical, "case_corrected"

    # 2. Fuzzy match
    lowered_to_canonical = {k.lower(): k for k in known}
    matches = get_close_matches(
        normalized,
        list(lowered_to_canonical.keys()),
        n=1,
        cutoff=threshold,
    )
    if matches:
        canonical = lowered_to_canonical[matches[0]]
        score = SequenceMatcher(None, normalized, matches[0]).ratio()
        return canonical, f"fuzzy_matched(score={score:.3f})"

    # 3. No match - new entry
    return name_raw, "new_entry"


def double_encode_uuid(meeting_uuid):
    return quote(quote(str(meeting_uuid), safe=""), safe="")


def parse_zoom_start_time(start_time: str):
    if not start_time:
        return None
    try:
        return datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def build_time_folder_ist(start_time: str) -> str:
    dt_utc = parse_zoom_start_time(start_time)
    if not dt_utc:
        return "Time-Unknown-IST"
    ist    = timezone(timedelta(hours=5, minutes=30))
    dt_ist = dt_utc.astimezone(ist)
    hour   = dt_ist.strftime("%I").lstrip("0") or "12"
    return f"Time-{hour}-{dt_ist.strftime('%M')}-{dt_ist.strftime('%p')}-IST"


# ══════════════════════════════════════════════════════════════════════════════
#  Storage-path resolution
#
#  Returns a dict with whichever person-name fields apply to the department:
#      candidate_name   - everyone
#      company_name     - Interview-Success only
#      round_name       - Interview-Success only
#      trainer_name     - Training only
#      hr_person_name   - HR only
#      all_candidates   - group programs only (full attendee list)
# ══════════════════════════════════════════════════════════════════════════════

def resolve_storage_info(
    department_folder,
    topic,
    participants,
    host_email,
    meeting_id,
    host_name,
    program_name=None,      # Salesforce program name (Training only)
    program_folder=None,    # its S3 folder segment (Training only)
    trainer_name_sf=None,
    session_type=None,      # Program_Version__c.Session_Type__c (Training only)
):

    # ── Interview-Success ──────────────────────────────────────────────────
    if department_folder == "Interview-Success":
        sf_round = lookup_round_from_sf(meeting_id)

        # Decide the type folder BEFORE topic parsing. An internal interview's
        # topic will not match the Candidate <> Company <> Round format, and
        # if it partially matched we would file it as an actual interview with
        # meaningless company/round values -- which is how these ended up under
        # Unknown_Company/Unknown_Round in the first place.
        interview_type, type_reason = resolve_interview_type_folder(meeting_id, sf_round)
        print(f"[INTERVIEW-TYPE] meeting={meeting_id} -> {interview_type} ({type_reason})")

        if interview_type != INTERVIEW_TYPE_FOLDER_ACTUAL:
            # Non-actual: generic layout, no company/round segments at all.
            # Placeholders are not written -- the absence of the data is
            # represented by the absence of the segments.
            return {
                "candidate_name": pick_candidate(participants, host_email),
                "company_name":   None,
                "round_name":     None,
                "trainer_name":   None,
                "hr_person_name": None,
                "canonical_host_name": None,
                "interview_type": interview_type,
            }

        parsed = parse_interview_success_topic(topic)

        if sf_round:
            candidate  = parsed["candidate_name"] if parsed else pick_candidate(participants, host_email)
            company    = parsed["company_name"]   if parsed else "Unknown_Company"
            round_name = sf_round
            print(f"Round resolved from Salesforce: {round_name}")
        elif parsed:
            candidate  = parsed["candidate_name"]
            company    = parsed["company_name"]
            round_name = parsed["round_name"]
            print(f"Round resolved from topic parsing: {round_name}")
        else:
            candidate  = pick_candidate(participants, host_email)
            company    = "Unknown_Company"
            round_name = "Unknown_Round"
            print("Round could not be resolved; using Unknown_Round")

        return {
            "candidate_name": candidate,
            "company_name":   company,
            "round_name":     round_name,
            "trainer_name":   None,
            "hr_person_name": None,
            "canonical_host_name": None,
            "interview_type": interview_type,
        }

    # ── Training  (program folder + topic parser + fuzzy trainer match) ────
    if department_folder == "Training":
        # program_folder is normally passed in already reconciled; the fallback
        # reconciles too so a direct call can't skip the case check.
        _pf = program_folder or resolve_program_folder(program_name)[0]

        # Group/batch session: many candidates, no single name belongs in the
        # path. Full attendee list still goes to participants.json.
        group, group_reason = is_group_session(
            program_name, participants, host_email, session_type=session_type
        )
        print(f"Group decision: {group} ({group_reason})")
        if group:
            candidates = pick_all_candidates(participants, host_email)
            if trainer_name_sf:
                trainer_raw = sanitize_name(trainer_name_sf)
            else:
                trainer_raw = sanitize_name((host_email or "").split("@")[0]) or "Unknown_Trainer"

            trainer_raw = titlecase_folder_name(trainer_raw)
            trainer_canonical, match_reason = find_canonical_folder(trainer_raw, f"Training/{_pf}/")
            print(f"Trainer name ({'normalized' if trainer_canonical != trainer_raw else 'kept'}): "
                  f"'{trainer_raw}' -> '{trainer_canonical}' ({match_reason})")

            return {
                "candidate_name":      "Group",
                "company_name":        None,
                "round_name":          None,
                "trainer_name":        trainer_canonical,
                "hr_person_name":      None,
                "canonical_host_name": None,
                "all_candidates":      candidates,   # -> participants.json
            }

        parsed = parse_training_topic(topic)

        if parsed:
            candidate = parsed["candidate_name"]
        else:
            candidate = pick_candidate(participants, host_email)

        # Priority: Salesforce's own field > topic-parsed text > Zoom email.
        # SF is the most controlled source; a topic is free text a human
        # typed for this one meeting; the email is the last-resort fallback.
        if trainer_name_sf:
            trainer_raw = sanitize_name(trainer_name_sf)
        elif parsed:
            trainer_raw = parsed["trainer_name"]
        else:
            trainer_raw = sanitize_name((host_email or "").split("@")[0]) or "Unknown_Trainer"
            print(f"Training topic parse failed - fallback: candidate={candidate}, trainer={trainer_raw}")

        trainer_raw = titlecase_folder_name(trainer_raw)
        trainer_canonical, match_reason = find_canonical_folder(trainer_raw, f"Training/{_pf}/")
        if trainer_canonical != trainer_raw:
            print(f"Trainer name normalized: '{trainer_raw}' -> '{trainer_canonical}' ({match_reason})")
        else:
            print(f"Trainer name kept as-is: '{trainer_raw}' ({match_reason})")

        return {
            "candidate_name": candidate,
            "company_name":   None,
            "round_name":     None,
            "trainer_name":   trainer_canonical,
            "hr_person_name": None,
            "canonical_host_name": None,
        }

    # ── HR  (no topic format - host is the HR person) ──────────────────────
    if department_folder == "HR":
        candidate         = pick_candidate(participants, host_email)
        hr_person_raw     = host_name or "Unknown_HR_Person"

        hr_person_canonical, match_reason = find_canonical_folder(hr_person_raw, "HR/")
        if hr_person_canonical != hr_person_raw:
            print(f"HR person name normalized: '{hr_person_raw}' -> '{hr_person_canonical}' ({match_reason})")
        else:
            print(f"HR person name kept as-is: '{hr_person_raw}' ({match_reason})")

        return {
            "candidate_name": candidate,
            "company_name":   None,
            "round_name":     None,
            "trainer_name":   None,
            "hr_person_name": hr_person_canonical,
            "canonical_host_name": None,
        }

    # ── Advanced-Training (legacy department, kept for old Zoom user depts) ─
    if department_folder == "Advanced-Training":
        internal_host = resolve_adv_training_host(participants, host_email, host_name)
        candidates    = pick_all_candidates(participants, host_email)
        group_folder  = build_group_candidate_folder(candidates)

        host_canonical, match_reason = find_canonical_folder(
            internal_host, "Advanced-Training/"
        )
        if host_canonical != internal_host:
            print(f"Adv-Training host normalized: '{internal_host}' -> '{host_canonical}' ({match_reason})")
        else:
            print(f"Adv-Training host kept as-is: '{internal_host}' ({match_reason})")

        print(f"Adv-Training candidates ({len(candidates)}): {group_folder}")

        return {
            "candidate_name":      group_folder,
            "company_name":        None,
            "round_name":          None,
            "trainer_name":        None,
            "hr_person_name":      None,
            "canonical_host_name": host_canonical,
            "all_candidates":      candidates,       # -> participants.json
        }

    # ── All other departments (generic — fuzzy host matching) ─────────────
    candidate = pick_candidate(participants, host_email)
    print(f"Candidate resolved from participants: {candidate}")

    canonical_host, match_reason = find_canonical_folder(host_name, f"{department_folder}/")
    if canonical_host != host_name:
        print(f"Host name normalized: '{host_name}' -> '{canonical_host}' ({match_reason})")
    else:
        print(f"Host name kept as-is: '{host_name}' ({match_reason})")

    return {
        "candidate_name":      candidate,
        "company_name":        None,
        "round_name":          None,
        "trainer_name":        None,
        "hr_person_name":      None,
        "canonical_host_name": canonical_host,
    }


def build_base_prefix(
    department_folder,
    host_name,
    year,
    month,
    candidate_name,
    date_only,
    time_folder,
    company_name=None,
    round_name=None,
    meeting_id=None,
    trainer_name=None,
    hr_person_name=None,
    canonical_host_name=None,    # used by generic branch
    program_folder=None,         # drives the {Program}/ segment, Training only
    interview_type=None,         # drives the {Type}/ segment, Interview-Success only
):
    """
    Interview-Success:
        Interview-Success/{Host}/{Year}/{MonthName}/{Candidate}/{Company}/{Date}/{Round}/{MeetingID}/

    Training:
        Training/{Program}/{Trainer}/{Year}/{MonthName}/{Candidate}/{Date}/{Time}/{MeetingID}/

        {Program} comes straight from the Salesforce program name, e.g.
        "Advanced Python" -> Training/Advanced-Python/...
        Legacy names stay pinned via PROGRAM_FOLDER_ALIASES, so
        "Advanced AI/ML Training" -> Training/Advanced/... exactly as before.
        No program resolvable at all -> Training/Other/...

    HR:
        HR/{HRPerson}/{Year}/{MonthName}/{Candidate}/{Date}/{Time}/{MeetingID}/

    All other departments:
        {Department}/{Host}/{Year}/{MonthName}/{Candidate}/{Date}/{Time}/{MeetingID}/

    {MonthName} is the full English month name (January..December).
    """
    meeting_id_folder = str(meeting_id).strip() if meeting_id else "Unknown_Meeting_ID"

    if department_folder == "Interview-Success":
        itype = interview_type or INTERVIEW_TYPE_FOLDER_ACTUAL

        if itype != INTERVIEW_TYPE_FOLDER_ACTUAL:
            # Internal interviews (and any other Purpose) use the GENERIC
            # layout. There is no company and no round for these, so the
            # segments are omitted entirely rather than filled with
            # Unknown_Company / Unknown_Round -- placeholders that made these
            # recordings look like malformed actual interviews.
            return (
                f"{department_folder}/"
                f"{itype}/"
                f"{host_name}/"
                f"{year}/"
                f"{month}/"
                f"{candidate_name}/"
                f"{date_only}/"
                f"{time_folder}/"
                f"{meeting_id_folder}/"
            )

        # Actual interview -- identical to the previous layout with the type
        # segment inserted. Every other segment keeps its position and meaning.
        company_name = company_name or "Unknown_Company"
        round_name   = round_name   or "Unknown_Round"

        return (
            f"{department_folder}/"
            f"{itype}/"
            f"{host_name}/"
            f"{year}/"
            f"{month}/"
            f"{candidate_name}/"
            f"{company_name}/"
            f"{date_only}/"
            f"{round_name}/"
            f"{meeting_id_folder}/"
        )

    if department_folder == "Training":
        trainer      = trainer_name or host_name or "Unknown_Trainer"
        prog_folder  = program_folder or UNKNOWN_PROGRAM_FOLDER

        return (
            f"{department_folder}/"
            f"{prog_folder}/"
            f"{trainer}/"
            f"{year}/"
            f"{month}/"
            f"{candidate_name}/"
            f"{date_only}/"
            f"{time_folder}/"
            f"{meeting_id_folder}/"
        )

    if department_folder == "HR":
        hr_person = hr_person_name or host_name or "Unknown_HR_Person"

        return (
            f"{department_folder}/"
            f"{hr_person}/"
            f"{year}/"
            f"{month}/"
            f"{candidate_name}/"
            f"{date_only}/"
            f"{time_folder}/"
            f"{meeting_id_folder}/"
        )

    # Generic layout: Date / Time / MeetingID
    host = canonical_host_name or host_name or "Unknown_Host"

    return (
        f"{department_folder}/"
        f"{host}/"
        f"{year}/"
        f"{month}/"
        f"{candidate_name}/"
        f"{date_only}/"
        f"{time_folder}/"
        f"{meeting_id_folder}/"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  S3 upload helpers
# ══════════════════════════════════════════════════════════════════════════════

def s3_object_is_secured(key, expected_size):
    """
    True if `key` already exists in S3 AND its size matches what Zoom reports.
    Lets webhook retries and the daily sweeper skip a 2 GB re-download.
    Same rule zoom-recording-cleaner uses, so the two can never disagree about
    whether a file is safely stored.
    """
    try:
        head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=key)
    except Exception:
        return False

    size = int(head.get("ContentLength", -1))
    if expected_size in (None, -1):
        return size > 0
    return size == int(expected_size)


def enqueue_delete_job(meeting_id, meeting_uuid, host_id, base_prefix,
                       department_folder, start_time, event_type):
    """
    Hand this meeting to zoom-recording-cleaner.

    We pass the prefix WE just used. The cleaner never recomputes a path, so it
    can never look in the wrong folder, decide a file is "missing", and either
    wrongly refuse to delete or wrongly re-upload.

    Both recording events (video + transcript) enqueue a job. The cleaner is
    idempotent — the second job finds Zoom already empty and no-ops.

    Never raises: a failure here must not break the upload path.
    """
    if not ZOOM_DELETE_ENABLED:
        print("ZOOM_DELETE_ENABLED=0 — not queueing Zoom cleanup")
        return
    if not DELETE_QUEUE_URL:
        print("DELETE_QUEUE_URL not set — not queueing Zoom cleanup")
        return

    msg = {
        "meeting_id":   str(meeting_id),
        "meeting_uuid": str(meeting_uuid),
        "host_id":      str(host_id or ""),
        "bucket":       S3_BUCKET_NAME,
        "prefix":       base_prefix,
        "department":   department_folder,
        "start_time":   start_time,
        "source_event": event_type,
        "enqueued_at":  datetime.now(timezone.utc).isoformat(),
    }
    try:
        resp = sqs.send_message(
            QueueUrl=DELETE_QUEUE_URL,
            MessageBody=json.dumps(msg),
            DelaySeconds=min(max(DELETE_DELAY_SECONDS, 0), 900),
        )
        print(f"Queued Zoom cleanup: meeting_uuid={meeting_uuid} -> {resp.get('MessageId')}")
    except Exception as exc:
        print(f"Could not queue Zoom cleanup for {meeting_uuid}: {exc}")


def upload_from_url(file_url, bearer_token, bucket, key):
    headers = {"Authorization": f"Bearer {bearer_token}"}
    with requests.get(file_url, headers=headers, stream=True, timeout=300) as r:
        print(f"DOWNLOAD {file_url[:120]}... -> {r.status_code}")
        r.raise_for_status()
        s3.upload_fileobj(r.raw, bucket, key)


def ensure_department_prefixes():
    for dept in (
        "Training/", "Interview-Success/", "Customer-Success/",
        "Marketing/", "COO/", "CEO/", "Executive-Assistant/", "Techsphere/",
        "HR/",
        "QMS/",
        "Business-Development/",
        "Accountant/",             # NEW
        "Operations-Manager/",     # NEW
    ):
        try:
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=dept, Body=b"")
            print(f"Ensured prefix: s3://{S3_BUCKET_NAME}/{dept}")
        except Exception as e:
            print(f"Failed to ensure prefix {dept}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  Training analysis rendezvous  (training-temp.json + SQS enqueue)
#
#  training-temp.json and result.json both live INSIDE the meeting prefix:
#      Training/{Program}/{Trainer}/{Year}/{Month}/{Candidate}/{Date}/{Time}/{MeetingID}/training-temp.json
#
#  Video and transcript arrive on SEPARATE Zoom events, transcript LAST. We act
#  only on the transcript event: by then the earlier video is already in S3, so
#  both halves exist. We verify the MP4 is actually present, resolve the Day,
#  write training-temp.json, and enqueue exactly ONE job for the EC2 worker.
#  Idempotent — guarded by `enqueued` + an existing-result.json check.
# ══════════════════════════════════════════════════════════════════════════════

def _temp_key(base_prefix):   return f"{base_prefix}training-temp.json"
def _result_key(base_prefix): return f"{base_prefix}result.json"


def read_temp_state(base_prefix):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=_temp_key(base_prefix))
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        print(f"read_temp_state failed for {base_prefix}: {exc}")
        return None


def write_temp_state(base_prefix, state):
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=_temp_key(base_prefix),
        Body=json.dumps(state, indent=2).encode(),
        ContentType="application/json",
    )


def result_exists(base_prefix):
    try:
        s3.head_object(Bucket=S3_BUCKET_NAME, Key=_result_key(base_prefix))
        return True
    except Exception:
        return False


def video_present(base_prefix):
    """True if at least one MP4 object exists under this meeting's prefix."""
    try:
        resp = s3.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=f"{base_prefix}MP4/",
            MaxKeys=1,
        )
        return resp.get("KeyCount", 0) > 0
    except Exception as exc:
        print(f"video_present check failed for {base_prefix}: {exc}")
        return False


def wake_worker():
    """
    Start the analysis EC2 worker. start_instances on an already-running
    instance is a harmless no-op, so we call it on EVERY enqueue.
    Failure is non-fatal: the job waits in SQS (retention 4 days) until the
    worker is up, so we never raise from here.
    """
    if not WORKER_INSTANCE_ID:
        print("WORKER_INSTANCE_ID not set — worker must be started manually")
        return
    try:
        ec2.start_instances(InstanceIds=[WORKER_INSTANCE_ID])
        print(f"Worker start requested: {WORKER_INSTANCE_ID}")
    except Exception as exc:
        print(f"Could not start worker ({exc}); job stays queued")


def enqueue_analysis_job(state):
    if not ANALYSIS_QUEUE_URL:
        print("ANALYSIS_QUEUE_URL not set — cannot enqueue analysis job")
        return
    msg = {
        "meeting_id":    state["meeting_id"],
        "bucket":        state["bucket"],
        "prefix":        state["prefix"],
        "day":           state["day"],
        "day_step_name": state["day_step_name"],
        "candidate":     state["candidate"],
        "trainer":       state["trainer"],
    }
    resp = sqs.send_message(QueueUrl=ANALYSIS_QUEUE_URL, MessageBody=json.dumps(msg))
    print(f"Enqueued analysis job meeting_id={state['meeting_id']} -> {resp.get('MessageId')}")
    wake_worker()   # boot the EC2 worker if it is stopped


def training_rendezvous(event_type, meeting_id, base_prefix, candidate_name,
                        trainer_name, program_name=None, program_folder=None):
    """
    Training department ONLY. Triggered on the TRANSCRIPT event — by then the
    video from the earlier recording.completed event is already in S3, so both
    halves exist. Verifies the MP4 is actually present, resolves the Day, writes
    training-temp.json, and enqueues exactly ONE analysis job for the EC2 worker.
    Idempotent against duplicate / redelivered events.

    program_name is passed in from process_recording_event so the folder and the
    analysis decision are always made from the SAME value — they can no longer
    disagree because of a second Salesforce round-trip.
    """
    # Act only on the transcript event. The video event just uploads and returns.
    if event_type != "recording.transcript_completed":
        print(f"Video event stored for {base_prefix} — waiting for transcript")
        return

    # Don't enqueue twice (SQS is at-least-once; Zoom can retry webhooks).
    if result_exists(base_prefix):
        print(f"result.json already present for {base_prefix} — skip enqueue")
        return
    existing = read_temp_state(base_prefix)
    if existing and existing.get("enqueued"):
        print(f"Already enqueued for {base_prefix} — skip")
        return

    # Confirm the video half actually landed in S3 (guards a failed video upload).
    if not video_present(base_prefix):
        print(f"Transcript arrived but no MP4 under {base_prefix} — skip; fallback sweep will catch")
        return

    (day, step_name, sf_candidate, sf_program, _trainer_sf,
     _host_user_id, _purpose, sf_session_type) = lookup_training_day_from_sf(meeting_id)

    # Prefer the program already resolved for the path.
    resolved_program = program_name or sf_program
    resolved_folder  = program_folder or resolve_program_folder(resolved_program)[0]

    training_type = classify_training_type(resolved_program)
    print(f"[CLASSIFY] meeting={meeting_id} program={resolved_program!r} -> {training_type} "
          f"folder={resolved_folder!r} (enforce={ENFORCE_TRAINING_TYPE_FILTER})")

    # UNCHANGED PRODUCTION RULE — NORMAL + RETRAINING only.
    if ENFORCE_TRAINING_TYPE_FILTER and training_type not in ("NORMAL", "RETRAINING"):
        print(f"Skipping analysis: meeting={meeting_id} classified as {training_type} (not NORMAL)")
        return

    state = {
        "meeting_id":       str(meeting_id),
        "prefix":           base_prefix,
        "bucket":           S3_BUCKET_NAME,
        "candidate":        sanitize_name(str(sf_candidate)) if sf_candidate else candidate_name,
        "trainer":          trainer_name,
        "day":              day,
        "day_step_name":    step_name,
        "program":          resolved_program,
        "program_folder":   resolved_folder,
        "session_type":     sf_session_type,
        "training_type":    training_type,
        "video_ready":      True,
        "transcript_ready": True,
        "enqueued":         False,
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }
    write_temp_state(base_prefix, state)

    enqueue_analysis_job(state)
    state["enqueued"]    = True
    state["enqueued_at"] = datetime.now(timezone.utc).isoformat()
    write_temp_state(base_prefix, state)
    print(f"training-temp.json created + job enqueued for {base_prefix} (day={day})")


# ══════════════════════════════════════════════════════════════════════════════
#  Webhook payload extraction helpers  (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def extract_payload_object(body):
    if "object" in body:
        return body.get("account_id"), body.get("object", {})
    payload = body.get("payload", {})
    return payload.get("account_id"), payload.get("object", {})


def extract_download_token(body):
    for candidate in (
        body,
        body.get("payload", {}),
        body.get("payload", {}).get("object", {}),
    ):
        if isinstance(candidate, dict) and candidate.get("download_token"):
            return candidate["download_token"]
    return None


def get_recording_files_from_body(body, obj):
    files = obj.get("recording_files", [])
    if files:
        print(f"Found {len(files)} recording files in webhook payload")
        return files
    files = body.get("payload", {}).get("object", {}).get("recording_files", [])
    if files:
        print(f"Found {len(files)} recording files in nested payload")
        return files
    print("No recording_files found in webhook payload")
    return []


def choose_download_token(file_info, webhook_token, api_token):
    url = file_info.get("download_url", "") or ""
    return webhook_token if "zoom.us/rec/webhook_download/" in url else api_token


# ══════════════════════════════════════════════════════════════════════════════
#  Core event processor
# ══════════════════════════════════════════════════════════════════════════════

def process_recording_event(body, event_type):
    account_id, obj = extract_payload_object(body)

    meeting_id   = obj.get("id")
    meeting_uuid = obj.get("uuid")
    host_id      = obj.get("host_id")
    start_time   = obj.get("start_time", "")
    host_email   = obj.get("host_email", "")
    topic        = obj.get("topic", "")

    print("=== RESOLVED RECORDING EVENT ===")
    print(json.dumps(body, indent=2))
    print(f"event_type   = {event_type}")
    print(f"account_id   = {account_id}")
    print(f"meeting_id   = {meeting_id}")
    print(f"meeting_uuid = {meeting_uuid}")
    print(f"host_id      = {host_id}")
    print(f"host_email   = {host_email}")
    print(f"start_time   = {start_time}")
    print(f"topic        = {topic}")

    if not meeting_id or not meeting_uuid or not host_id:
        raise RuntimeError("Missing meeting_id/meeting_uuid/host_id in SQS payload")

    ensure_department_prefixes()

    secret_obj       = get_zoom_secret()
    api_access_token = get_s2s_access_token(secret_obj)
    webhook_dl_token = extract_download_token(body)

    print(f"Webhook download_token {'found' if webhook_dl_token else 'NOT found'}")

    host              = zoom_get(api_access_token, f"/users/{host_id}")
    department_folder = normalize_department(host.get("dept") or "")

    if not department_folder:
        print(f"Skipped host - department not allowed. host_id={host_id}, dept={host.get('dept')}")
        return

    host_name = sanitize_name(
        ((host.get("first_name") or "") + " " + (host.get("last_name") or "")).strip()
        or host.get("display_name")
        or host_email
        or "Unknown_Host"
    )

    encoded_uuid = double_encode_uuid(meeting_uuid)
    participants = zoom_get(
        api_access_token,
        f"/past_meetings/{encoded_uuid}/participants?page_size=300",
    ).get("participants", [])
    print(f"participants_count = {len(participants)}")

    # Resolve the program BEFORE building the path — build_base_prefix() and the
    # candidate-naming logic both need it, and they run on EVERY webhook event
    # (not just the transcript one), so this has to happen here rather than
    # inside training_rendezvous(). Training department only.
    program_name    = None
    program_folder  = None
    trainer_name_sf = None
    session_type_sf = None
    if department_folder == "Training":
        (_, _, _, sf_program, trainer_name_sf, host_user_id,
         purpose_value, session_type_sf) = lookup_training_day_from_sf(meeting_id)
        program_name  = sf_program
        program_source = "salesforce"

        # Salesforce couldn't tell us — fall back to matching the Zoom topic
        # against the live program list rather than dumping into Other/.
        if not program_name:
            topic_program = program_name_from_topic(topic)
            if topic_program:
                program_name   = topic_program
                program_source = "zoom-topic-FALLBACK"
                print(f"[PROGRAM-FALLBACK] meeting={meeting_id} Salesforce gave no program; "
                      f"matched topic {topic!r} -> {program_name!r}. "
                      f"FIX THE SALESFORCE PERMISSION -- this fallback is not authoritative.")
            else:
                program_source = "none"

        # Reconcile against folders already in S3: exact match, or the same
        # name in different casing, reuses the existing folder. Anything else
        # is genuinely new.
        program_folder, folder_reason = resolve_program_folder(program_name)

        # First recording for a brand new program -> create its folder now.
        # No-op when the folder already exists.
        ensure_training_program_folder(program_folder, folder_reason)

        print(f"[PROGRAM-EARLY] meeting={meeting_id} program={program_name!r} -> "
              f"folder={program_folder!r} ({folder_reason}) (source={program_source}) "
              f"type={classify_training_type(program_name)} "
              f"session_type={session_type_sf!r} "
              f"group={is_group_session(program_name, participants, host_email, session_type_sf)} "
              f"analyze={should_analyze_program(program_name)} "
              f"trainer_sf={trainer_name_sf!r} host_user_id={host_user_id!r} purpose={purpose_value!r}")

    storage_info = resolve_storage_info(
        department_folder=department_folder,
        topic=topic,
        participants=participants,
        host_email=host_email,
        meeting_id=meeting_id,
        host_name=host_name,
        program_name=program_name,
        program_folder=program_folder,
        trainer_name_sf=trainer_name_sf,
        session_type=session_type_sf,
    )

    candidate_name      = storage_info["candidate_name"]
    company_name        = storage_info["company_name"]
    round_name          = storage_info["round_name"]
    trainer_name        = storage_info["trainer_name"]
    hr_person_name      = storage_info["hr_person_name"]
    canonical_host_name = storage_info.get("canonical_host_name")
    interview_type      = storage_info.get("interview_type")

    print(f"final_candidate_name  = {candidate_name}")
    if interview_type:
        print(f"final_interview_type  = {interview_type}")
    if program_folder:
        print(f"final_program_folder  = {program_folder}")
    if trainer_name:
        print(f"final_trainer_name    = {trainer_name}")
    if hr_person_name:
        print(f"final_hr_person_name  = {hr_person_name}")
    if company_name:
        print(f"final_company_name    = {company_name}")
    if round_name:
        print(f"final_round_name      = {round_name}")
    if canonical_host_name:
        print(f"final_host_name       = {canonical_host_name}")

    recording_files = get_recording_files_from_body(body, obj)
    if not recording_files:
        print("Falling back to /meetings/{meeting_id}/recordings API")
        recording_files = zoom_get(
            api_access_token, f"/meetings/{meeting_id}/recordings"
        ).get("recording_files", [])

    print(f"recording_files_count = {len(recording_files)}")
    if not recording_files:
        print(f"No recording files found for meeting_id={meeting_id}")
        return

    year        = start_time[:4]  if len(start_time) >= 4  else "UnknownYear"
    month       = month_folder_name(start_time)
    date_only   = start_time[:10] if len(start_time) >= 10 else "UnknownDate"
    time_folder = build_time_folder_ist(start_time)

    base_prefix = build_base_prefix(
        department_folder=department_folder,
        host_name=host_name,
        year=year,
        month=month,
        candidate_name=candidate_name,
        company_name=company_name,
        date_only=date_only,
        round_name=round_name,
        time_folder=time_folder,
        meeting_id=meeting_id,
        trainer_name=trainer_name,
        hr_person_name=hr_person_name,
        canonical_host_name=canonical_host_name,
        program_folder=program_folder,
        interview_type=interview_type,
    )

    try:
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=base_prefix, Body=b"")
        print(f"Ensured base prefix: s3://{S3_BUCKET_NAME}/{base_prefix}")
    except Exception as e:
        print(f"Could not create base placeholder prefix: {e}")

    # ── Attendance record — written for EVERY session, 1:1 or group ───────
    #
    # Previously only group sessions got a participants.json, because it was a
    # side effect of building the grouped folder name. That left 1:1 sessions
    # with no record of who actually attended -- and when more than one person
    # joined, only the one in the folder name was recoverable. Everyone else
    # was simply lost.
    #
    # Now every session gets one, containing EVERY attendee with their role
    # (host / internal / candidate), so attendance is answerable from S3 alone
    # regardless of what the folder happens to be called.
    #
    # Purely additive: nothing reads this file, so it cannot affect the
    # analyzer, the linker, Zoom cleanup or any existing path.
    try:
        attendees = build_attendance_record(participants, host_email)
        candidates_only = [a for a in attendees if a["role"] == "candidate"]

        manifest = {
            "version":          2,
            "meeting_id":       str(meeting_id),
            "meeting_uuid":     meeting_uuid,
            "topic":            topic,
            "department":       department_folder,
            "program":          program_name,
            "program_folder":   program_folder,
            "session_type":     session_type_sf,
            "training_type":    classify_training_type(program_name),
            # NOTE: no "day" here -- the day number is resolved later, during
            # the transcript-event rendezvous, and is not available at upload
            # time. It lives in training-temp.json alongside this file.
            "host":             canonical_host_name or host_name,
            "host_email":       host_email,
            "trainer":          trainer_name,
            "start_time":       start_time,
            "recorded_at_utc":  datetime.now(timezone.utc).isoformat(),
            "s3_prefix":        base_prefix,
            "attendee_count":   len(attendees),
            "candidate_count":  len(candidates_only),
            "attendees":        attendees,          # everyone, with roles
            "candidates":       candidates_only,    # back-compat with v1 readers
        }
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"{base_prefix}participants.json",
            Body=json.dumps(manifest, indent=2).encode(),
            ContentType="application/json",
        )
        print(f"participants.json written: {len(attendees)} attendee(s), "
              f"{len(candidates_only)} candidate(s)")
    except Exception as exc:
        # Never fatal -- the recording upload matters more than the manifest.
        print(f"Failed writing participants.json (non-fatal): {exc}")

    uploaded_count = failed_count = skipped_count = 0

    for idx, rf in enumerate(recording_files, start=1):
        try:
            raw_file_type = (rf.get("file_type") or "UNKNOWN").upper()
            file_ext      = (rf.get("file_extension") or "").lower()
            download_url  = rf.get("download_url")
            recording_id  = sanitize_name(rf.get("id") or f"{meeting_id}_{idx}")

            print(f"=== RECORDING FILE {idx} ===")
            print(json.dumps(rf, indent=2))

            if raw_file_type not in ALLOWED_FILE_TYPES:
                skipped_count += 1
                print(f"Skipping unwanted file type: {raw_file_type}")
                continue

            if not download_url:
                print(f"Skipping - no download_url for recording_id={recording_id}")
                failed_count += 1
                continue

            bearer = choose_download_token(rf, webhook_dl_token, api_access_token)
            if not bearer:
                print(f"Skipping - no bearer token for recording_id={recording_id}")
                failed_count += 1
                continue

            filename = f"{recording_id}.{file_ext}" if file_ext else recording_id
            s3_key   = f"{base_prefix}{raw_file_type}/{filename}"

            # Idempotency: Zoom retries webhooks and the daily sweeper replays
            # old meetings. Do not pull a 2 GB MP4 twice.
            raw_size = rf.get("file_size")
            try:
                expected_size = int(raw_size) if raw_size not in (None, "") else None
            except Exception:
                expected_size = None

            if s3_object_is_secured(s3_key, expected_size):
                skipped_count += 1
                print(f"Already in S3 (size verified) — skipping download: s3://{S3_BUCKET_NAME}/{s3_key}")
                continue

            print(f"Uploading to: s3://{S3_BUCKET_NAME}/{s3_key}")
            upload_from_url(download_url, bearer, S3_BUCKET_NAME, s3_key)
            uploaded_count += 1

        except Exception as exc:
            failed_count += 1
            print(f"Failed uploading recording file #{idx}: {exc}")

    print(
        f"Done - meeting_id={meeting_id}, dept={department_folder}, "
        f"program={program_name!r}, folder={program_folder!r}, "
        f"candidate={candidate_name}, trainer={trainer_name}, hr_person={hr_person_name}, "
        f"uploaded={uploaded_count}, failed={failed_count}, skipped={skipped_count}"
    )

    # ── Training analysis rendezvous (runs ONLY for Training) ───────────────
    if department_folder == "Training":
        try:
            training_rendezvous(
                event_type=event_type,
                meeting_id=meeting_id,
                base_prefix=base_prefix,
                candidate_name=candidate_name,
                trainer_name=trainer_name,
                program_name=program_name,
                program_folder=program_folder,
            )
        except Exception as exc:
            print(f"Training rendezvous failed (meeting_id={meeting_id}): {exc}")

    # ── Hand this meeting to zoom-recording-cleaner ────────────────────────
    #    Runs for EVERY department. The cleaner re-reads Zoom, re-verifies every
    #    file against S3 itself, and deletes from Zoom only if all of them check
    #    out. It does not trust the counters above.
    enqueue_delete_job(
        meeting_id=meeting_id,
        meeting_uuid=meeting_uuid,
        host_id=host_id,
        base_prefix=base_prefix,
        department_folder=department_folder,
        start_time=start_time,
        event_type=event_type,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Lambda entry-point
# ══════════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    print("=== LAMBDA EVENT ===")
    print(json.dumps(event))

    for record in event.get("Records", []):
        try:
            body = json.loads(record["body"])
        except Exception as exc:
            print(f"Failed to parse SQS record body: {exc}")
            print(f"Raw body: {record.get('body')}")
            raise

        print("=== SQS MESSAGE BODY ===")
        print(json.dumps(body, indent=2))

        event_type = body.get("event")
        if event_type in ("recording.completed", "recording.transcript_completed"):
            process_recording_event(body, event_type)
        else:
            print(f"Ignored event type: {event_type}")

    return {"statusCode": 200}