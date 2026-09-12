# Prior-Art Ledger - GhanaLSS interviewer visit dates

Search tier: ripgrep + git floor, source/config reads and metadata probes.
Inherits STANDING.md. Scope authorized by parent before implementation.

## §1 Task, restated

Extend GhanaLSS interview_date to explicit calendar visits while preserving
all existing intake sources and parseable intake dates. Add Y00A Round Two
for 1987-88/1988-89, S0B visits 2-11 for 1991-92, and SEC0C visits 2-3 for
1998-99. Keep 2005-06/2012-13/2016-17 intake-only at visit 1. Do not change
food, infer missing dates, repair reported years, or choose competing pages.

## §2 Existing machinery

| symbol | source | existing test surface | decision |
|--------|--------|-----------------------|----------|
| melt_visit_intervals | local_tools.py:2710 | dynamic import fingerprint test in tests/test_build_transform_hash.py; no dedicated helper behavior tests found | reuse shared start-only melt |
| Int_t | GhanaLSS wave mapping.py / year module | parser test precedent tests/test_timor_leste_interview_date.py | reuse unchanged parsers |
| Wave.column_mapping | country.py | existing YAML/source tests | reuse explicit mapping: Int_t for suffixed date columns |
| Wave.grab_data | country.py | table structure suite | reuse YAML dfs merge, then df_edit hook |
| GhanaLSS country formatting functions | GhanaLSS/_/ghanalss.py | GhanaLSS focused tests | extend with one shared interview_date hook |
| Country._finalize_result | country.py | STANDING.md §2 | reuse canonical index/dtype handling |

## §3 Definitions & conventions in force

- Canonical schema and index handling: STANDING.md §3 and
  lsms_library/data_info.yml, Index Info and Columns.interview_date.
- CONTENTS.org, 'The repeated-visit design, wave by wave': calendar visit is
  distinct from question number. GLSS1/GLSS2 food visit=1 is a consumption
  occasion although Section 12 is asked at the second household contact.
  Food is outside scope; no automatic same-visit join is implied.
- slurm_logs/2026-09-11_ghanalss_visit_dates/{wave}.org identify interviewer
  dates separately from proposed, verification, check-up and data-entry dates.
- melt_visit_intervals docstring: 'A visit whose start AND end are both NaT
  is DROPPED'. A missing date is not proof a visit never happened.

## §4 Invariants & assumptions

- Follow STANDING.md §4 for IO and configuration roots.
- Preserve existing intake date sources and parsers; do not replace GLSS3
  S0A with S0B or GLSS4 SEC0A with SEC0C. Their first-date fields disagree.
- Early household IDs are HID; GLSS3/4 use existing i(clust, nh) formatting.
- Keep parseable anomalous dates (including wrong-looking years, negative
  gaps, and identical first/second dates). Calendar-impossible dates remain
  NaT under the existing parsers, never replaced with proposed/check-up dates.
- The reused helper omits undated/invalid slots. Existing intake-only NaT rows
  can therefore disappear; quantify and test this explicitly rather than claim
  unchanged row coverage. No raw date-component preservation is promised.
- GLSS3 visits 9-11 are structurally absent for rural fieldwork; do not
  interpolate or infer a schedule from analysis Rural (11 urban clusters used
  rural fieldwork schedules).
- No Part B offsets or later GLSS5 visits in this stage. Shared schema becomes
  (t, i, visit); all seven waves participate through the shared hook.
- Tests use synthetic data for source precedence/shape and shared get_dataframe
  for source verification. Any real build gets private L2 and shared L1 only.

## §5 Reuse decision

| behavior | decision | reason |
|----------|----------|--------|
| calendar date parsing | reuse existing Int_t | preserves established source interpretation |
| per-visit reshape | reuse melt_visit_intervals | established integer visit and start-only API |
| intake plus later-page merge | reuse YAML dfs | existing source joins express this directly |
| all-wave dispatch | extend GhanaLSS interview_date hook | one wrapper avoids seven duplicate melts |
| GLSS4 long SEC0C slots | extend wave secondary df_edit hook | retain reported slots 2/3 and unstack without aggregation before YAML merge |
| source wiring regression | new targeted test module | no existing GhanaLSS date-wiring test found |

## §6 Open questions for the human

The user approved inferred GLSS4 Part B renumbering with provenance and
independent review. This ledger initially covers the unambiguous implementation
subtask; the parent coordinates the Part B extension and will record its rule
before implementation. Conflicting-date observation identity remains a pending
API decision. GLSS5 later slots are outside this subtask. The parent owns
CONTENTS.org documentation and commits.

## Verification

Unambiguous first stage complete; parent coordinates approved GLSS4 Part B
extension and independent review. No source repair or competing first-date
fallback was added.

- Shared country hook: OK (sections 2-5), delegates directly to the established
  melt_visit_intervals helper. No duplicate date parsing/reshape machinery.
- Four wave YAML mappings and GLSS4 secondary hook: OK (sections 3-5), existing
  intake sources/parsers retained; SEC0C unstack refuses duplicate keys.
- Synthetic source tests: 15 failed before implementation (4.99 seconds),
  15 passed afterward (4.09 seconds), executing Wave.interview_date with actual
  YAML/formatters and private temporary L2. Covers source precedence, intake-only
  households, missing-middle ordinals, invalid-date omission, anomalous parsed
  dates, and all seven waves' explicit visit 1.
- Private-L2/shared-L1 raw verification: every served visit-1 date equals its
  original first-source/parser extraction exactly; all seven indexes unique.
  Date rows by wave: 6281, 6382, 38430, 17989, 8687, 16757, 14008.
  Existing intake NaT rows omitted: GLSS3 3, GLSS6 15, GLSS7 1; all others 0.
  These omissions are the documented shared-helper contract, not date repairs.
- Reproduction: /tmp/ghana_visit_dates_real.py and
  /tmp/ghana_visit_dates_real.json; private L2 /tmp/ghana-visits-l2-l65uzms_;
  shared dvc-cache is symlinked only into that private data root. No shared L2
  cache was cleared, read as derived input, or overwritten.
- Targeted command: LSMS_BUILD_WORKERS=1 PYTHONPATH=<worktree>
  LSMS_COUNTRIES_ROOT=<worktree>/lsms_library/countries taskset -c 0-15
  <main-repo>/.venv/bin/python -m pytest tests/test_ghanalss_interview_date.py
  --no-purge -q. No food or sample builds were performed.

## Authorized inference extension (2026-09-12)

The user now authorizes chronology-only GLSS4/GLSS5 assignment under explicitly
registered modelling assumptions, superseding the first-stage exclusions above.
Dates remain reported; the constructed value is the calendar visit index.
Distinct valid dates are assumed to represent distinct scheduled contacts.
Enumerate monotone injections into 1..7 (GLSS4) or 1..11 (GLSS5), allowing holes;
serve an inferred date only when all nonempty candidates assign the same visit.
No offset fallback, gap-length rule, date repair, or synthetic date. Concordant
GLSS4 SEC0C anchors supplement existing intake; contradictory anchors prevent
inference. GLSS5 Part B intake disagreement likewise prevents inference.
Reported intake/Part A anchors remain served with their original precedence.
All original A/B/C or sec0/secb0/secb1/secb2 rows and fields are recoverable at
input grain through derivation_inputs, with source/source_record identity and
nullable assigned_visit/reason annotations. Exact date ties coalesce only in the
event model, never in raw inputs. Accepted but mistyped dates remain a stated
limitation of the model rather than a silently corrected or rejected value.

Reuse decisions: existing wave Int_t parsing semantics and sanctioned
get_dataframe; existing derivations registry/column/input-callable contract
(derivations.py and docs/guide/derivations.md). New visit_dates.py is warranted
because melt_visit_intervals reshapes known ordinals and cannot infer them from
multiple conflicting long-form source pages. Dedicated synthetic tests verify
candidate uniqueness, source preservation, and inference rather than reshape.

Raw-field preservation correction during verification: GLSS5 sec0 already has
an original numeric variable named reason. Diagnostic annotations therefore use
assignment_reason (not reason); the original reason values/dtype remain intact.
A targeted collision regression pins this, alongside full-source field equality.

### Inference verification (price_diagnosis)

- OK (authorized extension): fixed functions enumerate all monotone injections
  and serve only consensus assignments. No offset fallback or spacing rule.
  Inputs retain every record and original variable; assignment_reason avoids
  collision with GLSS5's original numeric reason field.
- OK (reuse): _load_raw memoizes each wave's existing Int_t parser by triplet;
  no replacement date parser. Inference annotations accumulate outside pandas
  extension arrays to avoid repeated full Arrow-column copies.
- 12 focused synthetic tests passed in 3.37 seconds, including duplicate-code
  examples, exact duplicates, missing slots/partial identification, invalid and
  unmatched records, contradictory anchors/excess dates, source-order invariance,
  raw replay, original-reason preservation, and the disclosed accepted-typo case.
- Real source checks through get_dataframe: GLSS4 59,971 input records -> 37,702
  date rows, 19,713 inferred; GLSS5 189,805 input records -> 69,127 date rows,
  60,440 inferred. All original input columns, values AND dtypes retained.
- Every original parseable intake date remains exact: 5,998 / 8,687 rows.
  Every inferred output joins a raw input on household, date and assigned_visit:
  19,726 / 120,244 links (more links than output rows because raw duplicates
  and overlapping pages remain intact). No served date is absent from inputs.
- One GLSS4 visit-2 date is recovered from a reported B date when C's visit-2
  triplet is impossible, via unique anchored ordering. This is a derived visit
  assignment, not a correction of the impossible C date.
- Timing: GLSS4 31.48 s; GLSS5 30.94 s (source loading 0.37 / 0.62 s).
  No L2 writes/builds in this check; only /tmp verification artefacts.
- Reproduce: /tmp/verify_glss_visit_inference.py (currently GLSS5 selected);
  /tmp/verify_glss_visit_inference.log and /tmp/verify_glss5_visit_inference.log;
  /tmp/glss_visit_inference_{1998-99,2005-06}.json. Run with worktree PYTHONPATH
  and LSMS_COUNTRIES_ROOT, LSMS_BUILD_WORKERS=1, main venv Python, taskset16-23.
