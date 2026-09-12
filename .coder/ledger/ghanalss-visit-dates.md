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

Pending implementation. Expected GLSS3 source-preserving date count: 38,430
(S0A valid intake 4,549 plus S0B valid visits 2-11). Early-wave calendar-valid
counts: 1987-88 3,147 + 3,134; 1988-89 3,194 + 3,188. These preserve parseable
out-of-survey years rather than applying historical audit filtering.
