# Prior-Art Ledger - GH #1148, Burkina Faso interview status

Baseline: `f8d236694c26fe9e709c5548f02c70f74ea72c33`, 2026-10-21.
Search tier: ripgrep + git; no GitNexus tool was available.
Inherits [STANDING.md](STANDING.md). Diagnosis only; no exposure policy or implementation.

## §1 Task, restated

Establish whether held Burkina Faso EMC 2014 cover-page interview results can
support a household-specific denominator for food flows. Preserve passage,
column, source, missingness, and disagreement. GhanaLSS is outside this lane.
The live issue describes a later API: this release baseline has Phase I only.

## §2 Existing machinery

| Symbol | Anchor at baseline | Existing behavior / tests | Decision |
|---|---|---|---|
| `RecallRecord`, `attach`, `merge_attrs` | `lsms_library/recall.py:118` | Immutable JSON-compatible wave records and attrs; `tests/test_recall_record.py` | reuse |
| `Country._finalize_result` | `lsms_library/country.py:4078` | Attaches recall on the read path | reuse |
| `Country._FOOD_DERIVED` | `lsms_library/country.py:6069`, generated method `:6150` | Generated food methods lack `period`; unknown kwarg raises before reading | dependency on #851 |
| `food_expenditures_from_acquired` | `lsms_library/transformations.py:2932` | Selects Expenditure and sums visit away | reuse; counts cannot ride through as an ordinary numeric column |
| `food_quantities_from_acquired` | `lsms_library/transformations.py:3088` | Selects Quantity, converts or retains native units, sums visit away | reuse |
| Burkina passage stamps / hook | `countries/Burkina_Faso/2014/_/data_info.yml:198`, `mapping.py` | Filename-to-visit stamps, missing-unit sentinel, quantity restoration; `tests/test_burkina_faso_2014_passages.py` | reuse |
| `get_dataframe`, `get_data_file` | `lsms_library/local_tools.py:1686`, `data_access.py:2444` | Sanctioned reads; this diagnosis supplied verified private local files and disabled source networking | reuse |
| `Wave._input_hash`, `Country._table_cache_hash` | `lsms_library/country.py:1209`, `:4184` | Whole wave config / country scheme / build inputs are hashed | reuse; measure any future implementation |

Read-only comparison: existing ref `origin/fix/851-recall-period` at
`cf117d101187799b9f985c0686a567adb2177937` contains `n_asks_for`,
`exposure_days`, `ask_shortfall`, and `to_period`. It distinguishes one
Quantity ask from four Expenditure asks and discloses a food-value-carrier
shortfall in `attrs['period']`. It is not implemented at this baseline.

## §3 Definitions and conventions in force

- Interface and data retention: `CLAUDE.md:3`; `STANDING.md` §§3-4.
- Four independent 7-day expenditure recalls, but a quantity grid only at
  passage 1: `Burkina_Faso/_/CONTENTS.org:968` and `:984`.
- Passage is stamped from each food filename, not an invented food column:
  `CONTENTS.org:978`. Other modules do have passage columns; that does not
  change the food-file fact.
- Questionnaire, page 1, A13: codes 1/2 say interview accepted / accepted
  reluctantly. They do not literally say questionnaire completed. Code 3 is
  abandonment; 4 temporary absence; 5 refusal; 6 other.
- Report, page 22, table 1-2: all 10,800 households participated in passage 1;
  accepted-plus-reluctant totals are 10,800 / 10,619 / 10,221 / 10,168.
- Dates are provenance, not elapsed-day denominators: charter
  `.coder/charter-recall-windows.md` §0.

## §4 Invariants and assumptions

- An accepted status counts even when there is no served food row. Missing
  carrier/status, contradictory carriers, and undecoded result codes remain
  unknown. No absence of purchases is treated as a missed interview.
- Raw `(zd, menage)` keys need the existing 2014 `i` formatter and exactly the
  same panel-ID handling as food before a future join. Two retained keys are
  outside the passage-1 panel; do not silently discard or remap them.
- Source carriers and the official report disagree. Neither is silently
  promoted to authoritative precedence. See the aggregate report below.
- Source code 6 changes meaning: other in p1/p2; household lost in the p3
  dictionary. P4 has no direct retained value dictionary for `res_entr4`.
- Period conversion and its missing-household contract are shared work,
  following #851. Do not add a parallel country conversion implementation.
- `period=None`, original food values, and price behavior must remain exact.

## §5 Reuse decision and implementation boundary

Reuse the current food transforms, passage preservation, sanctioned IO,
crosswalk handling, and cache gates. Recover/integrate #851 before adding its
household-exposure consumer. A future country producer should return status
evidence independently of food-row presence, at `(t, i, visit, carrier)` grain,
preserving raw code, source file/variable, dates where held, and conflicts.
A shared resolver may then produce a `(t, i)` exposure frame with separate
Expenditure and Quantity columns. Its policy for partial/missing/conflicting
status is an explicit prerequisite, not a choice made in this diagnosis.

This documentation change is outside build fingerprints. A future wave
config/script plus country scheme change implies a Burkina Faso re-warm;
shared read-path changes require their own fingerprint probe. No stash probe
or tracked-code edit was authorized during the diagnosis.

## §6 Open questions / freeze

1. Resolve eight p3 cross-carrier conflicts and the report/raw-result count
   disagreement; identify authoritative original household cover records if held elsewhere.
2. Bind the p4 numeric dictionary directly, and distinguish accepted interview
   from a completed food ask without assuming that partial interviews are zero.
3. Decide how `period=` handles an unknown household inside a status-bearing
   wave. #1148 permits declared fallback for waves without status, not an
   undocumented fill of missing household results.
4. Finish #851 integration; retain its historical 9.3% / -3.94% disclosure as a
   food-carrier estimate, separate from this status census.

Evidence and proposed verification:
`slurm_logs/isa_backlog_2026-10-21/burkina-1148/REPORT.md`.
Config/record checks: 35 passed, 2 deselected. Raw records, household IDs,
warnings and exploratory rate arithmetic remain in private node-local evidence.

Sue.
