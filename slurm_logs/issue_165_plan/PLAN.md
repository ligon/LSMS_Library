# Implementation Plan — GH Issue #165 (with #173 folded in)

**Author**: scrum-master session, 2026-04-13
**Source agents**: Plan agent (task a62428e7…) + Related-issues scan agent (task aef73469…) + Step 1 audit agent (task a3a0ee21…)
**Status**: PARTIALLY DISPATCHED. Prompts A (done), B (in flight), C (in flight). Prompt D superseded by audit findings — see "Scope revision after Step 1 audit" below.

## Scope revision after Step 1 audit (2026-04-13 ~16:45 PDT)

The Step 1 audit (`slurm_logs/issue_165_plan/step1_audit.md`) confirmed:

- **Togo 2018: GO.** `s01_me_tgo2018.dta` has DOB columns `s01q03a/b/c`, 100% of null-Age rows have birth year, month is integer (not French text — do NOT copy Senegal's `month_map`), no `9999` sentinel.
- **EHCVM harmonised-file batch (9 entries: Benin, Burkina_Faso ×2, CotedIvoire, Guinea-Bissau, Mali ×2, Niger ×2): BLOCK.** The `ehcvm_individu_*.dta` harmonised files carry only a pre-computed integer `age` column — **no DOB columns at all**. `age_handler()` has nothing to recover from.

**Consequence**: Prompt D (EHCVM fan-out) as originally drafted is dead. The EHCVM-batch Age concern reduces to the dtype-consistency work covered by Prompt B.

**Open question for the scrum master**: The audit only checked the EHCVM harmonised file per country. Raw section files (e.g. `s01_me_mli2018.dta`, `s01_me_bfa2018.dta`) may also exist in each country's `Data/` dir and may have the same DOB columns Togo uses. If so, switching the EHCVM countries' `household_roster` to read the raw section file (instead of `ehcvm_individu`) would unblock the fan-out. This is a **follow-up audit** to decide after Togo proves the pattern end-to-end.

## Problem restatement

`age_handler()` (`lsms_library/local_tools.py:1298`) is a well-tested helper that recovers an individual's age from date-of-birth components, prioritising DOB → age-in-years → interview-year minus birth-year. Only **Senegal** (both waves) and a local duplicate in **Niger** use it. Every other country maps `Age: <single_col>` directly, leaving recoverable Age values as `NaN` whenever that declared column is sparse.

The motivating case is **Togo 2018** with 94.5% of rows carrying `NaN` Age (tracked as issue **#164**). The 2026-04-13 `household_roster` rescan surfaced related Age-quality regressions across 5 countries (#173).

## Scope — with #173 folded in

**In scope (this issue):**

1. **Per-country**: adopt `age_handler()` in YAML + wave-script for Togo (proof-of-concept) and the EHCVM batch (Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau, Mali, Niger — 2018+ waves). Uses harmonised `ehcvm_individu_*.dta` files.
2. **Framework** (folded from #173): fix the Age-dtype coercion path so `Feature('household_roster')` and `Country(X).household_roster()` return `Age` as `Int64` (or nullable `Int32`) consistently, not `object` / `float64`. Current dtype inconsistency across countries shows in the 2026-04-13 rescan: Albania / Guyana / Pakistan / Tajikistan `Float64`; Senegal `float64`; everyone else `Int64`.
3. **Housekeeping**: fix the `age_handler_wrapper` closure bug at `local_tools.py:1392–1393` (`interview_year` rebound in closure → `UnboundLocalError` if ever called with a column name). No production code currently hits this path, but the bug blocks adoption.
4. **Unit tests** for `age_handler()` (none exist) and a regression test for the dtype-coercion framework fix.

**Out of scope / defer:**

- Senegal's remaining 10.5% Affinity-null rate (not Age; separate root cause — NA Relationship, ~15k rows).
- Age top-coding outliers (CotedIvoire 131, Niger 130) — 1–3 rows each, not sentinels, lower priority.
- Armenia / Guyana / Serbia-and-Montenegro never-built Age coverage — these are blocked on data availability, not `age_handler` adoption (issue #172).
- Uganda / Ethiopia / Nigeria moderate Age gaps (6–8%) — audit per-wave codebook before touching.
- `individual_education` harmonisation (#171) — orthogonal harmonisation task; same structural pattern as #165 but different feature.

## Affected files

| File | Change type | Notes |
|---|---|---|
| `lsms_library/countries/Togo/2018/_/data_info.yml` | Extend Age map | Add `s01q03a/b/c` + `-1`/`"-1"` null mapping |
| `lsms_library/countries/Togo/2018/_/2018.py` | New `household_roster(df)` fn | Call `tools.age_handler(...)` |
| `lsms_library/countries/{EHCVM}/{wave}/_/data_info.yml` | Extend Age map | Add `annee_nais`, `mois_nais` + `-1` null |
| `lsms_library/countries/{EHCVM}/{wave}/_/{wave}.py` | New / extend `household_roster(df)` | Pattern: Senegal 2018-19 |
| `lsms_library/local_tools.py:~1392` | Closure bug fix | Rename local var to avoid shadowing `interview_year` |
| `lsms_library/country.py` — `_enforce_declared_dtypes` or `_finalize_result` | Age dtype coercion | Int64 canonical; NaN-safe |
| `tests/test_age_handler.py` (new) | Unit tests | DOB-only, age-only, passthrough, `-1` sentinel → NA |
| `tests/test_age_dtype_consistency.py` (new) | Cross-country regression test | `Country(X).household_roster().Age.dtype == Int64` for every built country |

## Design approach

**Touch each country, not the framework — for the recovery logic.** `age_handler()` needs per-country knowledge of which columns are DOB components. Auto-detection in the framework would couple it to survey-specific naming conventions. The YAML-plus-wave-script pattern Senegal uses is the right shape: `data_info.yml` declares the multi-column Age list, the wave's `.py` script applies `tools.age_handler()` row-wise inside a `household_roster(df)` function.

**Touch the framework — for the dtype fix (#173 fold-in).** The dtype inconsistency isn't per-country; it's `_enforce_declared_dtypes` applying or not applying `Int64` coercion depending on the column's arrival state. The fix belongs in `_finalize_result` or `_enforce_declared_dtypes`, after the canonical-spellings step, coercing `Age` to `Int64` (nullable integer) if the declared type in `data_info.yml` is `int`. Note: this dtype fix is *independent* of the recovery logic — it runs on every country's output regardless of whether that country uses `age_handler` — so it can be landed and verified first, then the per-country recovery work layers on top.

**Togo is proof-of-concept** because its 94.5% gap dominates every other symptom. Its questionnaire `s01_me_tgo2018.dta` is the identical EHCVM instrument Senegal uses, so DOB columns `s01q03a/b/c` (year/month/day of birth) should be present. Confirmation via DVC pull is Step 1.

## Risks / unknowns

1. `s01q03a/b/c` presence in Togo — high confidence but unverified.
2. `annee_nais`/`mois_nais` in `ehcvm_individu_{cc}{year}.dta` — these are *harmonised* files that may have dropped DOB columns; needs audit.
3. `-1` Stata sentinel in birth year/month — pattern in Senegal's YAML is `mapping: {-1: null, '-1': null}`; must replicate for every country.
4. `age_handler()` requires non-None `interview_year` — fails with `TypeError` otherwise; per-row `interview_date` column or hard-coded year.
5. **Cache invalidation** — editing YAML/wave-script does NOT auto-rebuild the parquet. Also: `LSMS_NO_CACHE=1` bypasses the country-level `var/` cache but **NOT** the wave-level `_/{table}.parquet` for script-based materialisation. Delete both layers explicitly or use `lsms-library cache clear --country {Country}` (need to verify that hits both). *See 2026-04-13 Nigeria scatter surprise for precedent.*
6. **`age_handler_wrapper` closure bug** — block on fix (Step 2) before any downstream prompts depend on it.
7. **Dtype coercion side effects** — pre-existing `Age: object` dtype cells with string values like "Less than one year of age" (CotedIvoire), "98 years and more" (Timor-Leste) will fail `Int64` coercion. Either (a) coerce via `pd.to_numeric(..., errors='coerce')` first to NA, or (b) flag an `Age_quality` companion column. The latter is richer but out of scope for this issue.

## Step-by-step execution order

### Step 1 — Data audit (read-only, parallelisable)
Pull + column-inventory the candidate source files. Outputs a decision table: which countries have DOB columns, which have the `-1` sentinel.

### Step 2 — Framework dtype coercion + `age_handler_wrapper` bug fix + unit tests
Independent of Step 1. Can run in parallel. Landing this first means every subsequent per-country step has predictable dtype in its own verification.

### Step 3 — Togo 2018 proof-of-concept (depends on Step 1)
Wire `age_handler()` for Togo 2018. Verify coverage goes from 5.5% → ≥ 95%.

### Step 4 — EHCVM batch fan-out (depends on Step 1 per-country audit results, parallelisable)
One agent per (country, wave) pair where Step 1 confirmed DOB columns. Up to ~9 agents.

### Step 5 — Defer (Uganda / Ethiopia / Nigeria moderate gaps)
Only after the above prove out. Audit per-wave codebook first.

## Verification plan

```bash
# Step 2 — dtype + closure bug
.venv/bin/python -m pytest tests/test_age_handler.py tests/test_age_dtype_consistency.py -v
LSMS_NO_CACHE=1 .venv/bin/python -c "
import lsms_library as ll
for country in ['Albania','Burkina_Faso','Ethiopia','Malawi','Nigeria','Senegal','Tanzania','Uganda']:
    df = ll.Country(country).household_roster()
    print(f'{country:20s} Age dtype: {df.Age.dtype}')
"

# Step 3 — Togo coverage
LSMS_NO_CACHE=1 .venv/bin/python -c "
import lsms_library as ll
r = ll.Country('Togo').household_roster()
cov = r.Age.notna().mean()
print(f'Togo Age coverage: {cov:.1%}')
assert cov >= 0.95
"

# Step 4 — per-country
(as above, parametrised)

# Full re-scan after all steps land
.venv/bin/python slurm_logs/roster_scan_2026-04-13/run_scan.py
.venv/bin/python slurm_logs/roster_scan_2026-04-13/aggregate.py
# Expected: Age dtype = Int64 for every built country; Togo coverage ≥ 95%;
# CotedIvoire/Timor-Leste non-numeric Age counts → 0 (if Step 2 coerces).
```

## Rollout notes

- **Cache clears mandatory** for each touched country. `LSMS_NO_CACHE=1` alone is insufficient for script-based waves — also delete `~/.local/share/lsms_library/{Country}/{wave}/_/household_roster.parquet` if present.
- **No DVC push** — all edits are source-tree edits.
- **Release-note bullets**: "Togo and EHCVM-batch `household_roster` Age coverage substantially improved via `age_handler()` adoption."; "`Age` dtype now canonically `Int64` across all countries in both `Country(...)` and `Feature('household_roster')` outputs."

## Dispatch-ready prompts

### Prompt A — Step 1 audit (read-only)

Dispatch as a single agent. Uses `dvc pull` + `pyreadstat.read_dta(..., metadataonly=True)` to confirm DOB column availability in Togo 2018 `s01_me_tgo2018.dta` and the EHCVM harmonised files for Benin/Burkina/CotedIvoire/GuineaBissau/Mali/Niger 2018+. Read-only, no commits. Output: a decision table in `slurm_logs/issue_165_plan/step1_audit.md`.

### Prompt B — Step 2 framework dtype coercion + bug fix + tests

Fix `age_handler_wrapper` closure bug in `local_tools.py:1392` (rename inner var). Add `Age` → `Int64` coercion in `_enforce_declared_dtypes` (or `_finalize_result`) whenever `data_info.yml` declares `type: int` for the column. Non-numeric strings route through `pd.to_numeric(..., errors='coerce')` → `pd.NA`. Add `tests/test_age_handler.py` (DOB/age/passthrough/-1 sentinel) and `tests/test_age_dtype_consistency.py` (every built country returns `Int64` Age). Verify: 162 schema tests still pass + new tests pass + cross-country Age dtype is `Int64`.

### Prompt C — Step 3 Togo 2018

Depends on Prompt A. Extend `Togo/2018/_/data_info.yml` Age map with `s01q03a/b/c` + `-1` null mapping. Add `household_roster(df)` to `Togo/2018/_/2018.py` calling `tools.age_handler(...)` (model on Senegal 2018-19). Clear caches. Verify: Togo Age non-null ≥ 95%.

### Prompt D — Step 4 EHCVM fan-out

Per-country agent (one per row in Prompt A's decision table). Extends YAML with `annee_nais`/`mois_nais` + null sentinel; adds wave-script `household_roster(df)`. Parallelisable up to ~9 agents.

---

## Not addressed in this plan (by design)

- #172 (household_characteristics coverage gaps) — structural; Age fix helps but doesn't close it.
- #171 (individual_education harmonisation) — parallel pattern, separate milestone.
- Senegal 10.5% Affinity-null (non-Age).
- Rogue roster columns (`Marital`, `Birthplace`, etc.) — design decision on canonicalisation.
