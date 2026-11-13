# Prior-Art Ledger — GH #1250 (Uganda plot_labor: a missing use-flag served as "No")

**Search tier used:** ripgrep+git floor (the gitnexus MCP server failed to connect).

## §1 Task, restated
Uganda `plot_labor` (script-path, `materialize: make`; wave scripts
`Uganda/{wave}/_/plot_labor.py` → `uganda.plot_labor_for_wave`) builds
`PersonDays` per `(t, i, plot_id, source, season)` from AGSEC3A/3B. It zeroes
a source's days where the survey's use-flag says "No". The flag passes through
`_to_int_code` (nullable `Int64`), and `out.where(flag != code, 0.0)` reads the
`<NA>` of a missing flag as False. So every missing-flag row was served as
`PersonDays = 0`. Fix: zero only where `flag == code`. Report the plot-less
residual and the `dropna=False` max fold; do not decide either (GH #1120 D3).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_labor_persondays` | `countries/Uganda/_/uganda.py:3118` | row-sums the day cells, then applies the "No" gate | now: `tests/test_uganda_plot_labor_flag.py` | **fixed** |
| `plot_labor_for_wave` | `uganda.py:~3150` | builds the pieces, drops NaN `PersonDays`, `groupby(dropna=False).max()` | same | unchanged (comment corrected) |
| `_to_int_code` | `uganda.py:1216` | casts a code column to `Int64` (the source of `<NA>`) | indirect | reused, unchanged |
| WB rule | `reference/lsms-isa-harmonised/Reproduction_v2/Code/Cleaning_code/UGA_UNPS{1..8}{,_S2}.do` | `replace … = 0 if flag==code` | n/a | precedent |
| `total_*_labor_days` | `lsms_library/transformations.py:5032-5103` | drop NaN, sum to `(t, i)` | existing | downstream, measured |

Sibling textual instances of `.where(flag != code, 0)` (reported, NOT fixed;
scope rule):
- Niger `2011-12/_/plot_labor.py:51`, `2014-15/_/plot_labor.py:46`
- Tanzania `tanzania.py:1199` (crop `harvested`) and `:1727-1728` (`hire_yn`)
- Ethiopia `ethiopia.py:2608` (`n`, nullable) and `:2680-2681` (`did`)

Classification, measured: see §6 and the report.

## §3 Definitions & conventions in force
- Builder contract: "One row per (plot, source, season) with a non-NaN reported
  PersonDays value". This is the `plot_labor_for_wave` docstring, and the
  builder enforces it with `piece[piece['PersonDays'].notna()]`.
  **Amended by the follow-up (ruling @ligon 2026-10-23):** a HIRED row is also
  served on a reported `Wage` alone, with `PersonDays` NaN. This is Malawi's
  rule, `Malawi/_/malawi.py:3766-3767`; Mali's
  `test_wage_only_hired_observation_keeps_missing_person_days` pins the same
  contract there.
- `plot_labor` grain and columns: `Uganda/_/data_scheme.yml` `plot_labor:` and
  canonical `lsms_library/data_info.yml` (`plot_labor:` index, GH #569).
- Stata semantics: `replace x = 0 if flag==2` with a missing flag is false
  (missing is not equal to 2), so `x` keeps its value.

## §4 Invariants & assumptions
- The family gate `a3aq38` / `a3bq38` (2009-10, 2010-11) is a WORKER COUNT
  (0..94), not yes/no, so its "No" code is 0 (`LABOR_COLMAPS`). The hired gates
  are 1 Yes / 2 No. 2009-10 hired has off-scheme 0 (2 per season) and 3 (1,
  `a3bq41`); these are served as reported, before and after.
- The raw flags arrive as numpy `float64` with NaN. The `Int64` cast is what
  makes `!=` return `<NA>`; a bare `pd.to_numeric` would not.
- `plot_labor` is the only Uganda plot table whose de-dup uses `dropna=False`,
  which is why Site I fires on its NaN `plot_id` rather than the rows being
  deleted.
- `uganda.py` is in every Uganda table's hash, so any edit costs a Uganda-only
  re-warm.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| "No" gate | extend (`mask((flag==code).fillna(False))`) | identical semantics to the WB `replace … if flag==code` |
| plot-less residual | none (report) | GH #1120 D3 |
| `dropna=False` max fold | none (report) | needs a sentinel or row ordinal = D3 |
| hired rows with Wage but no days | **extend (follow-up)**: `keep = PD.notna() \| (hired & Wage.notna())` | ruled 2026-10-23; Malawi's wage disjunct only, NOT its `PD > 0` half |

## §6 Open questions for the human
- **D3** (GH #1120): 95 NaN-`plot_id` rows remain, 50 of them positive. The max
  fold collapses distinct plot-less source rows (2009-10 70 → 45 keys).
- ~~Seven hired rows with a missing flag, no days and a reported Wage (414,000
  UGX in all) are no longer served. Should `plot_labor` keep Wage-only hired
  rows, as Malawi's builder does?~~ **Ruled yes (@ligon 2026-10-23)**; see
  Phase 4. The class is 85 rows, not 7.
- Sibling `.where(x != code, 0)` instances in Niger, Tanzania and Ethiopia read
  numpy-float gates today, so they do not fire. They are one nullable cast away
  from the same defect. A shared helper or a lint could close that.

---
### Phase 3 — verification
- `_labor_persondays` — OK (§3 Stata semantics, §4 Int64 cast). Development vs
  fix, cold: 169,877 → 164,692 rows. 5,185 removed, all zeros (3,561 NaN-plot,
  1,624 valid-plot). 97 rows go 0 → reported days (+2,745). 0 positive values
  changed and 0 rows added. An independent raw oracle matches both sides in
  every (wave, season) cell.
- `plot_labor_for_wave` comment — OK (§2): it states the measured fold. No
  behaviour change.
- Cache — 28/28 Uganda L2-country, 224/224 Uganda L2-wave, 0/40 btf; no other
  country moved.

### Phase 4 — follow-up: wage-only hired rows (ruling @ligon 2026-10-23)
- Rule: `plot_labor_for_wave` keeps `PersonDays.notna() | (source == 'hired' &
  Wage.notna())`. This is Malawi's wage disjunct (`malawi.py:3766-3767`).
  Malawi's `PersonDays > 0` half is not adopted: Uganda's "No" zeros are
  reported values (#1250), and nobody ruled on dropping them.
- Measured, cold, `90fc1e18d` vs the follow-up: 164,692 -> 164,777 rows. 85
  added, 0 removed, 0 common-key changes (`PersonDays`, `Wage`, `v`). The added
  key set equals the raw oracle's wage-only set by join. 2009-10 A 38, B 29;
  2010-11 A 7, B 11. By flag: 78 "Yes", 7 missing. 2 have a zero wage. The 78
  were never served on development either, so the PR body's "7" undercounted
  the class by 78.
- "No" + wage: 17 raw rows are served with `PersonDays = 0` and their wage,
  before and after. That agrees with Malawi.
- D3: no added row lacks a plot, none folds, and Site I is unchanged.
- Downstream: `total_*_labor_days` are byte-identical. The median-wage
  valuation has the same price pool (21,513) and the same price and value on
  common rows. The new rows are valued NaN.
- Evidence: `slurm_logs/isa_backlog_2026-10-21/1250-uganda-labor-flag/r2/`.
