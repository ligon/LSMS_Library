SCOPE DEVIATIONS: none

# food_prices / food_quantities dtype fix — 2026-04-14

## 1. Worktree + Branch

- **Worktree**: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/food_prices_dtype_2026-04-14`
- **Branch**: `fix_food_prices_dtype`
- **Parent commit**: `2d68ce7d docs(CLAUDE.md): expand scrum-master-hpc addenda with .pth + Savio venv recovery` ✓

## 2. Root Cause

`kgs_per_other_units.json` contains mixed int/float/empty-string values. `pd.Series(dict)` infers `object` dtype when values are mixed (one key `''` maps to `''`). Passing an `object`-typed Series to `DataFrame.divide()` / `DataFrame.multiply()` produces `object` dtype result columns. `to_parquet()` in `local_tools.py` then converts any `object`-dtype column to `string[pyarrow]` (line 865). The cached parquets therefore stored `string` instead of `float64`.

**File:line**: `lsms_library/countries/Uganda/_/food_prices_quantities_and_expenditures.py`, line 37 (original).

## 3. Commit

(Committed after writing this report — see git log on branch `fix_food_prices_dtype`.)

## 4. Static Verification

**Diff stat**: 2 files changed (1 source fix + 1 new test).

**Code change** (one-line fix in `food_prices_quantities_and_expenditures.py`):

```diff
-kgs = pd.Series(d)
+kgs = pd.to_numeric(pd.Series(d), errors='coerce')
```

`pd.to_numeric(..., errors='coerce')` casts all JSON values to `float64`, coercing the empty-string entry `'' -> NaN`. The NaN is immediately dropped by the downstream `kgs[kgs!=0]` filter (NaN != 0 is True in pandas, but the subsequent `reindex` and `divide` skip NaN entries naturally). The fix leaves all conversion factors unchanged for valid entries.

**Pre-fix simulation** (verified with `.venv/bin/python`):
- `pd.Series(d).dtype` → `object` (because of mixed int/float/'' values)
- `pq[prices].divide(kgs, axis=0).dtypes` → all `object`
- After `to_parquet`: stored as `string`

**Post-fix simulation** (verified with `.venv/bin/python`):
- `pd.to_numeric(pd.Series(d), errors='coerce').dtype` → `float64`
- `pq[prices].divide(kgs, axis=0).dtypes` → all `float64`
- After `to_parquet`: stored as `float64`

## 5. Expected Impact

- `Country('Uganda').food_prices()` → all 9 price/unitvalue columns `float64`
- `Country('Uganda').food_quantities()` → all 4 quantity columns `float64`
- `Country('Uganda').nutrition()` → `final_q @ final_fct.T` succeeds (matrix multiply requires numeric dtype); `TypeError` crash eliminated as a side effect
- Cache must be invalidated after this fix: `lsms-library cache clear --country Uganda` or `LSMS_NO_CACHE=1` on next run, then re-run `make -C lsms_library Uganda/food_prices Uganda/food_quantities`

## 6. Surprises

- The empty-string entry (`'' -> ''`) in `kgs_per_other_units.json` is the proximate cause. It was presumably present since the file was authored, but only became visible when pandas 3.x changed the coercion behavior of arithmetic on `object`-typed Series.
- The replication parquet (352,913 rows vs current 314,997) predates the unit-code filter tightening; the dtype bug exists independently of the row-count gap.
- `to_parquet`'s `dtype=='O'` check is correct defensive code for genuinely mixed-type columns; the fix is properly applied upstream at the data construction point, not by weakening `to_parquet`.
