# Prior-Art Ledger — median-price-weight-col (WORKPLAN Phase 1, L5)

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED); per `CLAUDE.md` "Code intelligence:
GitNexus is OPTIONAL", the substitute for `gitnexus_impact` was a `rg` sweep of
every call site of `median_price_valuation` — declared here rather than skipped.

## §1 Task, restated
Add an optional `weight_col=` kwarg to `lsms_library.transformations.
median_price_valuation` — a Phase-2 METHODOLOGY transform, analyst-callable,
NOT registered in `_FOOD_DERIVED`/`_ROSTER_DERIVED` and NOT auto-surfaced as a
`Country` feature. When given, each cell statistic in the geography ladder
becomes a weighted median instead of a plain one. The threshold, the ladder and
the unconditional national fallback are untouched, and `weight_col=None` must
run the existing code path unchanged. Closes the one construction delta between
our ladder and EPAR TR#335's (`slurm_logs/2026-09-09_epar_curation/
LEARNINGS.org` L5).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `median_price_valuation` | `lsms_library/transformations.py:2918` | the WB `valuation_median_crops` ladder | **no** (none before this PR) | extend |
| `_weighted_median` | `lsms_library/transformations.py:2859` | per-group weighted median, interpolating at an exact tie | yes (new file) | new |
| `_kg_factor_series` / `_get_kg_factors` | `transformations.py:1657` ff. | unit -> kg | yes | untouched (bypassed via `kg_qty=` in tests) |
| any weighted median/quantile helper | — | **none exists** — `rg 'weighted_median\|weighted_quantile\|wquantile'` over `lsms_library/` + `tests/` returns 0 | — | so `new` is not reinvention |
| `_normalise_sample_weights` | `country.py` (see `CLAUDE.md` §sample) | divides `weight`/`panel_weight` by the wave's own mean | yes | consumer-side context only |
| callers of `median_price_valuation` | `rg` finds **none** in `lsms_library/`; prose refs in `GhanaLSS/_/CONTENTS.org:1228`, `scripts/api_surface_audit.py:9`, module comment `transformations.py:~1613` | — | — | blast radius = the module comment, amended |

## §3 Definitions & conventions in force
- Ladder selection rule: "the median observed price of the finest cell whose
  count >= threshold", `transformations.py` docstring (step 2). Unchanged.
- Weights: normalised to within-wave mean 1 at API time, `CLAUDE.md` §"`sample()`
  and Cluster Identity". So a `sample()`-sourced `weight` passed here is already
  mean-one; the weighted median is scale-invariant, so this does not matter —
  but it means we are NOT raking, unlike EPAR (`REDTEAM_A.org` "Cross-file
  reconciliation" §1).
- Aggregation policy: core never aggregates; analyst-callable transforms may
  (`SkunkWorks/grain_aggregation_policy.org` §3a, `transformations.py:1580` ff.
  module header). This function is on the analyst side of that line.
- Pandas 3.0 targets (`CLAUDE.md`): no `inplace=`, `pd.isna`, `.iloc[0]`.

## §4 Invariants & assumptions
- **Default byte-identical.** `weight_col=None` must take the exact prior
  expressions. Pinned by `tests/test_median_price_valuation.py::
  test_default_path_hand_computed_ladder` (+ `test_weight_col_none_is_the_
  default_path`), and by 0/13 moved cache hashes (§Phase 3).
- **The threshold counts ROWS, never summed weight** — otherwise `threshold=10`
  would mean two different things on the two paths. Pinned by
  `test_threshold_counts_rows_not_summed_weight`.
- Group keys are `.astype(str)` before every `groupby`, so there are no NA
  group labels and no unordered-categorical `first()` hazard
  (`CLAUDE.md` §Gotchas, "Categorical columns from `.dta`/`.sav`").
- `item_df.index` may carry duplicate labels (item rows repeat a household), so
  the helper's working frame is built from numpy arrays on a fresh RangeIndex
  rather than from Series (which would align on the duplicated index).
- Floating point: `groupby.cumsum` is sequential and `transform('sum')`
  pairwise, so at the exact tie the equal-weights case produces they can differ
  by an ulp. Compared with a `1e-12` relative slack; test weights are small
  integers so the arithmetic is exact anyway.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| weighted median | **new** (`_weighted_median`) | nothing in the library computes a weighted quantile; numpy/pandas have no grouped weighted median |
| observation count | reuse | same `groupby(...).transform('sum')` over a boolean, widened from `price.notna()` to `usable` |
| ladder / threshold / national fallback | reuse, untouched | the delta with EPAR is the statistic, not the selection |
| weight resolution (level or column) | reuse | the function's existing `_series()` closure |

## §6 Open questions for the human
- **The tie rule — SETTLED, 2026-09-09.** The first implementation took the
  *lower* weighted median (smallest value whose cumulative weight reaches half
  the total), which agrees with `Series.median()` only for ODD-count cells.
  @ligon's coordinator ruled: `weight_col` must be a strict **generalisation**
  of the default path, so the interpolating rule is now implemented — the value
  at the first row where `C >= W/2`, except where `C == W/2` exactly at that
  row, where it is the mean of that value and the next row's. Under equal
  weights that is the average of the two middles for even `n` and the middle
  value for odd `n`, i.e. `Series.median()` exactly in both parities. Why it
  mattered concretely: GhanaLSS GLSS1-3 weights are a genuine constant 1.0
  (`CLAUDE.md` §Weights), so `weight_col='weight'` there is an all-equal-weights
  call, and under the lower rule an even-count cell would silently differ from
  the unweighted call. Pinned by `test_equal_weights_reproduce_unweighted_
  exactly_{odd,even}_cells` and a 12x4 parity sweep. Whether Stata's
  `collapse (median) [aw=]` does the same at an exact tie is still unverified
  (`[R] summarize` Methods and formulas) — but it no longer gates anything,
  since the rule is now fixed by our own generalisation requirement.
- Should a future `valuation='median_price'` kwarg on `food_expenditures`
  (GH #585, LEARNINGS L6) default to weighted? Out of scope here.

---
### Phase 3 — verification
- `_weighted_median` — **OK (anchored on §2, §4, §5)**: no existing
  weighted-quantile machinery to duplicate (§2 row 4); duplicate-index and
  NA-key hazards handled per §4; the equal-weights identity with
  `Series.median()` verified by brute force over n=1..40 x 30 trials x 4 weight
  scales (0 mismatches) as well as by the committed tests.
- `median_price_valuation(weight_col=...)` — **OK (anchored on §3, §4)**: the
  ladder/threshold/fallback semantics of §3 are literally unchanged; the
  default path is pinned by test and by fingerprint.
- Cache impact — **OK (anchored on §4)**: `transformations.py` carries no
  `@build_transform()` (they all live in `build_transforms.py`, re-exported at
  `transformations.py:18`). Measured anyway: `build_transforms_fingerprint` for
  9 tables and `Country('Uganda')._table_cache_hash` for 4 tables — **0 of 13
  values moved** before/after.
- Module comment `transformations.py:~1613` — **CONTRADICTION found and fixed**:
  it read "EPAR's medians are weighted ... and ours are not", which this change
  makes false. Amended to say unweighted-by-default with `weight_col=` opting
  in, and that we still do not rake.
