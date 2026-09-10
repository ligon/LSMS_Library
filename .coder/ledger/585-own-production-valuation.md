# Prior-Art Ledger — 585-own-production-valuation (WORKPLAN Phase 2, L6)

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED); per `CLAUDE.md` "Code intelligence:
GitNexus is OPTIONAL", the substitute for `gitnexus_impact` was an `rg` sweep of
every caller of `food_expenditures_from_acquired` / `median_price_valuation`
and of `basis=` through `country.py` / `feature.py` — declared, not skipped.

## §1 Task, restated
`food_expenditures` is a DERIVED table (`Country._FOOD_DERIVED`, `country.py`),
computed at read time from `food_acquired` by
`transformations.food_expenditures_from_acquired`; it is never stored.  GH #575
gave it `basis='purchased'` (default) / `basis='total'`, where `'total'` sums
whatever `Expenditure` the source recorded and *fabricates nothing*.  For every
country built via the stock `food_acquired_to_canonical` that leaves
`s in {produced, inkind}` rows with a null `Expenditure`, so `'total'` equals
`'purchased'`.  This task adds an **opt-in** `valuation=` kwarg that imputes a
value for exactly those rows, at purchase-side unit prices, through the existing
`median_price_valuation` ladder plus a same-household rung.  It is never the
default, never stored, and the docstring must state which side of the
farmgate/market gap it lands on.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `food_expenditures_from_acquired` | `transformations.py:1146` | `basis=` filter + `groupby(['t','i','j','s']).sum()` | yes (`tests/test_food_expenditures_basis.py`) | **extend** (early return keeps the `valuation=None` body verbatim) |
| `median_price_valuation` | `transformations.py:3140` | WB `valuation_median_crops` geography-median ladder, `threshold=10`, unconditional national fallback, optional `weight_col` | yes (`tests/test_median_price_valuation.py`) | **reuse verbatim** — no edit |
| `_weighted_median` | `transformations.py:3040` | grouped weighted median | yes | untouched (not exposed here — see §6) |
| `food_prices_from_acquired(units='unitvalue')` | `transformations.py` | `Expenditure/Quantity` per native `u` | yes | **not reused** — see §5 |
| `harvest_kg_factors` / `harvest_kg` | `transformations.py:1915` / `:2124` | per-row layer provenance: `KgFactorSource` column on the item frame, `attrs['kg_factor_sources']` counts on the aggregate | yes | **pattern copied** (the shape this task's provenance must mirror) |
| `_kg_factor_series` / `_get_kg_factors` | `transformations.py:1657` ff. | unit → kg | yes | **bypassed** (`kg_qty=` supplies the native-unit denominator) — see §5 |
| generated `Country.__getattr__` method | `country.py:4377` ff. | validates `basis`/`units`/`labels` early, builds `transform_kwargs`, calls `_finalize_result`, `_relabel_j`, `_add_market_index`, `convert` | yes | **extend** (one more validated kwarg, same shape as #575 `f21784fd`) |
| `Feature.__call__` kwarg forwarding | `feature.py:17,593` | forwards any kwarg present in `inspect.signature(method).parameters` | yes | **no edit needed** — `valuation` is forwarded by signature |
| any existing own-production imputation | — | **none**: `rg 'valuation\|impute' lsms_library/` finds only `median_price_valuation` and the #575 docstring's "tracked follow-up" | — | so "new" is not reinvention |

Callers of `food_expenditures_from_acquired`: `country.py:4457` (the derived
path) and `tests/`.  Blast radius of the change = that one call site plus the
transform's own tests; every existing call passes no `valuation`.

## §3 Definitions & conventions in force
- `s` vocabulary: `purchased, produced, inkind, other` — `lsms_library/data_info.yml:42`, `:591`.
- `food_acquired` canonical index `(t, v, i, j, u, s)` — `data_info.yml:51`.
  `Country._aggregate_wave_data` has already run `_finalize_result`, so **`v` is
  on the frame the transform receives** (measured: Uganda/Malawi index is
  `['i','t','v','j','u','s']`).  The finest geographic rung is therefore free.
- `basis=` semantics — `transformations.py` `food_expenditures_from_acquired`
  docstring; `'total'` "no value is fabricated".  This kwarg is the opt-in that
  changes that, and only when asked.
- Ladder selection rule ("finest cell with ≥ threshold observations, national
  unconditional fallback") — `median_price_valuation` docstring step 2.  Not
  re-litigated here.
- The purchase-side bias: Uganda own-consumption valued at the purchase price
  sits ~23% above what a sale fetched — `slurm_logs/price_sources/SYNTHESIS.org:750-753`
  (Uganda-only; GhanaLSS has no sale-price source).  The UNPS interviewer manual
  asks for farm-gate — `:732-736`.  Deaton & Zaidi (2002) LSMS WP 135 treat the
  choice as producer vs market price.
- EPAR's consumption repo puts the household's own purchase price for the same
  item as the **first** rung, before any spatial cell, conditional on the unit
  matching (`LEARNINGS.org` L5 item 4; `EthiopiaW5_...do:423`, `UgandaW4_...do:693`).
  Its Ag repo instead keeps it as a parallel `value_harvest_hh` series.
  "EPAR's ladder" is ambiguous — say which.
- Core never aggregates silently; analyst-callable transforms may
  (`SkunkWorks/grain_aggregation_policy.org` §3a).  This is on the analyst side.
- Pandas 3.0 (`CLAUDE.md`): no `inplace=`, `pd.isna`, `.iloc[0]`.
- `attrs` survive an operation only when every input agrees (`CLAUDE.md`
  §"Panel ID Transitive Chains"); a disagreeing merge yields `{}`.

## §4 Invariants & assumptions
- **`valuation=None` is byte-identical to today.**  Pinned by
  `tests/test_own_production_valuation.py::TestDefaultPathUnchanged`, which
  holds a verbatim copy of the pre-change body as `_legacy()` and asserts
  `.equals()` on a synthetic frame and on warm Uganda at both bases.
- **`t` MUST be an item key of the median ladder.**  `median_price_valuation`
  has no wave axis; without `t` in `item_keys` the national rung would pool
  2004 and 2019 Kwacha, and a recurring cluster code would pool across waves at
  the `v` rung too.  Pinned by `test_waves_do_not_pool`.
- **A null geographic key must never become a cell.**  `median_price_valuation`
  stringifies its group keys, so a null `v` would collapse to the literal
  `'nan'` and, at ≥10 rows, qualify as a cluster that does not exist.  Measured:
  Uganda 328 null-`v` rows (0.092%), Malawi 0.  Mitigated here, not in
  `median_price_valuation`: every null geo key is replaced by a **per-row unique
  token**, so its cell count is 1 and can never clear `threshold` (any
  `threshold >= 2`).  Pinned by `test_null_geo_never_forms_a_cell`.
- **Unit alignment is exact by construction**: the price is per native `u`
  because `u` is an item key, so a produced row is valued at the price of the
  same label it was reported in.  No kg conversion enters.
- **Rows that already carry a value are `reported` and are never re-valued** —
  including a value the survey itself imputed for `s='inkind'`
  (`data_info.yml:602`).  Zero is treated as missing, exactly as the existing
  body's `replace(0, np.nan)` does, so a zero-valued produced row IS a
  candidate.
- **`valuation` requires `basis='total'`** — validated in both the transform and
  (early, per #575's comment) the generated `country.py` method, so it cannot be
  swallowed by the derive path's broad `except`.
- `attrs['valuation_sources']` must survive `_finalize_result` /
  `_relabel_j` / `_add_market_index` / `convert`.  It is re-attached explicitly
  at the end of the derived path (the `harvest_kg` `res.attrs[...] =` pattern),
  because `_add_market_index` and the `v`-join are disagreeing merges.
- `df.index` may carry duplicate labels; all valuation arithmetic is done on a
  fresh `RangeIndex` frame and mapped back positionally.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| geography median ladder, threshold, national fallback | **reuse, untouched** | `median_price_valuation` is exactly this construct and is tested |
| per-native-unit price basis | **reuse via `kg_qty=`** | passing the purchased `Quantity` as `kg_qty` makes the "per kg" price a per-native-`u` price; `u` in `item_keys` keeps it aligned.  No edit to the function |
| kg normalisation of the price basis | **rejected** | it would inherit #850 (`conversion_to_kgs` has no `j` axis and is under revision) for no measured gain: national-rung ≥10 coverage is 94.2% (Malawi) / 76.5% (Uganda) on the native-`u` key, and lower-casing `u` moves it **0.00pp** in both countries |
| `food_prices_from_acquired(units='unitvalue')` as the price source | **rejected** | it returns a *per-(t,i,j,u,s)* frame already summed and `s`-split; the ladder needs the per-row pool with geography attached, and re-joining that frame back onto `food_acquired` would be a strictly longer path to the same `Expenditure/Quantity`.  The formula, not the function, is what is shared — recorded here so it is not read as an oversight |
| same-household rung | **new** (`_own_price_rung`) | nothing computes a per-`(t,i,j,u)` household unit value; it is a 6-line groupby-sum ratio |
| provenance shape | **reuse of pattern** | `KgFactorSource` column on the item frame + `attrs[...]` counts on the aggregate (`harvest_kg`) |
| weighted medians | **not exposed** | see §6 |

## §6 Open questions for the human
- **Scalar `'median_price'` runs the ladder ALONE, not own-price-then-ladder.**
  The issue brief phrased the ladder as "applied after own_price"; this
  implementation makes the composition explicit —
  `valuation=('own_price','median_price')` — and a scalar means exactly the rung
  named.  Rationale: a scalar that silently ran a rung the caller did not name
  would make `valuation='median_price'` unreproducible from its own name, which
  is the very hazard `LEARNINGS.org` L5 item 4 warns about ("a
  `valuation='own_price'` option built on the wrong repo's ordering would
  silently invert which price wins when both exist").  The composed tuple is
  named in the docstring as EPAR's consumption-repo ordering.  **Flagged as a
  departure from the brief's plain reading; @ligon to confirm.**
- **Unweighted.**  `median_price_valuation(weight_col=...)` exists (Phase 1) but
  is deliberately NOT plumbed through: the rest of the library's transforms are
  unweighted, and weights are already normalised to within-wave mean 1 at API
  time (`CLAUDE.md` §sample), which is not EPAR's population raking anyway.  A
  `valuation_weights=` kwarg is the obvious later addition; not built.
- **Purchased rows with no recorded outlay are NOT valued** (Malawi: 170 null +
  643 zero).  They count as `none`.  A purchase with no price is a data defect,
  not an unvalued acquisition — but if the intent were coverage, they are the
  next candidates.
- Malawi carries `Kilogramme`, `Kilogram`, `Kg` and `kg` as distinct `u` labels
  **within the same wave** (2016-17, 2019-20).  The native-`u` key therefore
  splits one physical pool four ways.  It costs no measured coverage (94.2% at
  the national rung) and biases nothing (each label's median is a price per that
  label), but a `u`-vocabulary canonicalisation for Malawi would pool them.
  Out of scope; recorded because it is the one thing a kg basis would fix.

## §7 Measured effect (warm cache, 2026-09-09)

Ladder `v -> District -> Region -> national`, `threshold=10`, rungs
`('own_price', 'median_price')`, both countries.

| | Uganda | Malawi |
|---|---|---|
| `food_acquired` rows | 355,571 | 971,531 |
| candidates (non-purchased, unvalued) | **17** (0.005%) | **288,704** (29.7%) |
| `reported` | 355,537 | 682,014 |
| `own_price` | 3 (17.6% of candidates) | 2,933 (1.02%) |
| `median_price` | 12 (70.6%) | 280,623 (97.2%) |
| candidates left unvalued | 2 (11.8%) | 5,148 (1.78%) |
| `none` (incl. purchases with no outlay) | 19 | 5,961 |

Change in Σ`Expenditure` per wave, `basis='total'` with vs. without valuation:

| wave | Uganda Δ% | | wave | Malawi Δ% |
|---|---|---|---|---|
| 2005-06 | +0.019 | | 2004-05 | **+101.6** |
| 2009-10 | 0 | | 2010-11 | +63.3 |
| 2010-11 | 0 | | 2013-14 | +57.4 |
| 2011-12 | +4.27 | | 2016-17 | +50.3 |
| 2013-14 | +0.050 | | 2019-20 | +47.4 |
| 2015-16 | +0.010 | | **all waves** | **+51.8** |
| 2018-19 | 0 | | | |
| 2019-20 | +0.007 | | | |
| **all waves** | **+0.55** | | | |

**Uganda barely moves, and that IS the finding.**  Its source records a value
for produced and in-kind rows already (11 and 6 nulls out of 85,345 and
23,107), so 99.99% of rows grade `reported`.  What Uganda stores is the
household's *own* valuation — the very quantity the price study measured at
~1.234x a sale price (`SYNTHESIS.org:750-753`).  So for Uganda this kwarg is
close to a no-op by construction, not a failed imputation.

**Uganda's +4.27% in 2011-12 is ONE row**, and it is worth naming rather than
smoothing: household `210930004091101` received "Restaurant (soda)" in kind,
`u='---'`, `Quantity=4500`; its own purchase unit value for that
`(t, j, u='---')` is 1,500, so the row imputes 6,750,000 UGX.  The unit label
is uninterpretable, so the `Quantity` almost certainly is not a count.  The
valuation is only ever as good as the `Quantity` it multiplies, and it
faithfully propagates a source defect.  **Deliberately NOT screened here** —
count, never clip (the `harvest_kg` discipline), and a magic outlier threshold
is exactly what `CLAUDE.md` warns against.  The `u='---'` vocabulary is a
Uganda `food_acquired` question, not this kwarg's.

**Is the advertised ladder the ladder that ran?**  Checked, because a `(t, v)`
join that missed would degrade the ladder silently while
`attrs['valuation_geo_levels']` still said `['v','District','Region']`.
Measured hit rate of the coarser rungs on the rows that carry a cluster id:

| | District | Region |
|---|---|---|
| Malawi | **100%** | **100%** |
| Uganda | 92.0% | 98.1% |

Malawi's §7 numbers are therefore the full ladder.  Uganda's miss is **not** a
join bug: 99.73% of its `(t, v)` pairs ARE in `cluster_features`; the column is
simply **null** there — `District` for 27.8% of 2010-11 clusters and 33.0% of
2011-12's.  Either way the rung was not available for those rows, so
`_median_price_rung` now counts it and raises `ValuationLadderWarning` naming
the rung and the row count, and the warning text names BOTH causes because it
cannot distinguish them.

**The provenance does not survive `Feature()`, and that is stated, not
patched.**  `Feature('food_expenditures')(..., valuation=...)` forwards the
kwarg by signature and the VALUES are exact (Malawi via `Feature` and via
`Country` both sum to 426,562,493.98); but `Feature` concatenates frames whose
`attrs` disagree by construction, so `valuation_sources` lands in the `{}` row
of the propagation rule and is absent.  Pooling the counts across countries is
deliberately not built — a pooled `median_price: 280623` would say nothing
about which country was imputed, the same objection `harvest_kg`'s docstring
makes about its own pooled counts.

**Malawi is the case the kwarg exists for**: every one of its 288,704
produced/in-kind rows arrives with a null `Expenditure` (stock
`food_acquired_to_canonical`), so `basis='total'` equals `basis='purchased'`
there today, and valuing them raises measured food acquisition by about half
again.  The own-price rung reaches only 1.0% of them — a household rarely both
buys and grows the same item in the same unit in the same wave — which is
precisely why EPAR's consumption repo needs a spatial ladder behind it.

---
### Phase 3 — verification
- `food_acquired_valued` — **OK (anchored on §3, §4, §5)**.  Rungs run in the
  order given (pinned, `test_order_is_the_order_given`); `t` is an item key
  (`test_waves_do_not_pool`); the price pool is purchases only
  (`test_the_price_pool_is_purchases_only` — a survey-imputed in-kind value
  cannot become a "price" and feed itself back into its own median); zero
  treated as missing exactly as the existing `replace(0, np.nan)` does
  (`test_a_zero_value_is_a_candidate_not_a_report`).
- `_own_price_rung` — **OK (anchored on §5)**: no existing per-`(t,i,j,u)`
  household unit value; `rg 'own_price|unit_value'` over `lsms_library/` finds
  only `food_prices_from_acquired(units='unitvalue')`, whose output grain is
  wrong for the job (§5).  Pools a household's repeat purchases into ONE
  quantity-weighted price (`test_own_price_pools_a_households_repeat_purchases`).
- `_median_price_rung` — **OK (anchored on §2, §4)**: `median_price_valuation`
  is called, not reimplemented, and is not edited.  Null geo keys get unique
  tokens (`test_null_geo_never_forms_a_cell`); the working frame is built on a
  fresh `RangeIndex` so a duplicated `food_acquired` index cannot break the
  function's internal `reindex`.  The advertised-vs-actual ladder gap found by
  the red-team is closed by the resolve-to-nothing count (three tests: the
  null-column half, the absent-cluster half, and that a complete join is
  SILENT).
- `food_expenditures_from_acquired(valuation=None)` — **OK (anchored on §4)**:
  byte-identical, pinned against a verbatim copy of the pre-change body on a
  synthetic frame, a no-`s` frame, and **warm Uganda at both bases**
  (`TestDefaultPathUnchanged`).
- `Country._valuation_geo` — **OK (anchored on §3)**: degrades to what
  `cluster_features` carries and NAMES the missing rungs in a
  `ValuationLadderWarning` (two tests).  Measured: both Uganda and Malawi get
  the full `['v', 'District', 'Region']` ladder, so neither warns.
- Cache impact — **OK (anchored on §4)**: `transformations.py` carries no
  `@build_transform()`; the `country.py` edits are inside the generated
  `__getattr__` method and `_valuation_geo`, both read-path.  Measured against
  the base commit `9e4c0867` in a detached worktree: 9 `build_transforms_
  fingerprint` values + 8 `Country._table_cache_hash` values (Uganda, Malawi x
  4 tables) — **0 of 17 moved**.
- Nothing cached — **OK (anchored on §1)**: `food_expenditures` is derived at
  read time (`Country._FOOD_DERIVED`); no parquet is written on this path, so
  no imputed value can be stored.  Corroborated by the unmoved hashes above.
- Test suite — **OK**: `tests/test_own_production_valuation.py` (45) plus
  `test_food_expenditures_basis`, `test_median_price_valuation`,
  `test_food_labels`, `test_table_structure`, `test_currency`,
  `test_conversion`, `test_uganda_tables`, `test_sample`, `test_u_code_leak`,
  `test_nonfood_expenditures_schema`, `test_notes`, `test_catalog`,
  `test_build_transform_hash`, `test_schema_consistency`: **3,445 passed,
  10 failed**.  All ten failures are in `test_label_selection.py` and are
  **PRE-EXISTING and environmental** — verified by running that file against a
  clean checkout of the base commit `9e4c0867`, which fails the same ten.  They
  are a spawned-subprocess DVC config error in this worktree
  (`ConfigError: expected 'url' ... data['remote']['ligonresearch_s3']`),
  nothing to do with `valuation=`.
- **CONTRADICTION found and fixed**: the `basis='total'` docstring said
  "*Imputing* own-production value at purchase prices ... is a tracked
  follow-up, NOT done here", which this change makes false.  Amended to name
  `valuation=` as the one and only way the function fabricates a value.
