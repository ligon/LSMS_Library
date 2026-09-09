# Prior-Art Ledger — five sources of food prices, compared

> Per-task ledger for the cross-country price-source analysis
> (`slurm_logs/price_sources/PROTOCOL.org`). Inherits `STANDING.md`; cites
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git floor (gitnexus MCP failed to connect);
plus a warm-cache inventory probe (`slurm_logs/price_sources/inventory.json`).

## §1 Task, restated
For each country and wave, compare the food prices the library can observe
from up to five sources — the purchased **unit value** (`food_acquired`
`Expenditure/Quantity`, `s=purchased`), the **reported purchase price**
(`food_acquired.Price`, `s=purchased`, where stored), the household's
**own-consumption valuation** (`Price`, `s=produced`), the **crop sale price**
(`crop_production` `Value_sold/Quantity_sold`) and the **community price**
(`community_prices`) — at the cell grain `(t, j, u, geography)`, in levels and
in logs, along the repo's geography ladder; test whether log gaps behave as a
constant markup / iceberg cost (flat in the price level) or an additive cost
(falling in it); and evaluate the 30–50% marketing-margin claim. Analysis
first; data defects reported (and obvious config blockers fixed in-worktree).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country.food_prices(units=)` / `food_prices_from_acquired` | `transformations.py:1334` | `kgvalue`, `unitvalue`, `kgprice`, `unitprice`; `unitprice` = the stored `Price` (market for purchased, farmgate for produced) | `tests/test_food_prices_units_kwarg.py` | **reuse** — sources 1, 2, 3 |
| `Country.community_prices()` | `countries/{Ethiopia,GhanaLSS,Malawi,Mali,Niger,Nigeria,Tanzania,EthiopiaRHS}/_/community_prices.py` | surveyed cluster-level prices at `(t, v, j, u)`, reported fields only (`Price`, sometimes `NumberOfUnits`, `obs`) | via build; ledgers 562 / 591 | **reuse** — source 5 |
| `Country.crop_production()` | per-country scripts | harvest rows with `Value_sold`, `Quantity_sold` (14 declarers) | via build | **reuse** — source 4 |
| `transformations.median_price_valuation` | `transformations.py:2372` | WB geography-ladder median of observed sale prices, ≥10 obs per cell, finest → national | yes | **reuse** — the market-grain procedure (ladder + threshold) and the source-4 local price |
| `transformations._kg_factor_series` / `_get_kg_factors` | `transformations.py:1646` / `1004` | unit → kg factors, inferred from `Expenditure/Quantity` ratios + hand maps | partially | **reuse, flagged** — the factors are derived from source 1, so per-kg comparisons against source 1 are partly circular; native-unit matching comes first |
| `Country.cluster_features()` | — | `(t, v)` → `Region`, `District` | yes | **reuse** — the ladder's upper rungs |
| `food_acquired(labels='Aggregate')` | `.claude/skills/add-feature/food-acquired/aggregate-labels/` | coarse food labels; Uganda's `harmonize_crop` reuses them so crop `j` joins food `j` | yes | **reuse** — the only crop↔food join that exists |
| `country.py:5765-5793` | `_normalize_dataframe_index` additive collapse | re-derives `Price = Expenditure/Quantity` after summing duplicates | yes | **cite** — why source 2 must be proven independent |
| cell medians / gap matching | none in repo (`rg -i 'marketing margin|price gap|markup'` finds nothing) | — | — | **new**: `slurm_logs/price_sources/common.py` (numpy/pandas; self-test) |
| `fwl_regression`, `dummies` | `ligon/Metrics_Miscellany` `metrics_miscellany.org` §"Frisch-Waugh-Lovell Regression", §"DataFrame/Mat Manipulations" (not mirrored here; read via the GitHub API) | exact partialling-out of a regressor block via lstsq on an indicator matrix, then OLS on residuals | yes (its `test_fwl.py`) | **pattern reused, vendored** in `common.py` (`_dummies`, `fwl_partial`): the package depends on `datamat`, absent from this venv; statsmodels deliberately not used (not in venv, heavy) |

## §3 Definitions & conventions in force
- **`s`** canonical values `purchased, produced, inkind, other` — `data_info.yml:11`, `transformations.S_VALUES`.
- **`Price`** semantics per `s` — `data_info.yml:522-531`: market for purchased (back-calculated if not stored), farmgate for produced (survey-reported where available), imputed for inkind.
- **`kgvalue` ≠ literature "unit value"** — `STANDING.md §3`; `DESIGN_food_prices_units_kwarg_2026-05-06.org`.
- **Core never aggregates** — `CLAUDE.md` §"Grain Collapse"; the analysis aggregates in its own scripts, never by editing a table's reducer.
- **Geography ladder + ≥10 obs** — `median_price_valuation` docstring (WB `valuation_median_crops`).
- **Currency** per wave via `currency='column'`; cross-country levels only via `numeraire='PPP-2017'` (`conversion.convert`).

## §4 Invariants & assumptions
- Source 2 exists as an independent number only where a script *stores* `Price`; after any duplicate-index collapse it is `Exp/Qty` by construction (`country.py:5793`). Uganda stores it (97.7% populated); Guatemala/Panama (100%/99%) are presumed back-calculated and are out of scope.
- `food_acquired` has **no `Price` column at all** for Ethiopia, Malawi, Mali, Niger, Nigeria, Tanzania (inventory) — sources 2 and 3 are absent there in the API even if the instrument asks them (data-gap candidates).
- `community_prices` `j` overlap with `food_acquired` `j` from the inventory probe (union over all waves): Ethiopia 47/47, Mali 37/37, Nigeria 118/139, Niger 35/45, Tanzania 35/42, Malawi 19/51, GhanaLSS 166/662. **Corrected 2026-09-08 by the country agents:** GhanaLSS's 166/662 is an artefact of comparing unions — per wave, against the built table, every `Food: yes` price label joins (20/20, 20/20, 60/60, 59/59, 82/82, 132/132); the 400-odd non-joiners are non-foods. Niger's 10 non-joiners are 8 non-foods + feed + one leaf. The real cross-source blocker is the **unit** axis (the price survey weighs; the household names its own container), not labels. Malawi's 19/51 likewise: only 21 of the 51 priced items are foods and 19 of those 21 join.
- `crop_production` level naming is inconsistent: `j` (Uganda, Ethiopia, Tanzania) vs `crop` (Malawi, Nigeria, Mali, Niger); Nigeria and Mali carry **no `u` level** (Tanzania has `u` as a column, Kg on all sale rows; Niger has a `u` level -- the Phase-1 brief wrongly said otherwise). Where `u` exists it is the HARVEST unit; the sale is recorded in its own unit that the builds ignore (Nigeria 95-97% agreement, Niger 63-99%) -- GH #824.
- EthiopiaRHS `community_prices` returns 2,691 rows while the coverage matrix grades all eight waves `absent` — a grading contradiction to report.
- Regressing `log a - log b` on `log b` is biased by shared noise in `b` (synthetic pure markup gave beta -0.11, se 0.03); the regressor is the Bland–Altman midpoint, and a between-item spec keeps the cross-item variation an additive cost predicts on.
- The Lorenz-style scale invariance does not hold here: levels need a common currency within a wave (they have it) and a numeraire across countries; log *gaps* are unit-free.
- Fixes in this pass are config-only, in the agent's worktree, verified under `LSMS_COUNTRIES_ROOT` + a private `LSMS_DATA_DIR` (shared-cache hygiene, `CLAUDE.md` scrum-master addendum 3).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| sources 1–3 | reuse `food_prices(units=…)` filtered on `s` | §2; the units semantics are tested and documented |
| source 4 | reuse `crop_production` + `median_price_valuation` (+ `_kg_factor_series`) | the ladder is the repo's market-grain procedure |
| source 5 | reuse `community_prices()` | reported fields; per-unit via `NumberOfUnits` where stated |
| geography | reuse `cluster_features` | — |
| cell medians, gap matching, FE regression | **new** in `common.py` | nothing in the repo; shared so eight agents' outputs pool |

## §6 Open questions for the human
Resolved 2026-09-08: original docs in scope; Opus agents; ladder = existing
`median_price_valuation` procedure; Guatemala/Panama trimmed; agents may fix
obvious blockers in-worktree. Pooling: logs across countries, levels within
country-wave (PPP-2017 for one descriptive table).

---
### Phase 3 — verification (filled 2026-09-08)
Eight country analyses under `PROTOCOL.org`, a pooled synthesis (`SYNTHESIS.org`),
an independent red-team of the synthesis (verdict PRESENT-WITH-CORRECTIONS: every
CSV number reproduced to the digit from the country files; six framing
overstatements corrected in the revision, §8 of the synthesis lists them), and
the coordinator's own probes. Reproductions: pooled rows = Σ country rows
(38,214, row-identical); 124/124 threshold-regime groups reconciled; every §2–§5
statistic recomputed from `gaps.csv`.

- `common.cell_medians`, `match_gaps` (per-side thresholds), `model_rows` — OK
  (§5 "new"): the pairing and the ladder are what every country used; the
  thresholds are recorded per row.
- `common.fit_gap_model` — OK (§2 Metrics_Miscellany `fwl_regression` /
  `dummies` / `ols` reused, not reinvented; §4 Bland–Altman regressor). Two
  guards added mid-run from field failures: `MIN_DOF` (Nigeria: r² = 1 on 17
  cells / 16 effects; LinAlgError on a singular design) and a band-edge
  tolerance (two medians equal to log 1.3 to ten decimals). Known limitation
  stated in code and protocol: `within` is uninterpretable with a thin
  measurement side; `between` is primary.
- Sources 1–5 — OK (§5 reuse): built from `food_prices(units=)`,
  `community_prices()`, `crop_production` + `median_price_valuation` /
  row-level `Value_sold/Quantity_sold`; nothing read `food_acquired` or the
  roster directly. Corpus facts established: source 2 exists nowhere
  (Uganda's `Price` is `Exp/Qty` in the raw files); source 3 is not-asked in
  Ethiopia, Tanzania, Mali and unwired in Nigeria.
- §4 claims corrected by the agents (recorded in place): GhanaLSS 166/662 and
  Malawi 19/51 label gaps were union artefacts; Niger's `crop_production`
  has `u`; the crop sale unit ≠ harvest unit (#824).
- CONTRADICTIONs found and fixed in the corpus, not the analysis: #817, #818,
  #819, #829 landed; #820–#828, #830–#846 filed (DATA_PROBLEMS.csv, 69
  defects / 19 patterns; PHASE3_ISSUES.org).
- Result, as it survives the red-team: the two market sources are one price
  (median gap 0 in 11/16 country-waves; six of six instrument pairs
  independent, Nigeria 2019Q1 partly by construction); the farmgate margin is
  basis-dependent (native = margin proper, per-kg adds bulk-breaking); on the
  native sized same-good set, community/sale 1.31 (167 cells, in the 30–50%
  band) and purchase/sale 1.54 (253, above); "processed" pairs at 2.58 are
  mostly label folds (#833), not a measured processing wedge; no additive
  per-unit cost is identified either way; own-consumption valuations sit at
  the midpoint between farmgate and market (Uganda) and at the purchase price
  (GhanaLSS 2016-17), against instruments asking for farmgate.
