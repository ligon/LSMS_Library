# GH #1509 amendment: the disclosed non-purchase fallback rung, `item_unit_nonpurchase`

Agent: framework fix agent, Claude Opus 5.5 (`claude-opus-5-5`), for Sue (job 41201840, n0035).
Worktree `/local/job41201840/mirrors/LSMS_Library/.claude/worktrees/agent-ab25b116f4113864a`,
branch `fix/1509-1068-1129-food-unit-evidence-20261112`.
- Started at head `6c14097e91532d7281f8206d15da5050fc343ff2` (asserted).
- Base: development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.
- **Final head: `5b2496a123b011f0c9a58cde5a27d3fca997fdb4`.** The post-commit hook published every
  commit to the mirror. No GitHub writes.

Work files (scripts, per-arm JSON and positional parquet, census JSON, pytest logs, and the
generated comparisons `out/tables.md` and `out/tables2.md`) are in `work/` next to this file. Every
number below comes from `work/out/`.

## Verdict: READY

The rung implements the amendment as written:
- Evidence stays purchases only, except that a `(j, u)` cell with no purchase evidence in any wave
  reads self-valued evidence, on its own labelled rung.
- The rung sits below every purchase rung and above `none`.
- It is counted in the partition and in `attrs['kg_factor_evidence']`.
- It respects every gate, and a cell with any purchase never reads it. This is checked
  independently of the masking flag: 0 purchased rows in the 87 served cells.
- GhanaLSS gets back exactly the rows an item-keyed rung can return.
- Cambodia and Serbia (and Uganda, EthiopiaRHS, Burkina, Malawi, Niger) keep the #1509 numbers
  exactly.
- The hash footprint is GhanaLSS only.
- Data tests have 0 failures, and the full data-free suite's failure set is identical to head's.

What the maintainer should know: **the rung returns 87.8% of the lost factor ROWS but only 23.4%
of the lost KILOGRAMS.** Measured row by row, 95% of the kg still below base sits on rows that
purchases alone moved from their own item cell to a purchase-based `unit` factor. Those rows never
lost a factor, and the ruling keeps purchase rungs above the fallback (section 3).

## Commits (`git log 6c14097e9..5b2496a12`)

| sha | what |
|---|---|
| `25a2abd9e` | the rung: `transformations.py`, new `tests/test_gh1509_nonpurchase_fallback.py`, and the one #1509 test that asserted "no fallback" |
| `322530074` | the three gate tests get a surviving control cell (limits, item 3) |
| `f030278e8` | CLAUDE.md (= AGENTS.md) layer tuple and rung paragraph, kgprice figures (CLAUDE.md, `coverage.md`, GhanaLSS `CONTENTS.org`), SKILL, ledger, the `GHANALSS_FOOD_PRICES_ROWS` pin |
| `5b2496a12` | prose and comment only: the remaining GhanaLSS kg gap as measured row by row |

`transformations.py` at `5b2496a12` is byte-identical to `322530074`, the measured arm.

## 1. Design

**Name: `item_unit_nonpurchase`** (`transformations.FOOD_KG_NONPURCHASE_LAYER`).
- *Why the name:* it is the `item_unit` estimator (one row's own `(item, unit)` cell, the item
  arm's floors and dispersion gate) run on `nonpurchase` evidence.
- *Not `unit_nonpurchase`:* nothing is pooled across items. The ruling is worded per `(j, u)`
  cell, and the gates the brief names exist only on the item arm.
- *Not `item_unit_produced`:* `inkind` and `other` rows are read too, and a gift is not produced.

**Ladder:** `FOOD_KG_FACTOR_LAYERS = ('survey_kg', 'metric', 'item_unit', 'item_unit_tight', 'unit',
'item_unit_nonpurchase', 'none')`. The rung is reached only where `kg_item` and `kg_unit` (both
purchase-based) are NaN. That puts it below every purchase rung, `unit` included, and above `none`.

**Evidence:** `_nonpurchase_evidence(df)` is the complement of `_purchase_evidence`:
`Expenditure` is blanked on purchase rows. So the fallback ratio is self-valued on both sides: a
produced bunch over a produced kilogram, never a self-valued price over a purchase baseline, which
is the mixing #1509 removed. It runs through `_pooled_kg_rungs(..., unit_arm=False)`, so it gets:
- the same `min_reports`, `min_baseline`, tight exception and `baseline_max_spread`;
- the same #1450 size pool;
- the same #1452 tableless overlay.

**Eligibility:** a per-row array `cell_priced` (in `_KG_RUNG_ARRAYS`) is computed on the PURCHASE
frame. It means "this row's own cell holds at least one priced row (`_priced_rows`: E > 0, Q > 0,
not currency)", and it is judged:
- on the item arm's join key (stripped, lower-cased);
- over every wave read, before the `serve_waves` slice;
- through the size pool for an unsized label;
- independently of every floor.

So a cell with ANY purchase evidence never reads the rung, even when that evidence fails a floor or
the dispersion gate. Such a row falls to `unit` / `none` as before, and the `food_kg_factors` Notes
and the `FOOD_KG_FACTOR_LAYERS` comment say so. `kg_factor_baseline_gate`'s
`rows_refused == refused_to_unit + refused_to_none` holds: GhanaLSS 446,696 = 446,696 + 0,
unchanged. Sentinel units, currency units and `no_kg` declarations refuse the rung as they refuse
every inferred rung. A declared row the rung would have served counts in
`kg_factor_declared_none['rows_refused']`.

**Disclosure:**
- the partition (`attrs['kg_factor_sources']`, which still sums to the frame);
- two per-row columns: `kg_item_unit_nonpurchase` (the candidate, filled for every eligible row
  whichever rung wins) and `kg_cell_has_purchase`;
- `attrs['kg_factor_evidence']['fallback']`: `ran`, `priced_rows_read` by `s`, `rows_served`,
  `rows_served_by_s`, `cells_served`, `rows_tight`, `rows_single_wave`, `rows_over_wave_spread`,
  `rows_refused_spread`, and `none_by_reason`. The last partitions the `none` rows into
  `currency_unit` / `missing_unit` / `declared_no_kg` / `cell_has_purchase` / `no_estimate`.
- The attrs ride on `food_quantities` / `food_prices`, checked on the API table.
- Size pool: a fallback row counts under `served_by_item_rung`, because the rung is item-keyed.
- `kg_factor_wave_spread` stays the purchase item rungs' alone.

**Cost guard:** no second estimate is made when:
- the frame has no `s`;
- there is no priced non-purchase row (Burkina, Malawi, Niger: `ran: false`);
- or every cell has purchases.

**Not edited** (shared with the crop / plot-input ladders): `conversion_to_kgs`,
`_kg_inference_frame`, `_get_kg_factors`, `_unit_sentinel_mask`, `_is_currency_denominated`.

**Blast radius** (`rg`; the GitNexus MCP failed to connect):
- `food_kg_factors` is reached from `country.py`'s `_FOOD_DERIVED` dispatch and from
  `GhanaLSS/_/nutrition.py` (through `food_quantities_from_acquired`).
- `_inferred_kg_rungs` / `_pooled_kg_rungs` have no other callers.
- The `KgFactorSource` value tests in the corpus check membership in `FOOD_KG_FACTOR_LAYERS`.
  None pinned the literal tuple until the new test did.

## 2. Measurements

Measured cold, each arm in its own fresh private `LSMS_DATA_DIR` (plus private XDG / TMP /
MPLCONFIG / issues log). Every root was asserted private, and import identity was asserted, on a
clone of the mirror at the full SHA:

| arm | sha | job | node |
|---|---|---|---|
| base | `e5fdd2fa8` | 41240317 | n0164 |
| head (purchases only) | `6c14097e9` | 41240318 | n0164 |
| cand | `322530074` | 41241044 | another node |

- EthiopiaRHS was measured afterwards, in fresh per-arm roots (job 41243251).
- The comparisons ran in jobs 41242351 (`tables.md`) and 41243251 (`tables2.md`).
- `food_acquired` is frame-hash identical in all arms for every country, so row comparisons are
  positional.

### Total kg, `food_quantities(units='kgs')`

| country | base | head | cand | cand vs head |
|---|---|---|---|---|
| **GhanaLSS** | 11,007,391.9 | 9,572,742.1 (-13.03%) | **9,908,688.2 (-9.98% vs base)** | **+3.51%** |
| Uganda | 1,241,553.5 | 1,224,605.3 (-1.37%) | 1,224,605.3 | 0 |
| Cambodia | 57,198.5 | 56,578.9 (-1.08%) | 56,578.9 | 0 |
| Serbia | 260,550.5 | 255,022.6 (-2.12%) | 255,022.6 | 0 |
| EthiopiaRHS | 216,819.8 | 216,819.8 | 216,819.8 | 0 |
| Burkina_Faso | 657,253.5 | 657,253.5 | 657,253.5 | 0 |
| Malawi | 2,567,830.4 | 2,567,830.4 | 2,567,830.4 | 0 |
| Niger | 411,666.7 | 411,666.7 | 411,666.7 | 0 |

Head reproduces the previous report's numbers exactly. `food_kg_factors`' own total
(`Quantity x kg_per_unit`) equals the API figure in every arm.

### Rows on the new rung, by wave

| country | wave | rows (all `produced`) | kg they carry | wave kg base / head / cand |
|---|---|---|---|---|
| GhanaLSS | 1991-92 | 118,319 | 78,711.5 | 282,371 / 155,361 / 234,072 |
| GhanaLSS | 1998-99 | 19,170 | 46,005.1 | 392,777 / 242,805 / 288,810 |
| GhanaLSS | 2005-06 | 55,678 | 173,791.8 | 815,248 / 485,973 / 659,765 |
| GhanaLSS | 2012-13 | 14,923 | 37,437.6 | 1,167,914 / 819,242 / 856,679 |
| GhanaLSS | 2016-17 | 0 | 0 | 8,349,082 / 7,869,362 / 7,869,362 |
| Uganda, Cambodia, Serbia, EthiopiaRHS, Burkina_Faso, Malawi, Niger | all | **0** | 0 | unchanged from head |

The 335,946 kg those rows carry is exactly cand minus head.

### `kg_factor_sources` partition (sums to the frame in every arm)

| country | arm | metric | item_unit | item_unit_tight | unit | item_unit_nonpurchase | none |
|---|---|---|---|---|---|---|---|
| GhanaLSS | base | 86,495 | 993,954 | 70,333 | 605,938 | — | 3,571,848 |
| GhanaLSS | head | 86,495 | 532,603 | 5,980 | 894,613 | — | 3,808,877 |
| GhanaLSS | cand | 86,495 | 532,603 | 5,980 | 894,613 | **208,090** | 3,600,787 |
| Uganda | head = cand | 182,551 | 55,760 | 727 | 109,181 | 0 | 7,352 |
| Cambodia | head = cand | 30,566 | 0 | 0 | 4,625 | 0 | 3,010 |
| Serbia | head = cand | 146,823 | 0 | 0 | 49,451 | 0 | 3,772 |
| EthiopiaRHS | all three | 12,430 | 25,837 | 249 | 13,645 | 0 | 2,744 |

Burkina, Malawi and Niger have identical partitions in all three arms, with 0 rows on the new rung.

## 3. GhanaLSS: what comes back, and what does not

**Rows: 208,090 of the 237,029 lost (87.8%), exactly the item-keyed ceiling.**
- Purchases only had moved 166,548 `item_unit` rows and 41,542 `item_unit_tight` rows to `none`.
  166,548 + 41,542 = 208,090, and the rung serves exactly those rows.
- Measured: every one is at its base factor (`share_equal_1e-9 = 1.0`).
- Head -> cand, the only source change is `none -> item_unit_nonpurchase` (208,090). No factor
  moves on any other row.
- The rung serves 87 cells. Independent check: those cells hold 208,090 rows, all `produced`, and
  0 purchased rows of any kind in any wave.
- 41,542 rows are served through the tight-baseline exception (`rows_tight`). 143,140 rows come
  from single-wave cells. 46,346 rows come from cells whose per-wave estimates differ more than 2x:
  disclosed, not gated.

**Kilograms: 335,946 of the 1,434,650 lost (23.4%).** Decomposed row by row, base -> cand
(`tables2.md`), the -1,098,704 kg still missing is:

| rows | kg | what happened |
|---|---|---|
| 318,389 | -1,046,612 | moved from their own `item_unit` (295,578) / `item_unit_tight` (22,811) cell to a purchase-based `unit` factor. Per wave: 2016-17 -398,329, 2012-13 -312,064, 2005-06 -180,577, 1998-99 -100,077, 1991-92 -50,206 (`item_unit` part) |
| 531,828 + 576,224 | -42,104 | the purchase rung kept its name and changed its factor (`item_unit -> item_unit` -35,930; `unit -> unit` -6,174) |
| 775 | +75 | `unit -> item_unit` |
| 28,939 | -10,063 | u-pooled on base, still `none` |

The two `-> item_unit_nonpurchase` groups (166,548 and 41,542 rows) contribute 0: they are back at
their base kilograms.

**Rows still lost:** 28,939, all `unit` on base (a u-pooled factor), all with no purchase in their
cell (`no_estimate`). An item-keyed rung cannot replace a u-pooled factor. A `unit_nonpurchase`
rung would; it is not built (limits, item 2).

**`food_prices()` and `kgprice`:**

| | base | head | cand |
|---|---|---|---|
| `food_prices()` rows | 560,023 | 538,099 | **557,502** |
| 1998-99 | 26,490 | 20,142 | 25,392 |
| 2005-06 | 29,913 | 18,296 | 28,703 |
| 2012-13 | 48,662 | 44,703 | 48,449 |
| 2016-17 | 454,958 | 454,958 | 454,958 |
| `kgprice` rows | 187,096 | 151,446 | **182,675** |
| `kgprice` 1991-92 | 28,754 | 15,028 | **26,854** |

- The 2,521 `food_prices()` rows still short of base are rows still `none`.
- `unitvalue` is unchanged from head (568,130).
- Unchanged from head: the egg `no_kg` declaration still refuses 16,633 rows, and the purchase
  dispersion gate still refuses 446,696 rows, all to `unit`.
- The rung's own gate keeps 23,156 rows off it (`rows_refused_spread`).

`none_by_reason` (cand):

| reason | rows |
|---|---|
| `currency_unit` | 3,551,982 |
| `declared_no_kg` | 16,633 |
| `missing_unit` | 3,191 |
| `cell_has_purchase` | 1 |
| `no_estimate` | 28,980 |

## 4. Cambodia and Serbia (and the other purchases-only checks)

**Cambodia and Serbia keep the #1509 numbers exactly:**
- Partitions, kg (56,578.9 / 255,022.6) and every price mode are equal to head.
- Head -> cand: 0 source changes and 0 factor moves.
- 0 rows that were `none` on both base and head are served now.

**Why they cannot move.** The fallback estimate ran in both (`ran: true`; read produced 5,562 and
inkind 9,603 + produced 21,497). But no row there is open to it:
- Cambodia's 3,010 `none` rows are all `no_estimate`: their cells have no purchases, and no
  self-valued estimate clears the floors.
- Serbia's 3,772 `none` rows are all `currency_unit`.

**Uganda:** 0 rows on the rung. Its 18 rows lost under #1509 stay `none`: all were `unit` on base.
16 have no purchase in their cell, and 2 have one.

**EthiopiaRHS:** the estimate ran on 402 priced non-purchase rows (1989) and serves 0 rows.
`none_by_reason`: `cell_has_purchase` 2,097, `no_estimate` 247, `missing_unit` 226,
`currency_unit` 174.

**Burkina, Malawi, Niger:** no priced non-purchase row, so no second estimate (`ran: false`).
Frame-equal to head and to base.

**Non-purchase `Expenditure` x1000:** factor rows moved by more than 1e-9 relative:

| | base | head | cand |
|---|---|---|---|
| GhanaLSS | 700,073 | 0 | 0 (237 differ at the last bit, max relative 1.9e-16) |
| every other country | | | 0 |

On the candidate the scale cancels in the self-valued ratio.

**Cost of the second estimate**, `food_kg_factors` per call:

| country | base | head | cand |
|---|---|---|---|
| GhanaLSS | 25.8 s | 27.8 s | 44.3 s |
| Uganda | 3.7 s | 3.9 s | 5.7 s |
| Serbia | 1.5 s | 1.6 s | 2.6 s |
| Cambodia | 0.4 s | 0.5 s | 0.7 s |

- GhanaLSS `food_quantities()` goes 67 -> 78 s. Burkina, Malawi and Niger are unchanged.
- *Cross-node:* the cand job ran on a faster node than base and head, which shared n0164 (GhanaLSS
  cold build 348 s against 475 s). So the overhead is if anything understated.

## 5. Cache-hash footprint

The census was made with config reads only, in each measure job.

| | base -> head | head -> cand | base -> cand |
|---|---|---|---|
| `build_transforms_fingerprint` | 0 / 42 | 0 / 42 | 0 / 42 |
| `framework_imports_fingerprint` | 1 / 38 (GhanaLSS) | 1 / 38 (GhanaLSS) | 1 / 38 (GhanaLSS) |
| `Country._table_cache_hash` | 22 / 555 (GhanaLSS) | 22 / 555 (GhanaLSS) | 22 / 555 (GhanaLSS) |
| `Wave._input_hash` | 154 / 2,321 (GhanaLSS; 30 None on both) | same | same |

So the footprint is as expected: GhanaLSS only, through `nutrition.py`. GhanaLSS `nutrition`
changes value again: it calls `food_quantities_from_acquired(units='kgs')` without `no_kg`, so it
now sees the fallback kilograms.

## 6. Tests

**Synthetic subset** (28 files, data-free: no credentials, `unshare -rn`):
- Job 41240416 at `25a2abd9e`: 954 passed, 7 failed.
  - 3 were my new gate tests, whose frame let the gate refuse the only cell (limits, item 3);
    fixed in `322530074`.
  - 4 were the `test_build_transform_hash.py` subprocess tests that fail under this harness on
    every arm (auth prompt).
- Job 41241044 at `322530074`: **957 passed, 4 failed** (those same 4).

**Data-gated** (54 files, on each arm's warm private root):

| arm | job | result |
|---|---|---|
| cand `f030278e8` | 41242396 | **2,593 passed, 16 skipped, 4 xfailed, 0 failed** |
| head `6c14097e9` | 41242398 | 2,569 passed, 16 skipped, 4 xfailed, 0 failed |

- The 54 files are the previous report's 28 + 24 food files, plus the two #1509 files. They
  include `tests/test_food_kg_inference.py`, `test_food_kg_size_pool.py`,
  `test_currency_denominated_units.py`, `test_gh1487_kg_subset_is_a_slice.py` and
  `test_build_transform_hash.py`, which passes here.
- The 4 data-free harness failures pass in this run.
- Skips: 15 Uganda-replication (the package is not on this machine) and 1 warm Tanzania crop
  cache absent, identical in both arms.
- Job 41242396 printed its summary and then lingered; I cancelled it after the log was complete.

**At the final SHA `5b2496a12`** (prose and comments only since `f030278e8`), job 41243413 reran
`test_currency_denominated_units.py`, the two #1509 files, `test_food_kg_inference.py` and
`test_gh1487_kg_subset_is_a_slice.py`: **212 passed, 0 failed** (it too lingered after its summary and was cancelled).

**Full suite, data-free** (job 41242261):
- cand `322530074`: 112 failed / 14,832 passed.
- head `6c14097e9`: 112 failed / 14,808 passed.
- **The failure sets are identical** (`work/out/fails-{head,cand}-nodata.txt`). None is a food /
  kg / #1509 test. +24 passed is the new file.

**New file `tests/test_gh1509_nonpurchase_fallback.py`** (24 tests). Every test asserts that the
partition sums to the frame, the new layer included, and that `none_by_reason` sums to `none`.
The tests:
- pin the layer tuple and the rung's place;
- check that a cell without purchases reads the rung and is labelled (for `produced`, `inkind` and
  `other`);
- check that a purchase `unit` factor outranks it;
- check that the ratio is self-valued on both sides (3.0, not 0.6), with the x1000 control;
- check that adding a produced-only cell moves no purchase-served row;
- check that a cell with purchases never reads it:
  - below the floors;
  - refused by the dispersion gate, to `none` and to `unit`;
  - with the purchases in another wave, `serve_waves` included;
  - through a sized sibling in the size pool (and the produced-sibling contrast);
- check that `min_baseline`, the tight exception (counted), `min_reports` and the dispersion gate
  on a NON-purchase baseline (`rows_refused_spread`) all gate it;
- check that `no_kg`, a missing unit and a currency unit refuse it;
- check that `survey_kg` and `metric` outrank it;
- check that a frame with no `s`, or with no priced non-purchase row, makes no second estimate;
- check that a subset is a slice of the all-waves result;
- check that the derived tables convert and disclose the rows.

**Moved:**
- `test_gh1509_1068_1129_food_unit_evidence.py::test_no_purchase_in_a_cell_means_no_item_factor_and_no_fallback`
  is renamed `..._no_purchase_factor_only_the_disclosed_one`. It now asserts the labelled rung.
- `GHANALSS_FOOD_PRICES_ROWS` 538,099 -> 557,502, with the reason.

## 7. Limits and residuals

1. **No accuracy claim.** The recovered factors equal the pre-#1509 ones (measured); no external
   unit weight was scored.
2. **No `unit_nonpurchase` rung.** 28,939 GhanaLSS rows (10,063 kg) and 16 Uganda rows, all
   u-pooled on base, stay `none`. A second, u-pooled non-purchase rung would return them. It is not
   built: the ruling is per `(j, u)` cell, and the item arm is where the gates live. That is a
   maintainer decision.
3. **A pre-existing quirk, not changed.** `_inferred_kg_rungs` estimates the dispersion gate's
   counterfactual (`baseline_refused_spread`, `rows_refused`, and now `rows_refused_spread`) only
   when the gated estimate leaves at least one item cell. A frame whose ONLY cell the gate refuses
   therefore reports 0 refused rows. Not material on the measured countries.
4. **Disclosed, not gated:** 46,346 GhanaLSS fallback rows come from cells whose per-wave estimates
   differ more than 2x, and 143,140 from single-wave cells.
5. **GhanaLSS `nutrition`** reads the fallback kilograms (its call passes no `no_kg`). Only the data
   tests' FCT-coverage floors were checked.
6. **Countries not measured here:** Ethiopia, Panama, Nigeria, Tanzania, Mali, Senegal, Togo, Benin,
   CotedIvoire, Guinea-Bissau, GhanaSPS, Guatemala, Peru, GhanaAHIES and Nepal. Panama has no
   priced non-purchase row (previous report's composition), so it makes no second estimate.
7. **#1442:** not re-measured. A valued Burkina 2014 C05/C06 row on a cell that has purchases still
   moves no factor (the previous report's control), but one on a cell seen only as produced or
   in-kind would now set a factor on this rung.
8. **Compute on n0035:** git, file edits, and reading JSON / logs with `jq` / `grep` / `sed`. Ten
   `python3` one-offs ran on n0035, all plain string replacements in source, test and doc files,
   with no library import and no data (one was empty). Every test, measurement and comparison ran
   in burst jobs (listed above), at most 3 at a time.
