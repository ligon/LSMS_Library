# GH #1509, second amendment: the unit-level non-purchase rung, `unit_nonpurchase`

Agent: framework fix agent, Claude Opus 5.5 (`claude-opus-5-5`), for Sue (job 41201840, n0035).
Worktree `/local/job41201840/mirrors/LSMS_Library/.claude/worktrees/agent-ab25b116f4113864a`,
branch `fix/1509-1068-1129-food-unit-evidence-20261112`.
- Started at head `5b2496a123b011f0c9a58cde5a27d3fca997fdb4` (asserted).
- Base: development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.
- **Final head: `0966d62ef3dc3ee60ba9a17f6ab90dd26043c15c`.** The post-commit hook published every
  commit to the mirror. No GitHub writes.

Work files are in `work/` next to this file: job scripts, `measure_arm.py`, `compare.py`,
`hash_census.py`, per-arm JSON and positional parquet, census JSON, pytest logs, and the
generated comparison `out/tables.md`. Every number below comes from `work/out/`.

## Verdict: READY

The rung implements the ruling ("Add a unit-level rung") as written:
- `unit_nonpurchase` sits between `item_unit_nonpurchase` and `none`.
- It is the purchase `unit` rung's estimator, run on non-purchase evidence, with the `unit`
  rung's gates.
- It serves a row only when that row's own `(j, u)` cell has no priced purchase in any wave and
  no rung above serves it.
- It is labelled, and counted both in the partition and in
  `attrs['kg_factor_evidence']['fallback']`.

**GhanaLSS gets back all 28,939 rows, each at its pre-#1509 factor.** That adds 10,063 kg.
`food_prices()` and `kgprice` are back at the base counts: 560,023 and 187,096 (1991-92: 28,754).

**The judgment call to look at:** 23,156 of those 28,939 rows are ones the item fallback's
dispersion gate refused. The unit-level rung serves them anyway, at a u-pooled factor, because
the `unit` rung it twins is ungated. That is what `unit` already does for refused purchase rows,
and base served these rows the same factors.

**One of the "unchanged" countries moves, as the ruling's condition allows: Uganda.**
- 15 of the 18 rows it lost under purchases alone land on the rung: +65 kg, +15 `food_prices()`
  rows and +12 `kgprice` rows.
- Their factors are within 2x of base's but not equal to them (section 4).

Cambodia, Serbia, Burkina Faso, Malawi, Niger and EthiopiaRHS are frame-equal to `5b2496a12`.

The hash footprint is GhanaLSS only, as before. The data-gated tests show 0 failures on every arm.

## Commits (`git log 5b2496a12..0966d62ef`)

| sha | what |
|---|---|
| `ee9d6cd23` | the rung: `transformations.py`, new `tests/test_gh1509_unit_nonpurchase_rung.py` (19 tests), and the pins it moves in `tests/test_gh1509_nonpurchase_fallback.py` |
| `10b4d7923` | `GHANALSS_FOOD_PRICES_ROWS` 557,502 -> 560,023, with the reason |
| `0966d62ef` | prose only: CLAUDE.md (= AGENTS.md) tuple and a new rung paragraph; kgprice 1991-92 back to 28,754 in CLAUDE.md, `coverage.md` and GhanaLSS `CONTENTS.org`; the units SKILL; the #1509 ledger |

`transformations.py` is byte-identical from `ee9d6cd23` (the measured arm) to the final head.

## 1. Design

**Name: `unit_nonpurchase`** (`transformations.FOOD_KG_UNIT_NONPURCHASE_LAYER`).
- *Why the name:* the first fallback was named by a rule, and this follows it. The rung is the
  `unit` estimator run on `nonpurchase` evidence, so `item_unit` : `unit` ::
  `item_unit_nonpurchase` : `unit_nonpurchase`.
- *Not `unit_produced`:* `inkind` and `other` rows are read too; Uganda serves 3 in-kind rows.
- There is a new constant, `FOOD_KG_NONPURCHASE_LAYERS = ('item_unit_nonpurchase',
  'unit_nonpurchase')`.

**Ladder:** `FOOD_KG_FACTOR_LAYERS = ('survey_kg', 'metric', 'item_unit', 'item_unit_tight',
'unit', 'item_unit_nonpurchase', 'unit_nonpurchase', 'none')`.

**Estimator.**
- The fallback already made one non-purchase estimate through `_pooled_kg_rungs`, with its
  u-pooled arm switched off (`unit_arm=False`). The switch is removed, so that one estimate now
  returns both candidates:
  - `kg_item` -> `item_unit_nonpurchase`;
  - `kg_unit` -> `unit_nonpurchase`. This is `conversion_to_kgs(item_col=None)`: one factor per
    `u` label, the median over households of each household's own price per unit over its own
    price per kilogram.
- The #1450 size pool and the #1452 tableless overlay come with it, exactly as on the purchase
  side.
- `_inferred_kg_rungs` and `_pooled_kg_rungs` are back to their development signatures.
- The estimate's stated-vs-inferred contradictions are discarded, so `StatedVsInferredKgWarning`
  stays the purchase rungs' alone, like `kg_factor_wave_spread`.

**Evidence pool:** every non-purchase row of the label, whatever its item.
- This is the twin of `unit`, which pools every purchase of the label.
- Only the SERVED row's own cell must have no purchase.
- Purchase `Expenditure` is blanked (`_nonpurchase_evidence`), so each household's ratio is
  self-valued on both sides.

**Gates: those of the purchase `unit` rung, and no others.**

| gate | `unit` | `unit_nonpurchase` |
|---|---|---|
| currency-denominated labels leave the estimate (GH #770) | yes | yes |
| a missing-unit sentinel refuses the rung | yes | yes |
| a `no_kg` declaration refuses it (and counts in `rows_refused`) | yes | yes |
| `survey_kg` / `metric` / every rung above outrank it | yes | yes |
| rows with a survey `Quantity_kg` anchor the baseline but are not estimated | yes | yes |
| an unsized label reads its size-pooled family | yes | yes |
| the #1452 overlay | yes | yes |
| only a finite, positive factor is served | yes | yes |
| `min_reports`, `min_baseline`, tight exception, `baseline_max_spread` | **no** | **no** |

- The four floors belong to the item arm. Of the `item_col=None` arm, `conversion_to_kgs` says:
  "it is the FALLBACK rung, and gating a fallback would leave rows with nothing". So the twin has
  none either.
- The consequence is stated in the docstring. A row that the item fallback's floors or dispersion
  gate refuse lands on `unit_nonpurchase`, exactly as a refused purchase row lands on `unit`. In
  GhanaLSS that is 23,156 of the 28,939 rows.

**Eligibility: unchanged, `~cell_priced`.**
- One priced purchase row on the row's own `(j, u)` cell disqualifies the row, whatever the floors
  made of that purchase. It counts in any wave, judged on the item arm's join key and through the
  size pool.
- So a gated-out purchase cell never reaches either fallback.
- `kg_factor_baseline_gate`'s identity `rows_refused == refused_to_unit + refused_to_none` still
  holds. GhanaLSS: 446,696 = 446,696 + 0, unchanged.
- A frame without an item level makes no fallback estimate, because the cell is `(j, u)`.

**Disclosure:**
- The partition, `attrs['kg_factor_sources']`, still sums to the frame.
- There is the per-row `KgFactorSource`, plus a new candidate column, `kg_unit_nonpurchase`. It is
  filled for every eligible row, whichever rung wins.
- `attrs['kg_factor_evidence']['fallback']['unit_rung']` holds `layer`, `rows_served` (equal to
  the partition's count), `rows_served_by_s`, `cells_served` and `labels_served`.
- The item fallback's gate ledger is now split the way the purchase gate's is:
  `rows_refused_spread == refused_spread_to_unit_nonpurchase + refused_spread_to_none`. On head,
  `rows_refused_spread` meant "left `none`". The total is the same number (GhanaLSS 23,156 on both).
- `kg_factor_size_pool` counts the rung under `served_by_unit_rung`, so
  `rows_served == served_by_item_rung + served_by_unit_rung` still holds.
- `kg_factor_declared_none['rows_refused']` counts a declared row that this rung would have served.
- `none_by_reason.no_estimate` now means "no purchase evidence, and neither non-purchase estimate
  gives a factor".

Unchanged, as asked:
- `baseline_refused_spread` (the purchase item rung's column);
- the `no_kg` behaviour;
- `conversion_to_kgs`, `_kg_inference_frame`, `_get_kg_factors`, `_unit_sentinel_mask` and
  `_is_currency_denominated`, which are shared with the crop ladder.

**Blast radius** (`rg`; the GitNexus MCP failed to connect):
- `food_kg_factors` is reached from `country.py`'s `_FOOD_DERIVED` dispatch, and from
  `GhanaLSS/_/nutrition.py` through `food_quantities_from_acquired`.
- `_inferred_kg_rungs` and `_pooled_kg_rungs` have no other callers, and no caller passed
  `unit_arm`.
- Only the two #1509 test files pinned the literal tuple. Every other test checks membership in
  `FOOD_KG_FACTOR_LAYERS`.

## 2. Measurements

Measured cold, each arm in its own fresh private `LSMS_DATA_DIR`, plus private XDG / TMP /
MPLCONFIG / issues log. Every root was asserted private. Import identity was asserted on a clone
of the mirror at the full SHA.

| arm | sha | job | node |
|---|---|---|---|
| base | `e5fdd2fa8` | 41244889 | n0037 |
| head (item fallback only) | `5b2496a12` | 41244890 | n0040 |
| cand | `ee9d6cd23` | 41245126 | n0043 |

- The comparison ran in job 41247107 (`work/out/tables.md`).
- `food_acquired` has the same frame hash in all three arms for every country, so the row
  comparisons are positional.
- **Controls:** base and head reproduce the previous report's numbers exactly. That covers
  GhanaLSS kg (11,007,391.9 / 9,908,688.2), `food_prices()` (560,023 / 557,502) and every other
  country's kg.

### Total kg, `food_quantities(units='kgs')`

| country | base | head | cand | cand vs head | cand vs base |
|---|---|---|---|---|---|
| **GhanaLSS** | 11,007,391.9 | 9,908,688.2 | **9,918,751.3** | **+10,063.1** | -9.89% |
| **Uganda** | 1,241,553.5 | 1,224,605.3 | **1,224,670.4** | **+65.2** | -1.36% |
| Cambodia | 57,198.5 | 56,578.9 | 56,578.9 | 0 | -1.08% |
| Serbia | 260,550.5 | 255,022.6 | 255,022.6 | 0 | -2.12% |
| Burkina_Faso | 657,253.5 | 657,253.5 | 657,253.5 | 0 | 0 |
| Malawi | 2,567,830.4 | 2,567,830.4 | 2,567,830.4 | 0 | 0 |
| Niger | 411,666.7 | 411,666.7 | 411,666.7 | 0 | 0 |
| EthiopiaRHS | 216,819.8 | 216,819.8 | 216,819.8 | 0 | 0 |

`food_kg_factors`' own total (`Quantity x kg_per_unit`) equals the API figure in every arm.

### GhanaLSS: rows on the new rung, by wave

| wave | rows (all `produced`) | kg they carry, cand | the same rows' kg on base | wave kg base / head / cand |
|---|---|---|---|---|
| 1991-92 | 19,010 | 1,147.9 | 1,147.9 | 282,371 / 234,072 / 235,220 |
| 1998-99 | 3,623 | 4,378.1 | 4,378.1 | 392,777 / 288,810 / 293,188 |
| 2005-06 | 5,563 | 3,839.1 | 3,839.1 | 815,248 / 659,765 / 663,604 |
| 2012-13 | 743 | 698.0 | 698.0 | 1,167,914 / 856,679 / 857,377 |
| 2016-17 | 0 | 0 | 0 | 8,349,082 / 7,869,362 / 7,869,362 |
| **total** | **28,939** | **10,063.1** | **10,063.1** | |

- **All 28,939 rows are the target rows.** Every row on the rung was `unit` on base and `none` on
  head. 0 rows on the rung were `none` on base. That bound is the acceptance check: a
  non-purchase u factor implies a pooled base factor for the label.
- **Every row is at its base factor** (`share_equal_1e-9 = 1.0`, measured). No priced purchase
  carries any of the 7 labels, which is consistent with base's all-evidence pool and the
  non-purchase pool giving the same factor. The kg-baseline side (whether any household behind
  those ratios priced a purchased kilogram) was not checked separately; the equality is the
  measurement.
- The rung serves 89 cells and 7 labels:

  | label | rows | kg per unit |
  |---|---|---|
  | `All` | 20,941 | 0.1 |
  | `Margarine Tin` | 4,192 | 0.178 |
  | `Fruit` | 1,807 | 0.05 |
  | `Nut` | 1,341 | 0.04 |
  | `Gallon` | 543 | 0.976 |
  | `Sheet` | 87 | 7.04 |
  | `Tree` | 28 | 3.0 |
- 23,156 of the rows are ones the item fallback's dispersion gate refused
  (`refused_spread_to_unit_nonpurchase`; `refused_spread_to_none` is 0).
- **Independent eligibility check:** the 89 served cells hold 28,939 rows and 0 purchased rows of
  any kind, in any wave.
- From head to cand, the only source change is `none -> unit_nonpurchase` (28,939). No factor
  moves on any other row (0 at rtol 1e-12).
- **Remaining `none`:** 3,571,848, the same count as base. Its `none_by_reason`:

  | reason | rows |
  |---|---|
  | `currency_unit` | 3,551,982 |
  | `declared_no_kg` | 16,633 |
  | `missing_unit` | 3,191 |
  | `cell_has_purchase` | 1 |
  | `no_estimate` | 41 (28,980 on head) |

### `kg_factor_sources` partition (sums to the frame in every arm)

| country | arm | metric | item_unit | item_unit_tight | unit | item_unit_nonpurchase | unit_nonpurchase | none |
|---|---|---|---|---|---|---|---|---|
| GhanaLSS | base | 86,495 | 993,954 | 70,333 | 605,938 | — | — | 3,571,848 |
| GhanaLSS | head | 86,495 | 532,603 | 5,980 | 894,613 | 208,090 | — | 3,600,787 |
| GhanaLSS | cand | 86,495 | 532,603 | 5,980 | 894,613 | 208,090 | **28,939** | 3,571,848 |
| Uganda | head | 182,551 | 55,760 | 727 | 109,181 | 0 | — | 7,352 |
| Uganda | cand | 182,551 | 55,760 | 727 | 109,181 | 0 | **15** | 7,337 |
| Cambodia | head = cand | 30,566 | 0 | 0 | 4,625 | 0 | 0 | 3,010 |
| Serbia | head = cand | 146,823 | 0 | 0 | 49,451 | 0 | 0 | 3,772 |
| EthiopiaRHS | head = cand | 12,430 | 25,837 | 249 | 13,645 | 0 | 0 | 2,744 |

Burkina, Malawi and Niger have identical partitions in all three arms, with 0 rows on the rung.

### `food_prices()` and `kgprice`, GhanaLSS

| | base | head | cand |
|---|---|---|---|
| `food_prices()` rows | 560,023 | 557,502 | **560,023** |
| 1998-99 | 26,490 | 25,392 | **26,490** |
| 2005-06 | 29,913 | 28,703 | **29,913** |
| 2012-13 | 48,662 | 48,449 | **48,662** |
| 2016-17 | 454,958 | 454,958 | **454,958** |
| `kgprice` rows | 187,096 | 182,675 | **187,096** |
| `kgprice` 1991-92 | 28,754 | 26,854 | **28,754** |
| `unitvalue` | 1,676,514 | 568,130 | 568,130 |

- Every wave of both modes is back at its base count.
- `unitvalue` reads no kg factor, so it is unchanged from head.

**What GhanaLSS still lacks against base:** 1,088,641 kg (-9.89%). Measured row by row, all of it
sits on rows that a purchase rung serves at another factor:
- 1,046,612 kg on rows that moved from their own `item_unit` / `item_unit_tight` cell to a
  purchase `unit` factor;
- 42,104 kg on purchase rungs whose factor changed in place;
- less 75 kg on `unit -> item_unit` rows.

The ruling keeps purchase rungs above both fallbacks, so neither fallback can touch these rows.

## 3. Cambodia, Serbia, Burkina Faso, Malawi, Niger and EthiopiaRHS: unchanged

- Partitions, kg and every price mode equal head's. From head to cand there are 0 source changes
  and 0 factor moves.
- **Why they cannot move.** By the bound above, the rung can only serve a row that had a base
  factor and is `none` on head. None of these countries has such a row:
  - Cambodia's 3,010 `none` rows were `none` on base too (`no_estimate`).
  - Serbia's 3,772 are `currency_unit`.
  - EthiopiaRHS's are `cell_has_purchase` 2,097 and `no_estimate` 247 (both `none` on base too),
    `missing_unit` 226 and `currency_unit` 174.
  - Burkina, Malawi and Niger make no second estimate at all (`ran: false`), because they have no
    priced non-purchase row.

## 4. Uganda moves, and why

- 15 of the 18 rows that were `none` under purchases alone land on the rung. All 18 were `unit` on
  base.
  - 12 are produced and 3 in kind, across 5 labels and 10 cells.
  - By wave: 2005-06 6, 2009-10 1, 2011-12 1, 2013-14 2, 2015-16 5.
- The totals move:

  | | head | cand | base |
  |---|---|---|---|
  | kg | 1,224,605.3 | 1,224,670.4 (+65.2) | 1,241,553.5 |
  | `food_prices()` rows | 348,163 | 348,178 | 348,181 |
  | `kgprice` rows | 325,036 | 325,048 | 325,051 |
- **The factors differ from base's.** The median cand/base ratio is 0.77, 1 of 15 is equal, and all
  15 are within 2x. The rows carry 65.2 kg against 71.1 kg on base. Base's u-pooled pool read
  purchases too; this one reads only the self-valued households.

| label | rows | factor, cand | factor, base |
|---|---|---|---|
| `Sack (unspecified)` | 8 | 2.749 | 3.556 |
| `139093` | 3 | 0.567 | 0.292 |
| `142092` | 2 | 1.417 | 0.923 |
| `142090` | 1 | 0.909 | 0.882 |
| `181087` | 1 | 0.25 | 0.25 |

- 8 of the 15 rows sit on a label, `Sack (unspecified)`, that carries priced purchases of other
  items.
  - The purchase `unit` arm still gives that label no factor, because no household both bought a
    kg of something and bought in sacks.
  - Serving these rows is the plain reading of the ruling: the condition is the `(j, u)` cell, not
    the label. The docstring says so.
  - If the maintainer wants the label judged instead, that is a one-line change to `eligible`. It
    is not made here.
- The other 3 rows stay `none`: 2 are on cells with purchases (`cell_has_purchase`) and 1 has no
  estimate.
- An independent recount over all waves finds 0 purchased rows in the 10 served cells.

**Non-purchase `Expenditure` x1000** moves no factor by more than 1e-9 relative, in any country.
GhanaLSS has 237 rows that differ in the last bit (max relative 1.9e-16), as on head. Every other
country has 0.

**Cost of the u arm on the non-purchase rows,** `food_kg_factors` per call. The arms ran on
different nodes, so read this as an indication only:

| country | base | head | cand |
|---|---|---|---|
| GhanaLSS | 26.3 s | 45.4 s | 52.0 s |
| Uganda | 3.2 s | 5.3 s | 6.3 s |
| Serbia | 1.3 s | 2.7 s | 3.0 s |
| Cambodia | 0.4 s | 0.8 s | 0.9 s |

GhanaLSS `food_quantities()` goes from 80 s to 91 s. Burkina, Malawi and Niger make no second
estimate.

## 5. Cache-hash footprint

The census reads config only. It ran in each measure job, and again at the final SHA (job
41247581).

| | base -> cand | head -> cand | cand -> final `0966d62ef` |
|---|---|---|---|
| `build_transforms_fingerprint` | 0 / 42 | 0 / 42 | 0 / 42 |
| `framework_imports_fingerprint` | 1 / 38 (GhanaLSS) | 1 / 38 (GhanaLSS) | 0 / 38 |
| `Country._table_cache_hash` | 22 / 555 (GhanaLSS) | 22 / 555 (GhanaLSS) | 0 / 555 |
| `Wave._input_hash` | 154 / 2,321 (GhanaLSS; 30 None on both) | same | 0 / 2,321 |

- The footprint is the expected one: GhanaLSS only, through `nutrition.py`.
- The docs commit moves nothing.
- GhanaLSS `nutrition` changes value again. It calls `food_quantities_from_acquired(units='kgs')`
  without `no_kg`, so it now sees the rung's kilograms.

## 6. Tests

**Synthetic subset** (29 files, data-free: no credentials, `unshare -rn`), in job 41245126 at
`ee9d6cd23`:
- 1,017 passed, 9 skipped, 4 xfailed, 4 failed.
- The 4 failures are the `test_build_transform_hash.py` subprocess tests. They fail under this
  harness on every arm (auth prompt), and they pass in the data runs below.

**Data-gated** (54 files: the previous report's list plus the new file), each on its arm's warm
private root:

| arm | job | files | result |
|---|---|---|---|
| base `e5fdd2fa8` | 41246476 | 51 (the three #1509 files do not exist there) | 2,544 passed, 16 skipped, 4 xfailed, 0 failed |
| head `5b2496a12` | 41246478 | 53 (the new file does not exist there) | 2,593 passed, 16 skipped, 4 xfailed, 0 failed |
| cand `10b4d7923` | 41247107 | 54 | **2,612 passed, 16 skipped, 4 xfailed, 0 failed** |

- head -> cand adds +19 passed, which is the new file.
- The files include `tests/test_food_kg_inference.py`, `test_food_kg_size_pool.py`,
  `test_currency_denominated_units.py`, `test_gh1487_kg_subset_is_a_slice.py` and
  `test_build_transform_hash.py`.
- The skips are identical on every arm: 15 are Uganda-replication tests (the package is not on
  this machine) and 1 is a warm Tanzania crop cache that is absent.

**At the final SHA `0966d62ef`** (job 41247581, cand warm root), the named files ran:
- the new file, both #1509 files, `test_food_kg_inference.py`, `test_food_kg_size_pool.py`,
  `test_currency_denominated_units.py`, `test_gh1487_kg_subset_is_a_slice.py` and
  `test_build_transform_hash.py`;
- result: **381 passed, 0 failed**.

**Full suite, data-free** (job 41247101; candidate at `10b4d7923`, which differs from the final
head by prose only):

| arm | result |
|---|---|
| cand `10b4d7923` | 113 failed, 14,850 passed, 249 skipped, 4 xfailed |
| head `5b2496a12` | 113 failed, 14,831 passed, 249 skipped, 4 xfailed |

- **The failure sets are identical** (`work/out/fails-{head,cand}-nodata.txt`, `comm -3` empty).
  None is a food, kg or #1509 test. The +19 passed is the new file.
- One of the 113 differs from the previous report's 112:
  `test_gh932_safe_ci_failure_locations.py::test_real_wrapper_optin_preserves_stream_privacy_cleanup_status_and_static_names`.
  It is a CI-wrapper JUnit-mapping test that imports no survey code, and it fails on BOTH arms
  here.
- Run alone and data-free (job 41247902), that file passes 2 of 2 times per arm: 23 passed each
  time.
  - The job was set up for 3 runs per arm.
  - The second cand run lingered about 15 minutes after its summary. I cancelled the job, my own,
    after the second head run.
- **Overlap, stated.** Two runs shared private directories with a run still in progress:
  - the gh932 head reruns used `roots/head-nodata` and `private/head/tmp` while the head full
    suite was still running there;
  - the final-SHA job (41247581) shared `private/cand` with the tail of the cand full suite.

  The two full-suite failure sets are identical anyway, so I read neither overlap as having moved
  a result.

**New file `tests/test_gh1509_unit_nonpurchase_rung.py`** (19 tests). Every test asserts that:
- the partition sums to the frame;
- `unit_rung.rows_served` equals the partition's count;
- the item fallback's gate ledger sums;
- `none_by_reason` sums to `none`;
- the size-pool identity holds.

The tests:
- pin the rung's place in the ladder;
- check that a no-purchase cell the item rungs cannot serve reads it, and is labelled;
- check that it pools across items sharing the label;
- check that the item fallback, a purchase `unit` factor, `survey_kg` and `metric` outrank it;
- check that a purchase cell never reaches it:
  - below the floors;
  - gated out by the dispersion gate, with the label holding a non-purchase factor of 3.0;
  - with the purchases in another wave, `serve_waves` included;
  - through a purchased sized sibling;
- check that an unsized label reads its size-pooled family on this rung;
- check that the item fallback's dispersion refusal lands on it, and that the ledger says so;
- check that `no_kg`, a missing unit and a currency unit refuse it;
- check that the ratio is self-valued on both sides, with the x1000 control;
- check that adding it moves no purchase-served row;
- check that a subset is a slice of the all-waves result;
- check that the derived tables convert and disclose its rows.

**Pins moved, each with a one-line reason** (`tests/test_gh1509_nonpurchase_fallback.py`):
- The layer-tuple pin.
- `test_min_baseline_gates_the_fallback` and `test_min_reports_gates_the_fallback`. Gated out of
  the item fallback, those rows now land on the ungated unit-level rung, as gated purchase rows
  land on `unit`. Both tests still assert that the item candidate is NA.
- The purchase-cell tests (below the floors, dispersion-refused) now also assert
  `kg_unit_nonpurchase` is NA. In both frames the label has a live non-purchase u factor of 3.0.
- `GHANALSS_FOOD_PRICES_ROWS` 557,502 -> 560,023 (`test_currency_denominated_units.py`).

## 7. Limits and residuals

1. **No accuracy claim.** GhanaLSS's recovered factors equal the pre-#1509 ones, and Uganda's are
   within 2x of them. No external unit weight was scored.
2. **The rung is ungated, because `unit` is.**
   - It serves the item fallback's dispersion refusals (23,156 GhanaLSS rows) at a u-pooled
     factor. That is what `unit` does for purchase refusals.
   - GhanaLSS's `All` label (20,941 rows at 0.1 kg) is the largest group. Base served it the same
     factor.
3. **Cell, not label.** 8 Uganda rows are served on a label that has purchases in other items
   (section 4). That is the ruling as written. The alternative is the maintainer's choice.
4. **Pre-existing quirk, not changed** (from the previous report). The item gates'
   counterfactual is estimated only when the gated estimate leaves at least one item cell.
5. **GhanaLSS `nutrition`** reads the fallback kilograms. Only the data tests' FCT-coverage floors
   were checked.
6. **Uganda's move is not checked against the replication.** The 15 Uganda-replication tests
   skipped on every arm (the package is not on this machine), so the 15 rows on the rung were
   compared with base only.
7. **Countries not measured here:** Ethiopia, Panama, Nigeria, Tanzania, Mali, Senegal, Togo,
   Benin, CotedIvoire, Guinea-Bissau, GhanaSPS, Guatemala, Peru, GhanaAHIES and Nepal. By the
   bound in section 3, a country can move only where its pre-#1509 factor came from a u-pooled
   estimate on a no-purchase cell.
8. **Compute on n0035:**
   - Only these ran there: git; file edits with the editing tools; shell heredocs for this
     report, the PR body and two job scripts (`work/jobV.sh`, `work/jobR.sh`); `jq`, `grep`,
     `sed -n`, `cmp` and `comm`; and `sbatch` / `squeue` / `sacct` / `scancel`. No python ran on
     n0035.
   - A watch loop kept a dedup file at `/tmp/u1509seen.477118`; it has been removed.
   - Every test, measurement and comparison ran in burst jobs (listed above), at most 3 at a
     time. I cancelled my own job 41247581 after it printed its summary and lingered.
