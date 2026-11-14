# GH #1509 + #1068 + #1129: food kg evidence is purchases; unitvalue / units drop `Value` and `Unknown`

Agent: framework fix agent, Claude Opus 5.5 (`claude-opus-5-5`), for Sue (job 41201840, n0035).
Worktree `/local/job41201840/mirrors/LSMS_Library/.claude/worktrees/agent-ab25b116f4113864a`,
branch `fix/1509-1068-1129-food-unit-evidence-20261112`, base development
`e5fdd2fa831c4d03a247d264be7f339da459e6b0` (asserted), head
`6c14097e91532d7281f8206d15da5050fc343ff2`. Each commit was published to the mirror by the
post-commit hook (`origin/fix/1509-1068-1129-food-unit-evidence-20261112`). No GitHub writes.

Work files (scripts, raw JSON per country and arm, pytest logs): `work/` next to this file.
`work/out/tables.md` is the full generated comparison; the tables below are cut from it.

## Verdict: READY (implements the rulings as written; the maintainer should see the GhanaLSS consequence before merging)

**The ruling's premise "no factor lost" holds only where it was measured, Cambodia and Serbia. It
fails in GhanaLSS and Uganda:**
- **GhanaLSS:** 237,029 produced rows lose their inferred factor, and kg falls 13.0%.
  `food_prices()` goes 560,023 -> 538,099 rows; `kgprice` goes 187,096 -> 151,446, with 1991-92
  falling from 28,754 to 15,028.
- **Uganda:** 18 rows lose their factor, and kg falls 1.37%.

Both rulings are implemented as specified, base reproduces every old pin exactly, and the hash
footprint is the expected GhanaLSS-only one. The loss is reported here and not repaired.

## 1. What "purchase" means, per country

**Definition in code.** A row is evidence for the food price-ratio inference iff its `s` is
exactly `'purchased'` (`transformations._KG_EVIDENCE_SOURCE`). That is the `S_VALUES` spelling,
and the same test `food_expenditures_from_acquired(basis='purchased')` already applies.
- `produced`, `inkind`, `other`, an NA `s` and any non-canonical spelling (e.g. `Purchased`) are
  NOT purchases. Their `Expenditure` is blanked for the estimators only, so they never silently
  become purchases, and `attrs['kg_factor_evidence']['priced_rows_excluded']` counts them by `s`.
- A frame with no `s` at all is left whole, since there is nothing to filter on, and the attr says
  so (`source_level: False`). Only a direct call reaches this branch: `data_info.yml` declares `s`
  on `food_acquired`, every measured served frame carries it, and `validate_acquisition_source`
  refuses an NA `s` on every Country read. Tested by
  `test_a_frame_without_s_is_all_evidence_and_says_so`.
- No fallback: a cell with no purchase evidence gets no inferred factor
  (`test_no_purchase_in_a_cell_means_no_item_factor_and_no_fallback`).

Under the ruling, "all purchase rows" includes Cambodia's allocated purchased quantity. The
`allocated-quantity` caveat is recorded in the `food_kg_factors` docstring and in
`Cambodia/_/CONTENTS.org`.

Measured on the built `food_acquired` that the derived tables see (`Country._aggregate_wave_data`),
which is identical in both arms (equal frame hash in all nine countries). "Priced physical" means
`Expenditure > 0`, `Quantity > 0`, and a unit that is neither `Value` nor a missing-unit sentinel.

The first two columns are measured. The last column is a descriptive note drawn from each
country's `CONTENTS.org`, build script or `ehcvm.py`; it was not measured.

| country | s values (rows) | priced physical rows, by s | note on `purchased` (source, not measured) |
|---|---|---|---|
| Cambodia | produced 5,857, purchased 32,344 | produced 5,562, purchased 29,625 | `s05q05` spent. Quantity is ALLOCATED as `total x spent/(spent+obtained)` (#108). `produced` lumps own production and in-kind |
| Serbia | inkind 9,603, produced 21,497, purchased 168,946 | inkind 9,603, produced 21,497, purchased 165,174 | 7-day diary with a daily source flag. produced/inkind carry self-valued amounts |
| Uganda | inkind 23,107, produced 85,345, purchased 247,119 | inkind 21,472, produced 85,035, purchased 241,674 | home and away purchases folded into `purchased`. `value_own` / `value_inkind` are household valuations |
| GhanaLSS | produced 938,163, purchased 4,390,405 | produced 603,492, **purchased 882,108, all in 2016-17** | purchases are `u='Value'` in every wave before 2016-17. Produced rows carry the `8h-farmgate` derivation (1998-99 to 2016-17) |
| Burkina_Faso | inkind 17,002, produced 45,393, purchased 704,935 | purchased 234,524 only | non-purchase rows carry no `Expenditure`. 2014 passages 2-4 are money-only with `u='Unknown'`. 2018-19 and 2021-22 are EHCVM: see Niger |
| Malawi | inkind 107,370, produced 181,334, purchased 682,827 | purchased 680,373 only | non-purchase rows carry no `Expenditure` |
| Panama | inkind 25,867, other 5,666, produced 34,782, purchased 504,627 | purchased 483,661 only | `other` exists here, and it is not a purchase |
| EthiopiaRHS | inkind 1,634, produced 15,638, purchased 37,633 | inkind 28 and produced 374 (1989 only), purchased 37,258 | |
| Niger | inkind 15,253, produced 18,177, purchased 206,448 | purchased 165,029 only | EHCVM: purchased `Expenditure` is the derived last-purchase valuation (`ehcvm.derive_ehcvm_purchase_value`). It is still a source-labelled purchase |

Other countries were not measured. Read statically, every canonical build writes `s` from
`S_VALUES`.

## 2. Diff summary and blast radius

Commits on the base (`git log e5fdd2fa8..6c14097e9`):
- `7e42d1097`: implementation, synthetic tests and docs.
- `d79da8bc6`: prior-art ledger `.coder/ledger/1509-1068-1129-food-unit-evidence.md`, Phase 1.
- `df8075b11`: data pins moved, plus the CLAUDE.md paragraph.
- `d4837ec7a`: ledger Phase 3.
- `6c14097e9`: kgprice figures corrected in docs and comments (1991-92: 28,754 -> 15,028).

Totals: 14 files, +790 / -56.

Changes in `lsms_library/transformations.py` (mostly docstrings):
- **New helpers:** `_KG_EVIDENCE_SOURCE`, `_purchase_rows`, `_purchase_evidence`,
  `_kg_evidence_ledger`.
- **`food_kg_factors`:** passes `_purchase_evidence(df)` to `_pooled_kg_rungs` and to
  `_rows_older_than_their_table`. This is the one-line filter. It stamps
  `attrs['kg_factor_evidence']`, which `food_quantities(kgs)` and `food_prices(kg*)` copy.
- **`_drop_unpriceable`:**
  - `_CURRENCY_UNPRICEABLE_MODES` adds `unitvalue` to the existing `currency` bucket.
  - `_LABEL_DROP_MODES = ('unitvalue',)` drops currency rows and missing-unit rows
    (`_unit_sentinel_mask`) by label, whatever Price they computed.
  - New `unknown` / `unknown_expected` bucket.
  - Buckets are disjoint: `currency`, then `money_only`, then `unknown`. `unknown` holds only
    rows whose Price was otherwise servable, so a zero quantity on an `Unknown` row stays the loud
    GH #591 `inf`, and item non-response stays loss.
- **`food_quantities_from_acquired(units='units')`:** drops missing-unit rows and records
  `attrs['quantity_rows_dropped'] = {'units', 'rows_in', 'unknown'}`. `u='Value'` rows are kept.

Deliberately NOT edited: `conversion_to_kgs`, `_kg_inference_frame`, `_get_kg_factors`,
`_unit_sentinel_mask`, `_is_currency_denominated`, `_level_or_column`. These are shared with the
crop and plot-input ladders and with EHCVM `food_acquired_to_canonical`. `unitprice` and the kg
modes' handling of sentinel rows are unchanged, because neither was ruled on.

Docs changed:
- `CLAUDE.md` (= `AGENTS.md`): the `units=` bullets, and a new #1509 paragraph with the numbers.
- `country.py`: the generated `food_prices` / `food_quantities` docstrings. Regenerated
  `docs/guide/data-methods.md` with `scripts/gen_data_method_docs.py`; `test_api_discoverability`
  passes.
- `docs/guide/coverage.md:309`.
- `.claude/skills/add-feature/food-acquired/units/SKILL.md`.
- `GhanaLSS/_/CONTENTS.org` (the Trap 9 reference to the old CLAUDE.md sentence).
- `Cambodia/_/CONTENTS.org` (the caveat).

The other `docs/guide/` pages hold no "Kwacha per Kwacha" statement; `rg` found it only in the
generated page and in `coverage.md`.

**Blast radius (`rg`; the GitNexus MCP failed to connect).**
- **Live callers of the edited functions:** `country.py` (the `_FOOD_DERIVED` dispatch in
  `__getattr__`) and `GhanaLSS/_/nutrition.py` (`food_quantities_from_acquired(units='kgs')`).
- **Mentions only:** `ehcvm.py`, `errors.py`, `nigeria.py`, `ethiopiarhs.py` and
  `Nigeria/2012-13/_/food_acquired.py` mention them in comments or docstrings.
- **`_drop_unpriceable`:** one caller.
- **Import-closure probe:** `_build_registry._closure_parts` over every
  `from lsms_library... import` in every country/wave `.py`. It finds exactly one file that reaches
  an edited callable: `GhanaLSS/_/nutrition.py`. `Ethiopia/_/ethiopia.py:1634`'s `harvest_kg`
  import sits inside a docstring.
- **Risk:** MEDIUM for GhanaLSS values, because of the large kg move in section 3. LOW elsewhere.

## 3. Before / after

Measured cold in private roots, base `e5fdd2fa8` against candidate `d79da8bc6`. Jobs 41236013
(base) and 41236014 (candidate) ran on savio4_htc with 8 cores, each with a fresh `LSMS_DATA_DIR`
and its own L1.

SHAs per run:
- `d79da8bc6`: the measurement and the census.
- `df8075b11`: the data tests in jobs 41236522 / 41236756.
- `d4837ec7a`: the price modes and the remaining data tests in jobs 41237282 / 41237283.
- `6c14097e9` (head): adds only comments and prose. `transformations.py` is byte-identical from
  `7e42d1097` to head.

**Controls.** Base reproduces every pin the tests held:
- GhanaLSS: unitvalue 1,676,514, units 1,695,046, kgvalue 560,023.
- Panama: unitvalue 490,846.
- Malawi: kg 2.5678e6.
- Cambodia and Serbia: kg -1.0832% / -2.1216%, matching the 2026-11-07 comparison exactly
  (57,198.49 -> 56,578.90 and 260,550.47 -> 255,022.58).

### #1509: kilograms and prices

| country | `food_quantities()` kg | `food_prices()` rows | median price | factor rows moved | moved >2x | rows to `none` |
|---|---|---|---|---|---|---|
| Cambodia | 57,198 -> 56,579 (-1.08%) | 35,187 -> 35,187 | 5,000 -> 5,200 | 4,625 | 0 | 0 |
| Serbia | 260,550 -> 255,023 (-2.12%) | 196,274 -> 196,274 | 110 -> 120 | 49,451 | 0 | 0 |
| **Uganda** | 1,241,553 -> 1,224,605 (**-1.37%**) | 348,181 -> 348,163 | 2,000 -> 2,150 | 141,885 | 4,850 | 18 |
| **GhanaLSS** | 11,007,392 -> 9,572,742 (**-13.03%**) | 560,023 -> 538,099 | 8.16 -> 10.45 | 1,136,281 | 327,052 | **237,029** |
| Burkina_Faso | 657,254 (unchanged) | 234,524 (unchanged) | unchanged | 0 | 0 | 0 |
| Malawi | 2,567,830 (unchanged) | 680,372 (unchanged) | unchanged | 0 | 0 | 0 |
| Panama | 1,226,520 (unchanged) | 483,661 (unchanged) | unchanged | 0 | 0 | 0 |
| EthiopiaRHS | 216,820 (unchanged) | 35,625 (unchanged) | unchanged | 0 | 0 | 0 |
| Niger | 411,667 (unchanged) | 165,029 (unchanged) | unchanged | 0 | 0 | 0 |

The `food_kg_factors` totals (`Quantity x kg_per_unit` over the full frame) equal the
`food_quantities()` kg column in every country. No row moves FROM `none` anywhere. No finite
factor is lost in Cambodia or Serbia.

`kg_factor_sources` partition, in the two countries where it moved:

| country | arm | metric | item_unit | item_unit_tight | unit | none |
|---|---|---|---|---|---|---|
| Uganda | base | 182,551 | 59,738 | 718 | 105,230 | 7,334 |
| Uganda | cand | 182,551 | 55,760 | 727 | 109,181 | 7,352 |
| GhanaLSS | base | 86,495 | 993,954 | 70,333 | 605,938 | 3,571,848 |
| GhanaLSS | cand | 86,495 | 532,603 | 5,980 | 894,613 | 3,808,877 |

The other seven partitions are identical between base and candidate. In Cambodia and Serbia the
factor values on the `unit` rung move, but no row changes rung.

**GhanaLSS, read per wave.** Before 2016-17 its purchases are `u='Value'` in every wave. So in
1998-99, 2005-06 and 2012-13 the only physical-unit evidence was the section-8H farmgate valuation
of produced rows. Now 2016-17's 882,108 purchases are the only evidence; the factor has no `t`
axis, so they serve every wave.

| wave | kg, base -> cand | change | rows to `none` (all `produced`) |
|---|---|---|---|
| 1991-92 | 282,371 -> 155,361 | -45.0% | 137,329 |
| 1998-99 | 392,777 -> 242,805 | -38.2% | 22,793 |
| 2005-06 | 815,248 -> 485,973 | -40.4% | 61,241 |
| 2012-13 | 1,167,910 -> 819,242 | -29.9% | 15,666 |
| 2016-17 | 8,349,080 -> 7,869,360 | -5.7% | 0 |

- Rows that go to `none` are carried natively, not dropped.
- Source transitions (556,193 rows): item_unit -> unit 295,578; item_unit -> none 166,548;
  item_unit_tight -> none 41,542; item_unit_tight -> unit 22,811; unit -> none 28,939;
  unit -> item_unit 775.
- `food_prices()` rows by wave: 1998-99 26,490 -> 20,142; 2005-06 29,913 -> 18,296; 2012-13
  48,662 -> 44,703; 2016-17 454,958 unchanged. The wave set is unchanged (four waves).
- `units='kgprice'` (reported 8H `Price` / kg factor, all on `produced` rows) goes 187,096 ->
  151,446. By wave: 1991-92 28,754 -> **15,028**; 1998-99 26,490 -> 20,142; 2005-06 29,913 ->
  18,296; 2012-13 48,662 -> 44,703; 2016-17 53,277 unchanged. 1991-92 is still non-empty, so its
  `empty-pending-machinery` verdict still applies. Its stated evidence figure (28,754) is
  corrected in CLAUDE.md, `docs/guide/coverage.md` and `GhanaLSS/_/CONTENTS.org`.
- `unitprice` is unchanged (190,575), as it should be.
- The dispersion gate refuses more rows: 428,168 -> 446,696, all of them to `unit`.
- The egg no-kg declaration still refuses 16,633 rows.

**This is a movement, not a correction.** No external unit weight was scored. The loss is reported,
and no fallback was added to repair it.

**Other price modes** (jobs 41237282 / 41237283, warm roots):
- `kgprice`: Uganda 325,051 -> 325,036; Panama 483,661 and Serbia 196,274 unchanged.
- `unitprice`: unchanged everywhere (Uganda 325,816, Panama 490,846, Serbia 196,279).
- Cambodia and EthiopiaRHS carry no `Price`, so both modes raise `UnitsUnavailableError`, as on
  base.

**Uganda.** -1.37% overall; by wave from -0.3% (2011-12) to -4.8% (2009-10). 4,042 rows change
source:
- item_unit -> unit: 3,992.
- unit -> item_unit: 23.
- item_unit -> item_unit_tight: 9.
- unit -> none: 18 (2005-06 7, of which 1 is a purchased row; 2009-10 2; 2011-12 1;
  2013-14 2; 2015-16 6).

**Evidence ledger** (`attrs['kg_factor_evidence']`, candidate), priced rows read / excluded:

| country | read | excluded |
|---|---|---|
| Cambodia | 29,625 | produced 5,562 |
| Serbia | 165,174 | inkind 9,603, produced 21,497 |
| Uganda | 242,370 | inkind 21,599, produced 85,112 |
| GhanaLSS | 882,529 | produced 605,340 |
| EthiopiaRHS | 37,304 | inkind 28, produced 374 |
| Burkina, Malawi, Panama, Niger | | nothing excluded |

### #1509 controls

**Non-purchase `Expenditure` x1000.** Factor rows moved, base / candidate:

| country | base | candidate |
|---|---|---|
| Cambodia | 4,625 | 0 |
| Serbia | 49,451 | 0 |
| Uganda | 148,227 | 0 |
| GhanaLSS | 704,055 | 0 |
| EthiopiaRHS (402 such rows) | 0 | 0 |

Burkina, Malawi, Panama and Niger have no non-purchase `Expenditure`, so the control is vacuous
there (0 / 0).

**#1442 stand-in** (`work/control_1442.py`, inside jobs 41236531 and 41236522). Burkina 2014's
12,763 produced and inkind rows are given `Expenditure = Quantity x (median purchased unit value of
their (t, j, u) cell) x f`, for f = 0.5, 1 and 2.

| arm | factor rows moved (f = 0.5 / 1 / 2) | 2014 kg, from 334,742 |
|---|---|---|
| base | 33,270 / 12,618 / 44,782 | 332,340 / 336,064 / 340,331 (-0.7% / +0.4% / +1.7%) |
| candidate | 0 / 0 / 0 | unchanged at every f |

### #1129 / #1068: unitvalue and units

"Served" counts are rows of the returned frame; the drop-bucket counts are INPUT rows.

| country | unitvalue rows | served `Value` rows (at 1.0) | served `Unknown` | drop buckets: currency / unknown / money_only | units rows | served `Unknown` (units) | `quantity_rows_dropped.unknown` |
|---|---|---|---|---|---|---|---|
| GhanaLSS | 1,676,514 -> **568,130** | 1,107,464 (all) -> 0 | 920 -> 0 | 3,551,979 / 2,269 / 0 | 1,695,046 -> 1,694,111 | 935 -> 0 | 2,304 |
| Panama | 490,846 -> 483,661 | 6,795 (all) -> 0 | 390 -> 0 | 17,409 / 390 / 0 | 560,290 -> 559,847 | 443 -> 0 | 443 |
| EthiopiaRHS | 37,860 -> 37,660 | 154 (134) -> 0 | 46 -> 0 | 154 / 46 / 0 | 54,783 -> 54,671 | 112 -> 0 | 112 |
| Niger | 165,029 (unchanged) | 0 | 0 | 0 / 0 / 0 | 239,878 -> 239,870 | 8 -> 0 | 8 |
| Uganda | 355,510 -> 348,181 | 6,429 (all) -> 0 | 900 -> 0 | 6,429 / 900 / 0 | 355,544 -> 354,639 | 905 -> 0 | 905 |
| Burkina_Faso | 234,702 -> 234,524 | 0 | 178 -> 0 | 0 / 178 / 405,127 | 318,297 -> 318,027 | 270 -> 0 | 270 |
| Malawi | 681,614 -> 680,373 | 0 | 1,241 -> 0 | 0 / 1,275 / 0 | 971,062 -> 969,198 | 1,864 -> 0 | 1,967 |
| Serbia | 196,279 -> 196,274 | 5 (none) -> 0 | 0 | 3,772 / 0 / 0 | 196,279 (unchanged) | 0 | 0 |
| Cambodia | 35,187 (unchanged) | 0 | 0 | 0 / 0 / 0 | 35,190 (unchanged) | 0 | 0 |

GhanaLSS unitvalue rows by wave: 1987-88 71,602 -> **0**; 1988-89 72,649 -> **0**; 1991-92
103,821 -> **0**; 1998-99 161,244 -> 26,491; 2005-06 261,448 -> 30,024; 2012-13 512,320 ->
49,901; 2016-17 493,430 -> 461,714. GLSS1-3 `unitvalue` is empty, as ruled.

Three checks against the issues:
- The 1,107,464 served `Value` rows at exactly 1.0 match #1129's figure exactly.
- EthiopiaRHS's 46 served `Unknown` rows match #1068's figure exactly, and Niger's 0 does too.
- `price_rows_dropped` counts INPUT rows, before the `(t, i, j, u, s)` median, while the served
  counts are after it. GhanaLSS's 2,269 unknown input rows are 920 served rows; the two numbers are
  not the same quantity.

**`units` and `Value`: checked, not dropped.** `units` forms no ratio, so the 1.0 tautology cannot
arise; a served `Value` row is the recorded currency amount. Measured share of `u='Value'` rows with
`Quantity == Expenditure`:

| country | `Quantity == Expenditure` | note |
|---|---|---|
| GhanaLSS | 3,551,982 of 3,551,982 | |
| Uganda | 6,429 of 6,429 | |
| Panama | 6,795 of 17,409 | 10,614 Quantity NaN after GH #777 |
| EthiopiaRHS | 134 of 174 | |
| Serbia | 0 of 3,772 | 3,767 NaN |

So `units` serves those amounts as quantities in currency, which duplicates `food_expenditures`.
The `'kgs'` carry rule keeps the same rows under a pinned test
(`test_food_quantities_kgs_carries_lcu_rows`). Left for the maintainer (section 7).

**Warnings.** Taking currency and unknown rows out of the denominator pushes no measured country
over the 5% `UnpriceableRowsWarning` threshold. Cambodia (7.88%) and Burkina (15.8%) were already
over it, and both are unchanged.

## 4. Cache-hash footprint

`work/hash_census.py` (job 41236015) covers every country with a `data_scheme.yml` (38), every
declared table and every wave, comparing base and candidate clones:

| hash | moved | which |
|---|---|---|
| `build_transforms_fingerprint` (per table, and `None`) | 0 of 42 | none |
| `framework_imports_fingerprint` (country `_/*.py`) | 1 of 38 | GhanaLSS |
| `Country._table_cache_hash` | 22 of 555 | GhanaLSS, all 22 of its tables |
| `Wave._input_hash` | 154 of 2,321 (30 are `None` on both sides) | GhanaLSS, all 154 |

- This is the expected footprint, matching the #1487 / #1042 precedent of 22 / 154 / 1 / 0.
- The `country.py` docstring edit and both `CONTENTS.org` edits move nothing (Cambodia is unmoved).
- `tests/test_build_transform_hash.py` passes on the candidate (data run, credentials present).
- Consequence: GhanaLSS rebuilds once on its next read, and its `nutrition` table changes value,
  because it calls `food_quantities_from_acquired(units='kgs')`. `test_ghanalss_nutrition`'s
  FCT-coverage floors still pass.

## 5. Tests (job ids)

| run | job | base `e5fdd2fa8` | candidate |
|---|---|---|---|
| synthetic subset (20 files, data-free, network off) | 41236015 | 746 passed, 4 failed | 771 passed, 4 failed (the same 4) |
| full suite, data-free (`-m "not requires_s3"`, no creds, `unshare -rn`) | 41236015 | 112 failed, 14,783 passed | 113 failed, 14,807 passed |
| data-gated (28 files, warm private roots of each arm) | 41236531 / 41236522 | 970 passed, 15 skipped, 0 failed | 995 passed, 15 skipped, 0 failed |
| data-gated again, verbose with skip reasons | 41236756 | | 995 passed, 15 skipped |
| `test_gh932_safe_ci_failure_locations.py` rerun x2 per arm | 41236913 | 23 passed (job 41236913; it lingered after the summary and was cancelled) | 23 passed x3, run alone on n0035 with xdist off (2.2 s each; no data, network off) |
| remaining food data tests (24 files, from the widened search) | 41237282 / 41237283 | 1,574 passed, 1 skipped, 0 failed | 1,574 passed, 1 skipped, 0 failed (the skip: Tanzania crop warm cache absent) |

**The requested globs.** `tests/test_gh850*` and `tests/test_transformations*` match no files in
this tree. The #850 machinery is tested in `test_food_kg_inference.py`, which ran.

**Search for data tests.** It was widened, without truncation, to `kgvalue|kgprice|units='kgs'|
kg_factor|unitvalue|units='units'|food_quantities(|food_prices(` (73 files), minus the 28 already
run. That leaves 24 food files, run in jobs 41237282 / 41237283, and crop-side files that were
NOT run:
- `test_crop_*`, `*shipped_factors*`, `test_gh1043/1198/1457/1460/1475/1476/824/842/843/849`,
  `test_mali_/niger_crop_survey_kg`, `test_nigeria_safe_harvest`, `test_uganda_crop_condition`,
  `test_ethiopia_crop_codes_1252`.
- Reason: a closure walk shows that `harvest_kg`, `harvest_kg_factors`, `seed_kg`, `nitrogen_kg`,
  `yield_kg`, `fertilizer_rate`, `median_price_valuation`, `food_acquired_valued`,
  `food_expenditures_from_acquired`, `gross_crop_revenue`, `livestock_sales_value`,
  `crop_diversity`, `hdds` and `fcs` reach none of the edited callables.
- Their data-free parts ran in the full data-free suite, with identical outcomes on both arms.

**Where the runs executed.**
- Job 41236521 was scheduled onto n0035 and cancelled about 10 s after it started (it had only
  begun cloning). It was resubmitted with `--exclude=n0035.savio4`.
- Some light work did run on n0035 (no data, network off):
  - the synthetic smoke script and pytest runs of the new and edited synthetic files (`-n 3`,
    15-50 s each);
  - the docs regeneration;
  - the import-only closure probes;
  - the three single-file `gh932` reruns;
  - reading the JSON outputs.
- Everything that read survey data ran in burst jobs.

**Synthetic and full-suite failures.** The 4 synthetic failures are `test_build_transform_hash.py`
subprocess tests. They die on the auth prompt that the data-free harness provokes
(`LSMS_S3_CREDS=/nonexistent`), and they fail identically on both arms. They pass in the data run.
The full data-free failure sets are identical except for one candidate-only failure:
`test_gh932_safe_ci_failure_locations.py::test_real_wrapper_optin_preserves_stream_privacy_cleanup_status_and_static_names`.
That test is a CI-wrapper JUnit-mapping test that imports no survey code. Its rerun is in the table.
The rest are subprocess/make/auth tests that fail under the harness on both arms.

**The 15 skips** are all `test_uganda_api_vs_replication.py`: the replication package is not on
this machine. That comparison of Uganda `unitvalue` / `units` against the replication was
therefore NOT run (section 7).

**New file** `tests/test_gh1509_1068_1129_food_unit_evidence.py`: 24 tests. 22 fail on base, and
all 24 pass on the candidate.

**Pins moved, each with a one-line reason; none deleted:**
- `test_food_prices_units_kwarg.py::test_food_prices_unitvalue_kwacha_per_kwacha` is renamed
  `..._drops_kwacha_per_kwacha` and asserts the drop and the count.
- `test_962_unit_sentinel.py`: unitvalue no longer serves `Value` / `Unknown`; units no longer
  serves `Unknown`.
- `test_currency_denominated_units.py`:
  - `test_food_prices_unitvalue_unaffected` becomes `..._drops_the_currency_tautology`.
  - The `_drop_unpriceable` unit-mode test now checks the "keep in the denominator" property on
    `unitprice`; a new `unitvalue` test checks the drop.
  - `GHANALSS_FOOD_PRICES_ROWS`: 560,023 -> 538,099.
  - GhanaLSS unitvalue 1,676,514 -> 568,130; units 1,695,046 -> 1,694,111; unitprice 190,575
    unchanged.
  - Panama unitvalue 490,846 -> 483,661.
- `test_burkina_faso_2014_passages.py`: the tally identity now includes `unknown` (178) and
  `currency`.

## 6. Drafts

### Draft PR body

> **food kg evidence is purchases; `unitvalue` / `units` drop `Value` and `Unknown` (#1509, #1068, #1129)**
>
> Implements two rulings by @ligon of 2026-11-12. Both are read-time code in `transformations.py`.
>
> **#1509, "all purchase rows".** `food_kg_factors` blanks `Expenditure` on every row whose `s`
> is not exactly `'purchased'` before the price-ratio estimators run. Both `conversion_to_kgs`
> arms, the size pool and the #1452 tableless estimate therefore read purchases only.
> - Every row is still served. A produced or in-kind row reads the factor its `(j, u)` gets from
>   purchases.
> - `produced`, `inkind`, `other` and NA are not purchases.
> - No fallback is added.
> - `attrs['kg_factor_evidence']` counts the priced rows read and excluded, by `s`.
> - Cambodia's allocated purchased quantity stays evidence under the ruling. Its
>   `allocated-quantity` caveat is in the docstring and in `Cambodia/_/CONTENTS.org`.
> - The filter lives in `food_kg_factors`, not `conversion_to_kgs`, so the crop and plot-input
>   ladders are untouched.
>
> **#1068 / #1129, "drop and count".** `food_prices(units='unitvalue')` drops rows with
> `u='Value'` and with a missing unit by label. They are counted in `price_rows_dropped` as
> `currency` (the kg modes' bucket) and as a new `unknown` bucket.
> `food_quantities(units='units')` drops missing-unit rows and counts them in
> `attrs['quantity_rows_dropped']`. `unitprice` and the kg modes are unchanged.
>
> **Measured cold in private roots** (base `e5fdd2fa8`; base reproduces every old pin).
> `food_quantities()` kg:
> - GhanaLSS -13.0%. 237,029 produced rows of 1991-92 to 2012-13 lose their inferred factor,
>   because those waves' purchases are all `u='Value'`. "No factor lost" held only in Cambodia
>   and Serbia, where it was first measured.
> - `food_prices()` 560,023 -> 538,099 rows; `kgprice` 187,096 -> 151,446 (1991-92 28,754 ->
>   15,028, so the wave is still non-empty and keeps its `empty-pending-machinery` verdict).
> - Uganda -1.37%, Serbia -2.12%, Cambodia -1.08%.
> - Burkina, Malawi, Niger, Panama and EthiopiaRHS frame-equal.
>
> Non-purchase values x1000 move no factor. GhanaLSS `unitvalue` goes 1,676,514 -> 568,130 rows:
> its 1,107,464 rows at exactly 1.0 are gone, and GLSS1-3 is empty. Panama goes 490,846 -> 483,661.
> These are movements, not corrections; no external unit weight was scored.
>
> **Cache.** Only GhanaLSS moves, through `nutrition.py`: 22 of 555 table hashes, 154 of 2,321
> wave hashes, 1 of 38 framework-import fingerprints, 0 build-transform fingerprints.
>
> **Tests.** New `tests/test_gh1509_1068_1129_food_unit_evidence.py` (24). Pins the rulings change
> are moved, each with its reason. Data-gated run: base 970 passed, candidate 995 passed, 0 failed.
>
> Closes #1509, #1068, #1129. Unblocks #1442's kg-factor concern; #1442's own decision stands.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

### #1509: close

> Implemented as ruled on 2026-11-12 ("all purchase rows") in `<PR>`. `food_kg_factors`' price
> ratio reads only rows with `s == 'purchased'`, and every row is still served.
> - `produced`, `inkind`, `other` and NA are excluded and counted in
>   `attrs['kg_factor_evidence']`.
> - There is no whole-wave or source-less fallback.
> - Cambodia's allocated purchased quantity is admitted and documented as the
>   `allocated-quantity` caveat.
>
> Measured cold, `food_quantities()` kg:
> - Cambodia -1.08% and Serbia -2.12%, which reproduces the 2026-11-07 comparison.
> - Uganda -1.37%; 18 rows go to `none`.
> - **GhanaLSS -13.0%.** Its pre-2016 purchases are all `u='Value'`, so the 8H farmgate valuation
>   had been the only physical-unit evidence. 237,029 produced rows now carry their native unit,
>   `food_prices()` drops 21,924 rows, and `kgprice` drops 35,650 (1991-92 28,754 -> 15,028).
> - Burkina, Malawi, Niger, Panama and EthiopiaRHS are unchanged.
>
> Multiplying non-purchase `Expenditure` by 1000 moves 0 factor rows; on base it moved 704,055 in
> GhanaLSS and 148,227 in Uganda. Only GhanaLSS's cache hashes move. Closing.

### #1068: close (residuals named)

> Option (a), as ruled ("drop and count"), is implemented in `<PR>`.
> `food_prices(units='unitvalue')` drops `u='Unknown'` rows, along with legacy `Manquant` and a NaN
> `u` (`_unit_sentinel_mask`), and counts them in `price_rows_dropped['unknown']`.
> `food_quantities(units='units')` drops them and counts them in
> `attrs['quantity_rows_dropped']['unknown']`.
> - An `Unknown` row with a zero quantity stays the loud GH #591 `inf`.
> - EthiopiaRHS's 46 served `Unknown` unitvalue rows are now 0.
> - Served `Unknown` rows in `units` drop to 0: Malawi 1,864, Uganda 905, GhanaLSS 935, Panama
>   443, Burkina 270, EthiopiaRHS 112, Niger 8.
>
> Not ruled, and left as they were; open a follow-up if wanted:
> - The default `units='kgs'` still carries `Unknown` rows natively and sums them under
>   `u='Unknown'`.
> - `unitprice` still serves reported prices on `u='Unknown'` (GhanaLSS 2016-17 produced rows).
>
> Closing.

### #1129: close

> Option (a), as ruled, is implemented in `<PR>`. `food_prices(units='unitvalue')` drops
> `_CURRENCY_DENOMINATED_UNITS` rows and counts them in `price_rows_dropped['currency']`, the
> bucket the kg modes already use.
> - **GhanaLSS:** 1,676,514 -> 568,130 rows. All 1,107,464 rows at exactly 1.0 are gone, and
>   1987-88, 1988-89 and 1991-92 serve none.
> - **Panama**, measured in the same pass: 490,846 -> 483,661 (6,795 rows at 1.0).
> - **Uganda:** 6,429 rows at 1.0 gone.
>
> `units='units'` keeps `Value` rows. It forms no ratio, so it has no tautology: a `Value` row's
> quantity is the recorded amount, and `Quantity == Expenditure` on every GhanaLSS and Uganda
> `Value` row. The `kgs` carry rule keeps the same rows. That is a separate question if wanted.
> CLAUDE.md, the generated `food_prices` docstring and `docs/guide/coverage.md` are corrected.
> Closing.

### #1442: is it unblocked?

> **Yes for the #1509 hold; the issue's own Tier C decision remains.** With purchase-only evidence,
> a value served on Burkina 2014's produced or inkind rows no longer reaches the kg factors.
> Stand-in measurement: valuing 12,763 produced/inkind 2014 rows at 0.5x, 1x and 2x their cell's
> purchase unit value moves 33,270 / 12,618 / 44,782 factor rows on base, and 0 on the candidate.
>
> Serving C05/C06 would still change:
> - `food_expenditures(basis='total')`;
> - `unitvalue` and `kgvalue` prices on those rows;
> - the derivation registry (a constructed value must be registered).
>
> Those consequences are #1442's to decide.

## 7. Unverified limits

1. **No accuracy claim.** No external unit-weight answer key was scored for any movement. That
   includes GhanaLSS's -13% and Uganda's -1.37%, as well as Cambodia's and Serbia's moved `unit`
   factors.
2. **GhanaLSS older waves now borrow purchases from 2016-17.** That is a consequence of the
   ruling's design, and it is not repaired: 237,029 produced rows go to `none`. Whether they
   should is a maintainer question.
3. **The Uganda replication comparison did not run** (`test_uganda_api_vs_replication.py`, 15
   skips): the replication package is not on this machine. The Uganda `unitvalue` / `units`
   comparison against the replication's own figures is therefore unverified (it allows
   `max_na_asym=30`; 900 and 905 Unknown rows dropped).
4. **Countries not measured:** Ethiopia, Nigeria, Tanzania, Mali, Senegal, Togo, Benin,
   CotedIvoire, Guinea-Bissau, GhanaSPS, Guatemala, Peru, GhanaAHIES, Nepal. Their pins in other
   data tests passed where the data run reached them (Ethiopia via #1081/#919, Mali via #1487,
   Niger / Senegal / Burkina via #1452, Togo / Liberia via attrs carriers). Their kg and unitvalue
   movements were not tabulated.
5. **GhanaLSS `nutrition` values** change (hash moves, kg inputs move). Only the FCT-coverage
   floors were checked.
6. **`kgprice` / `unitprice`** were measured for GhanaLSS, Panama, Uganda and Serbia (section 3).
   They were not measured for Burkina, Malawi or Niger, whose kg factors are frame-equal, so
   `kgprice` cannot move there.
7. **The `#1442` stand-in is synthetic.** It uses cell-median purchase values, not the real C05/C06
   amounts.
8. **Not ruled, and left as they were:**
   - `units='kgs'` carries `Unknown` natively.
   - `unitprice` keeps `Unknown`.
   - `units` keeps `Value`.
   - The `food_prices_from_acquired` Notes sentence "only `s='purchased'` rows have a meaningful
     `Expenditure`" predates GhanaLSS `8h-farmgate` and is stale. It was not edited.
9. **The data-free full suite** was run in a deliberately crippled harness (no creds, no network).
   It is a differential check (identical failure sets bar `gh932`), not a green CI substitute.
10. **One D-candidate job** (41236522) printed its pytest summary and then lingered. I cancelled
    it after its results were written. The verbose rerun 41236756 reproduces those results.
