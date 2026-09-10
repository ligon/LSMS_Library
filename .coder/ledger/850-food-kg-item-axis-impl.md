# Prior-Art Ledger -- GH #850, IMPLEMENTATION

> Companion to `.coder/ledger/850-food-kg-item-axis.md` (the design-phase
> ledger). That file's §2-§5 are still in force and are cited, not repeated.
> Inherits the repo §0 baseline in `STANDING.md`.

**Search tier used:** ripgrep + git (floor). `gitnexus` MCP did not connect
(CONNECTION_CLOSED), so the `CLAUDE.md` §"Code intelligence" substitutes were
used -- `rg` over every caller of the touched symbols, plus the purpose-built
closure probe `slurm_logs/gh850_design/probe_hash_reach.py` and its assertion
form, `tests/test_build_transform_hash.py::test_no_transformations_symbol_reaches_any_fingerprint`.

**Branch:** `feat/850-food-kg-item-axis`, base `1b07c3e9`.

## §1 What was implemented

@ligon's decisions of 2026-09-09 (D1-D10), in the order they were landed:

1. **(c) the metric vocabulary** -- `KNOWN_METRIC`, `_FLUID_UNITS`,
   `_EXPLICIT_METRIC_PATTERNS`. Its own commit, because it moves a DIFFERENT
   feature (`harvest_kg`) and a reviewer should see that alone.
2. **(a) + (b) + (D)** -- the item axis, the per-unit price, per-row
   provenance.
3. Documentation (D9, D10) and the `CLAUDE.md` house register.
4. Tests.

## §2 Decisions taken, with the number that decided each

| decision | taken | measured on |
|---|---|---|
| **D4** `min_reports` for `(t, j, u)` | **5**, `FOOD_KG_MIN_REPORTS` | the floor sweep: 3->20 costs 1-2 pp of inference rows in every country. Set equal to `SURVEY_MEDIAN_MIN_REPORTS`; a test pins the tie |
| **D4-tight** the same exception on the step-2 rung | **not applied** | `d4_tolerance.csv`: <= 0.49 pp in 8 of 9 countries, max 0.57 pp (EthiopiaRHS at 2.00). Nothing to rescue on a floor that is already nearly free, and a second tolerance in the estimator would be a knob nobody can justify |
| **D5** spread statistic | **max/min of the per-report price per kg** | `d5_tolerance.csv`: at N = 3-4 an `IQR/median` gate admits 21 of Uganda's 28 in-band cells at tolerance 1.10 and 26 at 2.00 -- it does not discriminate -- and the cells it admits include ones whose three reports differ by 25x |
| **D5** tolerance | **1.25** (`FOOD_KG_TIGHT_TOLERANCE`) | see the table in §3 |
| **D5** the `item_unit_tight` tag | **"the cell exists ONLY because of the exception"** (no strict-baseline support clears `min_reports`) | the looser reading ("some contributing baseline was tight") flags 1.5-2.5x as many cells for a weaker claim: Malawi 12 vs 5, EthiopiaRHS 11 vs 4. The chosen one answers the consumer's question -- "would this row have fallen to `unit`?" |
| **D6** does the `(j, u)` factor carry `t`? | **No.** `t` lives inside the estimator; the delivered factor is one number per `(j, u)` | the D6 table: for cells estimated in >= 3 waves the median max/min across waves is 1.0-3.1 and 36-94% sit within 2x. A container's weight does not move between waves, so the spread is estimation noise and pooling is variance reduction. Safe across redenomination because each wave's estimate is a ratio of same-wave prices |
| **D8** per-row provenance | **yes** | between a fifth and a half of inferred rows are served by the fallback `unit` rung; a consumer could not tell |
| return shape | `item_col=None` -> `dict[str, float]` **unchanged**; `item_col='j'` -> `dict[(j, u), float]`; `_detail=True` -> private frame | nothing outside `transformations.py` calls `conversion_to_kgs` or `_get_kg_factors` (design ledger §2), so **no deprecation was needed and none was added**. `_get_kg_factors` keeps its dict because `_kg_factor_series` -> `harvest_kg` depends on it |

### §3 D5, the table that chose 1.25

`max/min` of the per-report price per kg inside a `(t, j)` cell, over the
cells with 3 or 4 reports ("in band"). `served` is the share of
inference-needing rows the `(j, u)` rung reaches.

| country | cells | in band | admitted @1.25 | admitted spread (med) | new (j,u) cells | served strict -> tight |
|---|---|---|---|---|---|---|
| Uganda | 652 | 28 | 1 | 1.20 | 6 | 0.4245 -> 0.4287 |
| Malawi | 612 | 33 | 3 | 1.09 | 5 | 0.8499 -> 0.8529 |
| Nigeria | 831 | 30 | 0 | -- | 0 | 0.9328 -> 0.9328 |
| Ethiopia | 253 | 11 | 1 | 1.25 | 0 | 0.9506 -> 0.9506 |
| Tanzania | 353 | 14 | 0 | -- | 0 | 0.9850 -> 0.9850 |
| Niger | 289 | 10 | 1 | 1.23 | 1 | 0.3489 -> 0.3490 |
| Mali | 405 | 11 | 0 | -- | 0 | 0.6943 -> 0.6943 |
| EthiopiaRHS | 341 | 15 | 3 | 1.20 | 4 | 0.6178 -> 0.6246 |
| GhanaLSS | 396 | 11 | 3 | 1.00 | 28 | 0.5568 -> 0.5603 |

**What decided it, and it is not the coverage.** The in-band cells taken
UNCONDITIONALLY would lift Uganda's coverage by 9.5 points (0.425 -> 0.520) --
but their spreads run 1.2x to **25x**, so nearly all of that would come from
baselines whose own three reports disagree by an order of magnitude. At 1.25
the exception admits 0-3 cells per country and 0-0.7 points, and the admitted
cells' median spread is 1.00-1.25. A tolerance of 2.00 admits reports that
differ two-fold, which is the floor giving up. The exception is a narrow,
principled admission, not a coverage lever, and the table says so.

## §4 Invariants honoured (and one corrected)

Everything in the design ledger's §4 still holds. Three things learned here:

- **`conversion_to_kgs`'s baseline dict was deliberately NOT widened.** It is
  now named (`_BASELINE_UNIT_CONVERSION`) but still holds the same eight keys:
  no litre, no ml, and none of the (c) spellings. DESIGN.org's summary says
  the vocabulary fix "enlarges the kg baseline"; **it does not**, and the
  prototype did not measure it doing so (`measure.py::prep` uses
  `LOCAL_UNIT_CONVERSION` even under `wide=True`). Widening it would move
  every inferred factor beyond what §2 of DESIGN.org measured. Stated
  follow-up, not a side effect.
- **The prototype and the implementation disagree on exactly one country, and
  the implementation is right.** Ethiopia: 7.406e5 kg (prototype) vs 7.248e5
  (shipped), a 2.1% gap. Cause, measured to the kilogram: `measure.py::prep`
  does `str(x)` on every `u`, turning a NaN unit label into the literal string
  `'nan'`, for which the prototype then infers a factor (0.5, plus a
  `('Salt', 'nan')` cell). Those 131 rows carry 15,808.8 kg, and
  740,559.0 - 15,809.0 = 724,750.0 is the shipped number to the kilogram. The
  shipped code drops NA keys from the delivered map and serves those rows
  `none`. Every other country agrees to four figures.
- **Test-plan item 7 was unimplementable as written.** It asked that a metric
  label "must NOT appear as a key of `conversion_to_kgs`'s output". The
  inference mints a key for every unit that is not in its baseline dict,
  `kg` included (that is what makes the self-test possible at all). The
  property that actually holds -- and is what the item was after -- is that the
  LADDER never uses it: the metric rung outranks both inference rungs, so
  such a row comes back `KgFactorSource == 'metric'` at the label's own value.
  That is what the test asserts.

## §5 Reuse decisions honoured

Per the design ledger §5: `_survey_median_factors`' fine -> coarse -> NaN ladder
is the pattern (not re-invented), `KgFactorSource` / `attrs['kg_factor_sources']`
is the provenance shape (`FOOD_KG_FACTOR_LAYERS` is a **sibling** tuple, never
a merge), `_normalise_join_key` / `_KEY_NA` are reused for the `(j, u)` join,
`_valid_factor` for the finite-and-positive screen, and `_as_float` for every
pd.NA-safe coercion. New: `_seeded_kg_factors` (split out of
`_get_kg_factors`), `_kg_inference_frame`, `_level_series`, `food_kg_factors`.

## §6 Verification

- **Cache: 0 of 13 values moved.** Recipe from
  `.coder/ledger/harvest-kg-shipped-factors.md` §Phase-3 (including its
  correction: the signature is `c._table_cache_hash(table, c.waves)`).
  `build_transforms_fingerprint` for 9 tables plus
  `Country('Uganda')._table_cache_hash` for 4, base `1b07c3e9`'s
  `transformations.py` vs the branch's, same process, same data root.
  Asserted going forward by
  `tests/test_build_transform_hash.py::test_no_transformations_symbol_reaches_any_fingerprint`.
- **Prototype vs implementation** (DESIGN.org test-plan item 11): agreement to
  four significant figures on the delivered kg total in 8 of 9 countries;
  Ethiopia's 2.1% gap is fully explained above and is the prototype's defect.
- **Tests**: 318 passed across `test_food_kg_inference` (34, new),
  `test_crop_kg_factor` (35), `test_shipped_factors`, `test_quantity_kg`,
  `test_volume_as_mass_kwarg`, `test_currency_denominated_units`,
  `test_nigeria_kg_factor`, `test_food_prices_units_kwarg`,
  `test_unpriceable_price_drop`, `test_median_price_valuation`,
  `test_food_prices_dtype`, `test_u_sentinel_protection`,
  `test_build_transform_hash`. `tests/test_own_production_valuation.py` does
  not exist on `development` and was not run.
- **Deliberate re-pins**, each with a dated docstring paragraph saying why:
  `test_volume_as_mass_kwarg.py::test_known_metric_unchanged` (the snapshot
  asks to be updated consciously), `test_quantity_kg.py::test_malawi_food_quantities_kg_total_preserved`
  (8.797e6 -> 2.907e6 after (c) -> 2.555e6 after (a)+(b)), and
  `test_crop_kg_factor.py::test_uganda_harvest_kg_baseline` (D7).
- **Measurement scripts**, all re-runnable, all read-only, all pointed at a
  scratch `LSMS_DATA_DIR` holding COPIES of the L2-country parquets:
  `measure_impl.py` (the shipped tables, before/after), `measure_d5.py` (the
  two tolerance sweeps), `measure_move.py` (per-row factor movement),
  `measure_prices.py` (top-10 median price per kg). Nothing was written to the
  shared cache; `find lsms_library/countries -name '*.parquet'` is empty.

## §7 Known gaps, stated rather than discovered later

1. **`_BASELINE_UNIT_CONVERSION` is not the seeded vocabulary** (§4). A
   `Grams` row still cannot anchor a price-per-kg baseline even though the
   library now knows a gram is 0.001 kg. Measurable, not measured.
2. **Range labels.** `_parse_explicit_metric` reads `Gourd (1-5lts)` as 5, not
   as the curator's 3.5. Six such labels in Uganda; pinned by
   `test_range_labels_are_read_as_one_endpoint_not_a_midpoint` so that a
   future midpoint rule goes red there first.
3. **`volume_as_mass=True` on the newly-read container labels.** D7's harvest
   move takes a `Jerrican (5 lts)` of grain as 5 kg; the disagreement audit's
   `reported_vs_inferred` share goes 5.4% -> 34.1% because of it. The gap is
   the specific-gravity assumption, not the vocabulary.
4. **The `unit` rung is still the old u-pooled factor**, with the old
   household baseline. The alternative (a `u` rung built as the median across
   items of the `(j, u)` estimates, on the `(t, j)` baseline) is unmeasured;
   DESIGN.org names it. It serves a fifth to a half of inferred rows, which is
   why (D) exists.

---

## §8 Red-team round (2026-09-10) -- what changed

`slurm_logs/2026-09-09_epar_curation/REDTEAM_P2_850_impl.org` reproduced every
number in §2-§6 and returned four actionable items plus one disclosure. All
four are landed; the disclosure is in `AGENTS.md` and in the docstrings.

### (1) The IQR paragraph was a scale error -- retired, not repaired

`max/min <= 1.25` means the reports lie within about +/-12%; `IQR/median <=
1.10` means the interquartile range is 110% *of the median*. They are not the
same tolerance, so "the IQR gate admits 21 cells at 1.10 and 26 at 2.00, and
is therefore not discriminating" compared incommensurable scales. Swept at its
own scale the IQR gate discriminates fine: at `IQR/median <= 0.10` Uganda
admits 2 cells whose `max/min` p90 is 1.32. **The choice of `max/min` stands
on the other ground the same docstring gave, and only that one**: at N = 3-4
an IQR *discards the extremes*, and the extremes are the whole question --
a single wild report barely moves an interpolated quartile. Rewritten to say
so, with the retired argument recorded rather than deleted.

### (2) `min_reports` now gates the PER-WAVE estimate

It screened `support` summed over waves while its docstring named the
`(t, j, u)` estimate, so one report per wave in five waves cleared a floor
that four reports in a single wave did not -- a divergence from
`_survey_median_factors`, the pattern §5 says was reused. Fixed at the source
(the filter runs before the cross-wave median, so a wave that cannot support
an estimate contributes none rather than lending its row count to waves that
can), and pinned by `test_min_reports_gates_the_wave_not_the_pooled_support`.

Coverage cost, measured -- `item_unit` rows, pooled gate -> per-wave gate:

| | Uganda | Malawi | Nigeria | Ethiopia | Tanzania | Niger | Mali | ERHS | GhanaLSS |
|---|---|---|---|---|---|---|---|---|---|
| before | 73,452 | 253,427 | 166,253 | 99,577 | 3,540 | 75,571 | 208,347 | 26,003 | 935,384 |
| after | 73,315 | 252,380 | 165,866 | 98,391 | 3,532 | 75,563 | 208,291 | 25,837 | 935,384 |
| kg total % | -0.00 | **+1.21** | **-3.08** | -0.49 | -0.04 | -0.02 | -0.00 | **-2.68** | 0.00 |

The rows lost fall to the `unit` rung, not to `none`. GhanaLSS is unmoved
because every one of its cells is single-wave.

### (3) D6: the delivered factor's wave provenance is now reported

The factor is a cross-wave median and nothing said across how many waves or
how far apart -- so a cell estimated in one of the two waves it serves, and a
cell whose estimates differ threefold, were indistinguishable from a cell
measured twice the same way. Added: `n_waves` and `wave_spread` on the
`_detail` frame, `kg_item_n_waves` / `kg_item_wave_spread` per row, and
`attrs['kg_factor_wave_spread']` on `food_kg_factors` and both derived tables.
`FOOD_KG_WAVE_SPREAD_REPORT = 2.0` is a REPORTING threshold; no row is refused.

| | Uganda | Malawi | Nigeria | Ethiopia | Tanzania | Niger | Mali | ERHS | GhanaLSS |
|---|---|---|---|---|---|---|---|---|---|
| rows served by an item rung | 74,033 | 253,253 | 165,866 | 98,391 | 3,532 | 75,593 | 208,291 | 26,086 | 941,364 |
| ... whose waves disagree > 2x | **27,480** | **60,500** | **30,512** | 13,961 | 20 | 699 | **46,980** | 4,571 | 0 |
| ... estimated in ONE wave | 19,180 | 127,964 | 42,362 | 9,988 | 124 | 39,500 | 122,659 | 3,975 | **941,364** |

### (4) `baseline_max_spread` -- PREPARED, NOT APPLIED

The finding behind it is bigger than D5's tolerance: `min_baseline` is an
ABSOLUTE floor with no dispersion check, so nine reports clear it and then
govern 10,107 rows (Niger millet) with no relation between the counts and no
warning when they disagree by 7.6x. Measured over the cells the strict rung
admits, `max/min` of their own per-report price per kg: median **7.5**
(Uganda), **178** (Malawi), **125** (Nigeria), **500** (Ethiopia), **1,019**
(GhanaLSS), max 9.0e7. A 4-report cell differing by 26% is refused; a 5-report
cell differing by nine million times is admitted in silence.

`conversion_to_kgs(baseline_max_spread=X)` refuses a `(t, j)` baseline whose
reports disagree by more than `X` -- `p90/p10` at N >= 10, `max/min` below it
(with 10+ reports a robust interdecile spread exists and one wild report
should not condemn the cell; below 10 there is no decile and the extremes are
the question). **Default `None`. @ligon chooses.** Rows moved off the item
rung, and the kg total against this branch:

| country | X=3 | X=5 | X=10 | X=30 |
|---|---|---|---|---|
| Uganda | 29,495 / -1.9% | 15,645 / -1.0% | 13,571 / -0.5% | 1,075 / -0.1% |
| Malawi | 183,222 / -9.4% | 95,954 / -1.7% | 26,784 / -0.1% | 5,662 / +0.1% |
| Nigeria | 110,145 / -3.9% | 71,718 / +0.8% | 40,103 / +1.4% | 14,947 / +1.8% |
| Ethiopia | 39,109 / +10.4% | 23,966 / +9.7% | 11,857 / +3.6% | 386 / -0.1% |
| Tanzania | 285 / -0.1% | 115 / -0.1% | 0 / 0.0% | 0 / 0.0% |
| Niger | 59,689 / **+19.6%** | 30,368 / **+18.7%** | 18,550 / **+14.8%** | 13,482 / **+16.7%** |
| Mali | 193,565 / -15.8% | 122,222 / -11.4% | 82,437 / -11.5% | 41,011 / -9.5% |
| EthiopiaRHS | 3,338 / -0.6% | 17 / +0.2% | 0 / 0.0% | 0 / 0.0% |
| GhanaLSS | 714,061 / -11.2% | 667,247 / -11.2% | 430,967 / +2.5% | 336,420 / +1.9% |

**Niger's millet baseline is refused at every candidate** (N = 9, max/min =
125.0, p90/p10 = 25.2), and Niger's kg total moves +14.8% to +19.6% -- toward
the answer key, since the tiya factor it removes is 8x too small. That is the
single strongest argument for arming the gate, and the reason it is a decision
and not a default: at X = 3 the same gate costs Mali 15.8% and GhanaLSS 11.2%
of their kilograms, and neither of those movements has been adjudicated.

### The disclosure that is not a code change

**Niger's -56.1% is not signed off as a correction.** The defect is 13 rows of
Niger `u='Kg'` data (millet at 2,286 FCFA/kg, maize at 2,840, against a retail
250-450); the branch is what makes it visible and localised. Recorded in
`AGENTS.md`, in `conversion_to_kgs`'s docstring ("removing a dilution is not
the same thing as removing an error"), and it wants its own Niger issue plus a
line in `Niger/_/CONTENTS.org`, which are follow-ups this branch does not make.

Also from the red team, and NOT acted on here (recorded so the next reader has
the numbers): §7.3's framing of the harvest movement as the volume assumption
is an overstatement -- 83% of the +1.78e7 kg rests on labels that state
KILOGRAMS (`Sack (100 kgs)` and friends, 30,217 rows), and for the volume rows
Uganda's own reported `KgFactor` and its curated `conversion_to_kgs.json`
independently agree that a 20-litre debe of beans is 20 kg. And §7.1's cost is
now measured: Uganda leaves 62% of its `metric`-served rows (112,701 of
182,550) outside the eight-key baseline vocabulary.
