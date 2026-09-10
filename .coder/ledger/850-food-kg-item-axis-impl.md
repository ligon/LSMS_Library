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
