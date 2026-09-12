# Prior-Art Ledger — GH #850, food-side kg inference: the item axis

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`; cites it,
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git (floor). The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED), so the substitutes named in
`CLAUDE.md` §"Code intelligence" were used: `rg` across `lsms_library/`,
`lsms_library/countries/*/_/`, `tests/` and `bench/` for every caller of
`conversion_to_kgs` / `_get_kg_factors` / `_apply_kg_conversion`, plus a
purpose-built closure probe (`slurm_logs/gh850_design/probe_hash_reach.py`)
that walks the same graph `_build_registry` walks. Blast radius reported in §2.

**Line anchors as of:** `9e4c0867` (drift expected — match on symbol name).

## §1 Task, restated

`food_quantities(units='kgs')` and `food_prices(units='kgvalue')` are derived at
runtime from `food_acquired` (`Country._FOOD_DERIVED`, never registered in
`data_scheme.yml`). Both need kilograms, and where the row's unit label `u` is
neither hand-coded metric nor self-describing ("50 kg Bag"), the kilograms come
from `transformations.conversion_to_kgs` — a price-ratio inference that returns
one factor per unit label for the whole country. This task designs, but does not
implement, two corrections to that inference: put the item `j` into the grouping
so a bunch of bananas and a bunch of groundnuts stop sharing a factor, and divide
by `Quantity` in the step that is documented as producing a per-unit price.
Deliverable is `slurm_logs/gh850_design/DESIGN.org` plus the measurement scripts
that produced its numbers; @ligon signs off on the measured movement before code.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `conversion_to_kgs` | `transformations.py:782` | the price-ratio inference; three groupbys, returns `dict[u, float]` | indirectly (`test_quantity_kg.py`, `test_volume_as_mass_kwarg.py`) | **extend** |
| `_get_kg_factors` | `transformations.py:1004` | assembles `KNOWN_METRIC` + explicit-metric parse + inference into one `dict` | `test_currency_denominated_units.py`, `test_volume_as_mass_kwarg.py` | **extend** |
| `_apply_kg_conversion` | `transformations.py:1121` | maps `u.lower() -> factor`, multiplies `Quantity`, defers to a survey `Quantity_kg` | `test_quantity_kg.py` | **extend** (needs a per-row factor, not a dict) |
| `_parse_explicit_metric` | `transformations.py:964` | kg factor from a self-describing label | doctests | reuse unchanged |
| `KNOWN_METRIC` / `_FLUID_UNITS` | `transformations.py:924` / `:936` | hand-coded metric tokens | yes | reuse unchanged |
| `_CURRENCY_DENOMINATED_UNITS` | `transformations.py:765` | `{'value'}`; dropped from the whole inference (GH #770) | `test_currency_denominated_units.py` | reuse unchanged |
| `food_quantities_from_acquired` | `transformations.py:1228` | `units='kgs'` carry rule | `test_food_prices_units_kwarg.py` | reuse unchanged |
| `food_prices_from_acquired` | `transformations.py:1334` | `units='kgvalue'` etc. | `test_food_prices_units_kwarg.py`, `test_unpriceable_price_drop.py` | reuse unchanged |
| `_kg_factor_series` | `transformations.py:1660` | **the crop side's `inferred` layer — calls `_get_kg_factors`** | `test_crop_kg_factor.py` | must not move |
| `harvest_kg_factors` | `transformations.py:1913` | four-layer ladder + `KgFactorSource` provenance | `test_crop_kg_factor.py` | **the model for option D** |
| `_survey_median_factors` | `transformations.py:1855` | `(country,t,u,condition)` → `(country,t,u)` with `N >= min_reports` | `test_crop_kg_factor.py` | **the precedent for the fallback ladder** |
| `SURVEY_MEDIAN_MIN_REPORTS` | `transformations.py:1813` | `= 5`, declared not measured | yes | cite; this task measures a food-side twin |
| `Country._FOOD_DERIVED` dispatch | `country.py:4449-4471` | `fa = _aggregate_wave_data(...)`; `transform_fn(fa)` | integration | read-only |

Blast radius of an edit to `conversion_to_kgs` / `_get_kg_factors`, from the
`rg` sweep: **nothing outside `transformations.py` calls either.** Call sites are
`transformations.py:1051` (inside `_get_kg_factors`), `:1303`
(`food_quantities_from_acquired`), `:1431` / `:1442` (`food_prices_from_acquired`,
`kgvalue` and `kgprice`), and `:1674` (`_kg_factor_series`, the crop side). No
country script, no test, and no `bench/` module imports either symbol; the
`conversion_to_kgs.json` files under `Uganda/_/`, `Tanzania/_/` and
`Ethiopia/_/` are country artefacts that share the name and nothing else.

## §3 Definitions & conventions in force

- **Derived food tables are runtime-derived, never registered** — `CLAUDE.md`
  §"Derived Tables"; `Country._FOOD_DERIVED`, `country.py:4297`.
- **`food_prices(units='kgvalue')` = `Expenditure / Quantity_kg`**, deliberately
  not the literature's "unit value" — `CLAUDE.md` §"`units=` kwarg";
  `slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org`.
- **`u='Value'` is the one canonical currency sentinel**, and it is dropped from
  the whole inference rather than given a constant factor — `transformations.py:704-765`
  (GH #770); mirrored by `country._RESERVED_U_SENTINELS`.
- **A survey-supplied `Quantity_kg` beats any inferred factor**, and is carried
  as a product rather than a factor because food rows are summed across units —
  `data_info.yml:566-580`; `_apply_kg_conversion`, `transformations.py:1121`.
- **`KgFactorSource` ∈ `KG_FACTOR_LAYERS`**, and `none` is a layer, not a
  failure — `transformations.py:1825`.
- **Cache tiers and what moves a hash** — `CLAUDE.md` §"Cache Behavior";
  `_build_registry.build_transforms_fingerprint`, `_build_registry.py:411`.

## §4 Invariants & assumptions

- **`_get_kg_factors` is SHARED with the crop side.** `_kg_factor_series`
  (`transformations.py:1660`) calls it, and `harvest_kg_factors` uses the result
  as its `inferred` layer. The price-ratio branch is gated on `Expenditure` and
  `Quantity` both being columns (`transformations.py:1045`), which
  `crop_production` is not, so today the crop side gets only `KNOWN_METRIC` plus
  the label parser. A change to the **return shape** of `_get_kg_factors` breaks
  the crop side even though a change to the inference's numbers would not.
- **The effective groupby is `['t','i']`, not `['t','m','i']`.** `group_levels`
  filters to levels present (`transformations.py:1048`) and `m` is minted by
  `_add_market_index` inside `_finalize_result`, which runs *after* the derived
  transform (`country.py:4470-4472`). Measured: `food_acquired` arrives indexed
  `(i, t, v, j, u, s)` in every country checked.
- **`conversion_to_kgs` builds its kg baseline from a LOCAL eight-key dict**
  (`transformations.py:831-841`), not from `KNOWN_METRIC`: no litre, ml or cl.
  A litre row can enter the baseline only through a survey `Quantity_kg`.
- **Never store an inferred factor.** The Uganda / Tanzania / Ethiopia
  `conversion_to_kgs.json` files are the anti-pattern this task must not repeat;
  a factor is a function of the frame it was inferred from.
- **Do not touch `food_acquired` itself.** The defect is in the derivation, and
  `food_acquired` is a cached, hashed, registered table (`CLAUDE.md` §"Cache
  Behavior"); the derived tables are not cached at all.
- **`groupby()` defaults to `dropna=True`** and deletes NA-keyed rows —
  `CLAUDE.md` §"Grain Collapse" §3b. `_survey_median_factors` passes
  `dropna=False` for exactly this reason (`transformations.py:1905`); any new
  `(j, u)` groupby must do the same.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| per-unit price for step 2 | **extend** `conversion_to_kgs` | `Expenditure / Quantity` is one division inside the existing chain |
| item axis in the baseline | **extend** `conversion_to_kgs` | the `index=` kwarg already exists; the change is which levels the caller passes and what the result is keyed on |
| thin-cell fallback ladder | **reuse the pattern** of `_survey_median_factors` | fine group → coarse group → NaN, gated on `N >= floor`, is already written and tested on the crop side |
| the `N` floor | **cite** `SURVEY_MEDIAN_MIN_REPORTS = 5` | declared not measured (`transformations.py:1806-1813`); this task sweeps `N ∈ {3,5,10,20}` and reports coverage |
| per-row provenance | **reuse the shape** of `KgFactorSource` | `KG_FACTOR_LAYERS` and the `attrs['kg_factor_sources']` counter already define the vocabulary |
| per-row factor application | **extend** `_apply_kg_conversion` | it maps `u -> factor`; a `(j, u)` factor needs a two-key join |
| the derived-table shape | reuse unchanged | nothing about `food_quantities` / `food_prices` grain changes |

## §6 Open questions for the human

Ten of them, each with the number that decides it, in
`slurm_logs/gh850_design/DESIGN.org` §4 (D1–D10). The two that bind: **D5**,
the floor on the `(t, j)` price-per-kg baseline, which is the parameter the
current code does not have and the one the coverage sweep shows actually costs
something; and **D7**, where defect (c) changes `harvest_kg`'s inferred layer
on 47.7% of Uganda's crop rows and therefore crosses the task's own "do not
touch `harvest_kg`" constraint.

Measured after this ledger's §4 was written, and correcting it: **no** callable
or constant of `transformations.py` appears in `build_transforms_fingerprint`'s
closure for any table, so none of the three defects invalidates a cache —
`harvest_kg` is applied to `crop_production` at analysis time and is not a
stored column.

---
### Phase 3 — verification (fill at task end)

Design-only task; no library symbol was added or changed. Verification of the
*measurement* code against this ledger:

- `slurm_logs/gh850_design/measure.py::infer_old` — OK (§4): re-implements the
  live chain and is checked against `conversion_to_kgs` itself
  (`reimpl_agree_with_live`), so the prototype's plumbing is anchored, not
  asserted.
- `slurm_logs/gh850_design/measure.py::infer_c` — OK (§5): the fine → coarse →
  NaN ladder is the `_survey_median_factors` pattern, not a new one. It returns
  `(per_ju, per_u, support, baseline_n)`; the two floors it takes (`floor` on
  step-2 support, `baseline_floor` on the `(t, j)` reference) are the ledger's
  §6 open question D4/D5 made measurable.
- `slurm_logs/gh850_design/probe_hash_reach.py` — OK (§3): walks
  `_build_registry._closure_parts`, the same graph the cache hash walks, rather
  than re-deriving a reachability rule.
