# Prior-Art Ledger — GH #591 (Nigeria food_prices drops ~99% of rows)

**Search tier used:** ripgrep + git (floor). gitnexus not consulted.
**Line anchors as of:** `d572d8a9` (drift expected — match on symbol name).

## §1 Task, restated

`Country('Nigeria').food_prices()` (default `units='kgvalue'`) returned 161–1,106
rows for waves whose `food_acquired` holds 120k–160k rows — six of eight waves,
all graded `sane`. `food_acquired` is a **script-path** (`materialize: make`)
table: four wave-level `_/food_acquired.py` scripts write L2-wave parquets, the
country-level `_/food_acquired.py` concatenates them, and `food_prices` /
`food_quantities` / `food_expenditures` are **derived at runtime** from it via
`_FOOD_DERIVED` (never registered in `data_scheme.yml` — per CLAUDE.md "Derived
Tables"). The defect is therefore in the *build* path (a wave-script variable
mapping), and its amplifier is in the *read* path
(`food_prices_from_acquired`).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `food_acquired_to_canonical` | `build_transforms.py:26` | 2-way melt: `purchased = Quantity − Produced` (clipped ≥0), `produced = Produced` | yes | **NOT reused** — hardcodes a 2-source split with one shared `u`; Nigeria needs 3 sources each with its OWN unit. Editing it would bust `build_transforms_fingerprint` → rebuild `food_acquired` for ~20 countries (`country.py:687`). |
| `_finalize_canonical_food_acquired` | `build_transforms.py:139` | shared filter (`Quantity>0 \| Expenditure>0`) + dedupe tail; sums `Quantity`/`Quantity_kg` with `min_count=1`, means `Price` | yes | **REUSED** — this is the blessed tail (GH #251) that Uganda/Tanzania/Malawi already call. |
| `uganda.food_acquired_to_canonical` | `countries/Uganda/_/uganda.py:243` | prior art for a **3-source** melt (purchased/produced/inkind) that calls the shared tail | via Uganda build | **PATTERN REUSED** — `nigeria.food_acquired_for_wave` is the same shape, generalized over a `sources` dict because Nigeria's per-source *units* differ. |
| `nigeria.harmonized_food_labels` | `countries/Nigeria/_/nigeria.py:88` | int-keyed `{code: Preferred Label}` from `harmonize_food` (GH #443) | via build | **REUSED** — the four wave scripts each had this inlined; now called once. |
| `*_for_wave` helpers | `countries/Nigeria/_/nigeria.py` (`plot_features_for_wave`, `community_prices_for_wave`, …) | Nigeria's established "thin wave script + country-module helper" idiom | via build | **PATTERN REUSED** — `food_acquired_for_wave` joins them. |
| `food_prices_from_acquired` | `transformations.py:~900` | derives `Price`; ends `v[['Price']].replace([0, inf, -inf], nan).dropna()` | `test_food_prices_units_kwarg.py` | **EXTENDED** — drop set unchanged, now accounted + warned. |
| `_get_kg_factors` / `conversion_to_kgs` | `transformations.py:344` | infers unit→kg factors from `Expenditure/Quantity` ratios | partially | untouched — but it is a **victim**: it saw ~677 usable Nigeria rows/wave instead of ~57k. |
| `community_prices` | `countries/Nigeria/_/community_prices.py` | surveyed cluster-level prices at `(t, v, j, u)` | via build | **REUSED as an INDEPENDENT YARDSTICK** — not as an input. See §5. |

## §3 Definitions & conventions in force

- **`s` (acquisition source)** — canonical values `purchased, produced, inkind,
  other`; `transformations.S_VALUES:31`, enumerated in `data_info.yml:11` and
  enforced by `validate_acquisition_source`. Gifts → `inkind`.
- **`food_expenditures` basis** — `'purchased'` (default) = **cash outlay only**,
  keep `s=='purchased'` (GH #575, `transformations.food_expenditures_from_acquired`).
  Own-production/in-kind rows carry `Expenditure = NaN`.
- **`food_prices(units='kgvalue')`** (the default) = `Expenditure / Quantity_kg`.
  Per STANDING.md §3 and `DESIGN_food_prices_units_kwarg_2026-05-06.org`. This is
  why the purchased row's `Quantity` and `Expenditure` **must describe the same
  transaction** — otherwise the flagship path is a wrong number.
- **Derived tables are not registered** in `data_scheme.yml` (CLAUDE.md).
- **`u` is stored RAW** in Nigeria's parquets (the wave scripts' `.replace(unitcodes)`
  is a no-op: the codes are `float64`, the map is string-keyed). Labels are applied
  at API time by the automatic categorical mapping against `categorical_mapping.org`
  `#+name:u`. Verified empirically — the cached parquet holds `u = '1.0'`, the API
  returns `'Kg'`. **New code must keep storing the raw code** or the `u` vocabulary
  shifts under every consumer.

## §4 Invariants & assumptions

- **Cache-hash coverage.** `Country._table_cache_hash` (`country.py:2230`) hashes
  the country module `_/nigeria.py` (`cmod=`), the country concatenator, the
  Makefile and `data_scheme.yml`; `Wave._input_hash` (`country.py:585`) hashes the
  wave's `_/food_acquired.py` **text**. So a helper in `nigeria.py` IS versioned,
  and script-path L2-wave parquets are evicted on the rebuild descent
  (`_evict_hashless_wave_caches`, GH #479). Still cleared explicitly with
  `lsms-library cache clear --country Nigeria` — Nigeria has round-name wave dirs
  (`2012Q3/`, `2013Q1/`…) that `Country.waves` does not enumerate.
- **`build_transforms_fingerprint(table)`** is folded into the cache hash for every
  country that builds that table (`country.py:687`). Editing `build_transforms.py`
  invalidates `food_acquired` for ~20 countries. → keep the fix in Nigeria-local files.
- **All Nigeria unit columns (`q2b/q3b/q5b/q6b/q7b/q9b`) share ONE code space** —
  verified: 3 uncoded values across ~500k rows in 8 files. So a per-source unit is
  safe to store.
- **`sum(Quantity over s) == total consumed`** held by construction pre-fix (because
  `purchased` was a residual). It does NOT hold post-fix for W4 — see §6.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| 3-source melt | **new, Nigeria-local** (`nigeria.food_acquired_for_wave`) | the shared `food_acquired_to_canonical` cannot express per-source units, and editing it costs a 20-country rebuild (§4). Modelled on `uganda.food_acquired_to_canonical` (§2). |
| filter + dedupe tail | **reuse** `_finalize_canonical_food_acquired` | the canonical rule (GH #251); reimplementing it is exactly the duplication that skill warns about. |
| food-item labels | **reuse** `nigeria.harmonized_food_labels` | was copy-pasted into all four wave scripts. |
| which quantity pairs with the expenditure | **decided by evidence, not fiat** | validated against `community_prices` — the independent surveyed price. See below. |
| price-loss accounting | **new** (`_drop_unpriceable`) | nothing in the repo counted what the `.dropna()` ate. |

**The denominator question, settled empirically.** W2/W3 offer two candidate
purchased quantities (q3a = quantity purchased, which q4 paid for; q5a = quantity
consumed out of purchases) and W4 likewise (q9a = most-recent purchase, which q10
paid for; q5a). Compared median `Expenditure/Quantity` per `(t, j, u)` against the
median `community_prices` for the same `(t, j, u)` — same native unit, so no kg
conversion enters:

| wave | variant | matched cells | median ratio survey/community | within ±25% |
|------|---------|---------------|-------------------------------|-------------|
| 2013Q1 | q5a | 30 | 1.021 | 47% |
| 2013Q1 | **q3a** | 30 | 1.036 | **50%** |
| 2019Q1 | q5a | 13 | 1.053 | 69% |
| 2019Q1 | **q9a** | 14 | **0.948** | **93%** |

The purchase-block quantity wins, decisively in W4. (Instrument validated: it
reproduces wave 1 sanely and returns ratio **exactly 1.000** on `u='Kg'`/`'l'`
cells. Its `u='g'` cells are junk — Nigeria's `community_prices` prices "g" rows
per kg — a separate, pre-existing finding, logged in §6.)

## §6 Open questions for the human

- **W4 reference-period asymmetry (deliberate, documented, NOT silent).** In
  2018-19 the purchase block (q9a/q10) is the household's *most recent purchase
  within 30 days*, while produced/in-kind are *7-day* consumption. Pairing q10 with
  q9a is the only way to get a real price, and every stored number stays
  survey-reported — but `Quantity` summed across `s` is no longer a 7-day
  consumption total for W4. The alternative (impute `Expenditure = q5a × price`,
  which is what the WB's own W4 consumption aggregate does) would fix that at the
  cost of **fabricating a number no respondent reported** and silently changing
  `food_expenditures` for two waves. Rejected here; worth its own issue.
- **`community_prices` `u='g'` rows look mis-scaled** (survey/community ratio
  ≈ 1/400 on gram cells; exactly 1.000 on Kg/l cells). Not touched. Own issue.
- **Cambodia** loses 7.9% of expenditure-bearing rows to the same `Price = inf`
  signature (`Quantity == 0` with `Expenditure > 0`). The new warning will now say
  so out loud. Own issue.
- **`is_this_feature_sane` never checks row-count collapse relative to the source
  table** — which is why all six broken cells graded `sane`. Guardrails added as
  Nigeria tests instead of changing audit grading for every country.

---
### Phase 3 — verification

- `nigeria.food_acquired_for_wave` — **OK (anchored on §2/§5)**: new Nigeria-local
  melt; delegates the filter/dedupe to the shared `_finalize_canonical_food_acquired`
  rather than re-deriving it; keeps `u` raw per §3.
- Four wave `_/food_acquired.py` — **OK (§3)**: variable maps now follow the
  questionnaire's own Stata labels; `Expenditure` on the purchased row only, per
  the GH #575 cash-outlay convention.
- `transformations._drop_unpriceable` — **OK (§2)**: drop *set* is provably
  identical to the pre-#591 `.replace([0,inf,-inf],nan).dropna()`
  (`tests/test_unpriceable_price_drop.py::test_same_rows_as_legacy_expression`), so
  no other country's `food_prices` shifts by a row; only the accounting is new.
- `food_expenditures_from_acquired` — **no code change**, comment only: its `0 →
  drop` is a sparsity convention (0 contributes 0 to every sum), not the class of
  loss #591 is about. Warning there would be noise that dilutes the real signal.
