# Prior-Art Ledger — GH #563: GhanaLSS `nutrition` from the West Africa FCT

> Per-task ledger. Living, git-tracked snapshot of the machinery, definitions
> and conventions that bear on THIS task. Inherits `STANDING.md`; cites
> `CLAUDE.md`, `lsms_library/data_info.yml` and `GhanaLSS/_/CONTENTS.org`
> rather than re-copying them.

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server timed out
at session start (CONNECT_TIMEOUT), so no call-graph queries were possible;
callers were located with `grep`/`sed`.
**Line anchors as of:** `9aea120f` (development, 2026-09-04).

## §1 Task, restated

Add a **registered, script-path** (`materialize: make`) country-level table
`nutrition` to `GhanaLSS/_/data_scheme.yml`, built by a new
`GhanaLSS/_/nutrition.py` that multiplies GhanaLSS food quantities (kg) by
per-kg nutrient densities taken from the **FAO/INFOODS West African Food
Composition Table 2019** (WAFCT 2019), and writes
`../var/nutrition.parquet` via `local_tools.to_parquet`.

Grain and shape must match the two existing precedents exactly, because
`Feature('nutrition')` concatenates across countries: index `(i, t)`,
columns = the `Preferred Label` nutrient axis of
`Ethiopia/_/nutrient_labels.org`, values = **absolute nutrient amounts
acquired by the household over the wave's recall period** (not per-capita,
not per-day, not per-100 g).

The FCT itself is materialised as a machine-readable org table under
`GhanaLSS/_/`, and the GhanaLSS food-label → FCT-code map is added as a new
column on the country's existing food-label table.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Uganda/_/nutrition.py` | `countries/Uganda/_/nutrition.py` | precedent: `Country('Uganda').food_quantities()` → filter `u=='kg'` → `groupby(['i','t','j']).sum()` → `unstack('j')` → `final_q @ final_fct.T` → `../var/nutrition.parquet` (+ `../var/fct.parquet`) | via build | **SHAPE REUSED**; its `sys.path.append('../../_/')` and embedded FDC key are NOT copied (§4) |
| `Ethiopia/_/nutrition.py` | `countries/Ethiopia/_/nutrition.py` | same shape; sources its FCT from `Tanzania/_/demands.org#fct_origin` via a cross-country `df_from_orgfile` read | via build | **SHAPE REUSED**; the cross-country read pattern is reused (for `nutrient_labels.org`) with `countries_root()` instead of a relative path |
| `Ethiopia/_/fct_tools.py: fct_filter` | `countries/Ethiopia/_/fct_tools.py` | subsets an FCT to the country's foods, renames FCT nutrient columns onto `Preferred Label`, `pd.to_numeric(errors='coerce')`, **`fct = fct*10`** (per-100 g → per-kg), `fillna(0)` | via build | **NOT imported** (lives under `Ethiopia/`, reachable only by the `sys.path` hack this task rejects). Its three semantics — rename-to-Preferred-Label, ×10, coerce — are re-implemented inline in `GhanaLSS/_/nutrition.py` and cited there |
| `Ethiopia/_/nutrient_labels.org` | `countries/Ethiopia/_/nutrient_labels.org` | **the canonical nutrient axis**: 21 rows, `Preferred Label \| FCT Label \| FDC Label` | via build | **REUSED, READ-ONLY**, resolved as `countries_root()/'Ethiopia'/'_'/'nutrient_labels.org'`. No fourth vocabulary is invented (§3) |
| `Tanzania/_/demands.org#fct_origin` | `countries/Tanzania/_/demands.org:299` | an FCT stored as an org table: `FCT Code \| FCT Label \| energy kcal \| protein g \| …` | via build | **FORMAT REUSED** for `GhanaLSS/_/fct_west_africa.org` |
| `transformations.food_quantities_from_acquired` | `lsms_library/transformations.py:984` | public; derives `food_quantities` from `food_acquired`; `units='kgs'` converts via `_get_kg_factors` and tags converted rows `u='kg'`, carrying unconvertible rows through with their native `u` | yes | **REUSED**, but called on a *pre-filtered* `food_acquired` (§4, the `u='Value'` defect) rather than through `Country.food_quantities()` |
| `transformations._get_kg_factors` / `conversion_to_kgs` | `lsms_library/transformations.py` | builds the kg-per-unit map: hand-coded `KNOWN_METRIC`, explicit-metric label parsing, then **price-ratio inference** | yes | **NOT reused directly**; its price-ratio inference is the defect this task works around (§4) |
| `local_tools.df_from_orgfile` | `lsms_library/local_tools.py` | reads a named org table into a DataFrame; accepts `str \| Path` (so absolute paths from `countries_root()` work) | yes | **REUSED** |
| `local_tools.to_parquet` | `lsms_library/local_tools.py` | writes to `data_root()`, inferring country/wave from the call stack; `../var/foo.parquet` from a country script | yes | **REUSED**, exactly as the precedents call it |
| `paths.countries_root` | `lsms_library/paths.py` | honours `LSMS_COUNTRIES_ROOT`; the sanctioned way to resolve config paths (GH #753) | yes | **REUSED** in place of `files('lsms_library')` and `sys.path` hacks |
| `GhanaLSS/_/food_items.org#food_label` | `countries/GhanaLSS/_/food_items.org:3` | the country's food-label table: `Preferred Label` + one column per wave. **195 unique Preferred Labels** | via build | **EXTENDED** with an `FCT Code` column (§6) |
| `GhanaSPS/_/food_items.org` | `countries/GhanaSPS/_/food_items.org` | carries `Food Codes` + `FCT Label` columns already | — | **evidence only**; GhanaSPS is a different country/vocabulary (§4) |

## §3 Definitions & conventions in force

- **`nutrition` grain/units**: index `(i, t)`, columns = `Preferred Label`,
  values = absolute nutrient amount acquired per household per wave.
  Measured off the shipped `Ethiopia/var/nutrition.parquet`
  (25,647 × 21, all `float64`) — not paraphrased from the script.
- **Nutrient vocabulary**: `Ethiopia/_/nutrient_labels.org` `Preferred Label`
  column, 21 values. Authoritative; this task adds no nutrient name.
- **Per-100 g → per-kg**: FCTs in this repo are stored *as published*
  (per 100 g EP) and multiplied by 10 in the consuming script —
  `fct_tools.fct_filter` ("Convert serving size to Kgs instead of
  hectograms") and `fct_tools.harmonize_nutrient` (`df * 10`).
- **`food_quantities(units='kgs')`**: per `CLAUDE.md` "Derived Tables" and
  `transformations.py:992-1004` — kg where convertible (tagged `u='kg'`),
  native quantity with native `u` otherwise. Output is
  mixed-physical-unit by design.
- **Derived tables are not registered**: `food_quantities` /
  `food_expenditures` / `food_prices` are auto-derived (`_FOOD_DERIVED`);
  `nutrition` is **not** derived — it is a registered `!make` table in both
  precedents, and so here.
- **WAFCT 2019 basis**: all component values are **per 100 g edible
  portion**; `EDIBLE1`/`EDIBLE2` are the as-purchased→edible coefficients.
  Datasheet `02 Components` is the component dictionary and states, per
  component, which datasheets carry it.

## §4 Invariants & assumptions

- **`u='Value'` is a currency amount, not a mass.** GhanaLSS pre-2016-17
  waves elicit expenditure, not quantity (`GhanaLSS/_/CONTENTS.org:1034`),
  so `food_acquired` carries `u='Value'` rows whose `Quantity` *is* the
  cedi expenditure. **Measured defect**: `conversion_to_kgs` price-ratio
  inference assigns `'value'` a kg factor of **0.49139**, so
  `food_quantities(units='kgs')` relabels every such row `u='kg'`. This
  contradicts `food_quantities_from_acquired`'s own docstring
  (`transformations.py:997-1002`), which names `u='Value'` as the
  canonical carry-through case. 1987-88 has 71,605 `u='Value'` rows and
  `food_quantities` emits exactly 71,605 `u='kg'` rows. **Consequence for
  this task**: the precedents' `q[u=='kg']` filter would admit cedis as
  kilograms in 5 of 7 waves. `nutrition.py` therefore drops non-physical
  `u` *before* deriving. Not fixed here — `transformations.py` is on the
  brief's stop-list; written up at
  `slurm_logs/ghana_audit/ISSUE_value_kg_factor.org`.
- **Native `u` is destroyed by the kg relabel**, so the filter cannot be
  applied post-hoc to `Country.food_quantities()` output. This is why
  `nutrition.py` calls `food_quantities_from_acquired` on a filtered
  `food_acquired` instead.
- **`attrs['id_converted']`** must survive any merge downstream of
  `id_walk` (`CLAUDE.md`); `nutrition.py` performs no merge across frames
  with disagreeing `attrs` — the matmul is a single-frame operation.
- **WAFCT parsing** (measured on the shipped workbook):
  three header rows (English / French / INFOODS tagname, row 2);
  14 category separator rows interleaved with 1,028 food rows;
  `Code` is a `NN_NNN` **string**; tagnames carry trailing whitespace
  (`'ENERC '`, `'PROTCNT '`); `ENERC` appears **twice** under one tagname
  (kJ and kcal) and must be disambiguated on the English header;
  values take four forms — plain, `[n]` (1,526; non-African source),
  `tr` (222), `[tr]` (4).
- **WAFCT has no Vitamin K.** No phylloquinone component exists in the
  table, so `Preferred Label` `Vitamin K` cannot be sourced. It is
  **omitted**, not zero-filled: a zero would be a claim.
- **Edible portion is NOT applied.** `food_quantities` is quantity
  *acquired*, and both precedents multiply acquired mass by per-EP
  densities. Matching the corpus (`CLAUDE.md`: harmonize the interface).
- **`optional:` is country-grain** while GhanaLSS's coverage problem is
  wave-grain — do not reach for it to silence a wave.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| nutrient name axis | **reuse** `Ethiopia/_/nutrient_labels.org` | brief's explicit instruction; the axis `Feature('nutrition')` concatenates on |
| FCT storage format | **reuse** Tanzania `fct_origin`'s org-table shape | already read by `df_from_orgfile`; keeps FCT next to the config that uses it |
| per-100 g → per-kg ×10 | **reuse** the `fct_tools.fct_filter` convention (re-implemented inline) | cross-country comparability; the module itself is unreachable without the rejected `sys.path` hack |
| kg derivation | **reuse** `food_quantities_from_acquired` (public) on a **filtered** frame | tested machinery; bypasses `Country.food_quantities()` only because native `u` is unrecoverable after the relabel (§4) |
| non-physical-unit filter | **new**, GhanaLSS-local, in `nutrition.py` | works around the §4 defect without editing `transformations.py` (stop-list) |
| GhanaLSS label → FCT code map | **new**, anchored on the public GhanaSPS companion sheet | the sheet is GhanaSPS's (39/195 overlap); the rest is derived here |
| FDC fallback | **not used** | WAFCT covers the GhanaLSS basket; avoids the precedents' embedded API key entirely |

## §6 Open questions for the human

- **`categorical_mapping.org#harmonize_food` does not exist for GhanaLSS.**
  The country's single food-label table is `food_items.org#food_label`.
  The `FCT Code` column is added there (Ethiopia's Unit #0 *pattern* —
  code lives on the label table — applied where GhanaLSS's table actually
  is). `food_items.org` is not on the brief's owned-file list; flagged.
- **`food_items.csv` duplicates `food_items.org`.** Whether the csv is
  generated or hand-maintained decides whether it must carry the new
  column too. Resolved during implementation; recorded in the report.
- **Pre-2016-17 nutrition coverage is partial and probably biased**
  (own-production rows survive the filter; purchases were elicited by
  value). Whether a partial-coverage `nutrition` should ship for those
  waves at all, or be declared absent pending #562 phase 3b, is a
  dispatcher/@ligon call. This task ships it **with the coverage measured
  and documented**, and ships 1987-88 / 1988-89 empty.
- **Is the `u='Value'` kg factor contaminating other countries?**
  `CLAUDE.md` already notes `u='Value'` rows exist elsewhere ("1 = Kwacha
  per Kwacha"), so Malawi and any `Feature('food_quantities')` consumer
  may carry the same defect. Not measured here (compute not owned).

---
### Phase 3 — verification (2026-09-04, after the cold build)

- `GhanaLSS/_/nutrition.py` — **OK (anchored on §2/§4/§5)**: reuses the
  precedents' shape and the public `food_quantities_from_acquired`.  The one
  departure — the non-physical-`u` filter — is the §4 defect work-around and
  is documented at length in the module docstring.  Verified cold: 29,095
  rows x 20 nutrients in 148 s.
- `GhanaLSS/_/fct_west_africa.org` — **OK (anchored on §3)**: stored per
  100 g EP as published, columns renamed onto `nutrient_labels.org`
  `Preferred Label`; the x10 happens in the consumer, per the corpus
  convention.  1,028 foods, codes preserved as `NN_NNN` strings.
- Vitamin K omission — **OK (anchored on §4)**: deliberate, and NOT a
  reinvention of Ethiopia's `fillna(0)`.  Pinned by
  `test_vitamin_k_omitted_not_zero_filled`.
- Nutrient vocabulary — **OK (anchored on §3)**: no fourth vocabulary; both
  the build script and `test_no_fourth_nutrient_vocabulary` assert the columns
  are a subset of Ethiopia's `Preferred Label` axis.
- `_household_size` in the test / verify script — **CONTRADICTION found and
  fixed (§3)**: the first verification run summed *every*
  `household_characteristics` column, including `log HSize`, giving median
  household sizes of 6.609438 (= 5 + ln 5).  Corrected to the 14 bucket
  columns; 2016-17 median per-capita Energy 784.1 -> 1,004.3 kcal/day.
- `food_items.csv` (§6) — **resolved as: stale, unread, left alone.**  Nothing
  reads it; GhanaLSS reads `food_items.org` only, via
  `ghanalss.py::harmonized_food_labels`.  It was deliberately NOT regenerated
  with the new `FCT Code` column, and that is recorded in `CONTENTS.org`.
- Index grain (§3) — **amended**: `nutrition.py` writes `(i, t)` as the
  precedents do, but the API returns `(i, t, v)`, because `data_info.yml`
  carries no canonical `nutrition` block to opt out of `_join_v_from_sample`
  (GH #436/#455).  Uniform across all three countries, so
  `Feature('nutrition')` assembles — verified for both
  `['GhanaLSS','Ethiopia']` (54,742 x 21) and `['GhanaLSS','Uganda']`
  (52,896 x 20).  A canonical block is proposed in the PR, not written.
- **REINVENTION avoided**: the kg conversion is the library's own
  `food_quantities_from_acquired`, called on a filtered frame.  No factor
  table, unit parser or price-ratio inference was re-implemented here.
