# Prior-Art Ledger -- `plot_id` as the canonical plot axis name (crop_production, plot_labor, plot_inputs)

> Per-task ledger. Inherits `.coder/ledger/STANDING.md`; cites `CLAUDE.md` and
> `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git (floor). The `gitnexus` MCP server reported
`CONNECTION_CLOSED` this session and this mirror has no `.gitnexus/` index, so
the CLAUDE.md-sanctioned substitutes were used and are DECLARED here: `rg`/`grep`
call-site sweeps for `_PLOT_LEVELS`, `_resolve_plot_level`, `level_aliases`,
`index_info`, `'plot'` as a literal in `lsms_library/countries/**/*.py`, and a
`data_scheme.yml` sweep of every country's four plot-axis tables.

## §1 Task, restated

@ligon's decision (2026-09-10): the plot axis is spelled `plot_id` everywhere.
`plot_features` already says `plot_id` in all 23 countries that declare it. But
`crop_production`'s canonical index in `lsms_library/data_info.yml`
(`Index Info > index_info`) says `(t, v, i, plot, j, u, condition, season)` --
Uganda's spelling -- and the `level_aliases` map installed by the #569/#775
canonical-index work aliases in the WRONG direction (`plot_id: plot`). Flip the
canonical name to `plot_id`, alias `plot -> plot_id`, make the
`transformations.py` plot-grain transforms emit `plot_id`, and then rename the
declared level (and the wave-script/country-module index name) in every country
that still says `plot`.

Two commits, deliberately separate:

* **A (canonical / read-path)** -- `data_info.yml`, `feature.py` behaviour via
  config, `transformations.py`, docs, tests. Must move **0** cache fingerprints.
* **B (per-country renames)** -- `_/data_scheme.yml` + `_/*.py` + wave scripts.
  Cache fingerprints WILL move; kept separate so the merge and the re-warms can
  be scheduled.

## §1a Inventory (step 1) -- who declares the plot axis, and how

Declared `index:` in `lsms_library/countries/{C}/_/data_scheme.yml`. `-` = the
country does not declare that table.

| country | crop_production | plot_labor | plot_inputs | plot_features |
|---|---|---|---|---|
| Benin | `(t, i, plot, crop, u)` | `(t, i, plot, source)` | `(t, i, input, crop, u)` | `(t, i, plot_id)` |
| Burkina_Faso | `(t, i, plot, crop, u)` | `(t, i, plot, source)` | `(t, i, input, crop, u)` | `(t, i, plot_id)` |
| CotedIvoire | `(t, i, plot, crop, u)` | `(t, i, plot, source)` | `(t, i, input, crop, u)` | `(t, i, plot_id)` |
| Ethiopia | `(t, i, plot_id, j, u)` | `(t, i, plot_id, source)` | `(t, i, plot_id, input, j)` | `(t, i, plot_id)` |
| EthiopiaRHS | `(t, i, j, u)` | - | - | `(t, i, plot_id)` |
| GhanaSPS | `(t, i, plot_id, j, u, season)` | `(t, i, plot_id, season, stage, source)` | - | `(t, i, plot_id)` |
| Guinea-Bissau | `(t, i, plot, crop, u)` | `(t, i, plot, source)` | `(t, i, input, crop, u)` | `(t, i, plot_id)` |
| Malawi | `(t, i, plot, crop, u, condition)` | `(t, i, plot, source)` | `(t, i, plot, input, crop, u)` | `(t, i, plot_id)` |
| Mali | `(t, i, plot, crop)` | `(t, i, plot, source)` | `(t, i, plot, input, crop)` | `(t, i, plot_id)` |
| Niger | `(t, i, plot, crop, u)` | `(t, i, plot, source)` | `(t, i, input, crop, u)` | `(t, i, plot_id)` |
| Nigeria | `(t, i, plot, crop)` | `(t, i, plot, source)` | `(t, i, plot, input, crop)` | `(t, i, plot_id)` |
| Senegal | `(t, i, plot, crop, u)` | `(t, i, plot, source)` | `(t, i, input, crop, u)` | `(t, i, plot_id)` |
| Tanzania | `(t, i, plot_id, j)` | `(t, i, plot_id, source)` | `(t, i, plot_id, input, crop)` | `(t, i, plot_id)` |
| Togo | `(t, i, plot, crop, u)` | `(t, i, plot, source)` | `(t, i, input, crop, u)` | `(t, i, plot_id)` |
| Uganda | `(t, i, plot, j, u, condition, season)` | `(t, i, plot, source, season)` | `(t, i, plot, input, j, season)` | `(t, i, plot_id)` |

Eight further countries declare `plot_features` and nothing else plot-shaped, all
on `plot_id`: Albania, Cambodia, China, GhanaLSS, Kosovo, Liberia, Tajikistan,
Timor-Leste. **`plot_features` says `plot_id` in all 23 -- zero renames there.**

**Rename targets (commit B).** 11 countries say `plot`:

* `crop_production` -- Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau, Malawi,
  Mali, Niger, Nigeria, Senegal, Togo, Uganda (11).
  Already `plot_id`: Ethiopia, GhanaSPS, Tanzania. EthiopiaRHS has no plot level
  (documented household-grain coarse table, GH #512).
* `plot_labor` -- the same 11.
* `plot_inputs` -- 4 only: Malawi, Mali, Nigeria, Uganda. The seven EHCVM
  countries declare `(t, i, input, crop, u)`, i.e. **no plot level at all** (§4).

**Who WRITES the name.** All three tables are script-path (`materialize: make`)
everywhere they say `plot`; only one wave `data_info.yml` in the whole corpus
contains a `plot:` key (`GhanaLSS/2012-13`, and that is a `mapping:` value
`plot: Plot` inside `plot_features`, not an index name). So the write side is
Python only -- 34 files:

| country | files carrying `'plot'` as an index name |
|---|---|
| Benin | `2018-19/_/crop_production.py`, `2018-19/_/plot_labor.py` |
| Burkina_Faso | `2018-19/_/crop_production.py`, `2018-19/_/plot_labor.py` |
| CotedIvoire | `2018-19/_/crop_production.py`, `2018-19/_/plot_labor.py` |
| Guinea-Bissau | `2018-19/_/crop_production.py`, `2018-19/_/plot_labor.py` |
| Malawi | `_/malawi.py`, `2010-11|2013-14|2016-17|2019-20/_/crop_production.py` |
| Mali | `_/mali.py`, `2014-15|2017-18/_/{crop_production,plot_inputs,plot_labor}.py` |
| Niger | `_/niger.py`, `2011-12|2014-15/_/plot_labor.py`, `2011-12|2014-15|2018-19|2021-22/_/crop_production.py` |
| Nigeria | `_/nigeria.py`, `_/crop_production.py` |
| Senegal | `2018-19/_/crop_production.py`, `2018-19/_/plot_labor.py` |
| Togo | `2018/_/crop_production.py`, `2018/_/plot_labor.py` |
| Uganda | `_/uganda.py` |

**NOT index names, excluded from the rename by hand** (§4): `Nigeria/_/nigeria.py`
`sold_on='plot'`, `fr.get('sold_on') == 'plot'`, `fr.get('plot')` (frame-spec
key); `GhanaSPS/2009-10/_/crop_production.py` and `Tanzania/_/tanzania.py`, whose
`'plot'` hits are prose/locals -- both already declare `plot_id`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_canonical_index_levels` | `feature.py:59` | parses the `index_info` tuple string | `tests/test_feature_canonical_index.py` | **reuse, untouched** |
| `_level_aliases` | `feature.py:84` | reads `Index Info > level_aliases`, `{emitted: canonical}` | same | **reuse; only its CONFIG flips** |
| `_rename_index_levels` | `feature.py:120` | applies the alias map to a country frame before alignment | same | **reuse, untouched** |
| `_align_to_canonical_levels` | `feature.py:147` | alias -> promote -> fabricate-sentinel | same | **reuse, untouched** |
| `_select_kept_shape` | `feature.py:272` | prefers the declared canonical shape, modal fallback | same | **reuse, untouched** |
| `_missing_level_sentinels` | `feature.py:101` | `u`/`condition`/`season` sentinels | same | **reuse, untouched** |
| `_PLOT_LEVELS` / `_resolve_plot_level` | `transformations.py:2173` / `2176` | `('plot','plot_id')` accepted-spelling tuple | `tests/test_863_transforms.py` | **extend** (order flips; a canonical constant is added) |
| `harvest_kg` | `transformations.py:3034` | normalised its output level to `plot` | `tests/test_shipped_factors.py`, `test_863_transforms.py` | **extend** (emits `plot_id`) |
| `nitrogen_kg` / `seed_kg` | `transformations.py:3590` / `3700` | same normalisation | `test_863_transforms.py` | **extend** |
| `yield_kg` / `fertilizer_rate` | `transformations.py:3208` / `4902` | join `harvest_kg`/`nitrogen_kg` to `plot_features.Area`; name their `on='plot'` output level `plot` | `test_863_transforms.py` | **extend** |
| `_parcel_from_crop_plot` / `_parcel_from_feature_plot` | `transformations.py:3180` / `3197` | the two plot VOCABULARIES (`{hhid}-{parcel}-{plot}` vs `{parcel}_{suffix}`) | yes | **untouched** -- this task renames the AXIS, not the vocabulary |
| `_compute_no_v_join` | `country.py:4997` | exempts a table whose `index_info` omits `v` | `tests/test_no_v_join_declarative.py` | **untouched** (`v` stays in the tuple) |
| `bench/feature_audit/scan.py:402` | | calls `_canonical_index_levels(feature)` | -- | reads the flipped name automatically |

## §3 Definitions & conventions in force

- **Alias direction**: `level_aliases: <name a country emits>: <canonical name>`
  -- `lsms_library/data_info.yml`, the comment above `level_aliases`, and
  `feature._rename_index_levels`. So the flip is `plot_id: plot` -> `plot: plot_id`.
- **Canonical crop grain**: `data_info.yml`, `Columns: crop_production: KgFactor`
  note -- rows sit at the plot-crop-unit-condition-season grain "and are never
  summed across `u` or `condition`". This task only re-SPELLS the plot level in
  that sentence.
- **`v` in `index_info` is load-bearing** -- `country._compute_no_v_join`; see
  `.coder/ledger/feature-canonical-crop-production.md` §4.
- **Two plot VOCABULARIES, one axis NAME.** `plot_features.plot_id` and
  `crop_production.plot` do not share literal values on Uganda
  (`transformations._parcel_from_crop_plot` docstring: 0 rows on `on='plot'`,
  509 on `on='parcel'`). Renaming the axis does NOT make them joinable, and this
  ledger records that so nobody later reads a shared name as a shared key.
- **`on={'parcel','plot'}`** on `yield_kg` / `fertilizer_rate` is a MODE string,
  not a level name. It is deliberately NOT renamed.

## §4 Invariants & assumptions

- **Commit A must move 0 cache fingerprints.** `lsms_library/data_info.yml` is
  the canonical schema file and is not in `Wave._input_hash` /
  `Country._table_cache_hash`'s inputs (CLAUDE.md "Automatic content-hash
  staleness"); `transformations.py` is reached only from read-path code
  (`_finalize_result`, in `_build_registry._EXCLUDED_CALLABLES`). Both MEASURED,
  three-way (base / after data_info.yml / after transformations.py).
- **Commit B moves fingerprints by WHOLE COUNTRY, not by table.**
  `Country._table_cache_hash` hashes the country `_/data_scheme.yml` and the
  country module (`uganda.py`, ...) **by whole file**, so a one-level rename in
  either moves every table's hash in that country. Measured, not assumed.
- **Not every `'plot'` literal is an index name.** `Nigeria/_/nigeria.py`'s
  `sold_on='plot'` / `fr.get('sold_on') == 'plot'` is a join-GRAIN mode flag and
  `fr.get('plot')` is a frame-spec KEY. Excluded from the rename by hand.
- **A NaN on a declared index level is a deferred silent deletion** (CLAUDE.md
  "Grain Collapse" §3b). A pure rename cannot introduce one; nothing here fills.
- **The seven EHCVM `plot_inputs` tables declare `(t, i, input, crop, u)`** --
  no plot level at all. Not a rename target; recorded so the omission is not
  read later as a miss.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| canonical crop index name | **reuse config** (`index_info`) | one string; no code change in `feature.py` |
| cross-country name bridging | **reuse config** (`level_aliases`), direction flipped | the #569 mechanism already exists |
| transform-output level name | **extend** `_PLOT_LEVELS` with a canonical constant | the three transforms already normalised; only the target spelling changes |
| per-country level name | **rename in place** (commit B) | the declared level and the script that writes it are the same string |
| plot vocabularies (`{hhid}-{parcel}-{plot}` vs `{parcel}_{suffix}`) | **untouched** | out of scope; §3 |

## §6 Open questions for the human

- (filled at task end)

---
### Phase 3 -- verification (fill at task end)
