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

- **`plot_labor` and `plot_inputs` are still NOT registered in `index_info`.**
  This task removed the plot-axis half of the blocker #569 names ("their
  per-country index NAMES diverge (plot vs plot_id, crop vs j)"); the `crop`/`j`
  half remains (`plot_inputs` is `crop` in Malawi/Mali/Nigeria/Tanzania and `j`
  in Ethiopia/Uganda), as do the genuinely different shapes -- the seven EHCVM
  `plot_inputs` carry **no plot level at all**, `plot_labor` is
  `(t,i,plot_id,source)` in ten countries but `(t,i,plot_id,source,season)` in
  Uganda and `(t,i,plot_id,season,stage,source)` in GhanaSPS. Registering them
  is a separate decision, not a mechanical follow-on from this rename.
- **The two plot VOCABULARIES are still not reconciled** (§3). `plot_id` now
  names the same axis in `crop_production` and `plot_features`, which makes the
  mismatch *easier to mistake for a joinable key*, not harder. That is the one
  way this change could mislead someone, and it is why `yield_kg` /
  `fertilizer_rate` keep `on='parcel'` as the default and keep the
  `PlotGrainMismatchWarning`.
- **`EthiopiaRHS` is in the re-warm bill for a COMMENT.** Its
  `_/data_scheme.yml:118` described the canonical grain as `(t,i,plot,j)`; the
  fix is one word, but `Country._table_cache_hash` hashes the file whole, so all
  18 of its tables invalidate. Taken deliberately -- a config file that
  contradicts `data_info.yml` is worse than one small country's rebuild -- but
  it is a real cost and it is named here rather than buried.

---
### Phase 3 -- verification (measured)

Measured in `/global/scratch/fsa/fc_jevons/ligon/tmp/wt-plot-id`, library
identity asserted on every invocation (`'wt-plot-id' in lsms_library.__file__`).
Warm reads from a **copy** of the shared cache in `.rt_data` / `.rt_dataB`; the
shared root at `~ligon/cache/lsms_library` was never written.

**Commit A (`ff9c1787`) -- 0 cache fingerprints moved.**
`Country._table_cache_hash(table, c.waves)` over all 337 `(country, table)`
cells of the 15 crop countries, probed THREE times so any move would be
attributable: base, after `data_info.yml`, after `transformations.py`. **0 / 337
each time.** `data_info.yml` is not a hash input; `transformations.py` is reached
only from `_finalize_result`, which is in `_build_registry._EXCLUDED_CALLABLES`.

**Commit B -- 274 / 337 moved, WHOLE-COUNTRY, in 12 countries.**
`_table_cache_hash` hashes `_/data_scheme.yml` and the country module as WHOLE
FILES, so renaming one declared level moves EVERY table's hash in that country.
This is the re-warm bill and it is bigger than "the three plot tables":

| country | hashes moved / probed | why |
|---|---|---|
| Benin | 21 / 21 | renamed |
| Burkina_Faso | 24 / 24 | renamed |
| CotedIvoire | 21 / 21 | renamed |
| Guinea-Bissau | 21 / 21 | renamed |
| Malawi | 26 / 26 | renamed |
| Mali | 24 / 24 | renamed |
| Niger | 23 / 23 | renamed |
| Nigeria | 24 / 24 | renamed |
| Senegal | 22 / 22 | renamed |
| Togo | 22 / 22 | renamed |
| Uganda | 28 / 28 | renamed |
| EthiopiaRHS | 18 / 18 | **comment only** -- no declared level changed |
| Ethiopia | **0** / 25 | already `plot_id` |
| GhanaSPS | **0** / 13 | already `plot_id` |
| Tanzania | **0** / 25 | already `plot_id` |

**The served frame is byte-identical up to the level name -- 33 / 33.**
Each of the 33 `(country, table)` frames the 11 renamed countries carry across
`crop_production` / `plot_labor` / `plot_inputs` was read warm at commit A
(declared `plot`) and again on branch B against a copy of the same parquets with
the level physically renamed, then compared with
`assert_frame_equal(base.rename_axis(index={'plot': 'plot_id'}), branch,
check_exact=True)`. **All 33 identical**, 848,509 rows in total. This validates
that the READ path is name-agnostic; it does not by itself validate the wave
scripts, which is what the cold builds below are for.

> *Method note, worth keeping.* The first attempt rewrote the parquets with
> `pyarrow.Table.rename_columns`, which renames the COLUMN but leaves the
> `b'pandas'` schema metadata still naming `plot` as an index column. The API
> frames came out right (`_normalize_dataframe_index` re-sets the index from the
> declared spec), but `pd.read_parquet` alone silently DROPPED the level -- and
> `tests/test_table_structure.py` reads the parquet directly, so 49 tests failed
> for a defect in the fixture, not in the change. Round-trip through pandas and
> re-attach only the non-`pandas` metadata keys (`lsms_cache_hash`,
> `lsms_grain_audit`).

**Cold builds reproduce the warm frames exactly -- 6 countries, 14 tables.**
With the country's parquets deleted from the sandbox data root, the wave scripts
and country module were re-run and the result compared to the pre-change frame
under the same rename. This is the check an `rg` sweep cannot do: it proves the
renamed scripts still produce the same rows.

| country | tables rebuilt cold | verdict |
|---|---|---|
| Benin | crop_production, plot_labor | identical |
| Mali | crop_production, plot_inputs, plot_labor | identical |
| Niger | crop_production, plot_labor | identical |
| Nigeria | crop_production, plot_inputs, plot_labor | identical |
| Malawi | crop_production, plot_inputs, plot_labor | identical (see below) |
| Uganda | crop_production, plot_inputs, plot_labor | identical |

That covers every country whose **country module** was edited (`malawi.py` 18
renames, `nigeria.py` 34, `mali.py` 9, `niger.py` 9, `uganda.py` 8) plus one
pure-wave-script EHCVM country; the five remaining EHCVM countries
(Burkina_Faso, CotedIvoire, Guinea-Bissau, Senegal, Togo) run the same
wave-script shape as Benin and were verified warm only.

> **One transient `COLD DIFF`, run down rather than waved off.**
> `Malawi/crop_production` reported "index level [5] are different" -- level 5 is
> `u` -- with **the same 131,548 rows**. Comparing the BUILT PARQUETS directly
> (`.rt_data` original renamed vs `.rt_dataB` cold-rebuilt) gives
> `assert_frame_equal(..., check_exact=True)` **identical**, as it does for
> `Uganda/crop_production` (130,606) and `Nigeria/plot_inputs` (96,620); and
> re-reading the cold-built parquet through the API compares identical to the
> pre-change frame too. So the difference lived in the FRESHLY BUILT in-memory
> frame's `u` level dtype, not in the data, and it disappears on the parquet
> round-trip that every real consumer goes through. It is not a property of this
> rename -- `plot_id` is level 3, and it compared equal.

**The merge -> re-warm hazard window, measured.** Between merging commit B and
re-warming, the shared cache holds parquets whose level is `plot` while the
config says `plot_id`. Measured on Benin against exactly that state:

| read | result |
|---|---|
| `Country('Benin').crop_production()` (ordinary) | **self-heals.** Hash mismatch -> rebuild descent -> scripts re-run -> 9,056 rows, `plot_id` in the index, byte-identical to the pre-change frame; the parquet is rewritten with `plot_id`. |
| `Country('Benin', assume_cache_fresh=True).crop_production()` | **7,571 rows, `plot_id` NOT in the index and `plot` left as a COLUMN** -- the declared level is absent from the stale parquet, so `_normalize_dataframe_index` drops it and collapses 1,485 rows away. It is **LOUD**: `GrainCollapseWarning` fires, which is #323's guard doing precisely its job. |

So the hazard is confined to readers that bypass the hash --
`assume_cache_fresh=True` / the deprecated `trust_cache=True`, and the
`LSMS_NO_CACHE` soft cases -- and it announces itself. `tests/test_863_transforms.py`'s
`_warm()` helper is one such reader, so **CI run against a stale cache will see
it**. The fix is the re-warm, not a code change.

**Tests.** 1,279 pass across the 16 crop/plot/feature/structure test modules
(`test_plot_id_canonical`, `test_feature_canonical_index`, `test_863_transforms`,
`test_shipped_factors`, `test_crop_kg_factor`, `test_quantity_screen`,
`test_malawi_shipped_factors`, `test_ethiopia_shipped_factors`,
`test_nigeria_kg_factor`, `test_niger_u_sentinel`, `test_ghanasps_crop_production`,
`test_table_structure`, `test_schema_consistency`,
`test_gh637_uganda_plot_inputs_season`, `test_uganda_crop_condition`,
`test_uganda_99999`), 0 failures.

**Anchored verdicts.**

- `data_info.yml` `index_info` / `level_aliases` -- **OK (anchored on §3, §5)**:
  the alias direction is the one `feature._rename_index_levels` documents, and
  the flip is asserted by a test rather than eyeballed (the two directions read
  identically at a glance).
- `_PLOT_CANONICAL` / `_canonicalise_plot_level` -- **OK (anchored on §5)**: the
  three transforms already normalised their output level; only the target
  spelling changed. Not a reinvention of `_resolve_plot_level`, which answers a
  different question (what came in, not what goes out).
- `_resolve_plot_level(names)` `None` guard -- **OK (anchored on §2)**: the old
  `names or []` raised on a `pd.Index`, which is what `yield_kg` /
  `fertilizer_rate` now hand it. Caught by `test_863_transforms`, not by
  inspection.
- per-country renames -- **OK (anchored on §4)**: the protected-token list in the
  rename script is exactly §4's "not every `'plot'` literal is an index name",
  and the leftovers were read one by one (Nigeria's `sold_on='plot'`, the
  `cm`/`c`/`sc`/`ic`/`spec`/`fr` column-mapping dicts in Uganda / Tanzania /
  GhanaSPS). Zero renames in the three countries that already said `plot_id`.
- `on={'parcel','plot'}` left alone -- **OK (anchored on §3)**: a mode string,
  not a level name; pinned by
  `test_plot_id_canonical.py::test_on_is_a_mode_string_not_a_level_name`.
- **Not done, and not silently:** no re-warm was run and no coverage matrix was
  regraded. The bill above is what needs scheduling.


