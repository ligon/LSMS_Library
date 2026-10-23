# Prior-Art Ledger — GH #1234 (`map_index` mints `u = 'unit'`)

> Per-task ledger. Inherits `STANDING.md`; cites `CLAUDE.md` and
> `lsms_library/data_info.yml`. Evidence (scripts + aggregate JSON, no
> row-level data): `slurm_logs/isa_backlog_2026-10-21/1234-map-index-unit/`.

**Search tier used:** ripgrep + git floor (the gitnexus MCP server did not
connect this session; substitutes per CLAUDE.md "Code intelligence").

## §1 Task, restated
`local_tools.map_index` runs on wave and country frames as they are built and
read.  When the frame has a `u` index level it renamed `pd.NA`, `'nan'` and
`'<NA>'` on that level to the literal `'unit'`.  The #1041 ruling (@ligon
2026-10-18, quoted in #1118: "a missing/blank unit code is `U_UNKNOWN`
(`'Unknown'`), never a minted spelling and never a silent drop or NaN") makes
`'unit'` a forbidden minted spelling.  Replace it with
`transformations.U_UNKNOWN`, keep every other behaviour of `map_index`, and
leave a genuine source `'unit'` label alone.

Round 2 (review, dispatcher's decision): a `u` that a wave script writes as a
COLUMN reaches `map_index` with no `u` level, so the cold build served `pd.NA`
where the warm read served `'Unknown'`.  A served value that depends on cache
state is a defect, so `Country._finalize_result` applies the same rename.

## §2 Existing machinery

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `map_index` | `lsms_library/local_tools.py:3923` | w→t rename, the `u` missing rename, the legacy j/i swap | `tests/test_1234_map_index_unit_sentinel.py`, `test_normalize_index_j_preserved.py` | extend (one constant) |
| `Country._finalize_result` | `lsms_library/country.py:3870` | read-path finalisation; in `_build_registry._EXCLUDED_CALLABLES` | `test_finalize_order_797.py` and the round-2 tests | extend (asks `map_index`) |
| `_normalize_dataframe_index` | `lsms_library/country.py:8461` | promotes a declared `u` column into the index; collapses duplicates with `groupby(dropna=True)` | | context: the promotion point |
| `U_UNKNOWN`, `_U_SENTINELS` | `lsms_library/transformations.py:3744,3748` | the canonical missing-unit sentinel; the kg ladder serves it `none` | `tests/test_962_unit_sentinel.py` | reuse |
| `missing_level_sentinels` | `lsms_library/data_info.yml:284` | `Feature()` fills a missing `u` with `Unknown` | `tests/test_feature_canonical_index.py` | reuse (same spelling) |
| `_read_parquet_with_index_policy` / `_restore_parquet_index_string_policy` | `lsms_library/local_tools.py:1749` | restores the recorded string NA policy on read (bd80d86c2, 2026-10-21) | | context: decides WHICH missing values `map_index` sees as `pd.NA` |
| `_build_registry._closure_parts` / `_import_map` | `lsms_library/_build_registry.py:515,383` | folds a function-level `from .transformations import U_UNKNOWN` as the constant `'Unknown'`; no `transformations` callable enters the closure | `test_no_transformations_symbol_reaches_any_fingerprint` | reuse |

**Call sites of `map_index`** (`lsms_library/country.py`, development `9a0ed5f3c`):

| line | enclosing | path | note |
|------|-----------|------|------|
| 2232 | `Wave.grab_data` (`@build_transform()`) | BUILD | YAML path: runs before the L2-wave write, so the renamed label is stored (Mali `food_acquired` 2014-15). Script path: runs on the script's parquet before country aggregation writes L2-country. Sees only a `u` that is already an index level. |
| 5303 | `_aggregate_wave_data.run_make_target` | BUILD | country make target / script output, before the L2-country write |
| 5662 | `load_dataframe_with_dvc.collect_stage_outputs` | BUILD | stage outputs (stage layer retired in v0.7.0) |
| 4639 | `Country._read_l2_country_cache` | READ | L2-country read gate (in `_EXCLUDED_CALLABLES`) |
| 4746, 4755 | `Country._read_assumed_fresh_cache` | READ | `assume_cache_fresh` serve (excluded) |
| 5723 | `load_dataframe_with_dvc` stage-cache read | READ | its text lives inside the tagged `_aggregate_wave_data` |
| new | `Country._finalize_result`, top | READ | round 2: asks `map_index` what it does to each distinct `u` value and applies exactly that (excluded: 0 hashes) |

`map_index` itself is in the build closure through `Wave.grab_data`
(`tests/test_build_transform_hash.py::test_grab_data_body_is_versioned` pins
it), so any AST-visible edit to it moves every hash: measured, see §Phase 3.

## §3 Definitions & conventions in force
- Missing unit = `Unknown`: `lsms_library/data_info.yml:25-56` (`u` sentinel
  block, GH #842/#847) and `missing_level_sentinels` (`data_info.yml:284`);
  #1041 ruling as quoted in #1118.
- Consumer rule: `u == 'Unknown'` is not a unit and is excluded from per-unit
  prices/conversions (`data_info.yml:44-47`); `_U_SENTINELS` does not contain
  `'unit'`, so a minted `'unit'` was treated as a real unit.
- A served value that depends on cache state is a defect to fix (CLAUDE.md,
  "Derived Values", build/read composition).

## §4 Invariants & assumptions
- **The rename is dtype-dependent.** It matches `pd.NA` (only produced by a
  `StringDtype(na_value=pd.NA)` level) and the strings `'nan'`/`'<NA>'`; a
  NaN-policy (`str`/object) `NaN` is not matched (measured; dict lookup by
  hash).  So whether a missing `u` is renamed depends on the stored string NA
  policy, which bd80d86c2 restores on read.  Unchanged by #1234 at both sites
  (the brief: keep every other behaviour identical).  The NaN residual is
  #1118's decision; the `str`-policy cells holding a NaN `u` (reviewer's
  `wave_null_census`, shared cache): GhanaLSS `food_acquired` 2016-17 13,380;
  GhanaSPS `food_acquired` 2009-10 4,839 + 2017-18 9,064; Malawi
  `food_acquired` 39 + 95 + 307; Senegal `food_acquired` 21.  (Malawi
  `plot_inputs` is NOT one: its wave parquets are `string`-policy, and a
  development cold build serves 0 NaN `u` there.)
- **A `u` column is promoted after `map_index` has run.**  All four Malawi
  `crop_production` wave scripts write `u` as a `string` (pd.NA) COLUMN.
  `map_index` at `grab_data` (2232) sees the index `['t','i','plot_id','crop']`
  and has nothing to rename; `load_from_waves`' per-wave
  `_normalize_dataframe_index` promotes the column; `var/crop_production.parquet`
  stores pd.NA; only the warm read gate (4639) renamed it.  So development
  served 8,902 rows NaN cold and `'unit'` warm, and the first #1234 commit
  NaN cold and `'Unknown'` warm (`Feature` the same).  *Round 1 of this ledger
  described the cold side as a NaN-policy NaN written as `string` by
  `to_parquet`; that was wrong.  The `str` dtype it saw was an artefact of
  re-reading the saved frame, not the in-memory state (reviewer's trace).*
  The reviewer's census finds one such column-to-index promotion between the
  wave and country tiers: Malawi `crop_production`.
- Genuine `'unit'` exists: Panama decodes `units.json` code 13 `unidad`
  (1997) and code 39 (2008 script) to `'unit'`.  Not in the rename keys, so
  untouched.
- `Mali/_/CONTENTS.org` (the 1,912 / 1,534 rows) is updated in round 2.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| the sentinel | reuse `transformations.U_UNKNOWN` | the canonical constant; a function-level import dodges the `transformations → local_tools` cycle (`_import_map` resolves it) |
| the rename keys | unchanged | brief; widening to NaN-policy NaN moves #1118's rows |
| the finalize rename | reuse `map_index` itself | ONE definition with 0 hashes: moving the keys into a shared constant would change `map_index`'s AST and move 549/40/2,109 hashes; asking `map_index` what it does to each distinct `u` value makes it the single definition by construction and leaves its body untouched |

## §6 Open questions for the human
- Widen the rename to a NaN-policy `NaN`?  It is what the ruling says, but it
  moves the `str`-policy cells listed in §4 (#1118).

---
### Phase 3 — verification

- `map_index` — OK (anchored on §3, §5): only the rename target changed;
  `_U_SENTINELS` now covers what it emits.
- `Country._finalize_result` (round 2) — OK (§4, §5): applies `map_index`'s
  own mapping, conditional on it being non-empty (an unconditional `rename`
  would rebuild every `u`-indexed frame from tuples), before the declared-index
  collapse, with `attrs` carried explicitly.
- `test_1234_map_index_unit_sentinel.py` — OK (§4): round 1's 16 tests fail
  11 / pass 5 on `9a0ed5f3c`.  Round 2's 3 tests drive a wave-script frame
  (`u` a pd.NA column) through `map_index` and the real
  `_normalize_dataframe_index`, then `_finalize_result` cold and through
  `to_parquet` → `get_dataframe` → `map_index` warm: 2 fail on `cb564888c`
  ("cold: a missing u is served as NA"), all 19 pass on the round-2 head.

**Census** (`unit_census.py`, shared warm cache read-only).  The parquets
date from 2026-10-14..19, BEFORE bd80d86c2, so the census is NOT what
development serves; the private cold builds below are authoritative.  177
parquets carry a `u` index level (48 L2-country in 20 countries, 129 L2-wave).
The served-`'unit'` cells, split minted vs genuine, are in
`census_summary.json`.  No column-`u` table holds a `'unit'`-like value.
Genuine `'unit'`: Panama only (1997 25,825; 2008 33,244).  Minted at build
(stored): Mali `food_acquired` 2014-15 (1,912 wave, 1,534 country).  Minted at
read from a `string`-policy NA: the EHCVM `crop_production` waves (Benin 1,005,
Burkina 546, CotedIvoire 6,365, Guinea-Bissau 1,596, Senegal 597, Togo 606),
Malawi `community_prices` (42,928), `plot_inputs` (28,724 in the stale wave
parquets; 6,067 on a development cold build), `crop_production` (8,902),
Panama (443).

**Real data** (private roots; `real_*.json`, `r2_diff_*.json`).  Development
`9a0ed5f3c` vs the round-2 head; cold build, then a warm re-read in the same root:

| table | development cold | development warm | head cold = head warm | rows dev → head | verdict |
|---|---|---|---|---|---|
| Malawi `plot_inputs` | `'unit'` 6,067 (370 / 101 / 5,576 / 20) | same | `'Unknown'` 6,075 (6,067 + 8 pre-existing) | 179,949 → 179,949 | relabel only |
| Malawi `crop_production` | NaN 8,902 | `'unit'` 8,902 | `'Unknown'` 8,902 | 135,078 → 135,078 | relabel only; cold now equals warm |
| `Feature('crop_production')(['Malawi'])` | | | `'Unknown'` 8,902, cold (fresh root) = warm | 135,078 | identical to Country |
| CotedIvoire `crop_production` | `'unit'` 6,358 | same | `'Unknown'` 6,358 | 22,216 → 22,216 | relabel only |
| Mali `food_acquired` | `'unit'` 1,534 | same | `'Unknown'` 1,534 | 431,046 → 431,046 | relabel only (10 duplicate keys pre-exist on both sides) |
| Panama `food_acquired` | `'unit'` 59,509 | same | `'unit'` 59,069 + `'Unknown'` 443 | 570,939 → 570,942 | see below |

Head cold vs head warm: `assert_frame_equal` passes for all five tables and
for `Feature`, with no duplicate keys introduced.  Head warm vs the round-1
branch's warm frames: identical, so the finalize edit is a no-op on a warm read.

**Site I** (`r2_site_i_malawi_crop_production.json`).  On development's cold
build the served Malawi `crop_production` frame carried NaN keys on `u` (8,902)
and `plot_id` (41, 2016-17).  On the head, cold and warm, only `plot_id`'s 41
remain.  So Site I stops counting the 8,902 `u` keys cold, as it already did
warm.  `NullIndexKeyWarning` still fires, for `plot_id`, which #1234 does not
touch.

**Panama.**  440 rows relabel cleanly.  On development 3 minted 2008
`produced` rows were SUMMED into genuine `'unit'` rows at the declared-index
collapse.  The head separates them: +3 rows, and 3 genuine Quantities are
restored (20→10, 110→100, 38→8).  8 purchased 2008 `Price` cells (`unit` 2,
`pound` 3, `gram` 3) change too.  Development agreed with `E/Q` on them only
because those duplicates made the frame-wide additive collapse fire, and it
re-derives `Price = Expenditure / Quantity` on EVERY row.  With no duplicate
key the collapse does not run, and the head serves the wave script's own value
(a mean of per-row prices, `Panama/2008/_/food_acquired.py:185`).  That script
defect is commented on #845 and not fixed here.  Served `Price != E/Q`:
development 0, head 8, the 2008 wave parquet 8.

**Downstream** (`r2_downstream.json`, `r2_downstream_factors.json`; warm roots):
- Panama `food_quantities(units='kgs')`: 1,227,237.66 → 1,226,520.35 kg
  (−717.30, −0.058%).  Rows 560,260 → 560,263; 443 rows are now carried
  native at `u = 'Unknown'`.
- Panama `kg_factor_sources`: `item_unit` 96,996 → 96,947, `unit` 59,660 →
  59,269, `none` 17,409 → 17,852.
- Panama `food_prices` rows: 484,051 → 483,661.
- The 440 relabelled rows move to `none` (from `unit` 394, from `item_unit` 46).
- The genuine `'unit'` rows lose the minted rows from their estimators.
  26,279 keep the `unit` rung at a factor ×0.99475; 3 fall from `item_unit` to
  `unit` (ratio up to 2.3).  No other row's factor moves.
- Mali `food_quantities`, `kg_factor_sources` and `food_prices`: unchanged.
  Its 1,534 rows are served `none` on both sides.
- Malawi and CotedIvoire `harvest_kg`: unchanged (38,427,582.75 and
  12,131,426.16 kg; same sources).

**Closure** (`closure_probe_branch.json`): the function-level import folds as the
constant `lsms_library.local_tools.U_UNKNOWN='Unknown'`; `map_index` stays in
the closure; no `lsms_library.transformations` callable is reached
(`test_no_transformations_symbol_reaches_any_fingerprint` passes).

**Cost.**  An AST-visible no-op in `map_index` moved 549/549 country, 40/40
`build_transforms_fingerprint` and 2,109/2,109 wave hashes (38 countries); the
`map_index` fix moves the same.  That is a corpus-wide cold re-warm of both
tiers.  The round-2 edits (`_finalize_result`, `Mali/_/CONTENTS.org`) move
0/549, 0/40 and 0/2,109 against `cb564888c`
(`hash_compare_cb564888c_vs_round2.json`).
