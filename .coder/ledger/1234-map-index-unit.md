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

## §2 Existing machinery

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `map_index` | `lsms_library/local_tools.py:3923` | w→t rename, the `u` missing rename, the legacy j/i swap | `tests/test_1234_map_index_unit_sentinel.py` (new), `test_normalize_index_j_preserved.py` | extend (one constant) |
| `U_UNKNOWN`, `_U_SENTINELS` | `lsms_library/transformations.py:3744,3748` | the canonical missing-unit sentinel; the kg ladder serves it `none` | `tests/test_962_unit_sentinel.py` | reuse |
| `missing_level_sentinels` | `lsms_library/data_info.yml:284` | `Feature()` fills a missing `u` with `Unknown` | `tests/test_feature_canonical_index.py` | reuse (same spelling) |
| `_read_parquet_with_index_policy` / `_restore_parquet_index_string_policy` | `lsms_library/local_tools.py:1749` | restores the recorded string NA policy on read (bd80d86c2, 2026-10-21) | | context: decides WHICH missing values `map_index` sees as `pd.NA` |
| `_build_registry._closure_parts` / `_import_map` | `lsms_library/_build_registry.py:515,383` | folds a function-level `from .transformations import U_UNKNOWN` as the constant `'Unknown'`; no `transformations` callable enters the closure | `test_no_transformations_symbol_reaches_any_fingerprint` | reuse |

**Call sites of `map_index`** (`lsms_library/country.py`, development `9a0ed5f3c`):

| line | enclosing | path | note |
|------|-----------|------|------|
| 2232 | `Wave.grab_data` (`@build_transform()`) | BUILD | YAML path: runs before the L2-wave write, so the minted label is stored (Mali `food_acquired` 2014-15). Script path: runs on the script's parquet before country aggregation writes L2-country. |
| 5303 | `_aggregate_wave_data.run_make_target` | BUILD | country make target / script output, before the L2-country write |
| 5662 | `load_dataframe_with_dvc.collect_stage_outputs` | BUILD | stage outputs (stage layer retired in v0.7.0) |
| 4639 | `Country._read_l2_country_cache` | READ | L2-country read gate (in `_EXCLUDED_CALLABLES`) |
| 4746, 4755 | `Country._read_assumed_fresh_cache` | READ | `assume_cache_fresh` serve (excluded) |
| 5723 | `load_dataframe_with_dvc` stage-cache read | READ | its text lives inside the tagged `_aggregate_wave_data` |

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

## §4 Invariants & assumptions
- **The rename is dtype-dependent.** It matches `pd.NA` (only produced by a
  `StringDtype(na_value=pd.NA)` level) and the strings `'nan'`/`'<NA>'`; a
  NaN-policy (`str`/object) `NaN` is not matched (measured; dict lookup by
  hash).  So whether a missing `u` is served minted or NaN depends on the
  stored string NA policy, which bd80d86c2 now restores on read.  Unchanged
  by this fix (the brief: keep every other behaviour identical); the NaN
  residual is #1118's per-country work.
- Genuine `'unit'` exists: Panama decodes `units.json` code 13 `unidad`
  (1997) and code 39 (2008 script) to `'unit'`.  Not in the rename keys, so
  untouched.
- `Mali/_/CONTENTS.org:1259-1270` documents the 1,912 / 1,534 minted rows;
  it will read stale after this fix (country config: out of scope here).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| the sentinel | reuse `transformations.U_UNKNOWN` | the canonical constant; a function-level import dodges the `transformations → local_tools` cycle (`_import_map` resolves it) |
| the rename keys | unchanged | brief; widening to NaN-policy NaN moves #1118's rows |

## §6 Open questions for the human
- Widen the rename to a NaN-policy `NaN`?  It is what the ruling says, but it
  moves Malawi `plot_inputs` 28,724, GhanaLSS `food_acquired` 13,380, GhanaSPS
  `food_acquired` 13,903 and the EHCVM `crop_production` cells (#1118).
- `Mali/_/CONTENTS.org`'s minted-`'unit'` paragraph needs a follow-up edit.

---
### Phase 3 — verification
(filled below)
