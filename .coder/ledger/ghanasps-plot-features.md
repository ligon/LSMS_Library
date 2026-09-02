# Prior-Art Ledger — ghanasps-plot-features (GH #732, #729, #140)

> Per-task ledger.  Inherits the repo §0 baseline in `STANDING.md`; cites it,
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git floor (gitnexus MCP failed to connect this
session: `CONNECT_TIMEOUT`).

## §1 Task, restated
Wire the canonical `plot_features` table for GhanaSPS in all three waves
(`2009-10`, `2013-14`, `2017-18`) on the **YAML path**: per-wave
`data_info.yml` blocks with a `dfs:` merge (size file primary, tenure file
secondary, `merge_how: left`), a `plot_features` entry in
`GhanaSPS/_/data_scheme.yml` with index `(t, i, plot_id)` and columns
`Area` (float, hectares, required), `AreaUnit` (str), `Tenure` (str, canonical
vocabulary).  `v` is NOT declared — `sample` is wired for every wave and
`Country._join_v_from_sample` attaches it.  No `Irrigated`, no `SoilType`.
The one Python piece is a `plot_features(df)` `df_edit` hook in
`GhanaSPS/_/ghanasps.py` (see §5 for why it is unavoidable).  Verify cold under
`LSMS_GRAIN_STRICT=1 LSMS_READ_STRICT=1`; the load-bearing check is per-wave
`Area` non-null >= 97% (GH #732: W3 must read `plotunit`, not `plotsizeunit`).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave.grab_data` `dfs:` branch | `lsms_library/country.py:1273-1390` | multi-file merge on `merge_on`, honours `merge_how`, sets `final_index`, runs `derived:` → `drop:` → per-table `df_edit` hook | roster/cluster tests, Niger geo test | **reuse** |
| `Wave._merge_subframes` | `country.py:1095` | the GH #323 site-4 cartesian test (key duplicated in BOTH frames, nulls count) | `tests/test_gh323_*` | reuse (assert 1:1 keys per wave myself, report counts) |
| `Wave.column_mapping` → `mappings: [table, key, value]` | `country.py:939-947` | extraction-time categorical-table lookup, keyed on the org table's first column | GhanaSPS `individual_education` | **reuse** for `Tenure` and `AreaUnit` |
| `Wave.formatting_functions` / `df_edit` dispatch | `country.py:871`, `978`, `1308` | a callable named after the table in `ghanasps.py` (or the wave's `mapping.py`) is the table's `df_edit` hook, applied post-merge | GhanaSPS `individual_education`, 2009-10 `household_roster` | **reuse** the pattern (new hook body) |
| `df_data_grabber` | `local_tools.py:1325` | idxvars auto-`format_id`; myvars `(cols, fn)` row-wise; `(col, dict)` → `.map(f.get(x, x))` | yes | reuse |
| `format_id` | `local_tools.py:2107` | NaN → `None` (so a null `plot_no` becomes a null index level) | yes | reuse; the hook must DROP those rows |
| `build_transforms.apply_derived` | `lsms_library/build_transforms.py:311` | `derived:` dispatcher; registry `_DERIVED_TRANSFORMERS` holds exactly ONE kind (`coalesce_coord_bin`) | cache tests | cannot reuse (no multiply kind; adding one is a stop-listed core edit) |
| `Country._apply_categorical_mappings` | `country.py:2403` | API-time auto-dispatch of a country org table whose name matches a column | housing tests | second, idempotent pass over `Tenure`/`AreaUnit` |
| `diagnostics._check_declared_spellings` | `lsms_library/diagnostics.py:448` | fails a value outside a `spellings` vocabulary; `tests/test_table_structure.py:287` runs it on the CACHED `var/*.parquet` (pre-finalize) | `tests/test_declared_spellings.py` | the reason `Tenure` is mapped at EXTRACTION time, not API time |
| `Country._join_v_from_sample` | `country.py:1633` | joins `v` for any household table when `sample` exists | every GhanaSPS table | reuse (STANDING §5) |
| `Country._normalize_dataframe_index` / `_audit_index_collapse` | `country.py:5358` | `groupby(dropna=True)` deletes NaN-key rows and files a `GrainCollapseWarning` (fatal under `LSMS_GRAIN_STRICT`) | `tests/test_gh323_*` | the second reason the hook must drop null `plot_id` rows |
| GhanaLSS `plot_features` | `countries/GhanaLSS/_/data_scheme.yml`, `GhanaLSS/2016-17/_/data_info.yml` | nearest precedent: `(t, i, plot_id)`, `Area`/`AreaUnit`/`Tenure`, tenure mapped inline at extraction, AreaUnit vocabulary `Acres/Poles/Ropes/Plot/Hectare/Other` | schema tests | follow (vocabulary and mapping targets) |
| `slurm_logs/ghanasps/FINDINGS_agriculture.org` | §"Plot size converts to hectares exactly", §"plot_features — CONSTRUCTIBLE" | the hectare factors (Acre 0.404694, Pole 0.409551, Rope 0.236342, Plot 0.102388), the W3 `plotunit` trap, the tenure vocabulary table, the `plotid` permutation test | n/a (research note) | source of record for the constants |

## §3 Definitions & conventions in force
- `plot_features` canonical columns and `Tenure` vocabulary: `lsms_library/data_info.yml:185-231` (`Area` required, hectares; `Tenure.spellings` = owned, leased, rented_in, rented_out, sharecropped_in, sharecropped_out, inherited, communal, use_right, squatted, other_tenure).  `index_info: plot_features: (t, v, i, plot_id)` at `data_info.yml:19`.
- `v` is joined from `sample()`; never declared in a feature index: `CLAUDE.md` §"`sample()` and Cluster Identity"; STANDING §4.
- `dfs:` merge semantics, `merge_how: left`, and "a merge whose key is non-unique in BOTH frames is a cartesian": `CLAUDE.md` §"Gotchas with Teeth".
- Grain: core never aggregates; NaN in a declared index level is deleted-and-reported: `CLAUDE.md` §"Grain Collapse".
- Household id form and `v` recovery per wave: `GhanaSPS/_/CONTENTS.org` §"Cluster identity IS available" (W1 `hhno` 9-digit + `id3`; W2/W3 `FPrimary`, EA = `[-6:-3]`).
- `plot_id` is a panel id W1→W2 (96.7% via `04h_preroster`) but NOT W2→W3 (permutation test): `CONTENTS.org` §"The unit of observation is the PLOT".
- Module 04 is agriculture BY CONTENT in every wave: `CONTENTS.org` §"Module 4 IS agriculture in every wave".
- Extraction-time table mapping syntax: `.claude/skills/add-feature/SKILL.md` §"Categorical mapping tables".

## §4 Invariants & assumptions
- **W3 unit column is `plotunit`, never `plotsizeunit`** (GH #732).  Measured: `plotsizeunit` 575/5,366 non-null, `plotunit` 5,366/5,366; `plotsizeacres == 'Yes'` for exactly the 4,791 rows `plotsizeunit` leaves null.
- `(hh, plot)` is unique in BOTH sub-frames in every wave — measured, not assumed: W1 5,686/5,686 keyed rows, W2 4,694/4,694, W3 5,366/5,366; 0 key values duplicated in either frame; intersection = both sides in every wave.
- W1 `S4AII.dta` has 12 rows with a null `plot_no` (no size, no unit, no `area_ha`); `S4AIV.dta` has 16 (15 of them one household, `105158048`, which also has one null row in S4AII → the left merge fans that row out 1→15).  All carry nothing; the hook drops every null-`plot_id` row.
- W1 `area_ha` == `s4aii_a10 × factor` wherever both exist (0 rows differ by >1e-4 ha) and is null exactly where unit or size is missing (0 derivable-but-absent rows).  The derive path is a true fallback that takes 0 rows in W1.
- W2 `plotsizeunit` has 10 `Other - Specify` rows (free text: `1 bedroom`, `25 BY 30 FEET`, `18 sq`, `16 Squares`…) → no factor → `Area` NaN; W2 therefore lands at 99.79%, not 100%.
- W3 `plotunit` carries the `plotsizeunitother` free text for its 6 `Other` rows (`2 miles squared`, `It has never been measured before…`, `-999` ×3, `Don't know`); `plotsizeunit` says `Other (please specify)` for all six.
- Site R (`LSMS_READ_STRICT`): all-null column share per source is W2 `04h_agsection` 27.2% (62/228) — under the 1/3 trigger — and ≤ 10.7% elsewhere.
- Editing `ghanasps.py` moves `Country._table_cache_hash` for EVERY GhanaSPS table (country module is hashed): one cold rebuild of the shared cache after merge.
- `test_table_structure.py::test_feature_is_sane` reads the cached `var/plot_features.parquet` pre-finalize → `Tenure` must already be canonical in the parquet.
- Vocabulary extension is unavailable: `data_info.yml` is stop-listed and `_check_declared_spellings` fails any value outside the declared list.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| plot key / index `(t, i, plot_id)` | reuse (`idxvars` + `format_id`) | STANDING §2; GhanaLSS precedent |
| size + tenure join | reuse `dfs:` + `merge_how: left` | keys 1:1 in every wave (§4); size file authoritative |
| `Area` in hectares | **new** — `plot_features(df)` `df_edit` hook in `ghanasps.py` | YAML cannot multiply: `derived:` has one registered kind and adding one edits `build_transforms.py` (stop-listed).  Factors are W1's own producer constants (§2 FINDINGS) so W2/W3 are internally consistent with W1's shipped `area_ha`; the standard acre is 0.404686 ha, a 2e-5 relative difference, noted and not used. |
| null-`plot_id` row drop | **new** — same hook | `format_id(NaN)` → `None` index level → NaN-key deletion → `GrainCollapseWarning`, fatal under strict.  Drop deliberately (FINDINGS: "must be dropped deliberately, not silently"); precedent: `ghanasps.individual_education` drops the 5 null-pid rows. |
| `Tenure` harmonisation | reuse `mappings: [table, key, value]` at extraction + `_/categorical_mapping.org` `Tenure` table | canonical in the parquet (§4); GhanaLSS targets reused: Purchase→owned, Inherit→inherited, Rent→rented_in, Sharecrop→sharecropped_in; `Allocated free of charge`→communal (I36 follow-up: lineage/community qualify 70-74%, grantor relative/chief 69-75%); Begged/Borrowed→use_right; Other→other_tenure |
| `AreaUnit` harmonisation | reuse the same mechanism, `AreaUnit` table | GhanaLSS vocabulary `Acres/Poles/Ropes/Plot/Other` so `Feature('plot_features')` agrees across the two Ghanas; folds `Robes`/`Rope`/`Ropes` |
| `v` | reuse `_join_v_from_sample` | STANDING §5 |
| `SoilType` | not wired | not a straight extraction: W1 is a multi-variable `S4AIII` block; W2/W3 are 12/15-way multi-select `soil*` flags |
| `Irrigated` | not declared | source is "has a non-rain water source" — weaker than the canonical note (FINDINGS risk b) |

## §6 Open questions for the human
- W2/W3 merge rent and sharecrop into one option; mapped to `rented_in` (sharecrop = in-kind rent) because the vocabulary cannot be extended from this branch.  If the owner prefers a `rented_or_sharecropped_in` value, that is a `data_info.yml` change.
- W3 carries two plots of `70000 Poles` (28,669 ha each).  Left as reported (core does not edit data); flagged for anyone using W3 `Area` tails.

---
### Phase 3 — verification (fill at task end)
- `ghanasps.plot_features` (df_edit hook) — OK (anchored on §5): computes `Area` from `_area_ha`/`_size` × `AreaUnit` factor, drops the temporaries, drops null-`plot_id` rows; no reducer, no aggregation, no fabricated value.
- `GhanaSPS/{wave}/_/data_info.yml` `plot_features` — OK (§2/§4): `dfs:` merge on a key measured 1:1 in both frames; `merge_how: left`; W3 reads `plotunit`.
- `GhanaSPS/_/categorical_mapping.org` `Tenure` / `AreaUnit` — OK (§3/§5): every Preferred Label is in the `data_info.yml` vocabulary / the GhanaLSS AreaUnit set; applied at extraction so the parquet is canonical.
- `GhanaSPS/_/data_scheme.yml` `plot_features` — OK (§3): index `(t, i, plot_id)` without `v`; only canonical columns.
- `tests/test_ghanasps_plot_features.py` — OK (§4): data-gated via `requires_s3`; reads the vocabulary from `data_info.yml`, does not hardcode it.
