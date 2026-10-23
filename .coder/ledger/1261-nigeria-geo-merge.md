# Prior-Art Ledger - #1261 Nigeria cluster_features geo merge (`merge_how: left`)

**Search tier used:** ripgrep + git floor (gitnexus MCP did not connect). Base: 9a0ed5f3c.

## §1 Task, restated

Nigeria's YAML-path `cluster_features` (`data_scheme.yml`, index `(t, v)`) is built per wave folder from a `dfs:` block: `df_key` + `df_main` from the post-planting cover and `df_geo` from the household geovariables file, merged on `i` with the default `merge_how: outer`. Both PP/PH rounds of a folder (`wave_folder_map`) read the same block. Where the geo file carries households the cover lacks they arrive with a null `v` and the Site 2 collapse (`Wave.cluster_features` -> `_collapse_to_cluster_grain`) deletes them. Declare `merge_how: left` (cover authoritative) where that shape exists, without changing served rows.

## §2 Existing machinery

| symbol | path | what it does | tested? | decision |
|---|---|---|---|---|
| `merge_how:` key | `lsms_library/country.py` (`Wave.grab_data`, dfs branch) | join type for every merge of the block; `dfs:` merged in order, running result on the left | `tests/test_gh323_site4_dfs_merge.py::test_grab_data_merge_how_left_is_honoured` | reuse |
| `Wave._merge_subframes` | `lsms_library/country.py` | cartesian guard + `pd.merge(how=...)` | same | reuse (unchanged) |
| `_collapse_to_cluster_grain` / `_audit_index_collapse` | `lsms_library/country.py` | Site 2 projection to `(t, v)`; reports `nan_key_rows`, destroyed, dropped | `tests/test_gh323_*` | reuse (unchanged) |
| `nigeria.cluster_id` / `v` | `Nigeria/_/nigeria.py` | composite `state/lga/ea`; `moved-{hhid}` singleton | Nigeria tests | reuse (unchanged) |
| Precedents | Ethiopia 2011-12..2021-22, Niger 2011-12, GhanaSPS, GhanaLSS `data_info.yml` | `merge_how: left`, primary sub-df authoritative | per-country tests | follow |

## §3 Definitions and conventions in force

- `merge_how: left` is "not a data fix": it stops manufacturing null-keyed rows for the collapse to delete and avoids dtype widening; its cost is a lost signal. CLAUDE.md, Gotchas, bullet "A `dfs:` merge whose `merge_on` key is non-unique in BOTH sub-frames ..."; `country.py` comment above `merge_how = data_info.get(...)`.
- Site 2 and the grain audit: CLAUDE.md "Grain Collapse" / "Site 2 -- `Wave.cluster_features` (GH #161)".
- Nigeria PP/PH rounds, `moved-` sentinel, composite `v`, W4 geo key note, W5 no geo file: `Nigeria/_/CONTENTS.org` "Cluster (PSU) identifier" and its subsections; `.claude/skills/add-feature/pp-ph/SKILL.md`.

## §4 Invariants and assumptions

- `v` comes only from the PP cover, for BOTH rounds of a folder; the geo file cannot supply a `v`, so a geo-only household can never be served by this block (under `outer` or `left`).
- `df_key` and `df_main` read the same file: their merge is 1:1 with identical key sets in every wave (measured), so `left` changes nothing at the first merge.
- A wave `data_info.yml` is in `Wave._input_hash` (binp) of every table of that wave, and every wave folds into `Country._table_cache_hash`: cost = Nigeria re-warm (24 country hashes) plus the four edited rounds' L2-wave hashes.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| join type | reuse `merge_how: left` (config only) | existing, tested key; Ethiopia/Niger precedent |
| W1, W4 | not declared | 0 geo-only households measured, `left` is a no-op there and would only invalidate L2-wave caches |
| W5 | not applicable | no `df_geo` until v02 (GH #978); CONTENTS.org says to measure then |

## §6 Open questions for the human

- The 39 W2 geo-only households are post-harvest-only households (39 of the 44 PH-cover households absent from the PP cover), in 7 EAs of which 2 are absent from the PP cover. They have no `v` in 2013Q1 `sample`/`cluster_features` under either merge. `left` removes the `nan_key_rows=39` report that pointed at this. Should 2013Q1 `sample`/`cluster_features` read the PH cover (a served-rows change, separate issue)?

---
### Phase 3 -- verification

- `Nigeria/2012-13/_/data_info.yml` `cluster_features.merge_how: left` -- OK (anchored on §3, §4): merged frame 4,898 -> 4,859 rows, null `v` 39 -> 0; served frame exact-equal.
- `Nigeria/2015-16/_/data_info.yml` `cluster_features.merge_how: left` -- OK (§3, §4): 4,697 -> 4,696, null `v` 1 -> 0; served frame exact-equal.
- Hash census (dev vs branch): btf 0/40, country 24/549 (all Nigeria), wave 96/2,109 (Nigeria 2012Q3/2013Q1/2015Q3/2016Q1 only).
- `tests/test_gh1261_nigeria_geo_merge.py`: branch 16 passed; development 10 failed / 6 passed (the 6 are the "cover is left frame" config checks and the served-unchanged pins, which hold on both by design).
