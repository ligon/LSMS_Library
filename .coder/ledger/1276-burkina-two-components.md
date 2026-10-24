# Prior-Art Ledger — GH #1276 / #1244: Burkina Faso 2021-22, two components; namespaced ids

**Search tier used:** ripgrep + git floor (the gitnexus MCP server did not connect this session).

## §1 Task, restated
`Burkina_Faso/2021-22/` holds only WB catalog 6224 (EHCVM-P, the panel
component: 3,227 re-interviewed 2018-19 households in grappes 2-585).  The same
round's cross-section, catalog 6277 (EHCVM-2, 7,176 households in 600 new
grappes 586-1185, drawn from the 2019 census frame), is disjoint from it (0
households overlap) and was not held.  @ligon's rulings on #1276: serve both,
Malawi-style (#600): 6224 in `Data/Panel/`, 6277 in `Data/Cross_Section/`;
acquire through `add_wave(subdir=)`; `weight` is each component's own weight,
`panel_weight` the panel's only.  Ruling on #1244: namespace the household ids
of the samples that are independent of the EHCVM panel lineage (EMC 2014 and the
6277 cross-section), following Niger's `E_` spelling, and keep the panel's ids.

Four phases: (1) move the held 6224 sidecars into `Data/Panel/` (config only);
(2) acquire 6277 into `Data/Cross_Section/` behind a pre-push gate; (3) wire
both components into every 2021-22 household table; (4) namespace the colliding
ids.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `add_wave(subdir=)` | `lsms_library/data_access.py:2656` | confined acquisition into `Data/<subdir>/`, `#+CATALOG_SUBDIR` / `#+CATALOG_HELD` | `tests/test_gh1276_acquisition_subdir.py`, `tests/test_gh1283_secondary_release.py` | reuse, unchanged |
| `_extract_all_from_zip` | `data_access.py:1376` | stages the zip, compares to sidecars, places members | `tests/test_staged_reacquisition.py` | reuse; observed by a driver-side spy |
| `push_to_cache_batch` | `data_access.py:1795` | batched `dvc add` + `dvc push` with writer creds | writer tests | reuse; gated driver-side (raise before push) |
| `unpushed_blobs` | `data_access.py:1883` | `dvc status --cloud` for named targets | -- | reuse (push verification) |
| `provenance.with_catalog_subdir` | `lsms_library/provenance.py:966` | adds `#+CATALOG_SUBDIR: <id> <subdir>` | `test_gh1276_acquisition_subdir.py` | reuse (Phase 1) |
| multi-file `file:` list + per-file override | `lsms_library/country.py:1534` | concat several files; override idxvars/myvars per file; `missing_ok=True` when >1 file (`country.py:1888`) | Malawi 2016-17 / 2019-20 tests | reuse (Phase 3) |
| Malawi `cs_i` | `Malawi/2016-17/_/data_info.yml:4-10` | per-file `i` formatter keeping the cross-section's ids apart from the panel's | Malawi tests | precedent for the cross-section namespace (Phase 3/4) |
| Niger `niger.i` `'E_'` prefix | `Niger/_/niger.py:15-48` | namespaces EHCVM ids apart from ECVMA ids | Niger tests | precedent for the prefix spelling (Phase 4) |
| Mali #536 `cur_set` | `Mali/_/panel_ids.py:116-134` | the "occupied" set is every 2021-22 household, panel or not | Mali tests | precedent for `panel_ids.py` (Phase 3) |
| `id_walk` exactly-once | `lsms_library/local_tools.py:3704` | single-pass rename; refuses a mixed raw/walked wave (`IdWalkError`) | `tests/test_id_walk.py` | reuse; verification (Phase 4) |
| `ehcvm.inputs_last_purchase` | `lsms_library/ehcvm.py:380` | raw 7B inputs behind the derived purchase value; assumes ONE file | `tests/test_ehcvm_7day.py` | extend or wrap (Phase 3; hash cost decides) |

## §3 Definitions & conventions in force
- Held release / secondary entry: `CLAUDE.md` §"Data Access", wave-provenance paragraph and the GH #600 bullets; `.claude/skills/add-wave/SKILL.md` Step 2b.
- `weight` vs `panel_weight`: `CLAUDE.md` §"`sample()` and Cluster Identity" (panel_weight NaN for refreshment households); weights normalised to within-wave mean 1 at API time (`_normalise_sample_weights`).
- A missing index value gets the registered sentinel, never NaN (AGENT_RULES); a value column may be NaN where the component did not ship it.
- `household_continuity` / `cluster_id_continuity` vocabulary: `lsms_library/sampling.py:207-224`.
- `universe_tag` vocabulary (`mixed-national+panel`): `lsms_library/population.py:93-104`.

## §4 Invariants & assumptions
- Never run the `dvc` CLI; acquire only through `add_wave` (`CLAUDE.md` §"Data Access").
- `countries_root()` is read-only at runtime except acquisition; raw copies placed by `add_wave` are deleted once their md5 matches the sidecar.
- `id_walk` is a single pass and refuses a raw frame holding both a map key and a map-only target (`local_tools.py:3765-3768`): a cross-section household whose raw id equals a panel target makes every 2021-22 read raise.  So the cross-section cannot be wired un-namespaced.
- `panel_ids.py` asserts that no crosswalk target is held by a 2021-22 household that keeps its id (`Burkina_Faso/_/panel_ids.py:127-140`, GH #1245 ruling).
- EHCVM: `v: grappe`; the panel keeps 2018-19's grappe; the cross-section's grappes 586-1185 are disjoint from 2018-19's 1-585 (Niger 2021-22's 900-996 precedent: `stable`, not `mixed`).
- 2021-22 `i` = `grappe + menage.zfill(3)` (country `i`), 2018-19 `i` = `grappe + '0' + menage.zfill(2)` (`ehcvm_i`) -- the #1240 divergence (CONTENTS.org).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| acquisition | reuse `add_wave(subdir="Cross_Section")` | sanctioned path |
| membership gate | new, driver-side, not committed | STOP before any push unless 55 members / md5 equal the investigation's download |
| two-component reads | reuse multi-file `file:` lists | Malawi 2016-17 / 2019-20 precedent |
| cross-section weights | per-file override `weight: hhweight`; `panel_weight` absent from the cross-section file -> NaN | Malawi precedent (`panelweight_2016` absent from the cross-sectional file) |

## §6 Open questions for the human
- (filled per phase)

---
### Phase log
- **Phase 1** (`93db8ef22`): 53 sidecars `git mv`'d to `Data/Panel/`; `#+CATALOG_SUBDIR: 6224 Panel` via `provenance.with_catalog_subdir` (parse -> render a fixed point; a second call returns the same object); 13 `file:` entries + 3 scripts + `sampling.yml` + 4 test paths.  `make panel-ids`: empty JSON diff.
- **Phase 2**: `add_wave("Burkina_Faso", "6277", wave="2021-22", subdir="Cross_Section", confirm=False)` under a driver-side spy (`_extract_all_from_zip`) and gate (`push_to_cache_batch`, raise before the original): 55 members / 55 inside / 0 outside / 0 revised / 0 leftovers / 0 md5 mismatches / Panel sidecars unchanged -> 55 pushed.  `unpushed_blobs` empty; empty-L1 read-back 55/55; raw copies deleted after md5 = sidecar.  The driver is not committed (Ethiopia #1127 / Nigeria #978 precedent).
- **Phase 3**: 12 two-file `file:` lists + the weights per-file override (`weight: hhweight` on `Cross_Section/ehcvm_ponderations_bfa2021.dta`; `panel_weight` NaN there through `missing_ok`); `plot_features.py` reads both `s16a` (cross-section built without the coordinate keys, columns added NA); `panel_ids.py` occupied set = both covers, Mali #536 drop-and-count (2 links dropped: 3,227 -> 3,225); registry `inputs:` repointed to the 2021-22 wrapper, which loops over files (`ehcvm.inputs_last_purchase` untouched: it is in the closure of 12 EHCVM waves); `sampling.yml` 2021-22 `rotating-panel` (Niger 2021-22 precedent); `population.yml` 2021-22 `mixed-national+panel` (Malawi 2016-17 / 2019-20 precedent), regenerated by `scripts/promote_population_records.py` with the evidence document and its counts moved (panel-inherited 5 -> 4, mixed 2 -> 3).  Silent-NaN audit (headers of every file each block reads): only `hhweight_panel` is absent from a component.
