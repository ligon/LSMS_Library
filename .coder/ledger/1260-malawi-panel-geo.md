# Prior-Art Ledger — GH #1260 Malawi IHPS panel geovariables

**Search tier used:** ripgrep+git floor (the `gitnexus` MCP server failed to
connect this session; call sites swept with `rg`/`grep`).

## §1 Task, restated
Malawi `cluster_features` (declared `(t, v)`, built from household-grain
`dfs:` sub-frames and projected onto the cluster by Site 2,
`Wave.cluster_features`) served no `Latitude`/`Longitude` for the IHPS panel
EAs of 2016-17 (101 of 880) and 2019-20 (102 of 819). Each of those wave
directories holds two catalog entries (GH #600): the IHS cross-section and the
IHPS long-term panel. The panel entries (2939, 3819) publish a household
geovariables file that we did not hold. Task: acquire exactly those two files
from the entries the waves already hold, publish them through the sanctioned
writer, and wire each into the wave's `df_geo` as a second geo source keyed on
the panel household id. Config + data only; no framework change.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `data_access._find_stata_zip_url` | `lsms_library/data_access.py:1034` | NADA resources API -> Stata zip URL | yes | reuse (download) |
| `data_access.push_to_cache_batch` | `lsms_library/data_access.py:1532` | batched `dvc add` + `dvc push` under the lock-retry writer | yes | reuse (publish) |
| `data_access._resolve_write_credentialpath` | `lsms_library/data_access.py:1364` | finds writer creds (legacy in-tree `.dvc/s3_write_creds`) | yes | reuse via gitignored symlink |
| `local_tools._ensure_dvc_pulled` | `lsms_library/local_tools.py:319` | sidecar md5 -> direct S3 GET into L1 | yes | reuse (fetch check) |
| `Wave.column_mapping` per-file overrides | `lsms_library/country.py:1387` | `file:` list entries `{path: {idxvar or myvar: override}}` | yes | reuse (stack two geo files) |
| `Wave._merge_subframes` | `lsms_library/country.py:1622` | cartesian guard on the `dfs:` merge | yes | unchanged; 1:1 merge |
| `country.most_complete_row` / Site 2 audit | `lsms_library/country.py` (`_collapse_to_cluster_grain`) | selects one household row per cluster, reports loss | yes | unchanged; reports grow |
| `grab_data` df_edit hook dispatch | `lsms_library/country.py:1962` | runs the wave's `cluster_features(df)` on the merged `dfs:` frame | yes | reuse (stayer filter) |
| `grab_data` >1e99 sentinel rule | `lsms_library/country.py` (`get_data`) | rewrites any value > 1e99 to NaN | yes | constraint: no numeric `inf` sentinel |

## §3 Definitions & conventions in force
- Two catalog entries per wave dir, `#+CATALOG_ID:` is a list: CLAUDE.md
  "catalog_id is NOT a 1:1 key (GH #600)".
- `#+CATALOG_IDNO:` names the release we HOLD for the primary entry; not moved
  here (CLAUDE.md "Wave provenance", #1114 ruling).
- `v` is owned by `df_main`; `df_geo` must not declare it:
  `tests/test_gh323_malawi_gb_cartesian.py::test_malawi_geo_subframe_is_keyed_on_the_household`.
- Panel `i` = `format_id(y3_hhid / y4_hhid)` via the per-file override on the
  panel cover (`Malawi/{2016-17,2019-20}/_/data_info.yml`, `df_main`).
- Core selects, never composes, a cluster row (GH #871): CLAUDE.md "Grain
  Collapse".

## §4 Invariants & assumptions
- `countries_root()` is read-only at runtime; raw sources never in-tree
  (GH #831/#803). The `.dta` was placed in-tree only for `dvc add`, then moved
  out.
- No `dvc` CLI; publish only through `push_to_cache_batch` (CLAUDE.md "Never
  invoke the dvc CLI").
- A two-file `file:` list runs `df_data_grabber(missing_ok=True)`
  (`country.py` `get_data`): a mis-spelled column becomes NaN, not `KeyError`.
  Guarded by `tests/test_gh1260_malawi_panel_geo.py` coverage tests.
- A file in a multi-file sub-df that is UNAVAILABLE drops the whole `df_geo`
  (GH #515 fallback), cross-sectional coordinates included. Accepted: the new
  blobs sit on the same S3 remote as every other file of the wave.
- IHPS coordinates are each household's CURRENT displaced location, not an EA
  fix (Malawi `CONTENTS.org`, 2013-14 "the GPS is the proof"; re-measured for
  2019-20 here).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| panel coordinates | reuse `file:` list + per-file override in `df_geo` | a third `dfs:` member would collide on `Latitude`/`Longitude` (`_x`/`_y`); no coalesce transformer exists and adding one is framework work |
| merge type | keep `outer` | both sides 1:1 on `i` (14,955 / 14,612), so `outer` = `left`; `outer` keeps the orphan signal |
| cluster coordinate for a panel EA | NEW wave hook: blank non-stayer panel coordinates (round 2, dispatcher judgement 2026-10-23) | stayers (`dist_to_IHS3location` <= 1 km, the 2013-14 test's rule) share exactly one coordinate per EA in both waves; no precedent filter exists (2013-14 reports, does not filter) |
| shared EA 30305580 | keep the cross-section's fix | IHS4 and IHPS stayer fixes differ by 4.1 km (independent displacement); keeping IHS4's leaves the row identical to development |

## §6 Open questions for the human
- 2016-17 held `Data/Panel/`: 5 of 98 files differ from the current 2939 v03
  zip (`hh_mod_a_filt_16` differs only in `case_id`, all rows). Re-acquire as a
  separate issue?
- Round 2 now serves the stayers' fix in 2016-17 / 2019-20.  Extend the same
  hook to 2013-14 (24 of 204 clusters serve a non-stayer coordinate there)?
- Is blanking non-stayer coordinates a "deletion" the derivations registry
  should name?  Not registered here: the served value is a recorded value,
  and only the choice of which household supplies it changed.
- Site 2 still reports every panel EA as conflicting (a blank counts as a
  value), so the grain report did not fall with the coordinates.
- 2019-20 `function: cs_i` is a no-op (unrecognised key), so the IHS5 `i` is a
  bare `case_id`. Intended?
- 9 IHS5 cross-sectional clusters serve coordinates outside Malawi's box
  (pre-existing).

---
### Phase 3 — verification

- `Malawi/2016-17/_/data_info.yml` `cluster_features.df_geo` — OK (§5): second
  `file:` entry, `i: y3_hhid`; wave frame 14,955 rows unchanged, no `_x`/`_y`.
- `Malawi/2019-20/_/data_info.yml` `cluster_features.df_geo` — OK (§5): second
  `file:` entry, `i: y4_hhid`, `Latitude: lat_mod`, `Longitude: lon_mod`; wave
  frame 14,612 rows unchanged.
- New sidecars `HouseholdGeovariablesIHPSY3.dta.dvc`,
  `householdgeovariables_y4.dta.dvc` — OK (§4): written by
  `push_to_cache_batch`, no existing sidecar changed.
- `tests/test_gh1260_malawi_panel_geo.py` — OK (§4): fails on development
  (6 config, 4 data), passes on this branch.
- Round 2: `Malawi/{2016-17,2019-20}/_/mapping.py` `ihps_baseline_status`,
  `cluster_features` and the `_ihps_baseline_status` panel-cover override —
  OK (§5): every panel EA serves its stayer fix (101/101, 102/102), 0 EAs
  with more than one coordinate, shared EA identical to development.  The
  11 new or changed tests fail on round 1 (`fd5c12474`).
- Evidence: `slurm_logs/isa_backlog_2026-10-21/1260-malawi-panel-geo/`.
