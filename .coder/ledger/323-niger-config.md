# Prior-Art Ledger — GH #323 Site 4 / Niger config (clears PR #627)

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`; cites
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git (floor). gitnexus not consulted; no core
symbol was read-modified, so no impact analysis was required.

## §1 Task, restated

PR #627 (`fix/323-site4-dfs-merge`) turns the GH #515 swallowed `KeyError` in
`Wave.grab_data`'s `dfs:` region into a hard `RuntimeError` when a dropped
sub-frame leaves a **required** declared column entirely absent. Its body listed
Niger's fix as "not yet written". The task was to write it, config-only, without
touching `lsms_library/*.py` or `Niger/_/niger.py` (GH #637 agent's territory).

**The fix had already landed on `development`** as `3488b791` ("fix(#323): geo
config for Ethiopia + Niger"), after #627's body was written. This ledger records
an independent re-derivation that reached the same answer from the data, the
cold verification that Niger is genuinely cleared, and the regression test that
keeps the trap closed.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_required_scheme_columns` | `country.py:477` (#627) | required (non-`optional`) columns of a `data_scheme.yml` entry | yes (#627, 16 tests) | reuse (read-only) |
| `Wave._merge_subframes` / `_cartesian_keys` | `country.py:944` / `908` (#627) | many-to-many detection on the `dfs:` merge; warns, fatal under `LSMS_GRAIN_STRICT=1` | yes (#627) | reuse (read-only) |
| `Wave.grab_data` `dfs:` handler | `country.py:~1140-1230` (#627) | the raise this task had to satisfy | yes (#627) | reuse (read-only) |
| `Niger/2011-12/_/data_info.yml` `cluster_features.df_geo` | — | the failing cell | **no test until this PR** | the gap this PR fills |
| `get_dataframe` | `local_tools.py:805` | the only sanctioned reader; every probe below | yes | reuse |
| `origin/fix/323-niger` @ `5c554ee9` | — | prior-sweep Niger work | 13 tests | **not adopted** — see §5 |

## §3 Definitions & conventions in force

- Required vs optional declared columns: `_required_scheme_columns`,
  `country.py:477` (#627). `optional: true` exempts a column from BOTH the `dfs:`
  guard and `Country._assert_built_required_columns`, for **every wave** —
  `data_scheme.yml` is country-grain, not wave-grain.
- No `aggregation:` keys — decision **D1**,
  `slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org`. `aggregation` sits in
  `_SCHEME_NON_COLUMN_KEYS` (`country.py:473`) so an old config carrying one is
  not mistaken for a required column.
- `cluster_features` owns `v`; no other feature declares it — `CLAUDE.md`,
  "`sample()` and Cluster Identity".
- Niger is EHCVM for 2018-19 / 2021-22 (`v: grappe`, `i: [grappe, menage]`);
  2011-12 / 2014-15 are ECVMA and are not — `CLAUDE.md`, "Gotchas".

## §4 Invariants & assumptions

- A `dfs:` merge is cartesian **iff** a key value is duplicated in *both* frames
  (`_cartesian_keys`, `country.py:908`). A 1:many merge is not cartesian.
- **The LSMS-ISA "geovariables" extract publishes derived raster covariates
  *instead of* coordinates.** Where coordinates are released at all they ship in
  a companion "EA offsets" file, displaced for confidentiality. This is the whole
  substance of the Niger cell, and it is a general trap: the two files sit side
  by side in the same distribution directory with similar names.
- `format_id` is applied to `idxvars`, not `myvars` (`CLAUDE.md`), so the
  offsets file's `float64` grappe and the cover page's `int16` grappe land in the
  same keyspace — `v: grappe` is an idxvar on both sub-frames.
- **`merge_how:` is introduced by #627 and does not exist on `development`**
  (`grep -n merge_how lsms_library/country.py` → nothing). So on `development`
  today the offsets file's one trailing null-key row still arrives through the
  default `outer` merge and is deleted by the Site-1 collapse (loudly, per D2);
  under #627's core `merge_how: left` drops it at the merge. Both end at 270
  clean clusters with no null `v` — verified on the built table.
- `data_scheme.yml` carries `!make` tags, so it must be read with
  `lsms_library.yaml_utils.load_yaml`, **not** `yaml.safe_load` (which raises
  `ConstructorError`). Cost one test iteration.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| 2011-12 cluster Latitude/Longitude | already wired on `development` (`NER_EA_Offsets.dta`, `LAT_DD_MOD`/`LON_DD_MOD`, `merge_how: left`) | independently re-derived here from the data; identical answer, so **nothing re-landed** |
| `optional: true` on Latitude/Longitude | **rejected** | the data exists and is DVC-tracked; and `data_scheme.yml` is country-grain, so this would disarm the guard for all four waves to fix one |
| `aggregation:` keys from `fix/323-niger` | **rejected** | D1 — dead config, nothing reads them |
| `fix/323-niger`'s EXTENSION re-key, majority-vote `cluster_features`, plot_inputs / crop_production reductions | **deferred** | all are GH #614 **Site 1** work; all route through `Niger/_/niger.py` (off-limits, GH #637) plus a mandatory `panel_ids.py` co-change. Out of scope for clearing #627. |
| a Niger geo regression test | **new** | no test in `tests/` mentions `EA_Offsets`, `LAT_DD_MOD`, or Niger `cluster_features`; the config comment is prose, and prose is not enforcement |

## §6 Open questions for the human

- **Niger 2014-15 ships no coordinates at all.** The ECVMA-II distribution
  contains no geovariables and no EA-offsets file, and its cover page
  (`ECVMA2_MS00P1.dta`, 30 columns) has none. That is an *acquisition* gap, and
  it is invisible to the #323/#515 guard, which fires only on a **dropped**
  sub-df. If `absent_verdicts.csv` ever grows a row for this cell it should read
  `asked-not-distributed`, not `not-asked` — but the ECVMA-II questionnaire has
  not been checked, so **no verdict is filed here** (an unevidenced closing
  verdict is refused by `load_verdicts()`, and rightly).
- **`Niger/_/niger.py` needs no change to clear #627**, but `cluster_features`'
  reduction to `(t, v)` remains an accident of `groupby().first()` row order for
  Region / District / Rural. GPS is safe (EA-grain sources → constant within the
  group). Flagged to the GH #637 / Site 1 owners; not fixed here.

---
### Phase 3 — verification

**Method.** #627's core (`origin/fix/323-site4-dfs-merge`) on `PYTHONPATH` with
`lsms_library.__file__` asserted to resolve there; this worktree's config tree via
`LSMS_COUNTRIES_ROOT`, asserted via `paths.countries_root()`; an **isolated
`LSMS_DATA_DIR`** whose only pre-existing content is a symlinked `dvc-cache`, wiped
before each run — cold, because `LSMS_NO_CACHE=1` alone is soft for script-path
L2-wave parquets. `LSMS_GRAIN_STRICT=1` throughout, so any cartesian is fatal.

**Negative control** — same core, the pre-`3488b791` config:

```
RuntimeError: Niger/2011-12/cluster_features: sub-df(s) 'df_geo'
  (file: NER_2011_ECVMA_v01_M_Stata8/NER_HouseholdGeovars_Y1.dta) loaded but do
  NOT carry the column(s) the YAML asks for, leaving required declared column(s)
  ['Latitude', 'Longitude'] ENTIRELY ABSENT from 'cluster_features'. ...
  First error: KeyError('lat_dd_mod not in columns of dataframe.')
```

Exactly one failing cell: `Niger / 2011-12 / cluster_features`.

**Source-file census** (`get_dataframe`, both files DVC-tracked in the same dir):

| file | shape | grain | coordinate columns |
|---|---|---|---|
| `NER_HouseholdGeovars_Y1.dta` (the old target) | 4051 × 43 | household (270 grappe) | **none of any name** — `dist_road`, `dist_popcenter`, `dist_market`, `af_bio_*`, `srtm`, `twi`, `sq1..sq7`, EVI/NDVI |
| `NER_EA_Offsets.dta` (the right one) | 271 × 3 | cluster (270 grappe + 1 trailing null-key row) | `LAT_DD_MOD` ∈ [11.876, 18.747], `LON_DD_MOD` ∈ [0.405, 13.698] |

Key alignment: `ecvmamen_p1.dta` and `NER_EA_Offsets.dta` share exactly 270
grappe with no side-only values; all 3968 households fall in a grappe that has
coordinates. The merge on `v` is therefore 1:many, not cartesian.

Coordinate columns by wave, for the record:

| wave | file | grain | columns |
|---|---|---|---|
| 2011-12 | `NER_EA_Offsets.dta` | cluster | `LAT_DD_MOD` / `LON_DD_MOD` |
| 2014-15 | none shipped | — | — |
| 2018-19 | `grappe_gps_ner2018.dta` | cluster | `coordonnes_gps__Latitude` / `__Longitude` |
| 2021-22 | `s00_me_ner2021.dta` | household | `GPS__Latitude` / `GPS__Longitude` |

**After** — `Country('Niger').cluster_features()` builds, 1599 rows:

```
           rows  Latitude  Longitude  Region
2011-12     270       270        270     270      <- was 0 coordinates
2014-15     270         0          0     270      <- genuinely not shipped
2018-19     504       493        493     504
2021-22     555       555        555     555
```

**All 23 Niger tables** (every `data_scheme` entry plus the runtime-derived
food/roster tables) build cold under `LSMS_GRAIN_STRICT=1` with **zero** raises
and **zero** cartesian warnings. Row counts are identical between `development`'s
config and the independently re-derived one, so the two agree numerically as well
as textually.

**Test negative control.** With `2011-12/_/data_info.yml` reverted to its
pre-`3488b791` state, 3 of the 4 config-level tests fail
(`..._points_at_the_ea_offsets_file`, `..._uses_the_uppercase_offset_column_names`,
`test_merge_how_is_left`). `test_geovariables_file_has_no_coordinates_at_all`
passes either way, correctly — it asserts a fact about the *data*, not the config.
All 9 pass on `HEAD`.

**Incidental, reported not fixed** (surfaced by #614's Site-1/Site-2 audit during
the test run, present on `development` today, unrelated to the geo wiring):
`Niger/cluster_features/2014-15` projects a household-grain frame onto `(t, v)`
and **destroys 42 of 3,617 rows across 4 conflicting clusters**. That is the
majority-vote work in `origin/fix/323-niger` item B — Site 1/2 territory,
`Niger/_/niger.py`, deferred per §5.

- `tests/test_niger_cluster_features_geo.py` — new, 9 tests: OK (anchored on §4,
  §5). Pins the file, the casing, `merge_how: left`, the one trailing null-key
  row that makes `left` load-bearing, the geovariables file's *absence* of
  coordinates (the negative control, made permanent), that Latitude/Longitude
  stay **required** in `data_scheme.yml`, and the built table's coverage + grain.
- No config re-landed; `development`'s `3488b791` already carries it — OK (§5).
- No `aggregation:` key — OK (§3, D1).
- No file under `lsms_library/*.py`; `Niger/_/niger.py` untouched — OK (§1).
