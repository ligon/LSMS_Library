Everything is committed and pushed, the tree is clean, and REPORT.md is written. Here is the report.

# niger-followup -- REPORT (branch `fix/niger-followup`, on top of `fix/niger-bundle`)

## SCOPE DEVIATIONS

- `tests/test_niger_u_sentinel.py` -- not named by the task. Five pinned counts (rows and `u=='Unknown'`) measured 2026-09-09 are moved by the bundle; moved with the before/after numbers and their causes in the commit message and in the file's docstring, as the guardrail allows. No assertion weakened.
- Nothing else. A build driver (`build_niger.py`) was created in the clone root during the rebuild and moved to `$STAGE/build_niger.py` before committing; it was never staged. No stop-list file touched. `find lsms_library/countries -name '*.parquet'` is empty. `$LSMS_DATA_DIR/dvc-cache` is still the read-only symlink.

## 1. Commits and branch

Branch `fix/niger-followup`, pushed to `origin` (the shared mirror), `git status --short` empty:

| SHA | what |
|---|---|
| `198f03ad1` | `tests/test_niger_region_labels.py` (GH #811) |
| `99095ecee` | `tests/test_niger_food_label_drift.py` (GH #833) |
| `53e2752cb` | `tests/test_niger_sold_unit.py` (GH #824) |
| `62662cfd3` | `Niger/_/CONTENTS.org`: sold-unit subsection rewritten and re-measured, new #833 section, #679 residual note |
| `fcea61515` | `tests/test_niger_u_sentinel.py` pins moved (before -> after in message); CONTENTS.org records hid 27010 |
| `cad6c8797` | re-measured per-wave Region spellings (test dict + the 16-before figure), the old 98.8/93.1 provenance sentence, and served rows for all 23 tables |

## 2. Verification

Idiosyncrasies read from `Niger/_/CONTENTS.org` (prose + LOGBOOK/WAITING/TODO) that bear on the five commits, named:

- **EHCVM grappe/vague** (2018-19, 2021-22): each grappe is visited in one vague, so `v = grappe`, `i = (grappe, menage)` with the `E_` prefix, no vague level. Bears on #824's 2021-22 keying and #833's food labels (EHCVM-only tables).
- **EXTENSION** (2014-15, #679): ECVMA2's tracking outcome per household (0 in place 3,478 / 1 moved 77 / 2 split-off 62); (GRAPPE, MENAGE) is not a household (3,617 households on 3,558 pairs); decision: extensions 0 and 1 link to 2011-12, 2 does not; one unlinked id `'20012'` also spells a 2011-12 hid.
- **The crop-level sale block** (2014-15 AS2E2, 2021-22 s16d, #824): no plot dimension, attributed only to single-plot crops; CONTENTS.org's old subsection said the sold unit "is not carried" and gave 2021-22 agreement as 63.0% -- both now corrected.
- **The s16d repetition** (#824): the 2021-22 sale file replicates crop-level lines; 5,786 exact duplicates; the old script summed them.
- **The `u` sentinel `'Unknown'`** (#842) vs **the all-nines VALUE sentinel** (#865): distinct things, both present on the same 2011-12 lines (unit code 99 + 999999).
- **2021-22 crop_production's 897 destroyed rows / 740 tuples** at the index collapse are PRE-EXISTING and already recorded in the #842 section (line ~706); not a bundle regression (same 1,856 duplicated index rows in the wave parquet before and after).

Commands, one line each (all foreground, output to `$STAGE/logs/`):

- `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library...assert'` -> `pkg ok /local/job39199460/clone/lsms_library/__init__.py`
- `cp -a $LSMS_DATA_DIR/Niger $STAGE/prebundle_Niger` (snapshot of the pre-bundle warm root, taken before any rebuild)
- `.venv/bin/python -m lsms_library.cli cache clear --country Niger` -> exit 0, 0 parquets left under `$LSMS_DATA_DIR/Niger`
- Cold rebuild, per table via `diagnostics.load_feature`, `build_1..6.log`: **all 23 data_scheme entries BUILT, 0 FAILED, no `_rebuild_failure_error`**. Warnings (none new): `NullReadWarning` sample.strata (2014-15..2021-22), cluster_features Latitude/Longitude 2014-15, shocks.HowCoped2 2018-19, crop_production harvest_month/intercropped, people_last7days Industry/hours 2018-19+; `GrainCollapseWarning` cluster_features/2014-15 (42 of 3,617), crop_production/2011-12 (19), crop_production/2021-22 (897, pre-existing per CONTENTS), plot_inputs all four waves (51/35/6/3); `QuantityImplausibleWarning` plot_features 2018-19 (9), crop_production 2018-19 (1); `NullIndexKeyWarning` crop_production `v` 2011-12 (2 rows = hid 27010, see below); `StatedVsInferredKgWarning` on food_prices/food_quantities.
- New tests: `pytest tests/test_niger_region_labels.py tests/test_niger_food_label_drift.py -n 2 --dist=loadfile -q -rs` -> **`36 passed, 10 warnings in 19.42s`**
- New test: `pytest tests/test_niger_sold_unit.py -n 1 --dist=loadfile -q -rs` -> **`26 passed in 13.20s`**
- Moved pins: `pytest tests/test_niger_u_sentinel.py -n 1 --dist=loadfile -q` -> **`25 passed in 23.06s`**
- Region test after the per-wave dict fix: `pytest tests/test_niger_region_labels.py -n 1 --dist=loadfile -q` -> **`10 passed, 3 warnings in 18.53s`**
- Wider selection, verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 ... -n 8 --dist=loadfile -q`, `tests/test_niger*.py tests/test_ehcvm_7day.py tests/test_schema_consistency.py tests/test_panel_ids_persistence.py tests/test_feature_canonical_index.py`): first run **`6 failed, 722 passed, 93 warnings in 459.73s`** (5 stale u-sentinel pins + 27010); after the pin move **`1 failed, 727 passed, 90 warnings in 141.50s (0:02:21)`**. The one red test is `tests/test_niger_2014_15_extension_key.py::TestServedKey::test_every_wave_keys_into_the_sample_universe[crop_production]`, committed by the previous agent in d0ec4b32f and NEVER green: 2011-12 hid `27010` is in the harvest module and in no welfare/roster file (see Surprises). Left red on purpose -- weakening it is out of scope, and the cell it names is real.

### Claimed vs measured (rebuilt private root; "before" = the pre-bundle snapshot served through the API with `assume_cache_fresh=True`)

| commit | claim | measured | ok |
|---|---|---|---|
| #679 | sample 2014-15 3,558 -> 3,617 households | 3,558 -> 3,617 | yes |
| #679 | cover sheet EXTENSION 0/1/2 = 3,478 / 77 / 62; pair sets {0} 3,421, {1} 75, {0,2} 57, {1,2} 2, {2} 3 | identical | yes |
| #679 | 210 (t,i,pid) groups in household_roster / individual_education -> 0 | 210 -> 0 both | yes |
| #679 | 210 in people_last7days -> 0 | after 0; served rows +210 (26,369 -> 26,579). "Before" not reproducible from the snapshot: its people_last7days and livestock wave parquets carried a stale id format (`'100001'`), i.e. hashless script-path parquets older than the development scripts | partial |
| #679 | 2,006 in assets -> 0 | 2,006 -> 0 | yes |
| #679 | 367 in plot_inputs -> 0 | **not reproduced**: 102 groups / 210 rows -> 39 groups / 84 rows in the built parquet. Raw file on the full key (G, M, E, AS02CQ02 slot, crop, unit): 0 dups (two-column key: 50 keys / 100 rows). The residual 39 are seed-slot merges (seven AS02CQ02 slots -> `'Seed'`), not the id; `GrainCollapseWarning` plot_inputs/2014-15 still fires (35 rows / 29 tuples) | no |
| #679 | 280 in livestock -> 0 | after 0; before not reproducible (stale snapshot parquet, as above) | partial |
| #679 | 46 in crop_production -> 0 | 46 -> 0 | yes |
| #679 | 165 of the 210 roster groups were two populated people | served roster 2014-15 22,506 -> 22,671 (+165) | yes |
| #679 | crosswalk 3,251 -> 3,529 links; no linked value is a key; 88 unlinked; `'20012'` collides with a 2011-12 hid | 3,529 keys `2014-15,*`; 0 values are keys; 3,617-3,529 = 88; `'20012'` present in both waves' served ids | yes (3,251 not re-measured; "guard skipped 5" not re-measured -- needs re-running panel_ids.py) |
| #865 | raw sentinel counts 11/3/2, 23/7/7, 1 | pinned by the existing test; passed | yes |
| #865 | served before: Quantity 999999x11 (2011-12), 9999x23 (2014-15); Quantity_sold 999999x3 / 9999x5; Value_sold 9999999x2 + x5; plot_inputs 9999x1 | identical on the snapshot | yes |
| #865 | after: 0 on every column | 0 | yes |
| #865 | plot_inputs 2011-12 / 2014-15 column max 999 (candidate, not stripped) | max 999 / 999 | yes |
| #865 | a line carrying only a sentinel becomes all-NA and is dropped | 11 crop_production 2011-12 lines (hid 13317 x9, 20607 x2) and 1 plot_inputs 2021-22 line dropped; that is the -11 / -1 in the pins | yes |
| #824 | Unit_sold string column, never `'Unknown'`, NA-bearing | yes; 15 labels; notna rows 1,294 / 564 / 3,226 / 1,425 | yes |
| #824 | no (i, crop) sells in two units, 2014-15 or 2021-22 | 0 / 0 | yes |
| #824 | AS2E2 is one row per (household, crop), 5,226 | 5,226 rows, 5,226 keys | yes |
| #824 | s16d 12,232 rows / 8,691 pairs; 2,633 multi pairs / 6,174 rows; 5,786 exact dups; all 1,320 sale-bearing multi-pair rows dup; 388 distinct lines, none with a sale | identical | yes |
| #824 | multiplicity == number of plots in s16c for 89.6% of multi-row pairs | **65.7%** with a plot = distinct (field, parcel); definition-dependent, the fix does not rest on it | no |
| #824 | 144 single-plot crops served 2-4x | 144 pairs changed; factors 2x 114, 3x 25, 4x 3, **5x 1, 6x 1** | yes (range 2-6) |
| #824 | single-plot Value_sold 78,934,620 -> 73,206,995; Quantity_sold 36,975 -> 32,420.5; 1,409 pairs | exact (per-pair sums; row sums are 79,929,670 -> 73,631,120 because 16 pairs have two harvest-unit rows) | yes |
| #824 | agreement 2011-12 98.7% (n=1,296) / 2014-15 89.4% (559) / 2018-19 92.3% (3,226) / 2021-22 88.7% (1,589) | served, priced = Qs>0 & Vs>0, both known: 98.7% (1,293) / 89.4% (558) / 92.3% (3,226) / **89.0% (1,423)**. n=1,589 matches nothing tried (raw dedup sale rows 1,818: 89.8%; single-plot 1,409: 89.4%) | 3 of 4 |
| #824 | 2021-22 raw-spelling agreement 13.5%, through the `u` table 89.9% (63.0% was an artefact) | 13.4% / 89.8% on the 1,818 de-duplicated sale rows | yes |
| #824 | "Corrected in CONTENTS.org with the rebuild" | the commit touched no CONTENTS.org; done in `62662cfd3` | was false, now done |
| #811 | served Region 16 -> 8 labels | 19 raw spellings (per wave measured); the pre-#811 table applied to them gives 16 distinct labels (measured); new table -> 8 pooled and 8 in every wave | yes |
| #833 | Chou 628 / Choux 973; bundle 1,156 / 'Aubergine,' 452; Sucre 4,883 / 5,462 / 80 | identical before | yes |
| #833 | after: one Chou, one Sucre label, 'Aubergine', bundle kept | Chou 628+973; Sucre 4,883 + **5,537** (5 rows merged at the (t,v,i,j,u,s) grain, hence food_acquired -5); Aubergine 452; bundle 1,156; retired labels 0 rows; crop rows -> 'Aubergine' (3) | yes |
| #833 | code 166: 468 rows, 466 in Sachet, median 1 sachet, median last purchase 150 FCFA; "CONTENTS.org says why" | 468 / 466 / 1.0 / s07bq08 median 150; **CONTENTS.org had no #833 section** -- written in `62662cfd3` | yes / was false |

Served rows per wave, before -> after (all tables that moved): sample 2014-15 3,558 -> 3,617; household_roster 22,506 -> 22,671; individual_education 2,821 -> 2,834; people_last7days 26,369 -> 26,579; assets 17,070 -> 17,267; livestock 4,998 -> 5,018; plot_inputs 2014-15 8,071 -> 8,134 and 2021-22 12,433 -> 12,432; crop_production 2011-12 11,948 -> 11,937 and 2014-15 8,753 -> 8,799; plot_labor 2014-15 15,803 -> 15,935; shocks 2014-15 3,771 -> 3,781; housing 2014-15 3,558 -> 3,617; food_security 2014-15 3,557 -> 3,616; food_acquired 239,882 -> 239,877. Every other (table, wave) serves the same count as before -- measured through the API on all 23 tables, not assumed.

## 3. Cache cost

The bundle itself re-warms all 18 Niger script/YAML tables once (`niger.py`, `categorical_mapping.org`, `data_scheme.yml`, 2014-15 `data_info.yml` are in every Niger hash); that cost was paid here, in the private root only. This follow-up adds **nothing** to it: `CONTENTS.org` is excluded from the hash by design and `tests/` is not a build input. No other country re-warms. The shared root has not been touched.

## 4. Surprises / contradictions

- **`CONTENTS.org` was not corrected by 04c1efbf9** despite its message ("Corrected in CONTENTS.org with the rebuild"); and **d36054864's "CONTENTS.org says why" and "recorded" claims referred to text that did not exist**. Both are a fourth lost artefact and are now written.
- **2011-12 hid 27010** (grappe 270, menage 10): two harvest lines in `ecvmaas2e_p2.dta`, no line in `ecvmamen_p1_en.dta` (3,968 hh) or the roster; the only such id. It is what the `NullIndexKeyWarning` on crop_production's `v` (2 rows, 2011-12) is. Pre-existing, unrelated to #679 (no 2011-12 crosswalk entry; served identically before). The committed test `test_every_wave_keys_into_the_sample_universe[crop_production]` is red on it and was never green. Recorded in CONTENTS.org; not adjudicated (may be a mis-keyed 2701).
- **The "367 -> 0" plot_inputs claim** does not hold on the built table (39 groups remain, seed-slot merges); it holds on the raw file's full key. Quoting CONTENTS.org (#679 section, previous agent): "367 in =plot_inputs= ... every one of them 0 once EXTENSION is in the key" -- true at the raw grain, false for the served grain. Now recorded beside it.
- **The pre-bundle snapshot's people_last7days and livestock 2014-15 wave parquets were stale** (id format `'100001'`, not the development scripts' `'1001'`): hashless script-path parquets that had never been re-run. So two of the commit's "before" collision counts (210, 280) cannot be checked against the snapshot; the "after" 0 is verified.
- Two of the previous agent's secondary numbers are definition-dependent and did not reproduce: 2021-22 agreement "88.7% (n=1,589)" and "multiplicity == plots for 89.6%". The measured values and their definitions are in the tests and CONTENTS.org.
- `test_niger_u_sentinel.py`'s pins were stale against the bundle and the bundle's commits did not say so (the previous rebuild never finished); moved with provenance.

## 5. Decisions taken

1. **Snapshot first**: copied the pre-bundle warm root to `$STAGE/prebundle_Niger` before any `Country('Niger')` call, so every "before" number is measured, not quoted.
2. **Agreement definition pinned**: priced = `Quantity_sold > 0` and `Value_sold > 0`; knows both = `Unit_sold` non-null and `u != 'Unknown'`; compared after the `u` table; on the served table. The commit's un-reproducible 88.7%/1,589 is noted, not pinned.
3. **Sugar 5,537 pinned as-is** with the merge explained (five households, both forms, same unit), rather than 5,542.
4. **Moved the u-sentinel pins** (a file the task did not name) rather than leave five red tests whose causes are the bundle's own stated effects; before/after and cause per number are in the commit message and docstring.
5. **Left `test_every_wave_keys_into_the_sample_universe[crop_production]` red** rather than weaken it or edit the 2011-12 script: the cell is real, the fix is outside the task's paths, and the finding is recorded in CONTENTS.org.
6. **Did not invent a cause** for the 0.1-0.8 point gap between the old 98.8/93.1 and the re-measured 98.7/92.3 agreement figures; CONTENTS.org says the old numbers were a raw-side join predating `Unit_sold` and leaves the gap unadjudicated.
7. **Rewrote the whole sold-unit subsection** of CONTENTS.org (its heading "the SOLD unit is not carried" was false), not only the 63.0% figure the task named.