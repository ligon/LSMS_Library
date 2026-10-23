# GH #1059 -- Burkina Faso `cluster_features.District` spellings: evidence

Base: development `9a0ed5f3c` (detached worktree `base-1059`).
Branch: `fix/1059-burkina-district-spellings-20261023`, fix commit `595340db5`.
Every run: `/local/job40390255/venv_img/bin/python`, `PYTHONPATH=<checkout>`,
cwd = that checkout, a fresh `LSMS_DATA_DIR` (only a `dvc-cache` symlink, so
every L2 tier is cold), `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
LSMS_BUILD_WORKERS=1`, `nice -n 19`.  Each script asserts that
`lsms_library` is imported from the checkout under test.  Row-level frames go
to a scratch dir and are not committed; the files here are aggregates.

| file | what |
|---|---|
| `probe.py` | cold `Country('Burkina_Faso').cluster_features()`, `sample()`, `_market_lookup('District')` -> `probe-{base,branch}.json` (per-wave/pooled District counts, shared-grappe residue) |
| `compare.py` | base vs branch rows -> `compare.json`: only `District` moves, and exactly as the table maps it; `sample` and the `market='District'` lookup |
| `valuation_probe.py` | `food_expenditures(basis='total', valuation='median_price')` (District is a price-ladder rung) -> `valuation-{base,branch}.json`; frame-equal (checked on the scratch parquets) |
| `disagreements.py` | the 3 shared grappes naming a different province: cover-page household counts and `grappe_EHCVM1` linkage -> `disagreements.json` |
| `hash_impact.json` | `1218-current-validation/hash_census.py` on base and branch, diffed |

```sh
python probe.py <checkout> <base|branch> <scratch>/rows probe-<label>.json
python compare.py <scratch>/rows <branch>/lsms_library/countries/Burkina_Faso/_/categorical_mapping.org compare.json
python valuation_probe.py <checkout> <label> <scratch>/val/val-<label>.json
python disagreements.py <branch> disagreements.json
python .../1218-current-validation/hash_census.py <checkout> hash-<label>.json
```

`probe-branch.json` is the cold run at `595340db5` (`tracked_changes: []`).
`valuation-branch.json` ran on the branch working tree before the commit,
with `categorical_mapping.org` byte-identical to `595340db5`.

## Results

- District distinct per wave 45 / 45 / 44 before and after; pooled 96 -> 45.
  2021-22's 44: no 2021-22 grappe is coded Kompienga.
- 954 of 2,035 rows relabelled (2014 900, 2018-19 25, 2021-22 29); index,
  Region, Rural, Latitude, Longitude frame-equal; `sample` frame-equal;
  `market='District'` lookup 96 -> 45 labels, row for row the mapped base.
- Shared 2018-19/2021-22 grappes: 550; District differs 53 -> 3:
  19 Séno/Yagha, 365 Poni/Noumbiel, 549 Kompienga/Gourma (12 / 6
  households; all 6 tracked from the same grappe).
- 2014 `GrainCollapseWarning` (#1060) text md5-identical base vs branch.
- `food_expenditures(basis='total', valuation='median_price')`: 435,085 rows,
  frame-equal, valuation-source counts identical.
- Hashes: `Country._table_cache_hash` 24 of 549 moved (all 24 Burkina_Faso),
  `Wave._input_hash` 72 of 2,109 (all 72 Burkina_Faso),
  `build_transforms_fingerprint` 0 of 40.
- `tests/test_burkina_faso_district_labels.py`: development 6 failed,
  5 errors, 1 passed; branch 12 passed.  With
  `test_categorical_mapping_resolution.py`, `test_burkina_faso_cluster_gps.py`,
  `test_categorical_mapping_empty_table.py`,
  `test_categorical_mapping_value_required.py`: development 90 passed,
  1 skipped; branch 102 passed, 1 skipped (the skip is the
  `LSMS_EXPECTED_ROOT` identity test, which passes when set: 43 passed).
