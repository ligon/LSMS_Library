# Reproduce the bounded Malawi correction

Use the frozen base `f8d236694c26fe9e709c5548f02c70f74ea72c33` and this candidate in separate checkouts. Mount the existing Savio image; no venv build is needed. All data roots below must be new private directories. Pick an existing hash-current validated Uganda comparator cache, not an older shared parquet or another candidate's cache.

For every Python invocation, set `PYTHONPATH` to its checkout and set `LSMS_COUNTRIES_ROOT` to that checkout's `lsms_library/countries`. Run from that checkout. Set `PYTHONDONTWRITEBYTECODE=1`, `PYTHONHASHSEED=1`, `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`, and `LSMS_BUILD_WORKERS=1`. Put the mounted image's `bin` first on PATH for Make's Python children. The actual image used was `/local/job40255479/venv_img/bin/python`.

The runnable scripts live alongside this file. Substitute paths in these commands; do not point `LSMS_DATA_DIR` at the original shared corpus:

```sh
python seed_sources.py --checkout "$CHECKOUT" --shared-data "$EXISTING_DATA" --comparator-data "$VALIDATED_RELEASE_DATA" --out-root "$PRIVATE_RUN"
```

`seed_sources.py` copies only the 28 relevant cached Malawi raw blobs and two Uganda comparator parquets into separate baseline/fixed roots. It asserts the source sidecars and blobs exist and makes physical copies, never L2 hardlinks. Confirm the comparator footer matches `Country('Uganda')._table_cache_hash('plot_inputs', waves)` for the source. No raw data acquisition is part of reproduction.

For each source separately, export the matching checkout/config paths and set `LSMS_DATA_DIR` to its corresponding private baseline/fixed directory. Append this evidence directory's `offline` directory to PYTHONPATH so Python children refuse outbound IP connections. Then:

```sh
python run_cold.py
python check_warm_feature.py
python /path/to/checkout/bench/feature_audit/scan.py --features plot_inputs --countries Malawi Uganda --phases 1 --jobs 1 --out "$PRIVATE_SCAN"
```

`run_cold.py` refuses an existing Malawi country parquet, asserts exact import/config/data identity, and leaves private pickles for exact comparison. Those pickles and raw logs contain survey data and must stay outside tracked evidence.

After both runs, with candidate import identity:

```sh
python compare.py --baseline-data "$BASELINE_DATA" --fixed-data "$FIXED_DATA" --out "$AGGREGATE_COMPARISON"
python -m pytest -q -n 2 --dist=loadfile tests/test_1109_malawi_input_quantity.py tests/test_867_nitrogen_content.py -k 'not ethiopia_warm'
```

The targeted run includes the private warm Malawi nitrogen check. Ethiopia is excluded because it is outside this change's private data seed. The focused plot declaration/harmonization checks were run separately:

```sh
python -m pytest -q tests/test_feature_canonical_index.py::TestPlotTableDeclarations tests/test_feature_canonical_index.py::TestPlotInputsHarmonize tests/test_feature_canonical_index.py::TestPlotTablesEndToEnd
```

The negative control runs the candidate regression file under the base checkout's import/config environment; it produces six failures and five compatibility passes. No fixture or baseline was regenerated.
