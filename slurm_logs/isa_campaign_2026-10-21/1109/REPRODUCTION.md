# Reproduce the source-backed Malawi amendment

Amended 2026-10-21 after independent source review withdrew the no-quantity premise.
Use frozen `c08abb3f1dd03fd211d5071b45369dcbae42857f` and the final Malawi candidate in separate checkouts.
The original f8/c08 JSONs remain historical evidence; current results are prefixed `source-amended-`.
Raw logs and row-level frames stay in private data roots.

Every run uses the mounted image, exact checkout/config identity, private writable data, offline guard, single BLAS/build thread and at most four loadfile test workers.
For example, from the candidate checkout:

```sh
export CHECKOUT=/local/job40255479/worktrees/isa-Malawi
export EVIDENCE="$CHECKOUT/slurm_logs/isa_campaign_2026-10-21/1109"
export PRIVATE_RUN=/absolute/new/private/directory
export PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=1
export PYTHONPATH="$CHECKOUT:$EVIDENCE/offline"
export LSMS_COUNTRIES_ROOT="$CHECKOUT/lsms_library/countries"
export LSMS_DATA_DIR="$PRIVATE_RUN/seed-context"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1 AWS_EC2_METADATA_DISABLED=true
export PATH=/local/job40255479/venv_img/bin:$PATH
export PY=/local/job40255479/venv_img/bin/python
cd "$CHECKOUT"
"$PY" "$EVIDENCE/seed_sources.py" --checkout "$CHECKOUT" \
  --shared-data /absolute/existing/cached/data \
  --comparator-data /absolute/validated/current/release/data \
  --out-root "$PRIVATE_RUN"
```

The script copies 28 relevant Malawi L1 blobs and only Uganda plot_inputs/sample L2 into physically separate baseline/fixed roots.
It verifies comparator footer hashes against the imported source before copying.
No acquisition or DVC CLI is authorized if a blob is absent.
For the baseline, switch cwd, PYTHONPATH and LSMS_COUNTRIES_ROOT to its frozen checkout, and set LSMS_DATA_DIR to the new private baseline root.
For the candidate, use its matching checkout and private fixed root.
For each run separately:

```sh
"$PY" "$EVIDENCE/run_cold.py"
"$PY" "$EVIDENCE/check_warm_feature.py"
"$PY" bench/feature_audit/scan.py --features plot_inputs \
  --countries Malawi Uganda --phases 1 --jobs 1 --out "$LSMS_DATA_DIR/scan.jsonl"
```

The actual final run used private `control/isa-backlog-20261021/Malawi/source-amendment/final-seeded` and compared with the independent review's frozen-c08 cold/warm artifacts in `control/isa-backlog-20261021/review-1109/fixed`.
The six-source accounting reads existing private L1 directly with pyreadstat, independently checks all seven headers and the organic unit dictionary, and compares every amount/unit at wave and finalized public keys.
It initializes the existing panel-ID crosswalk exactly as public loading does; raw Stata/parquet string NA representations are normalized to identical explicit types before exact value comparison.
It also asserts the six source-qualified unlabelled codes receive no inferred kg factor.
With candidate identity and its private warm root:

```sh
"$PY" "$EVIDENCE/source_compare.py" --manifest "$PRIVATE_RUN/seed-manifest.json" \
  --out "$PRIVATE_RUN/source-accounting.json"
"$PY" "$EVIDENCE/compare.py" --baseline-data "$PRIVATE_RUN/baseline" \
  --fixed-data "$PRIVATE_RUN/fixed" --out "$PRIVATE_RUN/comparison.json"
"$PY" "$EVIDENCE/compare_feature.py" --baseline-data "$PRIVATE_RUN/baseline" \
  --fixed-data "$PRIVATE_RUN/fixed" --baseline-scan "$PRIVATE_RUN/baseline/scan.jsonl" \
  --fixed-scan "$PRIVATE_RUN/fixed/scan.jsonl" --out "$PRIVATE_RUN/feature-comparison.json"
"$PY" -m pytest -q -n 2 --dist=loadfile \
  tests/test_1109_malawi_input_quantity.py tests/test_867_nitrogen_content.py \
  tests/test_feature_canonical_index.py::TestPlotTableDeclarations \
  tests/test_feature_canonical_index.py::TestPlotInputsHarmonize -k 'not ethiopia_warm'
```

For the original negative control, run the candidate regression file with c08 source/config identity before changing production code: 15 failures and 12 passes.
A second actual-Country regression fails the initial literal-numeric fallback because the food u aliases change bare 1/3 to physical units; it passes with the approved source-qualified labels.
The final amended regression has 28 cases.
No test fixture, scientific baseline or dependency was regenerated.

Sue
