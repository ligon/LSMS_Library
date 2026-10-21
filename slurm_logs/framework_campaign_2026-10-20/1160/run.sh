#!/bin/sh
# Run from this checkout. Output goes only into the supplied private directory.
set -eu
: "${LSMS_DATA_DIR:?set a new private /local data root}"
: "${REVIEW_OUTPUT:?set a private /local output directory}"
export PYTHONPATH="$PWD"
export LSMS_COUNTRIES_ROOT="$PWD/lsms_library/countries"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1
mkdir -p "$LSMS_DATA_DIR" "$REVIEW_OUTPUT"
PY=/local/job40255479/venv_img/bin/python
taskset -c 8-11 "$PY" slurm_logs/framework_campaign_2026-10-20/1160/reproduce.py > "$REVIEW_OUTPUT/probe.json"
taskset -c 8-11 "$PY" -m pytest -n 4 --dist=loadfile -q -m 'not requires_s3' \
 tests/test_gh1160_pooled_fertilizer_rate.py tests/test_gh1159_pooled_reducers.py \
 tests/test_gh1093_declared_unit_consumers.py tests/test_867_nitrogen_content.py \
 tests/test_plot_id_canonical.py tests/test_863_transforms.py \
 tests/test_crop_kg_factor.py tests/test_nigeria_sold_unit.py \
 --junitxml="$REVIEW_OUTPUT/focused.xml" > "$REVIEW_OUTPUT/focused.log" 2>&1
