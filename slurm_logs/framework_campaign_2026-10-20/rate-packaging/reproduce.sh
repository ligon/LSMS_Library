#!/usr/bin/env bash
set -eu
PACKAGING_REPO=/local/job40255479/worktrees/packaging-rate-532d587
PACKAGING_REPORT=/local/job40255479/control/framework-campaign/rate-packaging
PACKAGING_IMAGE_PY=/local/job40255479/venv_img/bin/python
PACKAGING_BUILD_PY=/global/home/users/ligon/.local/share/pipx/venvs/poetry/bin/python
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
export PIP_CACHE_DIR="$PACKAGING_REPORT/pip-cache" POETRY_CACHE_DIR="$PACKAGING_REPORT/poetry-cache"
export XDG_CACHE_HOME="$PACKAGING_REPORT/xdg-cache" PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
cd "$PACKAGING_REPO"
test "$(git rev-parse HEAD)" = 532d58700fa46e52a0b390198604eca1dc4de00e
# Reuse the completed private backend target read-only; do not install dependencies.
PACKAGING_TOOLS=/local/job40255479/control/framework-campaign/packaging-review/build-tools
test -d "$PACKAGING_TOOLS/poetry_dynamic_versioning"
PYTHONPATH="$PACKAGING_TOOLS" PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 \
 taskset -c 8-11 "$PACKAGING_BUILD_PY" -m build --no-isolation --outdir "$PACKAGING_REPORT/dist"
taskset -c 8 "$PACKAGING_IMAGE_PY" "$PACKAGING_REPORT/inspect_archives.py"
# Installation target must be new/private; the completed run used installed-target.
PACKAGING_WHEEL=$(find "$PACKAGING_REPORT/dist" -maxdepth 1 -name '*.whl' -print)
test -f "$PACKAGING_WHEEL"
"$PACKAGING_IMAGE_PY" -m pip install --no-index --no-deps --no-compile --target "$PACKAGING_REPORT/installed-target" "$PACKAGING_WHEEL"
cd "$PACKAGING_REPORT/installed-cwd"
env -u LSMS_COUNTRIES_ROOT PYTHONPATH="$PACKAGING_REPORT/installed-target" \
 PACKAGING_TARGET="$PACKAGING_REPORT/installed-target" \
 LSMS_DATA_DIR="$PACKAGING_REPORT/installed-smoke-data" LSMS_SKIP_AUTH=1 \
 PYTHONDONTWRITEBYTECODE=1 taskset -c 8-11 "$PACKAGING_IMAGE_PY" \
 "$PACKAGING_REPORT/installed_smoke.py" "$PACKAGING_REPORT/installed-smoke.json"
