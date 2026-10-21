#!/usr/bin/env bash
set -eu
PACKAGING_REPO=/local/job40255479/worktrees/packaging-4cab5f0
PACKAGING_REPORT=/local/job40255479/control/framework-campaign/packaging-review
PACKAGING_IMAGE_PY=/local/job40255479/venv_img/bin/python
PACKAGING_BUILD_PY=/global/home/users/ligon/.local/share/pipx/venvs/poetry/bin/python
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
export PIP_CACHE_DIR="$PACKAGING_REPORT/pip-cache" POETRY_CACHE_DIR="$PACKAGING_REPORT/poetry-cache"
export XDG_CACHE_HOME="$PACKAGING_REPORT/xdg-cache" PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
cd "$PACKAGING_REPO"
test "$(git rev-parse HEAD)" = 4cab5f0db7d375ad543e652f61f43bd28fa94ecd
# The first run used this backend constraint and resolved1.10.1; pin it to reproduce.
# Install only into this private target if it has not already been populated.
if test ! -d "$PACKAGING_REPORT/build-tools/poetry_dynamic_versioning"; then
  "$PACKAGING_IMAGE_PY" -m pip install --target "$PACKAGING_REPORT/build-tools" poetry-dynamic-versioning==1.10.1
fi
PYTHONPATH="$PACKAGING_REPORT/build-tools" PYTHONNOUSERSITE=1 \
 taskset -c 12-15 "$PACKAGING_BUILD_PY" -m build --no-isolation --outdir "$PACKAGING_REPORT/dist"
taskset -c 12 "$PACKAGING_IMAGE_PY" "$PACKAGING_REPORT/inspect_archives.py"
# Installation target must be new/private; the completed run used installed-target.
"$PACKAGING_IMAGE_PY" -m pip install --no-deps --no-compile --target "$PACKAGING_REPORT/reproduced-target" \
 "$PACKAGING_REPORT/dist/lsms_library-0.15.0.post392.dev0+4cab5f0db-py3-none-any.whl"
cd "$PACKAGING_REPORT/installed-cwd"
env -u LSMS_COUNTRIES_ROOT PYTHONPATH="$PACKAGING_REPORT/reproduced-target" \
 PACKAGING_TARGET="$PACKAGING_REPORT/reproduced-target" \
 LSMS_DATA_DIR="$PACKAGING_REPORT/reproduced-smoke-data" LSMS_SKIP_AUTH=1 \
 PYTHONDONTWRITEBYTECODE=1 taskset -c 12-15 "$PACKAGING_IMAGE_PY" \
 "$PACKAGING_REPORT/installed_smoke.py" "$PACKAGING_REPORT/reproduced-smoke.json"
