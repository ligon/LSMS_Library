#!/bin/bash
set -eu
feature_tree=/local/job39919092/release-work/runtime/corpus
cd "$feature_tree"
export PYTHONPATH="$feature_tree"
export LSMS_COUNTRIES_ROOT="$feature_tree/lsms_library/countries"
export LSMS_DATA_DIR=/local/job39919092/release-work/runtime/data-feature-audit
export LSMS_SKIP_AUTH=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1
export PATH="/local/job39919092/venv_img/bin:$PATH"
unset PYTHONSAFEPATH LSMS_NO_CACHE LSMS_BUILD_BACKEND
taskset -c 0-7 /local/job39919092/venv_img/bin/python -c 'import lsms_library as ll, os; from pathlib import Path; from lsms_library.paths import countries_root; from lsms_library.local_tools import data_root; assert Path(ll.__file__).resolve() == Path(os.environ["PYTHONPATH"])/"lsms_library/__init__.py"; assert countries_root().resolve() == Path(os.environ["LSMS_COUNTRIES_ROOT"]); assert data_root().resolve() == Path(os.environ["LSMS_DATA_DIR"]); print(ll.__file__, countries_root(), data_root())'
exec taskset -c 0-7 /local/job39919092/venv_img/bin/python bench/feature_audit/scan.py --features all --phases 1,2 --jobs 1 --include-no-microdata --out /local/job39919092/release-work/control/runtime/feature-audit/results.jsonl
