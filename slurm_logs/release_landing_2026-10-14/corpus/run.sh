#!/bin/bash
set -eu
corpus_tree=/local/job39919092/release-work/runtime/corpus
cd "$corpus_tree"
export PYTHONPATH="$corpus_tree"
export LSMS_COUNTRIES_ROOT="$corpus_tree/lsms_library/countries"
export LSMS_DATA_DIR=/local/job39919092/release-work/runtime/data-corpus
export LSMS_SKIP_AUTH=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1
export PATH="/local/job39919092/venv_img/bin:$PATH"
unset PYTHONSAFEPATH LSMS_NO_CACHE LSMS_BUILD_BACKEND
exec taskset -c 0-7 /local/job39919092/venv_img/bin/python /local/job39919092/release-work/control/runtime/corpus/run_corpus.py "$@"
