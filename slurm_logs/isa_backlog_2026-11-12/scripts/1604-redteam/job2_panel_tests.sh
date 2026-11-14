#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 3:00:00
#SBATCH -J rt1604-panel
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-redteam/logs/job2-%j.out
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-redteam
SCR=$OUT/scripts
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
CAND=7735489cceb0fd68acf273eff850a5974d71cc58
J=/local/job$SLURM_JOB_ID
mkdir -p $OUT/results $OUT/logs
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND

git clone -q --no-hardlinks $MIRROR $J/cand || exit 2
git -C $J/cand checkout -q --detach $CAND || exit 2
echo "cand HEAD $(git -C $J/cand rev-parse HEAD)"
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python

envp() {  # envp <subroot> cmd...
  P=$J/$1; shift; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/cand "$@"
}

# 1. guard vs plain on the real frame, and against the committed JSON
( cd $J/cand && envp priv-panel $PY $SCR/panel_check.py $J/cand $OUT/results/panel_check.json > $OUT/logs/panel_check.log 2>&1 ); echo "panel_check rc=$?"

# 2. make panel-ids regenerates the committed JSON byte-identically?
( cd $J/cand/lsms_library/countries/Malawi/_ && envp priv-panel bash -c "PATH=$J/venv_img/bin:\$PATH; python -c 'import lsms_library,os; print(os.path.realpath(lsms_library.__file__))'; make panel-ids" > $OUT/logs/make_panel_ids.log 2>&1 ); echo "make panel-ids rc=$?"
git -C $J/cand status --porcelain > $OUT/results/make_panel_ids_status.txt
git -C $J/cand diff --stat -- lsms_library/countries/Malawi/_/panel_ids.json lsms_library/countries/Malawi/_/updated_ids.json > $OUT/results/make_panel_ids_diff.txt
git -C $J/cand diff --exit-code --quiet -- lsms_library/countries/Malawi/_/panel_ids.json lsms_library/countries/Malawi/_/updated_ids.json; echo "panel-ids json diff exit=$?" | tee -a $OUT/results/make_panel_ids_diff.txt
sha256sum $J/cand/lsms_library/countries/Malawi/_/panel_ids.json $J/cand/lsms_library/countries/Malawi/_/updated_ids.json >> $OUT/results/make_panel_ids_diff.txt
git -C $J/cand checkout -q -- lsms_library/countries/Malawi/_/panel_ids.json lsms_library/countries/Malawi/_/updated_ids.json

# 3. which branch is reached, default vs make backend (fresh roots)
( cd $J/cand && envp priv-branch-default $PY $SCR/branch_probe.py $J/cand default $OUT/results/branch_default.json > $OUT/logs/branch_default.log 2>&1 ); echo "branch default rc=$?"
( cd $J/cand && envp priv-branch-make env LSMS_BUILD_BACKEND=make $PY $SCR/branch_probe.py $J/cand make $OUT/results/branch_make.json > $OUT/logs/branch_make.log 2>&1 ); echo "branch make rc=$?"

# 4. targeted pytest
( cd $J/cand && envp priv-pytest $PY -m pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -p no:cacheprovider \
    tests/test_malawi_panel_id_guard.py tests/test_malawi_food_borrowing.py tests/test_gh970_no_json_make.py \
    tests/test_gh1190_selected_farm_carrier.py tests/test_recall_record.py tests/test_panel_ids_persistence.py \
    tests/test_id_walk.py tests/test_derivations.py tests/test_categorical_mapping_resolution.py \
    tests/test_schema_consistency.py tests/test_build_transform_hash.py tests/test_failure_is_not_absence.py \
    tests/test_gh1278_malawi_cs19_namespace.py tests/test_gh1228_malawi_cowpea_label.py tests/test_gh1210_invalid_food_waves.py \
    > $OUT/results/pytest_targeted.txt 2>&1 ); echo "pytest targeted rc=$?"
( cd $J/cand && envp priv-pytest $PY -m pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -p no:cacheprovider \
    tests/test_sample.py -k Malawi > $OUT/results/pytest_sample_malawi.txt 2>&1 ); echo "pytest sample rc=$?"

echo "in-tree parquets cand: $(find $J/cand/lsms_library/countries -name '*.parquet' | wc -l)"
git -C $J/cand status --porcelain | head -20
echo JOB2 DONE
