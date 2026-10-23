#!/bin/bash
#SBATCH -A co_carleton -p savio4_htc -q carleton_htc4_normal
#SBATCH -N 1 -n 1 -c 24 --mem=160G -t 08:00:00
#SBATCH -J lsms-retro-scan
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/retro-scan-20261022/slurm-%j.out
# Retro regression net for PRs #1221/#1222/#1223 (Sue, 2026-10-22):
# bench/feature_audit/scan.py --features canonical, cold (fresh private data
# roots, shared L1 blobs only), on base development 60aa81e9e and on current
# development 9a0ed5f3c, side by side.
OUT=/global/scratch/fsa/fc_jevons/ligon/retro-scan-20261022
L=/local/job$SLURM_JOB_ID
M=/global/home/users/ligon/mirrors/LSMS_Library
L1=/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache
declare -A SHA=([base]=60aa81e9eda3b747e25a37c9e7354315b842b408 [cand]=9a0ed5f3cc81b5d2ce8d1fdffba4f477e33b9f7f)
mkdir -p $L
for n in base cand; do
  git clone -q --no-checkout $M $L/$n && git -C $L/$n checkout -q --detach ${SHA[$n]} || { echo "clone $n failed"; exit 2; }
  [ "$(git -C $L/$n rev-parse HEAD)" = "${SHA[$n]}" ] || { echo "sha mismatch $n"; exit 2; }
  mkdir -p $L/data-$n && ln -s $L1 $L/data-$n/dvc-cache
done
(cd $L/cand && bash bin/savio_venv.sh mount) > $OUT/venv-mount.log 2>&1 || { echo "venv mount failed"; cat $OUT/venv-mount.log; exit 3; }
PY=$L/venv_img/bin/python
run() {
  n=$1; cd $L/$n
  E="PYTHONPATH=$L/$n LSMS_DATA_DIR=$L/data-$n LSMS_ISSUES_LOG=$L/data-$n/issues.log OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1"
  env $E $PY -c "import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith('$L/$n/'), lsms_library.__file__; print('import ok', lsms_library.__file__, '${SHA[$n]}')" > $OUT/$n.log 2>&1 || return 4
  start=$(date +%s)
  env $E $PY bench/feature_audit/scan.py --features canonical --jobs 10 --build-timeout 3600 \
      --out $OUT/results-$n.jsonl >> $OUT/$n.log 2>&1
  echo "exit $? elapsed $(( $(date +%s) - start ))s" >> $OUT/$n.log
  echo "in-tree parquets: $(find lsms_library/countries -name '*.parquet' | wc -l)" >> $OUT/$n.log
}
run base & run cand & wait
echo done > $OUT/DONE
