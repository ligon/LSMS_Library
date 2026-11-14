#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 3:00:00
#SBATCH --exclude=n0035.savio4
#SBATCH -J sc-validate
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections/logs/validate-%j.out
# Cold base-vs-candidate builds in separate private roots, inputs probe, compare,
# hash footprint.  CAND_SHA (full) is cloned from the mirror; CAND_PATCH, if set,
# is applied on top of it (smoke runs before committing).  TAG names the run.
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections
SCR=$OUT/scripts; LOG=$OUT/logs
TAG=${TAG:?}; RES=$OUT/results/$TAG; mkdir -p $RES
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
CAND=${CAND_SHA:?}
J=/local/job$SLURM_JOB_ID
FR=$J/frames; mkdir -p $FR $J/tmp
export TMPDIR=$J/tmp
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_DATA_DIR
echo "node $(hostname) start $(date -Is) TAG=$TAG CAND=$CAND PATCH=${CAND_PATCH:-none}"
for arm in base cand; do
  sha=$BASE; [ $arm = cand ] && sha=$CAND
  git clone -q --no-hardlinks $MIRROR $J/$arm || exit 2
  git -C $J/$arm checkout -q --detach $sha || exit 2
  echo "$arm HEAD $(git -C $J/$arm rev-parse HEAD)"
done
if [ -n "${CAND_PATCH:-}" ]; then
  git -C $J/cand apply --whitespace=nowarn $CAND_PATCH || { echo "patch failed"; exit 2; }
  git -C $J/cand status --porcelain | head -20
fi
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
[ -x $PY ] || PY=$J/cand/.venv/bin/python

envp() {  # envp <arm> <subroot> cmd...
  local arm=$1 P=$J/$2; shift 2; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$arm "$@"
}

# A. hash footprint (config / sidecar / source hashes only)
for arm in base cand; do
  ( cd $J/$arm && envp $arm priv-hash-$arm $PY $SCR/hashprobe.py $J/$arm $arm $RES/hash_$arm.json > $LOG/${TAG}_hash_$arm.log 2>&1 )
  echo "hash $arm rc=$?"
done
$PY $SCR/hashcompare.py $RES/hash_base.json $RES/hash_cand.json > $RES/hash_compare.json 2> $LOG/${TAG}_hashcompare.err
echo "hashcompare rc=$?"

# B. cold builds: each (arm, country) in its own private root, 4 at a time
run_build() {  # arm country
  ( cd $J/$1 && envp $1 priv-$1-$2 $PY $SCR/build.py $J/$1 $1 $FR $2 > $LOG/${TAG}_build_$1_$2.log 2>&1; echo "build $1 $2 rc=$?" )
}
for C in Mali Senegal Niger Uganda; do run_build base $C & done; wait
for C in Mali Senegal Niger Uganda; do run_build cand $C & done; wait
echo "builds done $(date -Is)"
cp $FR/*.build.json $RES/ 2>/dev/null

# B2. the Uganda sample parquet fingerprint (tests/fixtures/uganda_baseline.json)
for arm in base cand; do
  ( cd $J/$arm && envp $arm priv-$arm-Uganda $PY $SCR/uganda_fp.py $J/$arm $RES/uganda_fp_$arm.json > $LOG/${TAG}_ufp_$arm.log 2>&1 ); echo "uganda fp $arm rc=$?"
done

# C. inputs probe (candidate, warm roots per country share nothing; use a fresh root)
( cd $J/cand && envp cand priv-probe $PY $SCR/probe.py $J/cand cand $FR > $LOG/${TAG}_probe.log 2>&1 ); echo "probe rc=$?"
cp $FR/cand.probe.json $RES/ 2>/dev/null
$PY $SCR/compare.py $FR $RES/compare.json base cand > $LOG/${TAG}_compare.log 2>&1; echo "compare rc=$?"

for arm in base cand; do
  echo "in-tree parquets $arm: $(find $J/$arm/lsms_library/countries -name '*.parquet' | wc -l)"
done
for P in $J/priv-*; do echo "issues $(basename $P): $(wc -l < $P/issues.log 2>/dev/null || echo 0)"; done
du -sh $J/priv-* $FR 2>/dev/null
echo "VALIDATE DONE $(date -Is)"
