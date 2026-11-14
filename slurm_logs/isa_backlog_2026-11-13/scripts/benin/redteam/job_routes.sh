#!/bin/bash
#SBATCH -J rt-benin-routes
# Routes the worker's measurements did not reach, each in its own private EMPTY root:
#   subset-first calls (cand, base); LSMS_BUILD_WORKERS=2 (cand, base);
#   LSMS_BUILD_BACKEND=make (cand, base).
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh benin-redteam $CAND $BASE || exit 1
O=$OUT/routes; mkdir -p $O
JL=/local/job$SLURM_JOB_ID
run() {  # run <name> <src> <mode> [env...]
  local name=$1 src=$2 mode=$3; shift 3
  local d=$JL/private/data-$name; mkdir -p $d $O/$name
  ident "$src" "$d" > $O/$name/ident.txt 2>&1
  ( cd $src && env PYTHONPATH=$src LSMS_DATA_DIR=$d "$@" $PY $RT/scripts/rt_build.py $mode $name $O/$name > $O/$name/log.txt 2>&1; echo "exit $?" >> $O/$name/log.txt )
}
run subset_cand $SRC_1 subset &
run subset_base $SRC_2 subset &
run workers2_cand $SRC_1 script7 LSMS_BUILD_WORKERS=2 &
run workers2_base $SRC_2 script7 LSMS_BUILD_WORKERS=2 &
wait
run makeb_cand $SRC_1 script7 LSMS_BUILD_BACKEND=make &
run makeb_base $SRC_2 script7 LSMS_BUILD_BACKEND=make &
wait
for s in 1 2; do v=SRC_$s; ( cd ${!v} && git status --short > $O/git_status_$s.txt; find lsms_library/countries -name '*.parquet' > $O/intree_parquets_$s.txt ); done
echo ALLDONE > $O/ALLDONE
