#!/bin/bash
# Usage: job.sh <dev|br> <seed> <workers> <country> <table> [suffix]
# Cold build in a fresh root named <tag>-s<seed>-w<workers>-<country>; a second
# call with the same args re-reads warm (output name gets the suffix).
S=/local/job40390255/tmp/claude-43292/-local-job40390255-mirrors-LSMS-Library/aad69bbf-39c1-40e1-bf95-cbd7d49b5cce/scratchpad/1063
TAG=$1; SEED=$2; W=$3; C=$4; T=$5; SUF=${6:-}
case $TAG in dev) CO=/local/job40390255/worktrees/base-1240;; br) CO=/local/job40390255/worktrees/1063;; esac
ROOT=v-$TAG-s$SEED-w$W-$C
OUT=v_${TAG}_s${SEED}_w${W}_${C}_${T}${SUF}
PYTHONHASHSEED=$SEED LSMS_BUILD_WORKERS=$W LSMS_MAKE_JOBS=1 $S/run.sh $CO $ROOT $C $OUT $T > $S/out/$OUT.log 2>&1
echo "$OUT rc=$? $(tail -n 1 $S/out/$OUT.log)"
