#!/bin/bash
# warm re-read of the branch seed-1 cold root, in a process with seed 3
S=/local/job40390255/tmp/claude-43292/-local-job40390255-mirrors-LSMS-Library/aad69bbf-39c1-40e1-bf95-cbd7d49b5cce/scratchpad/1063
C=$1; T=$2
OUT=v_br_s1_w1_${C}_${T}_warm
PYTHONHASHSEED=3 LSMS_BUILD_WORKERS=1 $S/run.sh /local/job40390255/worktrees/1063 v-br-s1-w1-$C $C $OUT $T > $S/out/$OUT.log 2>&1
echo "$OUT rc=$? $(tail -n 1 $S/out/$OUT.log)"
