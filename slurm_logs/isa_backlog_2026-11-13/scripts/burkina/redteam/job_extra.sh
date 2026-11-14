#!/bin/bash
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina-redteam
source $B/tools/burst-env.sh burkina-redteam "$1" "$2" || exit 9
J=$SLURM_JOB_ID
FRC=$JOBLOCAL/x-cand; FRB=$JOBLOCAL/x-base; mkdir -p $FRC $FRB
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/extra.py cand $FRC $OUT/extra-cand.json > $S/logs/extra-cand-$J.log 2>&1 ) &
P1=$!
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" $S/scripts/extra.py base $FRB $OUT/extra-base.json > $S/logs/extra-base-$J.log 2>&1 ) &
P2=$!
wait $P1; echo "cand $?"; wait $P2; echo "base $?"
cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" - $FRB $FRC $OUT/extra-diff.json > $S/logs/extra-diff-$J.log 2>&1 <<'PYEOF'
import json, sys
import numpy as np, pandas as pd
b, c, o = sys.argv[1:4]
res = {}
for n in ('gcr', 'cdiv', 'mpv'):
    try:
        x, y = pd.read_parquet(f'{b}/{n}.parquet'), pd.read_parquet(f'{c}/{n}.parquet')
        y18 = y[y['t'].astype(str) == '2018-19'][[k for k in x.columns]]
        cols = list(x.columns)
        try:
            pd.testing.assert_frame_equal(x.sort_values(cols, na_position='last').reset_index(drop=True),
                                          y18.sort_values(cols, na_position='last').reset_index(drop=True))
            eq = True
        except Exception as e:
            eq = f'{type(e).__name__}: {str(e)[:400]}'
        res[n] = {'rows_base': len(x), 'rows_cand': len(y), 'rows_cand_2018': len(y18),
                  'cand_cols_extra': sorted(set(y.columns) - set(x.columns)), 'equal_2018_slice': eq}
    except Exception as e:
        res[n] = f'ERR {e}'
json.dump(res, open(o, 'w'), indent=1, default=str)
print(res)
PYEOF
echo "diff $?"
