#!/bin/bash
#SBATCH -J benin-extra
# usage: job_extra.sh <label> <sha> -- data-free suite, plus (cand) member control and make backend
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
LABEL=$1; SHA=$2
source $BATCH/tools/burst-env.sh benin $SHA || exit 1
O=$OUT/extra-$LABEL-${SHA:0:12}; mkdir -p $O
cd "$SRC_1"; export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
if [ "$LABEL" = cand ]; then
  "$PY" $BATCH/reports/benin/scripts/explore3.py > $O/explore3.txt 2>&1; echo "exit $?" >> $O/explore3.txt
  mkdir -p $DATA_1-make
  LSMS_BUILD_BACKEND=make LSMS_DATA_DIR=$DATA_1-make "$PY" $BATCH/reports/benin/scripts/make_backend.py $O/make_backend > $O/make_backend.txt 2>&1; echo "exit $?" >> $O/make_backend.txt
  LSMS_DATA_DIR=$DATA_1-default "$PY" - $O/make_backend <<'PYEOF' >> $O/make_backend.txt 2>&1
import pickle, sys, warnings
from pathlib import Path
import pandas as pd
warnings.simplefilter('ignore')
import lsms_library as ll
o = Path(sys.argv[1]); c = ll.Country('Benin')
for t in ('sample', 'plot_inputs', 'household_roster'):
    p = o / f'{t}.pkl'
    if not p.exists():
        print(t, 'no make-backend frame'); continue
    a = pickle.load(open(p, 'rb')); b = getattr(c, t)()
    try:
        pd.testing.assert_frame_equal(a.sort_index(), b.sort_index(), check_like=True); print(t, 'make == default')
    except AssertionError as e:
        print(t, 'DIFF', str(e)[:600])
PYEOF
  echo "exit $?" >> $O/make_backend.txt
fi
"$PY" -m pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -p no:cacheprovider -q -rfE -m "not requires_s3" tests/ > $O/pytest-datafree.txt 2>&1; echo "exit $?" >> $O/pytest-datafree.txt
find lsms_library/countries -name '*.parquet' > $O/intree_parquets.txt; git status --short > $O/git_status.txt
