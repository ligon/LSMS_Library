#!/bin/bash
#SBATCH -J mqrt-pf
set -uo pipefail
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant-redteam
CAND=fc40960d27b800045984735a38c68cf91c9b2194
BASE=f138a3d61c203f892148592675a7ef72e160a322
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant-redteam $CAND $BASE || exit 1
J=/local/job$SLURM_JOB_ID; O=$OUT/pf; mkdir -p $O
mk() { # mk <label> <sha> <broken 0|1>
  local d=$J/clone-$1; git clone --quiet --shared --no-checkout "$MIRROR" "$d" && git -C "$d" checkout --quiet --detach "$2"
  $PY $RT/scripts/rt_instrument.py "$d"
  if [ "$3" = 1 ]; then printf 'raise SystemExit("RT injected: Senegal 2021-22 plot_features wave script fails")\n' > "$d/lsms_library/countries/Senegal/2021-22/_/plot_features.py"; fi
  ln -sfn $SRC_1/.venv "$d/.venv"
  echo "$d"
}
run() { # run <clone> <label> <backend default|make>
  local d=$1 L=$2 be=$3 R=$J/root-$2-$3; mkdir -p $R
  ident "$d" "$R" >/dev/null || { echo "ident fail $d"; return 6; }
  ( cd "$d"; if [ $be = make ]; then export LSMS_BUILD_BACKEND=make; fi
    RT_MARKER=$O/marker-$L-$be.jsonl PYTHONPATH=$d LSMS_DATA_DIR=$R timeout 2400 $PY $RT/scripts/rt_pf_call.py $O/$L-$be.pkl $O/$L-$be.json ) > $O/$L-$be.log 2>&1
  echo "$L $be exit $?"; tail -1 $O/$L-$be.log
  find $R/Senegal -name 'plot_features.parquet' | sed "s#$R##" | sort > $O/$L-$be.parquets.txt
}
HC=$(mk healthy-cand $CAND 0 | tail -1); HB=$(mk healthy-base $BASE 0 | tail -1)
BC=$(mk broken-cand $CAND 1 | tail -1); BB=$(mk broken-base $BASE 1 | tail -1)
run $HC healthy-cand default & run $HB healthy-base default & run $HC healthy-cand make & run $HB healthy-base make & wait
run $BC broken-cand default & run $BB broken-base default & run $BC broken-cand make & run $BB broken-base make & wait
# Direct runs on copies of the healthy default roots
for L in cand base; do
  if [ $L = cand ]; then d=$HC; else d=$HB; fi
  for variant in varonly wave2122; do
    R2=$J/direct-$L-$variant; rm -rf $R2; mkdir -p $R2; cp -a $J/root-healthy-$L-default/Senegal $R2/
    rm -f $R2/Senegal/var/plot_features.parquet
    if [ $variant = wave2122 ]; then rm -f $R2/Senegal/2021-22/_/plot_features.parquet; fi
    ( cd $d/lsms_library/countries/Senegal/_ && RT_MARKER=$O/marker-direct-$L-$variant.jsonl PYTHONPATH=$d LSMS_DATA_DIR=$R2 timeout 900 $PY plot_features.py ) > $O/direct-$L-$variant.log 2>&1
    echo "direct $L $variant exit $?" | tee -a $O/direct-$L-$variant.log
    ( cd $d && PYTHONPATH=$d $PY -c "
import pandas as pd, os, sys
p='$R2/Senegal/var/plot_features.parquet'
print('var exists', os.path.exists(p))
if os.path.exists(p):
    df=pd.read_parquet(p); print('rows', len(df), df.index.get_level_values('t').value_counts().to_dict()); df.to_pickle('$O/direct-$L-$variant.pkl')
" ) >> $O/direct-$L-$variant.log 2>&1
  done
done
( cd $SRC_1 && PYTHONPATH=$SRC_1 $PY - <<PYEOF
import pandas as pd
O='$O'
for a, b in [('healthy-base-default', 'healthy-cand-default'), ('healthy-base-make', 'healthy-cand-make'),
             ('healthy-cand-default', 'healthy-cand-make'), ('direct-base-varonly', 'direct-cand-varonly')]:
    try:
        x, y = pd.read_pickle(f'{O}/{a}.pkl'), pd.read_pickle(f'{O}/{b}.pkl')
        pd.testing.assert_frame_equal(x, y); print(a, '==', b, len(x))
    except Exception as e:
        print(a, 'vs', b, 'DIFF/ERR', type(e).__name__, str(e)[:300])
PYEOF
) > $O/compare.txt 2>&1
cat $O/compare.txt; for f in $O/marker-*.jsonl; do echo "== $f"; cat $f; done
echo "in-tree:"; find $HC/lsms_library/countries $HB/lsms_library/countries $BC/lsms_library/countries $BB/lsms_library/countries -name '*.parquet' | head
