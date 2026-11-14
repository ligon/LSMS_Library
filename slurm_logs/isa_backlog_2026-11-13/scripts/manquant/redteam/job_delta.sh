#!/bin/bash
#SBATCH -J mqrt-delta
set -uo pipefail
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant-redteam
T=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools
CAND=63aa569cad2d2d4d2d8f9f928a686dd443af931b
PREV=fc40960d27b800045984735a38c68cf91c9b2194
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant-redteam $CAND $PREV || exit 1
D=$OUT/delta; mkdir -p $D
J=/local/job$SLURM_JOB_ID
mkdir -p $J/private/census
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$J/private/census $PY $T/hash_census.py $D/census-63aa.json ) > $D/census.log 2>&1 &
PC=$!
for C in Senegal CotedIvoire Niger; do
  for L in cand prev; do
    if [ $L = cand ]; then S=$SRC_1; else S=$SRC_2; fi
    R=$J/private/b-$C-$L; mkdir -p $R $D/$C/$L
    ( cd $S && PYTHONPATH=$S LSMS_DATA_DIR=$R timeout 3000 $PY $RT/scripts/rt_build.py $D/$C/$L $C ) > $D/$C/$L.log 2>&1 &
  done
done
# raw sentinel probe on acquisition quantities
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY - <<'PYEOF' ) > $D/raw_qty.txt 2>&1 &
import warnings; warnings.simplefilter('ignore')
import pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
for rel in ['Senegal/2018-19/Data/s16b_me_sen2018.dta', 'CotedIvoire/2018-19/Data/Menage/s16b_me_CIV2018.dta',
            'Niger/2018-19/Data/s16b_me_ner2018.dta', 'Niger/2021-22/Data/s16b_me_ner2021.dta', 'Togo/2018/Data1/s16b_me_tgo2018.dta']:
    c = get_dataframe(countries_root() / rel, convert_categoricals=False)
    c = c[c['s16bq02'] == 1]
    for v in ('s16bq03a', 's16bq07a', 's16bq09a', 's16bq09c'):
        x = pd.to_numeric(c[v], errors='coerce')
        big = x[x >= 999].value_counts().sort_index(ascending=False).head(6).to_dict()
        print(rel.split('/')[0], rel.split('/')[1], v, 'n', int(x.notna().sum()), 'max', x.max(), '>=999 top', big)
PYEOF
wait
echo "builds done"; cat $D/raw_qty.txt
$PY $T/hashdiff.py $OUT/census-base.json $D/census-63aa.json $D/hashdiff-f138-63aa.json > $D/hashdiff-f138-63aa.txt 2>&1; cat $D/hashdiff-f138-63aa.txt
$PY $T/hashdiff.py $OUT/census-cand.json $D/census-63aa.json $D/hashdiff-fc40-63aa.json > $D/hashdiff-fc40-63aa.txt 2>&1; cat $D/hashdiff-fc40-63aa.txt
for C in Senegal CotedIvoire Niger; do
  case $C in Senegal) W=2018-19;; CotedIvoire) W=2018-19;; Niger) W=2018-19,2021-22;; esac
  ( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_compare2.py $D/$C/prev $D/$C/cand $C $W $D/$C/compare.json ) > $D/$C/compare.log 2>&1; echo "$C compare exit $?"; tail -n 1 $D/$C/compare.log
done
cd $SRC_1
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 4000 $PY -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider -q -rfEs $(cat $RT/out/delta_tests.txt | tr '\n' ' ') > $D/pytest.txt 2>&1
echo "pytest exit $?"; tail -n 2 $D/pytest.txt
echo "in-tree:"; find $SRC_1/lsms_library/countries $SRC_2/lsms_library/countries -name '*.parquet' | head
