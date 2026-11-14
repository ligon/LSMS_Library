#!/bin/bash
#SBATCH -J manquant-final3
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
T=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools
CAND=${1:?cand}
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $CAND || exit 1
S=${CAND:0:12}
( cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" -c "
import pandas as pd
k = pd.read_parquet('$OUT/gift4-cand-14606914c8ac/Niger__plot_inputs.parquet')
t = k.index.get_level_values('t')
for w in ['2011-12', '2014-15']:
    sub = k[t == w]
    print(w, {c: int(sub[c].notna().sum()) for c in ['Unit_purchased', 'Received_as_gift', 'Quantity_received_as_gift', 'Unit_received_as_gift']})
print('dtypes', k.dtypes.astype(str).to_dict())
" ) > "$OUT/final3-ecvma-na.txt" 2>&1
cat "$OUT/final3-ecvma-na.txt" | grep -v Warning
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $T/hash_census.py $OUT/census-cand-$S.json ) > "$OUT/census_cand_$S.log" 2>&1
"$PY" $T/hashdiff.py $OUT/census-base.json $OUT/census-cand-$S.json $OUT/hashdiff-$S.json > "$OUT/hashdiff_$S.txt" 2>&1
"$PY" $T/hashdiff.py $OUT/census-cand-14606914c8ac.json $OUT/census-cand-$S.json $OUT/hashdiff-14606914c8ac-to-$S.json > "$OUT/hashdiff_14606914c8ac_to_$S.txt" 2>&1
echo "census exit $?"; cat "$OUT/hashdiff_$S.txt" "$OUT/hashdiff_14606914c8ac_to_$S.txt"
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider -q -rfEs \
  tests/test_gh1500_senegal_missing_food_unit.py tests/test_ehcvm_plot_inputs_identity.py tests/test_senegal_1483_s16b.py > "$OUT/pytest-final3-$S.txt" 2>&1
echo "pytest exit $?"; grep -E "^(FAILED|ERROR)" "$OUT/pytest-final3-$S.txt" | head; tail -2 "$OUT/pytest-final3-$S.txt"
echo "in-tree parquets:"; find "$SRC_1/lsms_library/countries" -name '*.parquet' | head
echo end
