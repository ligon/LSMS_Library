#!/bin/bash
#SBATCH -J benin-compare2
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh benin $1 || exit 1
cd "$SRC_1"; O=$OUT
UPD=$SRC_1/lsms_library/countries/Benin/_/updated_ids.json
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $BATCH/reports/benin/scripts/compare.py $2 $3 $UPD $O/compare-${1:0:12}.json > $O/compare-${1:0:12}.txt 2>&1; echo "exit $?" >> $O/compare-${1:0:12}.txt
PYTHONPATH=$SRC_1 "$PY" $BATCH/reports/benin/scripts/pw_check.py $2/sample.pkl $3/sample.pkl $UPD > $O/pw-${1:0:12}.txt 2>&1; echo "exit $?" >> $O/pw-${1:0:12}.txt
