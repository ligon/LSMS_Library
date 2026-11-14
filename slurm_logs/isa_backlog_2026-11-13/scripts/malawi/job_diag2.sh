#!/bin/bash
#SBATCH -J mw-diag2
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 4
#SBATCH --mem=32G
#SBATCH -t 1:30:00
#SBATCH --exclude=n0035.savio4,n0034.savio4,n0046.savio4
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
# Read-only GH #1228 / #1712 diagnostics on the base SHA.  Restartable: writes
# only to out/diag-<sha12>/ and its own job-local private root.
BASE=${SHA:-f138a3d61c203f892148592675a7ef72e160a322}
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh malawi $BASE || exit 9
D=$OUT/diag-${BASE:0:12}
mkdir -p $D
cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $REP/scripts/diag2.py $SRC_1 $D > $D/diag2.log 2>&1
echo "diag rc=$?"
tail -5 $D/diag2.log
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"
git -C $SRC_1 status --porcelain | head
echo "JOB DONE $(date -Is)"
