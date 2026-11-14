#!/bin/bash
#SBATCH -J rt-benin-bid
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
source $BATCH/tools/burst-env.sh benin-redteam $CAND || exit 1
O=$OUT/bid; mkdir -p $O
{ which pdftotext gs mutool qpdf 2>&1; module avail 2>&1 | grep -i -E 'poppler|ghost|mupdf' ; } > $O/tools.txt
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_bid.py $O > $O/log.txt 2>&1; echo "exit $?" >> $O/log.txt )
P=$(ls $DATA_1/dvc-cache 2>/dev/null | head -1)
if command -v gs >/dev/null; then F=$(find $DATA_1 $SRC_1/lsms_library/countries/Benin/2021-22/Documentation -name '*information_de_base_eng*' -o -type f -size +100k -path '*dvc-cache*' 2>/dev/null | head -3); echo "$F" >> $O/tools.txt; fi
echo ALLDONE > $O/ALLDONE
