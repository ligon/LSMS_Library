#!/bin/bash
#SBATCH -J srt-config
# Red team, strata stream: config-only checks.  Census base vs cand, record
# dumps both sides, validator probes, and the whole non-S3 suite on the
# candidate (failures re-run on base).
CAND=0d62f49df61bb43190275aaa3a2520f724eed6af
BASE=f138a3d61c203f892148592675a7ef72e160a322
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/strata-redteam
source $BATCH/tools/burst-env.sh strata-redteam "$CAND" "$BASE" || exit 1
O=$OUT/j1; mkdir -p $O
echo "start $(date -Is)"
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $O/census-cand.json) > $O/census.log 2>&1
(cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $O/census-base.json) >> $O/census.log 2>&1
$PY $BATCH/tools/hashdiff.py $O/census-base.json $O/census-cand.json $O/hashdiff.json >> $O/census.log 2>&1
echo "census done $(date -Is)"; tail -2 $O/census.log
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/dump_records.py $O/records-cand.json) > $O/records.log 2>&1
(cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $RT/scripts/dump_records.py $O/records-base.json) >> $O/records.log 2>&1
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/probe_validator.py) > $O/probes.txt 2>&1
echo "records/probes done $(date -Is)"
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 4500 $PY -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider \
  -q -rfE -m "not requires_s3" tests/ > $O/pytest-nons3-cand.txt 2>&1
echo "cand pytest exit $?" >> $O/pytest-nons3-cand.txt
echo "cand pytest done $(date -Is)"; tail -3 $O/pytest-nons3-cand.txt
grep -E '^(FAILED|ERROR) ' $O/pytest-nons3-cand.txt | sed -E 's/^(FAILED|ERROR) //; s/ - .*//' | sort -u > $O/failed-cand.txt
if [ -s $O/failed-cand.txt ]; then
  cd "$SRC_2"
  PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 timeout 2400 $PY -m pytest -p no:cacheprovider -q -rfE \
     $(cat $O/failed-cand.txt | tr '\n' ' ') > $O/pytest-failed-on-base.txt 2>&1
  echo "base rerun exit $?" >> $O/pytest-failed-on-base.txt
fi
echo "end $(date -Is)"
