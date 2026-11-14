#!/bin/bash
#SBATCH -J srt-delta
# Red team delta review: 79739f816 vs base f138a3d61 (and vs 0d62f49df).
NEW=79739f8162b706b72bbc54dffe739e855b154f80
BASE=f138a3d61c203f892148592675a7ef72e160a322
OLD=0d62f49df61bb43190275aaa3a2520f724eed6af
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/strata-redteam
source $BATCH/tools/burst-env.sh strata-redteam "$NEW" "$BASE" "$OLD" || exit 1
O=$OUT/j7; mkdir -p $O
echo "start $(date -Is)"
# 1. hash census
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $O/census-new.json) > $O/census.log 2>&1
(cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $O/census-base.json) >> $O/census.log 2>&1
(cd "$SRC_3" && PYTHONPATH=$SRC_3 LSMS_DATA_DIR=$DATA_3 $PY $BATCH/tools/hash_census.py $O/census-old.json) >> $O/census.log 2>&1
$PY $BATCH/tools/hashdiff.py $O/census-base.json $O/census-new.json $O/hashdiff-base-new.json >> $O/census.log 2>&1
$PY $BATCH/tools/hashdiff.py $O/census-old.json $O/census-new.json $O/hashdiff-old-new.json >> $O/census.log 2>&1
# 2. validator probes (F2, F8)
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/probe_validator.py) > $O/probes.txt 2>&1
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/dump_records.py $O/records-new.json) > $O/records.log 2>&1
# 3. F4 mutations on a copied config tree
MUT=/local/job${SLURM_JOB_ID}/mut; rm -rf $MUT; mkdir -p $MUT
cp -r "$SRC_1/lsms_library/countries" $MUT/countries
C=$MUT/countries
printf '\n\ndef strata(x):\n    return str(x).upper()\n' >> $C/Niger/2011-12/_/mapping.py
sed -i 's/^\(\s*\)strata: stratum$/\1strata:\n\1    - stratum\n\1    - mapping: {Kampala: KLA}/' $C/Uganda/2009-10/_/data_info.yml
perl -0pi -e "s/(    try:\n        return reduce_to_agreed)/    df['strata'] = df['strata'].astype(str).str.title()\n\$1/" $C/Niger/2014-15/_/mapping.py
printf '\n\ndef _relabel(df):\n    df["strata"] = "X"\n    return df\n\n\ndef sample(df):\n    return _relabel(df)\n' >> $C/Mali/2017-18/_/mapping.py
printf '\n\ndef strata(x):\n    return {"Kampala": "KLA"}.get(x, x)\n' >> $C/Uganda/_/uganda.py
(cd $C && diff -r "$SRC_1/lsms_library/countries" . ) > $O/f4-mutation.diff 2>&1
(cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 LSMS_COUNTRIES_ROOT=$C $PY $RT/scripts/f4_mutations.py) > $O/f4-mutations.txt 2>&1
# 4. tests: full non-S3 suite, then the Mali delivered tag tests (S3)
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 4500 $PY -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider \
  -q -rfE -m "not requires_s3" tests/ > $O/pytest-nons3-new.txt 2>&1
echo "nons3 exit $?" >> $O/pytest-nons3-new.txt
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 2400 $PY -m pytest -p no:cacheprovider -q -rfEs -m requires_s3 \
  tests/test_sampling.py -k "test_the_tag_agrees_with_the_served_strata and Mali" > $O/pytest-s3-mali-new.txt 2>&1
echo "s3 exit $?" >> $O/pytest-s3-mali-new.txt
echo "end $(date -Is)"
