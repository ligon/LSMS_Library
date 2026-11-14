#!/bin/bash
# Hash census base -> cand.  Usage: job_census_hash.sh <cand-sha> <base-sha> <tag>
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $B/tools/burst-env.sh burkina "$1" "$2" || exit 9
TAG=${3:-h1}
(cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $OUT/census-$TAG-cand.json)
(cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $OUT/census-$TAG-base.json)
$PY $BATCH/tools/hashdiff.py $OUT/census-$TAG-base.json $OUT/census-$TAG-cand.json $OUT/hashdiff-$TAG.json
