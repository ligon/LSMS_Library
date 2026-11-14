#!/bin/bash
#SBATCH -J mqrt-census
set -uo pipefail
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant-redteam
T=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools
CAND=fc40960d27b800045984735a38c68cf91c9b2194
BASE=f138a3d61c203f892148592675a7ef72e160a322
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant-redteam $CAND $BASE || exit 1
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $T/hash_census.py $OUT/census-cand.json ) > $OUT/census-cand.log 2>&1 &
( cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $T/hash_census.py $OUT/census-base.json ) > $OUT/census-base.log 2>&1 &
wait
$PY $T/hashdiff.py $OUT/census-base.json $OUT/census-cand.json $OUT/hashdiff.json > $OUT/hashdiff.txt 2>&1; cat $OUT/hashdiff.txt
# YAML-level equality of the three data_scheme.yml (comments only changed?)
( cd $SRC_1 && $PY - <<PYEOF
import yaml, subprocess
for c in ('Togo', 'Senegal', 'CotedIvoire'):
    p = f'lsms_library/countries/{c}/_/data_scheme.yml'
    a = yaml.safe_load(subprocess.check_output(['git', 'show', f'$BASE:{p}'], text=True))
    b = yaml.safe_load(open(p).read())
    print(c, 'data_scheme.yml parsed-equal:', a == b)
PYEOF
) > $OUT/yaml_eq.txt 2>&1; cat $OUT/yaml_eq.txt
FILES="$(cat /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant/out/pytest_files.txt | tr '\n' ' ') tests/test_863_transforms.py tests/test_age_handler.py tests/test_benin_2021_wave.py tests/test_burkina_faso_plot_inputs_identity.py tests/test_gh323_ethiopia_config.py tests/test_gh323_grain_contract.py tests/test_metric_decimal_comma.py tests/test_burkina_plot_concatenator_refusal.py tests/test_gh1188_burkina_input_acquisition_fields.py"
cd $SRC_1
ls $FILES > /dev/null || echo "missing some test files"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 4800 $PY -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider -q -rfEs $FILES > $OUT/pytest-cand.txt 2>&1
echo "pytest exit $?"; tail -3 $OUT/pytest-cand.txt
echo "in-tree:"; find $SRC_1/lsms_library/countries -name '*.parquet' | head
