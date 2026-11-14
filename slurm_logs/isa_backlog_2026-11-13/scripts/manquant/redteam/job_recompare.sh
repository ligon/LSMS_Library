#!/bin/bash
#SBATCH -J mqrt-recmp
set -uo pipefail
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant-redteam
CAND=fc40960d27b800045984735a38c68cf91c9b2194
BASE=f138a3d61c203f892148592675a7ef72e160a322
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant-redteam $CAND || exit 1
for C in Togo Senegal CotedIvoire; do
  O=$OUT/build-$C
  ( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_compare.py $O/base $O/cand $C $O/compare2.json ) > $O/compare2.log 2>&1; echo "$C compare2 exit $?"
done
( cd $SRC_1 && $PY - <<PYEOF
import yaml, subprocess
class L(yaml.SafeLoader): pass
L.add_multi_constructor('!', lambda loader, suffix, node: f'!{suffix}:' + str(loader.construct_scalar(node) if isinstance(node, yaml.ScalarNode) else ''))
for c in ('Togo', 'Senegal', 'CotedIvoire'):
    p = f'lsms_library/countries/{c}/_/data_scheme.yml'
    a = yaml.load(subprocess.check_output(['git', 'show', f'$BASE:{p}'], text=True), Loader=L)
    b = yaml.load(open(p).read(), Loader=L)
    print(c, 'data_scheme.yml parsed-equal:', a == b)
PYEOF
) > $OUT/yaml_eq2.txt 2>&1; cat $OUT/yaml_eq2.txt
