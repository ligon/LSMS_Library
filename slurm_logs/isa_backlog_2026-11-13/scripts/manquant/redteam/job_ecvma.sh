#!/bin/bash
#SBATCH -J mqrt-ecvma
set -uo pipefail
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant-redteam
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant-redteam 63aa569cad2d2d4d2d8f9f928a686dd443af931b || exit 1
cd $SRC_1 && PYTHONPATH=$SRC_1 $PY - <<PYEOF > $OUT/delta/ecvma_na.txt 2>&1
import pandas as pd
k = pd.read_pickle('$OUT/delta/Niger/cand/plot_inputs.pkl')
t = k.index.get_level_values('t')
for w in sorted(set(t)):
    sub = k[t == w]
    print(w, len(sub), {c: int(sub[c].notna().sum()) for c in ['Unit_purchased','Received_as_gift','Quantity_received_as_gift','Unit_received_as_gift']}, {c: str(sub[c].dtype) for c in ['Unit_purchased','Received_as_gift','Quantity_received_as_gift','Unit_received_as_gift']})
PYEOF
cat $OUT/delta/ecvma_na.txt
