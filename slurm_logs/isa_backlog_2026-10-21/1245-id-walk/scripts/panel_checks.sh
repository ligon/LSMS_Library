#!/bin/bash
# panel_checks.sh <label> <checkout>
label=$1; checkout=$2
source /local/job40390255/data-private/1245/env.sh
export PYTHONPATH=$checkout
out=/local/job40390255/data-private/1245/$label/panel_checks.jsonl
: > $out
cd $checkout
for c in Burkina_Faso Ethiopia EthiopiaRHS GhanaLSS Malawi Mali Niger Nigeria Senegal Tanzania Uganda; do
  LSMS_DATA_DIR=/local/job40390255/data-private/1245/$label/$c nice -n 19 $PY /local/job40390255/data-private/1245/panel_check.py $checkout $c 2>/dev/null | tail -1 >> $out
done
echo DONE >> /local/job40390255/data-private/1245/$label/panel_checks.done
