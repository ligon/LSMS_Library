#!/bin/bash
# usage: probe_make.sh <checkout> <root> 
# For each table x wave, make -n the data-root wave target; print outcome class.
co=$1; root=$2
cd $co/lsms_library/countries/Malawi/_ || exit 1
for t in food_acquired food_coping months_food_inadequate crop_production plot_inputs livestock anthropometry plot_labor people_last7days community_prices; do
  for w in 2004-05 2010-11 2013-14 2016-17 2019-20; do
    out=$(make -n LSMS_DATA_ROOT=$root $root/Malawi/$w/_/$t.parquet 2>&1); rc=$?
    if echo "$out" | grep -q "No rule to make target"; then cls="NO-RULE"
    elif echo "$out" | grep -q "python $t.py"; then cls="RUNS"
    elif echo "$out" | grep -q -E "up to date|Nothing to be done"; then cls="UP-TO-DATE"
    else cls="OTHER"; fi
    printf "%-24s %-8s rc=%s %-10s %s\n" $t $w $rc $cls "$(echo "$out" | head -1 | cut -c1-110)"
  done
done
