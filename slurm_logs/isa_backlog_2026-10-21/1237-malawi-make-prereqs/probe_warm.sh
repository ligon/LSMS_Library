#!/bin/bash
# usage: probe_warm.sh <checkout> <root>
# Warm (stand-in) root: every wave target that has a script exists and is newer
# than every source.  For each table x scripted wave report:
#   plain : make -n with nothing changed
#   W-ws  : make -n -W ../<w>/_/<t>.py   (wave script "just edited")
#   W-org : make -n -W categorical_mapping.org
co=$1; root=$2
cd $co/lsms_library/countries/Malawi/_ || exit 1
cls() { if echo "$1" | grep -q "No rule to make target"; then echo NO-RULE; elif echo "$1" | grep -q "python $2.py"; then echo REBUILD; elif [ -z "$1" ] || echo "$1" | grep -q -E "up to date|Nothing to be done"; then echo CURRENT; else echo OTHER; fi; }
for t in food_acquired food_coping months_food_inadequate crop_production plot_inputs livestock anthropometry plot_labor people_last7days community_prices; do
  for w in 2004-05 2010-11 2013-14 2016-17 2019-20; do
    [ -f ../$w/_/$t.py ] || continue
    tgt=$root/Malawi/$w/_/$t.parquet; mkdir -p $(dirname $tgt); [ -f $tgt ] || touch $tgt
  done
done
for t in food_acquired food_coping months_food_inadequate crop_production plot_inputs livestock anthropometry plot_labor people_last7days community_prices; do
  for w in 2004-05 2010-11 2013-14 2016-17 2019-20; do
    [ -f ../$w/_/$t.py ] || continue
    tgt=$root/Malawi/$w/_/$t.parquet
    a=$(make -n LSMS_DATA_ROOT=$root $tgt 2>&1)
    b=$(make -n -W ../$w/_/$t.py LSMS_DATA_ROOT=$root $tgt 2>&1)
    c=$(make -n -W categorical_mapping.org LSMS_DATA_ROOT=$root $tgt 2>&1)
    printf "%-24s %-8s plain=%-8s W-wave-script=%-8s W-org=%-8s\n" $t $w $(cls "$a" $t) $(cls "$b" $t) $(cls "$c" $t)
  done
done
