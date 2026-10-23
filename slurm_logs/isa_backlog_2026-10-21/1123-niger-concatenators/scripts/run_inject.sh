# break one wave script on purpose, scratch config tree, fresh data root per case
source "$JOBDIR/scripts/common.sh"
W="$JOBDIR/work-$LABEL"; mkdir -p "$W" "$OUT/inject"
run() { "$PY" "$S/inject.py" "$CLONE" "$1" "$2" "$3" "$4" "$5" "$W" "$OUT/inject/$1.json" > /dev/null 2>&1; echo "$1 rc=$?"; }
for t in crop_production livestock plot_labor people_last7days; do
  run "make-$t-2018-19" $t 2018-19 make raise
  run "dvc-$t-2018-19" $t 2018-19 dvc raise
done
run make-crop_production-2021-22 crop_production 2021-22 make raise
run make-plot_inputs-2018-19 plot_inputs 2018-19 make raise
if [ "$LABEL" = branch ]; then
  for t in crop_production livestock plot_labor people_last7days; do
    run "andandonly-$t-2018-19" $t 2018-19 make andand-only
  done
fi
