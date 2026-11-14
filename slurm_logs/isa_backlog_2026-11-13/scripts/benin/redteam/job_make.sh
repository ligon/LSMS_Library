#!/bin/bash
#SBATCH -J rt-benin-make
# The new Benin _/Makefile: make -n checks, the country-builder probe on both sides,
# and a FAULT INJECTION into one wave script on both sides (job-local clones only).
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh benin-redteam $CAND $BASE || exit 1
O=$OUT/make; mkdir -p $O
JL=/local/job$SLURM_JOB_ID
CD=$SRC_1/lsms_library/countries/Benin/_
{
  cd $CD
  orig=$(stat -c %Y panel_ids.py)
  touch -d "@$(( $(date +%s) + 120 ))" panel_ids.py
  ls -l --time-style=full-iso panel_ids.py panel_ids.json updated_ids.json
  for t in panel_ids.json updated_ids.json; do echo "== make -n $t (script newer)"; make -n $t; echo "exit $?"; done
  echo "== make -n panel-ids"; make -n panel-ids; echo "exit $?"
  echo "== make -n (default goal)"; LSMS_DATA_DIR=$DATA_1 make -n; echo "exit $?"
  touch -d "@$orig" panel_ids.py
  echo "== make -n clean"; LSMS_DATA_DIR=$DATA_1 make -n clean; echo "exit $?"
  echo "== make -n clean, no LSMS_DATA_DIR"; env -u LSMS_DATA_DIR HOME=/nonexistent make -n clean; echo "exit $?"
  for w in 2018-19 2021-22; do for t in community_prices crop_production livestock people_last7days plot_features plot_inputs plot_labor; do
    echo "-- make -n ../$w/_/$t.parquet"; make -n ../$w/_/$t.parquet; echo "exit $?"; done; done
  for t in plot_features plot_inputs sample panel_ids; do echo "-- make -n VAR_DIR/$t.parquet"; LSMS_DATA_DIR=$DATA_1 make -n $DATA_1/Benin/var/$t.parquet; echo "exit $?"; done
  echo "== every rule target or prerequisite naming a json, make -pn"; make -pn -f Makefile 2>/dev/null | grep -n 'ids\.json' | head -20
  echo "== git status (cand clone)"; git -C $SRC_1 status --short
} > $O/make_checks.txt 2>&1

# Country-builder probe, base vs candidate (decides the make-backend route, GH #1039)
cat > $JL/probe.py <<'EOF'
import json, sys, warnings
warnings.simplefilter('ignore')
import lsms_library as ll
c = ll.Country('Benin')
out = {t: c._probe_country_builder(t) for t in c.data_scheme}
json.dump(out, open(sys.argv[1], 'w'), indent=1)
print(out)
EOF
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $JL/probe.py $O/probe_cand.json > $O/probe_cand.txt 2>&1 )
( cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $JL/probe.py $O/probe_base.json > $O/probe_base.txt 2>&1 )

# Fault injection (job-local clones; discarded with the job).  One wave script fails.
cat > $JL/inject.py <<'EOF'
import json, sys, traceback, warnings, subprocess
import lsms_library as ll
res = {}
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    try:
        df = ll.Country('Benin').plot_labor()
        res['rows'] = len(df)
        res['rows_by_t'] = {str(k): int(v) for k, v in df.index.get_level_values('t').value_counts().items()}
    except BaseException as e:
        res['raised'] = f'{type(e).__module__}.{type(e).__name__}: {str(e)[:600]}'
        traceback.print_exc()
res['warnings'] = sorted({f'{x.category.__name__}: {str(x.message)[:300]}' for x in w
                          if x.category.__name__ not in ('FutureWarning', 'DeprecationWarning')})
json.dump(res, open(sys.argv[1], 'w'), indent=1)
print(json.dumps(res, indent=1))
EOF
for s in 1 2; do
  v=SRC_$s; src=${!v}; side=$([ $s = 1 ] && echo cand || echo base)
  f=$src/lsms_library/countries/Benin/2021-22/_/plot_labor.py
  sed -i '1i raise SystemExit("RT injected failure")' $f
  d=$JL/private/data-inject-$side; mkdir -p $d
  ( cd $src && PYTHONPATH=$src LSMS_DATA_DIR=$d $PY $JL/inject.py $O/inject_$side.json > $O/inject_$side.txt 2>&1 )
done
echo ALLDONE > $O/ALLDONE
