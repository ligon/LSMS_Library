#!/bin/bash
#SBATCH -J manquant-final2
# Usage: job_final2.sh <cand-sha>
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
T=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools
BASE=f138a3d61c203f892148592675a7ef72e160a322
CAND=${1:?cand sha}
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $CAND $BASE || exit 1
J=/local/job$SLURM_JOB_ID; S=${CAND:0:12}; O=$OUT/final2-$S; mkdir -p "$O"
# (c) census at the final head
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $T/hash_census.py $OUT/census-cand-$S.json ) > "$O/census.log" 2>&1
"$PY" $T/hashdiff.py $OUT/census-base.json $OUT/census-cand-$S.json $OUT/hashdiff-$S.json > "$OUT/hashdiff_$S.txt" 2>&1
echo "census+hashdiff exit $?"; cat "$OUT/hashdiff_$S.txt"
# (a) healthy make-backend build, base and cand
for label in cand base; do
  if [ $label = cand ]; then SR=$SRC_1; else SR=$SRC_2; fi
  D=$J/private/make-$label; mkdir -p "$D"
  ( cd "$SR" && PYTHONPATH=$SR LSMS_DATA_DIR=$D LSMS_BUILD_BACKEND=make timeout 1800 "$PY" "$R/scripts/pf_call2.py" "$O/make-$label.parquet" ) > "$O/make-$label.log" 2>&1
  echo "make $label exit $?"
  grep -E "rows_by_wave|Python fallback|plot_features" "$O/make-$label.log" | grep -v FutureWarning | head -8
done
# (b) direct run on a copy of a healthy default root with only the var parquet removed
for label in cand base; do
  if [ $label = cand ]; then SR=$SRC_1; else SR=$SRC_2; fi
  D=$J/private/default-$label; mkdir -p "$D"
  ( cd "$SR" && PYTHONPATH=$SR LSMS_DATA_DIR=$D timeout 1800 "$PY" "$R/scripts/pf_call2.py" "$O/default-$label.parquet" ) > "$O/default-$label.log" 2>&1
  echo "default $label exit $?"
  D2=$J/private/direct-$label; mkdir -p "$D2"; cp -a "$D/Senegal" "$D2/"
  ls "$D2/Senegal/var/" "$D2/Senegal/2018-19/_/" "$D2/Senegal/2021-22/_/" 2>&1 | grep plot_features
  rm -f "$D2/Senegal/var/plot_features.parquet"
  ( cd "$SR/lsms_library/countries/Senegal/_" && PYTHONPATH=$SR LSMS_DATA_DIR=$D2 timeout 900 "$PY" plot_features.py ) > "$O/direct-$label.log" 2>&1
  echo "direct $label exit $?"
  cp "$D2/Senegal/var/plot_features.parquet" "$O/direct-$label-var.parquet" 2>/dev/null
done
( cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" - <<PYEOF
import pandas as pd
O = '$O'
for name in ('make', 'default', 'direct'):
    suffix = '-var' if name == 'direct' else ''
    try:
        a = pd.read_parquet(f'{O}/{name}-base{suffix}.parquet'); b = pd.read_parquet(f'{O}/{name}-cand{suffix}.parquet')
        pd.testing.assert_frame_equal(a, b)
        print(name, 'base == cand', len(a), a.index.get_level_values('t').value_counts().to_dict())
    except Exception as e:
        print(name, 'DIFF/ERR', type(e).__name__, str(e)[:400])
PYEOF
) > "$O/compare.txt" 2>&1
cat "$O/compare.txt"
# (d) extra test files
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider -q -rfEs tests/test_863_transforms.py tests/test_age_handler.py tests/test_benin_2021_wave.py tests/test_burkina_faso_plot_inputs_identity.py tests/test_gh323_ethiopia_config.py tests/test_gh323_grain_contract.py tests/test_metric_decimal_comma.py ) > "$OUT/pytest-extra-$S.txt" 2>&1
echo "pytest extra exit $?"; tail -3 "$OUT/pytest-extra-$S.txt"
# (e) in-tree parquets
echo "in-tree parquets:"; find "$SRC_1/lsms_library/countries" "$SRC_2/lsms_library/countries" -name '*.parquet' | head
echo "end"
