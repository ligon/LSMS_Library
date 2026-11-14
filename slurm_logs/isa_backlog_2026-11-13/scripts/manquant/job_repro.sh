#!/bin/bash
#SBATCH -J manquant-repro
# Usage: job_repro.sh <cand-sha>
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
BASE=f138a3d61c203f892148592675a7ef72e160a322
CAND=${1:?cand sha}
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $CAND $BASE || exit 1
J=/local/job$SLURM_JOB_ID
mkdir -p "$OUT/repro"
for label in cand base; do
  if [ $label = cand ]; then sha=$CAND; else sha=$BASE; fi
  B=$J/broken-$label
  git clone --quiet --shared --no-checkout "$MIRROR" "$B" && git -C "$B" checkout --quiet --detach "$sha"
  printf 'raise SystemExit("injected GH1287 repro: Senegal 2021-22 plot_features wave script fails")\n' > "$B/lsms_library/countries/Senegal/2021-22/_/plot_features.py"
  git -C "$B" diff --stat
  for backend in default make; do
    D=$J/private/repro-$label-$backend; mkdir -p "$D"
    ident "$B" "$D" || exit 6
    ( cd "$B" && if [ $backend = make ]; then export LSMS_BUILD_BACKEND=make; fi
      PYTHONPATH=$B LSMS_DATA_DIR=$D timeout 1800 "$PY" "$R/scripts/pf_call.py" ) > "$OUT/repro/broken-$label-$backend.json" 2> "$OUT/repro/broken-$label-$backend.log"
    echo "broken $label $backend exit $?"
    find "$D/Senegal" -name 'plot_features.parquet' | sort > "$OUT/repro/broken-$label-$backend.parquets.txt"
  done
done
# Direct concatenator: healthy build, then remove one wave parquet and run the country script.
for label in cand base; do
  if [ $label = cand ]; then S=$SRC_1; else S=$SRC_2; fi
  D=$J/private/healthy-$label; mkdir -p "$D"
  ( cd "$S" && PYTHONPATH=$S LSMS_DATA_DIR=$D timeout 1800 "$PY" "$R/scripts/pf_call.py" ) > "$OUT/repro/healthy-$label.json" 2> "$OUT/repro/healthy-$label.log"
  echo "healthy $label exit $?"
  D2=$J/private/direct-$label; mkdir -p "$D2"; cp -a "$D/Senegal" "$D2/"
  rm -f "$D2/Senegal/var/plot_features.parquet" "$D2/Senegal/2021-22/_/plot_features.parquet"
  ( cd "$S/lsms_library/countries/Senegal/_" && PYTHONPATH=$S LSMS_DATA_DIR=$D2 timeout 900 "$PY" plot_features.py ) > "$OUT/repro/direct-$label.log" 2>&1
  echo "direct $label exit $?" | tee -a "$OUT/repro/direct-$label.log"
  ( cd "$S" && PYTHONPATH=$S "$PY" -c "
import pandas as pd, os
p='$D2/Senegal/var/plot_features.parquet'
print('var parquet exists', os.path.exists(p))
if os.path.exists(p):
    df=pd.read_parquet(p); print(df.index.get_level_values('t').value_counts().to_dict())
" ) >> "$OUT/repro/direct-$label.log" 2>&1
done
find "$SRC_1/lsms_library/countries" "$SRC_2/lsms_library/countries" -name '*.parquet' | head
