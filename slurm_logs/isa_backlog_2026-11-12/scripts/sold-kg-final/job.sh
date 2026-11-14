#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 2:00:00
#SBATCH --exclude=n0035.savio4
#SBATCH -J soldkg-final
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-final/logs/%x-%j.out
# Usage: sbatch --chdir=<out dir> job.sh NEW_SHA OLD_SHA BASE_SHA
set -uo pipefail

NEW=$1; OLD=$2; BASE_SHA=$3
for s in "$NEW" "$OLD" "$BASE_SHA"; do [ ${#s} -eq 40 ] || { echo "need full SHAs"; exit 2; }; done
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-final
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/sold-kg-final/job-${SLURM_JOB_ID}
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
OUT=$REP/out

# Private roots FIRST, before anything mounts or imports.
JOBLOCAL=/local/job${SLURM_JOB_ID}
[ -d "$JOBLOCAL" ] || JOBLOCAL=$PRIV/local
mkdir -p "$JOBLOCAL"
export TMPDIR=$JOBLOCAL/tmp
export XDG_CACHE_HOME=$JOBLOCAL/xdg
export MPLCONFIGDIR=$JOBLOCAL/mpl
export LSMS_ISSUES_LOG=$JOBLOCAL/issues.log
DATA_COLD=$PRIV/data
DATA_CENSUS=$PRIV/census-data
export LSMS_DATA_DIR=$DATA_COLD
mkdir -p "$TMPDIR" "$XDG_CACHE_HOME" "$MPLCONFIGDIR" "$DATA_COLD" "$DATA_CENSUS"
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_QUANTITY_STRICT LSMS_READ_STRICT LSMS_GRAIN_STRICT || true
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
echo "host $(hostname) job $SLURM_JOB_ID start $(date)"
echo "data(cold) $DATA_COLD census $DATA_CENSUS tmp $TMPDIR"
[ -r "$HOME/.config/lsms_library/config.yml" ] && echo "config readable" || echo "CONFIG NOT READABLE"

clone() {  # clone <dir> <sha>
  git clone --quiet --shared --no-checkout "$MIRROR" "$1"
  git -C "$1" checkout --quiet --detach "$2"
  [ "$(git -C "$1" rev-parse HEAD)" = "$2" ] || { echo "clone $1 not at $2"; exit 3; }
  echo "clone $1 at $(git -C "$1" rev-parse HEAD)"
}
CLONE=$JOBLOCAL/new;  clone "$CLONE" "$NEW"
OCLONE=$JOBLOCAL/old; clone "$OCLONE" "$OLD"
BCLONE=$JOBLOCAL/base; clone "$BCLONE" "$BASE_SHA"

cd "$CLONE"
bin/savio_venv.sh mount || { echo "venv mount failed"; exit 4; }
PY=$CLONE/.venv/bin/python
export PYTHONPATH=$CLONE

identity() {  # identity <clone> <data root>
  (cd "$1" && PYTHONPATH=$1 LSMS_DATA_DIR=$2 "$PY" - <<'EOF'
import lsms_library, os, subprocess
from lsms_library import paths
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
r = os.path.realpath(str(paths.data_root()))
assert '/private/sold-kg-final/' in r, r
assert '/cache/lsms_library' not in r and '.local/share' not in r, r
print('identity ok', subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
      lsms_library.__file__, r)
EOF
  )
}
identity "$CLONE" "$DATA_COLD" || exit 5
identity "$OCLONE" "$DATA_CENSUS" || exit 5
identity "$BCLONE" "$DATA_CENSUS" || exit 5

step() { echo "=== step $1 $(date)"; }

step synthetic
"$PY" -m pytest -q -p no:cacheprovider tests/test_quantity_screen.py \
  > "$OUT/pytest-synthetic-${SLURM_JOB_ID}.txt" 2>&1
echo "synthetic exit $?"; tail -3 "$OUT/pytest-synthetic-${SLURM_JOB_ID}.txt"

step census
for pair in "new:$CLONE" "old:$OCLONE" "base:$BCLONE"; do
  label=${pair%%:*}; dir=${pair#*:}
  (cd "$dir" && PYTHONPATH=$dir LSMS_DATA_DIR=$DATA_CENSUS "$PY" "$REP/scripts/hash_census.py" \
     "$OUT/census-$label-${SLURM_JOB_ID}.json")
  echo "census $label exit $?"
done
"$PY" "$REP/scripts/hashdiff.py" "$OUT/census-old-${SLURM_JOB_ID}.json" "$OUT/census-new-${SLURM_JOB_ID}.json" \
  "$OUT/hashdiff-old-new-${SLURM_JOB_ID}.json"
"$PY" "$REP/scripts/hashdiff.py" "$OUT/census-base-${SLURM_JOB_ID}.json" "$OUT/census-new-${SLURM_JOB_ID}.json" \
  "$OUT/hashdiff-base-new-${SLURM_JOB_ID}.json"

step coldcheck
"$PY" "$REP/scripts/coldcheck.py" "$OUT/coldcheck-${SLURM_JOB_ID}.json" \
  > "$OUT/coldcheck-${SLURM_JOB_ID}.txt" 2>&1
echo "coldcheck exit $?"; grep -E "^(checks|COLDCHECK)" "$OUT/coldcheck-${SLURM_JOB_ID}.txt"

step tests
TESTS=$( { ls tests/test_*niger*.py | grep -v nigeria; ls tests/test_*mali*.py; \
           ls tests/test_build_transform_hash.py tests/test_schema_consistency.py tests/test_derivations.py \
              tests/test_crop_survey_kg.py tests/test_gh1569_sold_diagnostics.py tests/test_gh1528_sold_condition.py \
              tests/test_median_price_valuation.py tests/test_gh1460_crop_level.py tests/test_gh1475_country_crop_transforms.py \
              tests/test_gh1535_crop_month_carriers.py tests/test_ehcvm_crop_production_condition.py \
              tests/test_crop_kg_factor.py tests/test_quantity_kg.py tests/test_gh824_mali_ehcvm_crop_production.py \
              tests/test_gh1457_niger_multi_plot_sales.py tests/test_gh1457_mali_multi_plot_sales.py \
              tests/test_gh1462_niger_dry_season.py tests/test_gh1437_mali_harvest_total.py \
              tests/test_gh1476_mali_tree_farm_level.py tests/test_gh1267_mali_tree_quantity.py \
              tests/test_gh824_mali_unit_sold.py tests/test_gh824_mali_field_sold_factors.py \
              tests/test_quantity_screen.py tests/test_gh1482_1485_quantity_diagnostics.py; } | sort -u )
echo "$TESTS" > "$OUT/pytest-main-${SLURM_JOB_ID}.files"
"$PY" -m pytest -n 4 --dist=loadfile -q -rs -p no:cacheprovider $TESTS \
  > "$OUT/pytest-main-${SLURM_JOB_ID}.txt" 2>&1
echo "pytest main exit $?"; tail -4 "$OUT/pytest-main-${SLURM_JOB_ID}.txt"
"$PY" -m pytest -n 2 --dist=loadfile -q -rs -p no:cacheprovider -m "not requires_s3" \
  tests/test_gh1493_near_zero_screen.py > "$OUT/pytest-gh1493-${SLURM_JOB_ID}.txt" 2>&1
echo "pytest gh1493 (not requires_s3) exit $?"; tail -3 "$OUT/pytest-gh1493-${SLURM_JOB_ID}.txt"

step hygiene
echo "issues log lines: $(wc -l < "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
for d in "$CLONE" "$OCLONE" "$BCLONE"; do
  echo "in-tree parquet $d: $(find "$d/lsms_library/countries" -name '*.parquet' | wc -l)"
  echo "git status $d: $(git -C "$d" status --porcelain | wc -l) lines"
done
echo "untracked or ignored paths in the new clone (excluding .venv and __pycache__), paths only:"
git -C "$CLONE" status --porcelain --ignored | grep -v -e '\.venv' -e '__pycache__' | head -20
(cd "$CLONE" && bin/savio_venv.sh umount) || true
echo "done $(date)"
