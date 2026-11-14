#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 3:00:00
#SBATCH --exclude=n0035.savio4
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-1560/logs/%x-%j.out
# Usage: sbatch -J NAME job.sh COMMIT LABEL STEP [STEP...]
#   STEP in: raw build tests analysis compare census
set -euo pipefail

COMMIT=$1; LABEL=$2; shift 2
BASE=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-1560
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/sold-kg-1560
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
[ ${#COMMIT} -eq 40 ] || { echo "need a full SHA"; exit 2; }

JOBLOCAL=/local/job${SLURM_JOB_ID}
[ -d "$JOBLOCAL" ] || JOBLOCAL=${TMPDIR:-/tmp}/job${SLURM_JOB_ID}
mkdir -p "$JOBLOCAL"

export LSMS_DATA_DIR=$PRIV/$LABEL-${SLURM_JOB_ID}/data
export XDG_CACHE_HOME=$JOBLOCAL/xdg
export TMPDIR=$JOBLOCAL/tmp
export MPLCONFIGDIR=$JOBLOCAL/mpl
export LSMS_ISSUES_LOG=$JOBLOCAL/issues.log
mkdir -p "$LSMS_DATA_DIR" "$XDG_CACHE_HOME" "$TMPDIR" "$MPLCONFIGDIR"
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT || true

CLONE=$JOBLOCAL/LSMS_Library
git clone --quiet --shared --no-checkout "$MIRROR" "$CLONE"
git -C "$CLONE" checkout --quiet --detach "$COMMIT"
echo "clone at $(git -C "$CLONE" rev-parse HEAD) on $(hostname); data $LSMS_DATA_DIR"
[ "$(git -C "$CLONE" rev-parse HEAD)" = "$COMMIT" ]

cd "$CLONE"
bin/savio_venv.sh mount
PY=$CLONE/.venv/bin/python

export LSMS_CLONE=$CLONE
export PYTHONPATH=$CLONE
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1

"$PY" - <<'EOF'
import lsms_library, os, subprocess
from lsms_library import paths
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
r = str(paths.data_root().resolve())
assert '/private/sold-kg-1560/' in r, r
assert '/cache/lsms_library' not in r and '.local/share' not in r, r
print('identity ok', lsms_library.__file__, r)
EOF

OUT=$PRIV/$LABEL-${SLURM_JOB_ID}/out
mkdir -p "$OUT"
set +e
for step in "$@"; do
  echo "=== step $step $(date)"
  case $step in
    raw)      "$PY" "$BASE/scripts/raw_codes.py" "$BASE/out/raw-$LABEL" ;;
    build)    "$PY" "$BASE/scripts/build.py" "$OUT" "$LABEL" ;;
    analysis) "$PY" "$BASE/scripts/part2.py" "${CAND_OUT:-$OUT}" "$BASE/out/part2-$LABEL" ;;
    compare)  "$PY" "$BASE/scripts/compare.py" "${COMPARE_BASE:?export COMPARE_BASE}" "${CAND_OUT:-$OUT}" "$BASE/out/compare-$LABEL" ;;
    census)   "$PY" "$BASE/scripts/hash_census.py" "$BASE/out/census-$LABEL.json" ;;
    compare2) "$PY" "$BASE/scripts/compare2.py" "${COMPARE_BASE:?}" "${COMPARE_PRIOR:?}" "$OUT" "$BASE/out/compare2-$LABEL" ;;
    followup) "$PY" "$BASE/scripts/followup.py" "$BASE/out/followup-$LABEL" ;;
    census_at)
      ACLONE=$JOBLOCAL/LSMS_at
      git clone --quiet --shared --no-checkout "$MIRROR" "$ACLONE"
      git -C "$ACLONE" checkout --quiet --detach "${CENSUS_AT_COMMIT:?}"
      (cd "$ACLONE" && PYTHONPATH=$ACLONE LSMS_CLONE=$ACLONE "$PY" "$BASE/scripts/hash_census.py" "$BASE/out/census-${CENSUS_AT_COMMIT:0:9}.json") ;;
    census_base)
      BCLONE=$JOBLOCAL/LSMS_base
      git clone --quiet --shared --no-checkout "$MIRROR" "$BCLONE"
      git -C "$BCLONE" checkout --quiet --detach "${CENSUS_BASE_COMMIT:?export CENSUS_BASE_COMMIT}"
      (cd "$BCLONE" && PYTHONPATH=$BCLONE LSMS_CLONE=$BCLONE "$PY" "$BASE/scripts/hash_census.py" "$BASE/out/census-base.json") ;;
    tests)
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
      echo "$TESTS" > "$BASE/out/pytest-$LABEL-${SLURM_JOB_ID}.files"
      "$PY" -m pytest -n 4 --dist=loadfile -q -rs -p no:cacheprovider $TESTS \
        > "$BASE/out/pytest-$LABEL-${SLURM_JOB_ID}.txt" 2>&1
      echo "pytest exit $?"
      tail -5 "$BASE/out/pytest-$LABEL-${SLURM_JOB_ID}.txt" ;;
    *) echo "unknown step $step" ;;
  esac
  echo "=== step $step exit $?"
done
echo "issues log lines: $(wc -l < "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
echo "in-tree parquet: $(find "$CLONE/lsms_library/countries" -name '*.parquet' | wc -l)"
bin/savio_venv.sh umount || true
echo done
