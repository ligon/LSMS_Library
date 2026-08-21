#!/bin/bash
#SBATCH --job-name=cube-aggregate
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=2
#SBATCH --mem=12G
#SBATCH --time=00:20:00
#SBATCH --output=slurm_logs/2026-06-25_coverage_matrix/cube/logs/aggregate_%j.out
#SBATCH --error=slurm_logs/2026-06-25_coverage_matrix/cube/logs/aggregate_%j.err

set -uo pipefail
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"
CUBE=slurm_logs/2026-06-25_coverage_matrix/cube

bash bin/savio_venv.sh mount
PY="$REPO/.venv/bin/python"

echo "=== shards present ($(ls "$CUBE/shards" 2>/dev/null | wc -l) of 34) ==="
ls "$CUBE/shards" 2>/dev/null | sed 's/\.csv$//' | sort > /tmp/cube_have.txt
echo "=== missing countries (built no shard) ==="
sort "$CUBE/countries.txt" | comm -23 - /tmp/cube_have.txt || echo "  (none)"
echo

# Merge shards -> committed snapshot + full HTML readout (pure pandas; no builds).
"$PY" -m bench.matrix_aggregate "$CUBE/shards" \
      --snapshot .coder/coverage/latest.csv \
      --html "$CUBE/matrix_full.html"
echo "=== aggregate done $(date) ==="
