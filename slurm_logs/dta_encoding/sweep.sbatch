#!/bin/bash
#SBATCH --job-name=dta_encoding
#SBATCH --account=co_carleton
#SBATCH --partition=savio3
#SBATCH --qos=savio_lowprio
#SBATCH --nodes=1
#SBATCH --time=02:00:00
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/dta_encoding/sweep_%j.log
# ONE node saturated from inside via xargs -P.  savio3 allocates WHOLE nodes
# (~32 cores); an array of single-core tasks would hold ~1,120 cores for work
# needing 32.  Pattern per slurm_logs/sample_warnings/.
set -u
D=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/dta_encoding
N=${SLURM_CPUS_ON_NODE:-32}
echo "host=$(hostname) cores=$N files=$(wc -l < $D/manifest.tsv) start=$(date -Is)"
cut -f1,2 "$D/manifest.tsv" | xargs -P "$N" -L1 -d '\n' -I{} "$D/run_one.sh" {} \
  > "$D/sweep_$SLURM_JOB_ID.jsonl" 2> "$D/sweep_$SLURM_JOB_ID.err"
echo "done=$(date -Is) lines=$(wc -l < $D/sweep_$SLURM_JOB_ID.jsonl)"
