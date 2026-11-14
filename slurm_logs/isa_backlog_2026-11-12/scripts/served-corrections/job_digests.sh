#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 0:30:00
#SBATCH --exclude=n0035.savio4
#SBATCH -J sc-digests
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections/logs/digests-%j.out
set -uo pipefail
J=/local/job$SLURM_JOB_ID; mkdir -p $J/tmp; export TMPDIR=$J/tmp
git clone -q --no-hardlinks /global/home/users/ligon/mirrors/LSMS_Library $J/clone && git -C $J/clone checkout -q --detach e1c23e848810902f3b00365876d629a2470a8efd || exit 2
echo "HEAD $(git -C $J/clone rev-parse HEAD)"
( cd $J/clone && bin/savio_venv.sh mount ) || exit 3
$J/venv_img/bin/python -I /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections/scripts/digests.py $J/clone /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections/results/digests.json
echo "DIGESTS DONE"
