#!/bin/bash
#SBATCH --account=fc_jevons
#SBATCH --partition=savio4_htc
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=00:02:00
#SBATCH --job-name=fc_jevons_probe
#SBATCH --output=slurm_logs/test_2026-04-10/fc_jevons_probe_%j.out
#SBATCH --error=slurm_logs/test_2026-04-10/fc_jevons_probe_%j.err

# Diagnostic probe: does fc_jevons + --cpus-per-task=8 on savio4_htc
# yield an 8-core shared allocation or a whole 56-core node?
# Cost: ~0.05 SUs (2 min wall × 8 cpus × multiplier, or 2 min × 56 cpus
# if whole-node).  Either way, negligible.

set -euo pipefail

echo "=== Submission request ==="
echo "Account:        $SLURM_JOB_ACCOUNT"
echo "Partition:      $SLURM_JOB_PARTITION"
echo "QoS:            ${SLURM_JOB_QOS:-<default>}"
echo "cpus-per-task:  $SLURM_CPUS_PER_TASK (from --cpus-per-task)"
echo

echo "=== What Slurm actually gave us ==="
echo "Node:               $(uname -n)"
echo "SLURM_CPUS_PER_TASK: $SLURM_CPUS_PER_TASK"
echo "SLURM_CPUS_ON_NODE:  ${SLURM_CPUS_ON_NODE:-unset}"
echo "SLURM_MEM_PER_NODE:  ${SLURM_MEM_PER_NODE:-unset}"
echo "SLURM_MEM_PER_CPU:   ${SLURM_MEM_PER_CPU:-unset}"
echo

echo "=== Linux view of the allocation ==="
# nproc returns cgroup-effective CPU count on modern Linux
echo "nproc:                $(nproc)"
# os.cpu_count() in Python ignores cgroups — the pytest-xdist -n auto trap
echo "python os.cpu_count(): $(python3 -c 'import os; print(os.cpu_count())' 2>&1 || echo "n/a")"
# Exact cpuset mask the kernel scheduled us onto
echo "Cpus_allowed_list:    $(grep Cpus_allowed_list /proc/self/status | awk '{print $2}')"
# cgroup cpuset file
echo "cpuset.cpus cgroup:   $(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || cat /sys/fs/cgroup/cpuset/cpuset.cpus 2>/dev/null || echo "unknown")"
echo

echo "=== Memory view ==="
free -h | head -2
echo

echo "=== Physical node capacity (from lscpu) ==="
lscpu | grep -E '^CPU\(s\)|^Core\(s\)|^Socket|^Thread|^Model name' 2>&1 | head -6
echo

echo "=== Verdict hint ==="
if [ "${SLURM_CPUS_ON_NODE:-0}" = "${SLURM_CPUS_PER_TASK:-0}" ]; then
    echo "SHARED partition: SLURM_CPUS_ON_NODE == SLURM_CPUS_PER_TASK == $SLURM_CPUS_PER_TASK"
    echo "We got the 8 cores we asked for; other users may be on the same node."
else
    echo "WHOLE-NODE: SLURM_CPUS_ON_NODE ($SLURM_CPUS_ON_NODE) != SLURM_CPUS_PER_TASK ($SLURM_CPUS_PER_TASK)"
    echo "We got the entire node regardless of --cpus-per-task."
fi

echo
echo "=== Probe complete $(date +%H:%M:%S) ==="
