#!/bin/bash
# xargs worker: $1 = "sidecar<TAB>blob"
D=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/dta_encoding
PY=${PROBE_PY:-/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.venv.lustre/bin/python}
IFS=$'\t' read -r a b <<< "$1"
"$PY" "$D/probe_one.py" "$a" "$b"
