#!/bin/bash
# Verify the built wheel actually SHIPS the bundled credential files (GH #741).
# If s3_reader_creds.enc is missing from the wheel, the fix works in the repo
# and never reaches a single pip user -- the exact population #741 is about.
#
#SBATCH --job-name=lsms-wheel
#SBATCH --output=slurm_logs/release_v0.10.0/wheel_%j.log
set -uo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
IMG="$REPO/.venv.sqfs"
SQUASHFUSE=/usr/libexec/apptainer/bin/squashfuse_ll
MNT="/local/job$SLURM_JOB_ID/venv_img"
OUT="/local/job$SLURM_JOB_ID/wheel"

# Two Savio traps, both documented in CLAUDE.md, both hit by the first version
# of this script:
#
#   1. $REPO/.venv is a symlink to the SUBMITTING job's /local mount, which does
#      not exist on this node.  So mount our own image at this job's own path
#      and do NOT repoint the shared symlink -- that belongs to the persistent
#      condo slice and repointing it breaks the interactive session.
#   2. Do not invoke the `poetry` CLI.  With no usable venv it tries to CREATE
#      one, which failed here with "the destination . is not write-able at
#      /local", and pointing it at Lustre instead round-trips every import
#      through the MDS.  Driving the poetry-core build backend directly needs no
#      venv at all.  The version resolves to 0.0.0 without the
#      dynamic-versioning plugin; irrelevant, this checks FILE INCLUSION.
shim="/local/job$SLURM_JOB_ID/.fuseshim"
mkdir -p "$shim" "$MNT" "$OUT"; ln -sf /usr/bin/fusermount "$shim/fusermount3"
PATH="$shim:$PATH" "$SQUASHFUSE" "$IMG" "$MNT" || { echo "mount failed"; exit 1; }

cd "$REPO"
export PYTHONPATH="$REPO"
"$MNT/bin/python" - "$OUT" <<'PY'
import sys, zipfile
from poetry.core.masonry.api import build_wheel
out = sys.argv[1]
name = build_wheel(out)
names = zipfile.ZipFile(f"{out}/{name}").namelist()
print("built:", name, "| entries:", len(names))
rc = 0
for f in ("s3_reader_creds.enc", "s3_reader_creds.gpg"):
    hit = [n for n in names if n.endswith(f)]
    print(f"  {f:26s} -> {'SHIPPED: ' + hit[0] if hit else '*** MISSING ***'}")
    if not hit:
        rc = 1
sys.exit(rc)
PY
rc=$?
echo "=== wheel check exit=$rc ==="
fusermount -u "$MNT" 2>/dev/null
exit $rc
