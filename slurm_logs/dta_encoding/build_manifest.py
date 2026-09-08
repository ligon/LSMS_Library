"""Join every .dta DVC sidecar to its L1 blob, tagging declared vs undeclared.

Writes manifest.tsv:  sidecar_relpath \t blob_path \t declared(0/1)
"""
import json, re, sys
from pathlib import Path
REPO = Path('/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library')
CACHE = Path('/global/home/users/ligon/.local/share/lsms_library/dvc-cache')

declared = set()
d = json.load(open(REPO/'slurm_logs/dta_encoding/declared.json'))
for r in d['manifest']:
    f = r.get('file')
    if f:
        declared.add(Path(str(f)).name.lower())
print(f"declared source basenames: {len(declared)}", file=sys.stderr)

rows, n_cached, n_missing = [], 0, 0
for sc in sorted(REPO.glob('lsms_library/countries/*/*/Data/**/*.dta.dvc')):
    try:
        m = re.search(r'md5:\s*([0-9a-f]{32})', sc.read_text())
    except Exception:
        continue
    if not m:
        continue
    md5 = m.group(1)
    blob = CACHE/md5[:2]/md5[2:]
    rel = str(sc.relative_to(REPO))
    dec = int(sc.name[:-4].lower() in declared)     # strip '.dvc'
    if blob.exists():
        n_cached += 1
        rows.append(f"{rel}\t{blob}\t{dec}")
    else:
        n_missing += 1
out = REPO/'slurm_logs/dta_encoding/manifest.tsv'
out.write_text("\n".join(rows) + "\n")
print(f"cached (swept): {n_cached}   uncached (NOT swept): {n_missing}", file=sys.stderr)
print(f"  of swept, declared: {sum(int(r.split(chr(9))[2]) for r in rows)}", file=sys.stderr)
