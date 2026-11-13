"""For each held sidecar in a Panel dir, compare its md5 to the same-named zip member (streamed, nothing written)."""
import sys, zipfile, hashlib, re
from pathlib import Path
zpath, panel = sys.argv[1], Path(sys.argv[2])
held = {}
for sc in panel.glob('*.dvc'):
    t = sc.read_text()
    held[sc.name[:-4].lower()] = (re.search(r'md5: (\w+)', t).group(1), int(re.search(r'size: (\d+)', t).group(1)))
same = diff = absent = 0
diffs = []
with zipfile.ZipFile(zpath) as zf:
    members = {Path(i.filename).name.lower(): i for i in zf.infolist()}
    for name, (md5, size) in sorted(held.items()):
        i = members.get(name)
        if i is None:
            absent += 1; diffs.append((name, 'absent-from-zip')); continue
        h = hashlib.md5()
        with zf.open(i) as f:
            while c := f.read(1 << 20): h.update(c)
        if h.hexdigest() == md5: same += 1
        else: diff += 1; diffs.append((name, f'held size {size} zip size {i.file_size}'))
print(f'held sidecars {len(held)}: identical {same}, differ {diff}, absent from zip {absent}')
for d in diffs: print('  ', *d)
