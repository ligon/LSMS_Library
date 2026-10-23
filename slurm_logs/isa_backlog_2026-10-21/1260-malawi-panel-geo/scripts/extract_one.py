"""Extract exactly one named member (flat, no path components) into an EMPTY dir."""
import sys, zipfile, hashlib, os, stat
from pathlib import Path, PurePosixPath
zpath, member, outdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
assert not any(outdir.iterdir()), f"{outdir} not empty"
with zipfile.ZipFile(zpath) as zf:
    info = zf.getinfo(member)
    rel = PurePosixPath(info.filename.replace('\\', '/'))
    assert len(rel.parts) == 1 and '..' not in rel.parts and not rel.is_absolute(), rel
    assert not stat.S_ISLNK(info.external_attr >> 16)
    dest = outdir / rel.name
    with zf.open(info) as src, open(dest, 'wb') as dst:
        while chunk := src.read(1 << 20):
            dst.write(chunk)
print(dest, dest.stat().st_size, 'md5', hashlib.md5(dest.read_bytes()).hexdigest())
