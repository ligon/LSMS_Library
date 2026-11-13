"""md5 of named zip members, streamed in memory (nothing written)."""
import sys, zipfile, hashlib
with zipfile.ZipFile(sys.argv[1]) as zf:
    for m in sys.argv[2:]:
        h = hashlib.md5()
        with zf.open(m) as f:
            while c := f.read(1 << 20):
                h.update(c)
        print(m, h.hexdigest())
