"""mutate.py <file> <old> <new>: replace exactly one occurrence (fails otherwise)."""
import sys
from pathlib import Path
p, old, new = Path(sys.argv[1]), sys.argv[2].encode().decode("unicode_escape"), sys.argv[3].encode().decode("unicode_escape")
s = p.read_text(); n = s.count(old)
assert n == 1, f"{p}: {n} occurrences of the mutation target"
p.write_text(s.replace(old, new)); print("mutated", p)
