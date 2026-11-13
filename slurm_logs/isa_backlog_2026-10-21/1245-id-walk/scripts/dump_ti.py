"""Dump the served (t, i[, pid]) index of selected tables from a WARM root, plus
the 2021-22 cover mapping, for base/cand comparison of Burkina's served ids.
Usage: python dump_ti.py <checkout> <country> <out.pkl> tables...
Writes row-level ids to SCRATCH only (never committed)."""
import os, pickle, sys, warnings
warnings.simplefilter("ignore")
checkout, country, out = sys.argv[1:4]
tables = sys.argv[4:]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
c = ll.Country(country)
res = {}
for t in tables:
    df = getattr(c, t)()
    res[t] = df.index.to_frame(index=False).astype(str)
pickle.dump(res, open(out, "wb"))
print({t: len(v) for t, v in res.items()})
