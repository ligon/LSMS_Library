"""GH #1237 default-backend equality: build tables in a fresh root and pickle
the API frames to a PRIVATE directory (never committed).  Prints aggregates and
every make/script subprocess the library launched (to show which wave targets
it asks for).

Usage: python eq_build.py <checkout> <outdir> <table> [<table> ...]
"""
import json
import os
import pickle
import sys
import time
import warnings

checkout, outdir, tables = sys.argv[1], sys.argv[2], sys.argv[3:]
import lsms_library as ll  # noqa: E402
import lsms_library.country as C  # noqa: E402
from lsms_library.paths import data_root  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
os.makedirs(outdir, exist_ok=True)
root = str(data_root("Malawi"))
calls = []
_run = C.subprocess.run


def spy(cmd, *a, **k):
    if isinstance(cmd, list):
        calls.append(" ".join(str(x).replace(root, "<root>/Malawi") for x in cmd))
    return _run(cmd, *a, **k)


C.subprocess.run = spy
warnings.simplefilter("ignore")
c = ll.Country("Malawi")
for t in tables:
    calls.clear()
    t0 = time.time()
    df = getattr(c, t)()
    with open(os.path.join(outdir, f"{t}.pkl"), "wb") as f:
        pickle.dump(df, f)
    print("EQ " + json.dumps({"table": t, "rows": int(len(df)),
                              "cols": list(map(str, df.columns)),
                              "index": list(map(str, df.index.names)),
                              "seconds": round(time.time() - t0, 1),
                              "subprocess_calls": list(calls)}))
