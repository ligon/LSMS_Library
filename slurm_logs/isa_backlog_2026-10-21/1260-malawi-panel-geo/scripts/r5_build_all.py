"""Cold-build every Malawi table into $OUT (one pickle per table) + the
cluster_features wave frames.  Idempotent: skips tables already written.
Prints shapes / errors only -- never data."""
import os as _os, lsms_library as _ll
_top = _os.path.realpath(_os.environ.get("CLONE") or _os.getcwd())
assert _os.path.realpath(_ll.__file__).startswith(_top + _os.sep), _ll.__file__

import os, sys, time, traceback, warnings
import pandas as pd
import lsms_library as ll
from lsms_library.diagnostics import load_feature
out = sys.argv[1]; os.makedirs(out, exist_ok=True)
log = open(os.path.join(out, "build.log"), "a")
def say(*a):
    print(*a, file=log, flush=True); print(*a, flush=True)
warnings.simplefilter("ignore")
c = ll.Country("Malawi")
tables = sorted(c.data_scheme, key=lambda t: (t != "cluster_features", t))
for t in tables:
    p = os.path.join(out, f"{t}.pkl")
    if os.path.exists(p):
        say("skip (done)", t); continue
    t0 = time.time()
    try:
        df = load_feature(c, t)
        if isinstance(df, pd.DataFrame):
            df = df.copy(); df.attrs = {}
        pd.to_pickle(df, p + ".tmp"); os.replace(p + ".tmp", p)
        shape = getattr(df, "shape", len(df) if hasattr(df, "__len__") else None)
        say(f"OK {t} {shape} {time.time()-t0:.0f}s")
    except Exception as e:  # noqa: BLE001
        say(f"ERR {t} {type(e).__name__} {time.time()-t0:.0f}s")
        with open(os.path.join(out, f"{t}.err"), "w") as f:
            traceback.print_exc(file=f)
for w in c.waves:
    p = os.path.join(out, f"wf_cluster_features_{w}.pkl")
    if os.path.exists(p):
        continue
    try:
        wf = c[w].grab_data("cluster_features"); wf = wf.copy(); wf.attrs = {}
        pd.to_pickle(wf, p); say(f"OK wf {w} {wf.shape}")
    except Exception as e:  # noqa: BLE001
        say(f"ERR wf {w} {type(e).__name__}")
say("BUILD DONE")
