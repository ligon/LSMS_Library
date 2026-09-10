"""Re-warm the shared L2 cache for the countries whose config changed today.
Reads through the API so stale hashes rebuild; panel_ids/updated_ids via
diagnostics.load_feature.  Usage: python rewarm.py Niger Malawi"""
import sys, time, warnings, traceback
warnings.simplefilter("ignore")
import lsms_library as ll
from lsms_library import diagnostics
for name in sys.argv[1:]:
    c = ll.Country(name)
    for tbl in sorted(c.data_scheme):
        t0 = time.time()
        try:
            df = diagnostics.load_feature(c, tbl)
            n = len(df) if hasattr(df, "__len__") else "-"
            print(f"{name:10s} {tbl:28s} OK   rows={n:>10} {time.time()-t0:7.1f}s", flush=True)
        except Exception as e:
            print(f"{name:10s} {tbl:28s} FAIL {type(e).__name__}: {str(e)[:120]} {time.time()-t0:7.1f}s", flush=True)
print("REWARM DONE", flush=True)
