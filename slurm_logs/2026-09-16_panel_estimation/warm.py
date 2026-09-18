import os, glob
from pathlib import Path
from lsms_library.local_tools import read_parquet_cache_waves
D = Path(os.path.expanduser("~/.local/share/lsms_library"))
rows=[]
for var in sorted(D.glob("*/var")):
    c = var.parent.name
    rec={}
    for t in ("food_acquired","household_roster","sample"):
        p = var/f"{t}.parquet"
        if not p.exists(): rec[t]=None; continue
        try: w = read_parquet_cache_waves(p)
        except Exception as e: w = f"ERR:{type(e).__name__}"
        rec[t]=w
    rows.append((c,rec))
print(f"{'country':<22}{'food_acq':<10}{'roster':<10}{'sample':<10}  roster waves")
print("-"*95)
for c,rec in rows:
    def n(w):
        if w is None: return "-"
        if isinstance(w,str): return w
        return str(len(w))
    hr = rec["household_roster"]
    waves = ",".join(sorted(hr)) if isinstance(hr,(list,set,tuple)) else str(hr)
    print(f"{c:<22}{n(rec['food_acquired']):<10}{n(hr):<10}{n(rec['sample']):<10}  {waves[:60]}")
