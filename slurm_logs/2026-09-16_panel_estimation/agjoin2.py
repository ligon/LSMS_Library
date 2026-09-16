import warnings, sys; warnings.filterwarnings("ignore")
import lsms_library as ll, pandas as pd
PL=('plot_id','plot')
def plev(df):
    for p in PL:
        if p in df.index.names: return p
    return None
for c in sys.argv[1:]:
    print("="*72); print(c, flush=True)
    C=ll.Country(c); got={}
    for t in ("crop_production","plot_features","plot_inputs","plot_labor"):
        try:
            df=getattr(C,t)(); got[t]=df
            print(f"  {t:<16} rows={len(df):>9,}  plotlev={plev(df)!s:<8} idx={list(df.index.names)}", flush=True)
        except Exception as e:
            print(f"  {t:<16} FAIL {type(e).__name__}: {str(e)[:60]}", flush=True)
    cp=got.get("crop_production")
    if cp is None: continue
    f=cp.index.to_frame(index=False)
    g=f[['i','t']].drop_duplicates().groupby('i')['t'].nunique()
    print(f"  --> ag hh {len(g):,} | >=2w {int((g>=2).sum()):,} | >=3w {int((g>=3).sum()):,} | maxT {int(g.max())}", flush=True)
    pc=plev(cp)
    for other in ("plot_features","plot_inputs","plot_labor"):
        o=got.get(other)
        if o is None: continue
        po=plev(o)
        if pc is None or po is None:
            print(f"  join {other:<14} NO PLOT LEVEL (cp={pc}, {other}={po}) -> household-grain only", flush=True); continue
        L=set(map(tuple,cp.index.to_frame(index=False)[['i','t',pc]].drop_duplicates().values))
        R=set(map(tuple,o.index.to_frame(index=False)[['i','t',po]].drop_duplicates().values))
        print(f"  join {other:<14} cp_keys={len(L):,} match={len(L&R):,} ({100*len(L&R)/max(len(L),1):.1f}%)", flush=True)
