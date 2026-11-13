"""Which IHS3 (2010-11) EAs reappear in later cluster_features, by kind, and
how far their served coordinate moves.  Counts and km only."""
import sys
import numpy as np, pandas as pd
cf = pd.read_pickle(sys.argv[1])
def fx(w):
    x = cf.xs(w, level="t").copy(); x.index = x.index.astype(str); return x
w10, w13, w16, w19 = (fx(w) for w in ("2010-11", "2013-14", "2016-17", "2019-20"))
wf16 = pd.read_pickle(sys.argv[2]).reset_index(); wf19 = pd.read_pickle(sys.argv[3]).reset_index()
def panel_eas(wf):
    s = wf["i"].astype(str); p = ~s.str.startswith("cs-") & s.str.contains("-")
    v = wf["v"].astype(str); return set(v[p]), set(v[~p])
p16, x16 = panel_eas(wf16); p19, x19 = panel_eas(wf19)
def km(a, b, vs):
    vs = sorted(vs)
    la1, lo1, la2, lo2 = (np.radians(f.loc[vs, c].astype(float).to_numpy()) for f, c in ((a, "Latitude"), (a, "Longitude"), (b, "Latitude"), (b, "Longitude")))
    return 6371.0 * np.arccos(np.clip(np.sin(la1)*np.sin(la2) + np.cos(la1)*np.cos(la2)*np.cos(lo2-lo1), -1, 1))
v10, v13 = set(w10.index), set(w13.index)
cont = v13 & set(w16.index) & set(w19.index)
for name, w, pe, xe in (("2016-17", w16, p16, x16), ("2019-20", w19, p19, x19)):
    again = v10 & set(w.index)
    groups = {
        "IHPS panel EAs (continuing)": again & cont,
        "short-term 2013-14 EAs, cross-section only": (again & (v13 - cont)),
        "IHS3-only EAs drawn again by the cross-section": again - v13,
    }
    print(f"{name}: IHS3 EAs present {len(again)}")
    for g, vs in groups.items():
        if not vs:
            print(f"  {g}: 0"); continue
        d = km(w10, w, vs)
        n_panel_hh = len(vs & pe); n_cs = len(vs & xe)
        print(f"  {g}: {len(vs)} EAs (with panel households {n_panel_hh}, with cross-sectional households {n_cs}); "
              f"km from 2010-11 coordinate: min {d.min():.2f} median {np.median(d):.2f} max {d.max():.2f}; <0.001 km: {int((d < 0.001).sum())}")
