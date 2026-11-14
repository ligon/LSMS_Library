#!/usr/bin/env python
"""Niger 2018-19: is an off-grid s07bq07a = s07bq08 / a reference price?  If so the
unit value s07bq08/s07bq07a repeats across households with DIFFERENT s07bq08 inside a
(item, unit, size[, region]) cell.  Aggregates only.  Writes out_followup/Niger_q7a_uv.json."""
import json, os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_s07bq03a as C  # noqa: E402
out = {"env": C.assert_env(), "waves": {}}
for country, (wave, s07b, welf, s00) in [("Niger", C.WAVES["Niger"][0]), ("Niger", C.WAVES["Niger"][1]), ("Senegal", C.WAVES["Senegal"][0])]:
    df, W, S0, disc = C.load_wave(country, wave, s07b, welf, s00)
    d, raw = C.prepare(df, W, S0, disc)
    q7 = d["s07bq07a"].to_numpy(); q8 = d["s07bq08"].to_numpy()
    ok = (q7 > 0) & (q8 > 0)
    off = ok & ~C.on_grid(q7); on = ok & C.on_grid(q7)
    f = pd.DataFrame({"item": d["item"].astype(object).fillna("NA"), "u": d["u7k"].astype(object).fillna("NA"),
                      "s": d["s7k"].astype(object).fillna("NA"), "reg": d["region"].astype(object).fillna("NA"),
                      "uv": C.sigkey(np.where(ok, q8 / np.where(q7 > 0, q7, np.nan), np.nan), 5),
                      "q8": q8})
    res = {}
    for name, keys in (("item_unit_size", ["item", "u", "s"]), ("item_unit_size_region", ["item", "u", "s", "reg"])):
        g = f.groupby(keys + ["uv"], dropna=False)
        n = g["q8"].transform("size").to_numpy(); nq = g["q8"].transform("nunique").to_numpy()
        rep = (n >= 3) & (nq >= 3) & pd.notna(f["uv"]).to_numpy()
        res[name] = {"offgrid_repeat_share": C.share(int((rep & off).sum()), int(off.sum())),
                     "ongrid_repeat_share": C.share(int((rep & on).sum()), int(on.sum()))}
    vc = pd.Series(f.loc[off, "uv"]).value_counts().head(8)
    res["offgrid_rows"] = int(off.sum()); res["ongrid_rows"] = int(on.sum())
    res["offgrid_top_unit_values"] = {repr(float(k)): int(v) for k, v in vc.items()}
    res["offgrid_q8_multiple_of_25_share"] = C.share(int((off & (np.abs(q8 / 25 - np.round(q8 / 25)) < 1e-6)).sum()), int(off.sum()))
    res["ongrid_q8_multiple_of_25_share"] = C.share(int((on & (np.abs(q8 / 25 - np.round(q8 / 25)) < 1e-6)).sum()), int(on.sum()))
    out["waves"][f"{country} {wave}"] = res
json.dump(out, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "out_followup", "Niger_q7a_uv.json"), "w"), indent=1, default=str)
print(json.dumps(out, indent=1, default=str))
