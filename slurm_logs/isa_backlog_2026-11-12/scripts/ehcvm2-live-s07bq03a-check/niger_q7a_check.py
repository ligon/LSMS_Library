#!/usr/bin/env python
"""Niger 2018-19 side check: off-grid s07bq07a (last-purchase quantity) on rows
bought within 30 days.  Served Expenditure impact and whether the off-grid q7a
look derived (value / price).  Aggregates only.  Writes out_followup/Niger_q7a.json."""
import json, os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_s07bq03a as C  # noqa: E402
out = {"env": C.assert_env()}
from lsms_library import paths  # noqa: E402
res = {}
for wave, s07b, welf, s00 in C.WAVES["Niger"] + C.WAVES["Mali"][:1] + C.WAVES["Senegal"][:1]:
    country = "Niger" if "ner" in s07b else ("Mali" if "mli" in s07b else "Senegal")
    df, W, S0, disc = C.load_wave(country, wave, s07b, welf, s00)
    d, raw = C.prepare(df, W, S0, disc)
    cls, _ = C.classify(d)
    c4, exp_c, keep = C.check4(d, raw, cls)
    q7 = d["s07bq07a"].to_numpy(); q8 = d["s07bq08"].to_numpy()
    off7 = (q7 > 0) & ~C.on_grid(q7)
    val = keep & ~np.isnan(exp_c)
    tot = float(np.nansum(exp_c[val]))
    uv = np.where(q7 > 0, q8 / np.where(q7 > 0, q7, np.nan), np.nan)
    integral_uv = np.abs(uv - np.round(uv)) < 1e-3
    mult5_uv = np.abs(uv / 5 - np.round(uv / 5)) < 1e-3
    on7 = (q7 > 0) & C.on_grid(q7)
    # within (item, unit, size) unit-value ratio off/on
    f = pd.DataFrame({"item": d["item"], "u": d["u7k"].astype(object).fillna("NA"), "s": d["s7k"].astype(object).fillna("NA"),
                      "uv": uv, "off": off7, "on": on7})
    a = f[f["off"]].groupby(["item", "u", "s"])["uv"].median()
    b = f[f["on"]].groupby(["item", "u", "s"])["uv"].median()
    jn = pd.concat([a.rename("off"), b.rename("on")], axis=1, join="inner").dropna()
    jn = jn[(jn["on"] > 0)]
    dec = C.decimals_repr(q7[off7]) if off7.any() else np.array([])
    res[f"{country} {wave}"] = {
        "q7a_pos": int((q7 > 0).sum()), "q7a_offgrid": int(off7.sum()),
        "served_valued_rows": int(val.sum()),
        "served_valued_rows_q7a_offgrid_share": C.share(int((val & off7).sum()), int(val.sum())),
        "served_expenditure_share_q7a_offgrid": C.share(float(np.nansum(exp_c[val & off7])), tot),
        "unit_value_integral_share_offgrid": C.share(int((off7 & integral_uv).sum()), int(off7.sum())),
        "unit_value_integral_share_ongrid": C.share(int((on7 & integral_uv).sum()), int(on7.sum())),
        "unit_value_multiple_of_5_share_offgrid": C.share(int((off7 & mult5_uv).sum()), int(off7.sum())),
        "unit_value_multiple_of_5_share_ongrid": C.share(int((on7 & mult5_uv).sum()), int(on7.sum())),
        "q8_multiple_of_5_share_offgrid": C.share(int((off7 & (np.abs(q8 / 5 - np.round(q8 / 5)) < 1e-6)).sum()), int(off7.sum())),
        "cells_compared": int(len(jn)),
        "median_offgrid_over_ongrid_unit_value": round(float((jn["off"] / jn["on"]).median()), 3) if len(jn) else None,
        "p10_p90": [round(float((jn["off"] / jn["on"]).quantile(q)), 3) for q in (0.1, 0.9)] if len(jn) else None,
        "offgrid_decimals": {str(k): int(v) for k, v in pd.Series(dec).value_counts().sort_index().items()},
        "offgrid_by_vague": {str(v): C.share(int((off7 & (d["vague"].to_numpy() == v)).sum()), int(((q7 > 0) & (d["vague"].to_numpy() == v)).sum())) for v in (1.0, 2.0)},
        "offgrid_grappe_share_p10_p50_p90": [round(float(x), 1) for x in pd.Series(off7[q7 > 0]).groupby(d.loc[q7 > 0, "grappe"].to_numpy()).mean().mul(100).quantile([0.1, 0.5, 0.9])],
    }
out["waves"] = res
json.dump(out, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "out_followup", "Niger_q7a.json"), "w"), indent=1, default=str)
print(json.dumps(out, indent=1, default=str))
