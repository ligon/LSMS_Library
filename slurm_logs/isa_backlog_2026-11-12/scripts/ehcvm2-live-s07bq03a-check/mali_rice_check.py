#!/usr/bin/env python
"""Mali 2018-19: do the no-vague 'Riz local Gambiaka | kg' rows (q3 = 1.4 x hhsize)
reach the SERVED food_acquired, and are they summed into a household's own rice
row when `visit` is dropped?  Aggregates only.  Raw side via get_dataframe; served
side via Country('Mali').food_acquired(waves=['2018-19']) in the private root.
Writes out_followup/Mali_rice.json."""
import json
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_s07bq03a as C  # noqa: E402

out = {"env": C.assert_env()}
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library import paths  # noqa: E402

os.chdir(os.path.join(str(paths.countries_root()), "Mali", "2018-19", "_"))
df = get_dataframe("../Data/s07b_me_mli2018.dta")
W = get_dataframe("../Data/ehcvm_welfare_mli2018.dta")
q3 = C.num(df["s07bq03a"]).to_numpy()
q4 = C.num(df["s07bq04"]).fillna(0).to_numpy()
q5 = C.num(df["s07bq05"]).fillna(0).to_numpy()
item = df["s07bq01"].astype(object).map(C.label_text)
unit = df["s07bq03b"].astype(object).map(C.label_text)
nv = pd.isna(C.num(df["vague"])).to_numpy()
hh = (C.num(df["grappe"]).astype("Int64").astype(str) + "-" + C.num(df["menage"]).astype("Int64").astype(str)).to_numpy()
rice = item.fillna("").str.contains("gambiaka").to_numpy() & (unit.fillna("") == "kg").to_numpy()
pos = q3 > 0
blk = nv & pos
reg = ~nv & pos & rice
pq = np.clip(np.nan_to_num(q3) - q4 - q5, 0, None)
out["raw"] = {
    "block_rows": int(blk.sum()),
    "block_rice_kg_rows": int((blk & rice).sum()),
    "block_households": int(len(set(hh[blk]))),
    "regular_rice_kg_rows": int(reg.sum()),
    "regular_rice_kg_households": int(len(set(hh[reg]))),
    "households_with_both": int(len(set(hh[blk]) & set(hh[reg]))),
    "block_q3_sum_kg": round(float(np.nansum(q3[blk])), 1),
    "regular_rice_purchased_q_sum_kg": round(float(np.nansum(pq[reg])), 1),
    "regular_rice_q3_sum_kg": round(float(np.nansum(q3[reg])), 1),
    "regular_rice_produced_sum_kg": round(float(np.nansum(q4[reg])), 1),
    "rice_kg_households_union": int(len(set(hh[blk]) | set(hh[reg | (rice & ~nv & (q4 > 0))]))),
    "block_q7a_pos": int((blk & (C.num(df["s07bq07a"]).to_numpy() > 0)).sum()),
    "block_q8_pos": int((blk & (C.num(df["s07bq08"]).to_numpy() > 0)).sum()),
}
whh = W[["grappe", "menage", "hhsize"]].copy()
whh["k"] = (C.num(whh["grappe"]).astype("Int64").astype(str) + "-" + C.num(whh["menage"]).astype("Int64").astype(str))
hs = dict(zip(whh["k"], C.num(whh["hhsize"])))
r = np.array([q3[i] / hs.get(hh[i], np.nan) for i in np.flatnonzero(blk)])
out["raw"]["block_q3_over_hhsize_at_1.4"] = int((np.abs(r - 1.4) < 1e-6).sum())
out["raw"]["block_households_in_welfare"] = int(sum(1 for i in np.flatnonzero(blk) if hh[i] in hs))
del df

# served side
t0 = time.time()
import lsms_library as ll  # noqa: E402
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    fa = ll.Country("Mali").food_acquired(waves=["2018-19"])
out["served_seconds"] = round(time.time() - t0, 1)
out["served_warnings"] = sorted({f"{w.category.__name__}: {str(w.message)[:160]}" for w in caught})[:20]
f = fa.reset_index()
out["served_columns"] = list(fa.columns)
out["served_index"] = list(fa.index.names)
j = f["j"].astype(object).map(C.label_text).fillna("")
u = f["u"].astype(object).map(C.label_text).fillna("")
out["served_rice_labels"] = {str(k): int(v) for k, v in f.loc[j.str.contains("gambiaka"), ["j", "u"]].astype(str).value_counts().head(10).items()}
m = j.str.contains("gambiaka").to_numpy() & (u == "kg").to_numpy()
pur = (f["s"] == "purchased").to_numpy()
out["served"] = {
    "rows_total": int(len(f)), "purchased_rows": int(pur.sum()),
    "rice_kg_purchased_rows": int((m & pur).sum()),
    "rice_kg_purchased_quantity_sum": round(float(f.loc[m & pur, "Quantity"].sum()), 1),
    "rice_kg_purchased_expenditure_na": int(f.loc[m & pur, "Expenditure"].isna().sum()),
    "rice_kg_produced_rows": int((m & (f["s"] == "produced").to_numpy()).sum()),
    "rice_kg_households": int(f.loc[m, "i"].nunique()),
}
if "Quantity_kg" in f.columns:
    out["served"]["rice_kg_purchased_quantity_kg_sum"] = round(float(f.loc[m & pur, "Quantity_kg"].sum()), 1)
out["served"]["purchased_quantity_kg_sum_all"] = round(float(f.loc[pur, "Quantity_kg"].sum()), 1) if "Quantity_kg" in f.columns else None
dest = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out_followup", "Mali_rice.json")
json.dump(out, open(dest, "w"), indent=1, default=str)
print(json.dumps(out, indent=1, default=str))
