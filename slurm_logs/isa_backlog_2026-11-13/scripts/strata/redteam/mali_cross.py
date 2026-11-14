"""Tabulate Mali 2018-19 -> 2021-22 stratum changes on the served all-wave sample (candidate pickle)."""
import sys
import pandas as pd
f = pd.read_pickle(sys.argv[1]).reset_index()
a = f[f.t == "2018-19"].drop_duplicates("i").set_index("i")
b = f[f.t == "2021-22"].drop_duplicates("i").set_index("i")
both = a.index.intersection(b.index)
x = pd.DataFrame({"s18": a.loc[both, "strata"].astype(str), "s21": b.loc[both, "strata"].astype(str),
                  "v18": a.loc[both, "v"].astype(str), "v21": b.loc[both, "v"].astype(str)})
ch = x[x.s18 != x.s21]
print("linked households:", len(x), "change stratum:", len(ch), "clusters (2021-22 v):", ch.v21.nunique())
print(ch.groupby(["s18", "s21"]).size().sort_values(ascending=False).to_string())
print("rows per wave:", f.groupby("t").size().to_dict())
