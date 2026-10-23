"""GH #1059: base vs branch, from the probe's row-level frames.

Usage: python compare.py <rows_dir> <categorical_mapping.org> <out.json>

Claims checked (aggregates only in the output):
  * cluster_features: index, Region, Rural, Latitude, Longitude frame-equal;
    branch District == base District.replace(District table) row for row.
  * sample: frame-equal (it carries no District column).
  * _market_lookup('District'): branch m == base m mapped by the table.
  * the shared-grappe residue is exactly the 3 province disagreements.
"""
import json
import sys

import pandas as pd

from lsms_library.local_tools import df_from_orgfile

rows, org, out = sys.argv[1:4]
tbl = df_from_orgfile(org, name="District")
rdict = dict(zip(tbl["Alternate Spelling"].astype(str).str.strip(),
                 tbl["Preferred Label"].astype(str).str.strip()))

res = {"table_rows": len(rdict), "table_labels": len(set(rdict.values()))}
cb = pd.read_parquet(f"{rows}/cluster_features-base.parquet")
cn = pd.read_parquet(f"{rows}/cluster_features-branch.parquet")
res["cf_index_equal"] = bool(cb.index.equals(cn.index))
others = [c for c in cb.columns if c != "District"]
res["cf_columns_equal"] = list(cb.columns) == list(cn.columns)
try:
    pd.testing.assert_frame_equal(cb[others], cn[others])
    res["cf_non_district_frame_equal"] = True
except AssertionError as e:  # noqa: PERF203
    res["cf_non_district_frame_equal"] = str(e)[:300]
mapped = cb["District"].map(lambda s: rdict.get(s, s) if pd.notna(s) else s).astype(cb["District"].dtype)
res["cf_district_equals_mapped_base"] = bool(mapped.equals(cn["District"]))
res["cf_district_rows_relabelled"] = int((cb["District"] != cn["District"]).sum())
res["cf_district_rows_relabelled_per_wave"] = {
    t: int(n) for t, n in (cb["District"] != cn["District"]).groupby(level="t").sum().items()}
res["cf_district_null_base_branch"] = [int(cb["District"].isna().sum()), int(cn["District"].isna().sum())]
res["base_spellings_not_in_table"] = sorted(set(cb["District"].dropna()) - set(rdict))

sb = pd.read_parquet(f"{rows}/sample-base.parquet")
sn = pd.read_parquet(f"{rows}/sample-branch.parquet")
try:
    pd.testing.assert_frame_equal(sb, sn)
    res["sample_frame_equal"] = True
except AssertionError as e:
    res["sample_frame_equal"] = str(e)[:300]

mb = pd.read_parquet(f"{rows}/market_district-base.parquet")
mn = pd.read_parquet(f"{rows}/market_district-branch.parquet")
res["market_rows_base_branch"] = [len(mb), len(mn)]
mb2 = mb.assign(m=mb["m"].map(lambda s: rdict.get(s, s)))
res["market_m_equals_mapped_base"] = bool(
    mb2.sort_values(["t", "v"]).reset_index(drop=True).equals(mn.sort_values(["t", "v"]).reset_index(drop=True)))
res["market_m_distinct_pooled_base_branch"] = [int(mb["m"].nunique()), int(mn["m"].nunique())]

def residue(cf):
    f = cf.reset_index()
    a = f[f.t == "2018-19"].set_index("v")
    b = f[f.t == "2021-22"].set_index("v")
    sh = a.index.intersection(b.index)
    return len(sh), sorted((v, a.loc[v, "District"], b.loc[v, "District"])
                           for v in sh if a.loc[v, "District"] != b.loc[v, "District"])
nb, rb = residue(cb)
nn, rn = residue(cn)
res["shared_grappes"] = [nb, nn]
res["shared_differs_base_branch"] = [len(rb), len(rn)]
res["shared_residue_branch"] = rn
# Every base difference that the mapping does NOT erase must survive unchanged.
res["base_diffs_kept_are_province_pairs"] = sorted(
    (v, x, y) for v, x, y in rb if rdict.get(x, x) != rdict.get(y, y))
with open(out, "w") as f:
    json.dump(res, f, indent=1, ensure_ascii=False, default=str)
print(json.dumps(res, ensure_ascii=False, default=str, indent=1))
