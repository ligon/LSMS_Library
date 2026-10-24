"""Part 2 diagnostics on raw Malawi modules (counts and crop codes only)."""
import os as _os, lsms_library as _ll
_top = _os.path.realpath(_os.environ.get("CLONE") or _os.getcwd())
assert _os.path.realpath(_ll.__file__).startswith(_top + _os.sep), _ll.__file__

import sys, warnings
import numpy as np, pandas as pd
warnings.simplefilter("ignore")
from lsms_library.paths import countries_root
from lsms_library.local_tools import get_dataframe, format_id
ROOT = countries_root() / "Malawi"
sys.path.insert(0, str(ROOT / "_"))
import malawi as M
cmap = M._malawi_code_map("harmonize_crop")
print("== 2016-17 garden/plot ids in G and P, both halves")
for half, files in (("xs", ("Cross_Sectional/ag_mod_g.dta", "Cross_Sectional/ag_mod_p.dta")),
                    ("panel", ("Panel/ag_mod_g_16.dta", "Panel/ag_mod_p_16.dta"))):
    for f in files:
        d = get_dataframe(str(ROOT / "2016-17" / "Data" / f), convert_categoricals=False)
        gid, pid = d["gardenid"].isna(), d["plotid"].isna()
        q = "ag_g13a" if "ag_g13a" in d.columns else "ag_p09a"
        cc = "crop_code" if "crop_code" in d.columns and "ag_p0c" not in d.columns else ("ag_p0c" if "ag_p0c" in d.columns else "crop_code")
        key = d["gardenid"].apply(format_id) + "_" + d["plotid"].apply(format_id)
        nk = key.isna()
        print(f"  {half} {f}: rows {len(d)}; gardenid NaN {int(gid.sum())}; plotid NaN {int(pid.sum())}; key NaN {int(nk.sum())}; "
              f"key-NaN rows with crop code {int((nk & d[cc].notna()).sum())}, with quantity {int((nk & pd.to_numeric(d[q], errors='coerce').notna()).sum())} "
              f"(>0: {int((nk & (pd.to_numeric(d[q], errors='coerce') > 0)).sum())}); households with key-NaN rows {d.loc[nk].groupby(d.columns[0]).ngroups if nk.any() else 0}")
        if nk.any():
            hhcol = "case_id" if "case_id" in d.columns else "y3_hhid"
            per = d.loc[nk].groupby(hhcol).size()
            print(f"    key-NaN rows per household: max {int(per.max())}; households {per.size}; rows by (hh, crop code) duplicated {int(d.loc[nk].duplicated([hhcol, cc]).sum())}")
            other = d.loc[~nk]
            print(f"    same households' other rows in this module: {int(d[hhcol].isin(per.index).sum()) - int(nk.sum())}")
print("== seed rows (Module G) with no crop label")
for w, f, cc, q in (("2010-11", "Full_Sample/Agriculture/ag_mod_g.dta", "ag_g0d", "ag_g04a"),
                    ("2019-20", "Cross_Sectional/ag_mod_g.dta", "crop_code", "ag_g04a")):
    d = get_dataframe(str(ROOT / w / "Data" / f), convert_categoricals=False)
    codes = pd.to_numeric(d[cc], errors="coerce").astype("Int64")
    lab = codes.map(cmap)
    qq = pd.to_numeric(d[q], errors="coerce")
    miss = lab.isna()
    print(f"  {w} {f}: no label {int(miss.sum())}; blank code {int(codes.isna().sum())}; unmapped code values (count, rows with seed qty>0): "
          f"{ {int(k): (int(v), int(((codes == k) & (qq > 0)).sum())) for k, v in codes[miss & codes.notna()].value_counts().sort_index().items()} }")
    print(f"    blank-code rows with seed qty>0: {int((codes.isna() & (qq > 0)).sum())}; of them with a harvest quantity ag_g13a>0: {int((codes.isna() & (qq > 0) & (pd.to_numeric(d['ag_g13a'], errors='coerce') > 0)).sum())}")
    try:
        d2 = get_dataframe(str(ROOT / w / "Data" / f), convert_categoricals=True)
        labs = d2.loc[(miss & codes.notna()).to_numpy(), cc].astype(str).value_counts().to_dict()
        print(f"    their Stata value labels: {labs}")
    except Exception as e:
        print("    labels unreadable", type(e).__name__)
