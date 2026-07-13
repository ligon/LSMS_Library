#!/usr/bin/env python
"""Functional verification of the 4 clean coverage cells (2026-06-06).

Reads go through get_dataframe()'s lock-free bypass, so this is safe to run
concurrently with anything else. Forces a fresh build (LSMS_NO_CACHE) since
these features are newly declared.
"""
import os
os.environ["LSMS_NO_CACHE"] = "1"
import traceback
import lsms_library as ll
from lsms_library import diagnostics as dx

CELLS = [
    ("Guatemala", "assets"),
    ("Guatemala", "individual_education"),
    ("China", "individual_education"),
    ("Azerbaijan", "individual_education"),
]

for country, feature in CELLS:
    print(f"\n===== {country} / {feature} =====")
    try:
        c = ll.Country(country)
        df = dx.load_feature(c, feature)
        print(f"  shape={df.shape}  index.names={list(df.index.names)}  cols={list(df.columns)}")
        # key-column NaN fraction
        for col in df.columns:
            frac = float(df[col].isna().mean())
            print(f"    {col}: NaN={frac:.1%}")
        rep = dx.is_this_feature_sane(df, country, feature)
        bad = [ck for ck in rep.checks if getattr(ck, "status", "") not in ("pass", "ok", "PASS", "OK")]
        print(f"  is_this_feature_sane.ok = {rep.ok}")
        for ck in bad:
            print(f"    [{getattr(ck,'status','?')}] {getattr(ck,'name','?')}: {getattr(ck,'message','')}")
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        traceback.print_exc()
