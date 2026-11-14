#!/usr/bin/env python
"""Diagnose the cells that got worse in the v0.17.0 regrade.

Prints column NAMES, row counts and check messages only -- no microdata values.
"""
import sys
import warnings

import pandas as pd

import lsms_library as ll
from lsms_library.diagnostics import is_this_feature_sane, load_feature

warnings.simplefilter("ignore")
C = sys.argv[1]
co = ll.Country(C, preload_panel_ids=False)


def report(feature, waves=None):
    df = load_feature(co, feature)
    print(f"\n=== {C}/{feature}: {len(df)} rows; index {list(df.index.names)}")
    print("columns:", list(df.columns))
    t = df.index.get_level_values("t").astype(str)
    print("rows by t:", dict(pd.Series(t).value_counts().sort_index()))
    allnull = [c for c in df.columns if df[c].isna().all()]
    print("all-null across the WHOLE country frame:", allnull)
    for c in df.columns:
        nn = df[c].notna().groupby(t).sum()
        print(f"  non-null by t  {c}: {dict(nn)}")
    populated = {c for c in df.columns if df[c].notna().any()}
    for w in (waves if waves is not None else sorted(set(t))):
        sl = df[t == str(w)]
        if not len(sl):
            print(f"  [{w}] 0 rows")
            continue
        rep = is_this_feature_sane(sl, C, feature, extra_optional=populated)
        for chk in rep.checks:
            if chk.status != "pass":
                print(f"  [{w}] {chk.status} {chk.name}: {chk.message}")
    return df


if C == "Malawi":
    report("plot_inputs")
elif C == "Nigeria":
    for q in ("2010Q3", "2011Q1", "2015Q3", "2016Q1"):
        print(q, "declares individual_education:",
              "individual_education" in co[q].data_scheme,
              "| community_prices:", "community_prices" in co[q].data_scheme)
    report("individual_education", waves=[])
    report("community_prices", waves=[])
elif C == "Uganda":
    report("plot_labor", waves=[])
