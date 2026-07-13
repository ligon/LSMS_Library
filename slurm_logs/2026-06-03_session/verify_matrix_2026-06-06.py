#!/usr/bin/env python
"""Observed (country x feature) coverage for the 4 matrix-fill features.

Verification of SESSION_SUMMARY.md claims against the live cache, 2026-06-06.
Reports, per feature: declared countries (data_scheme) vs observed countries
(actual Feature() build) + row counts. Read-only.
"""
import sys, time, traceback
import lsms_library as ll

FEATURES = ["plot_features", "interview_date", "assets", "individual_education"]

# Summary's post-sweep claims (countries) for cross-check.
CLAIMED = {"plot_features": 12, "interview_date": 20, "assets": 12, "individual_education": 16}

print(f"lsms_library {getattr(ll, '__version__', '?')}\n")

for f in FEATURES:
    t0 = time.time()
    try:
        df = ll.Feature(f)()
        dt = time.time() - t0
        if "country" in df.index.names:
            countries = sorted(df.index.get_level_values("country").unique())
        else:
            countries = ["<no country index level>"]
        n = len(countries)
        claimed = CLAIMED.get(f)
        flag = "" if claimed is None else (" OK" if n == claimed else f"  <-- claimed {claimed}")
        print(f"### {f}: {n} countries, {len(df):,} rows  ({dt:.1f}s){flag}")
        print(f"    {', '.join(countries)}\n")
    except Exception as e:
        dt = time.time() - t0
        print(f"### {f}: ERROR after {dt:.1f}s -> {type(e).__name__}: {e}")
        traceback.print_exc()
        print()
