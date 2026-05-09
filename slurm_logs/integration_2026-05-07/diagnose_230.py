#!/usr/bin/env python
"""Diagnose where the #230 fix is breaking.

For each failing country: inspect the cached roster parquet, then call
the API at three levels and report shape + v state at each layer.
"""
import os
import sys

import pandas as pd

import lsms_library as ll
from lsms_library.country import data_root


COUNTRIES = ["Guyana", "Azerbaijan", "Serbia and Montenegro"]


def describe_v(df, label):
    """Return a one-liner describing v presence/state on df."""
    in_idx = isinstance(df.index, pd.MultiIndex) and "v" in df.index.names
    in_cols = "v" in df.columns
    if in_idx:
        vals = df.index.get_level_values("v")
        n_nan = int(vals.isna().sum())
        n_total = len(vals)
        n_uniq = vals.nunique(dropna=True)
        return f"{label}: v in INDEX, {n_nan}/{n_total} NaN, {n_uniq} unique non-NaN"
    if in_cols:
        n_nan = int(df["v"].isna().sum())
        n_total = len(df)
        n_uniq = df["v"].nunique(dropna=True)
        return f"{label}: v in COLUMN, {n_nan}/{n_total} NaN, {n_uniq} unique non-NaN"
    return f"{label}: v ABSENT"


def main():
    print(f"lsms_library: {ll.__file__}")
    print(f"data_root: {data_root()}")
    print()

    for name in COUNTRIES:
        print(f"\n{'='*70}\n{name}\n{'='*70}")

        # Layer 0: raw cache
        cache = data_root() / name / "var" / "household_roster.parquet"
        if cache.exists():
            df_cache = pd.read_parquet(cache)
            print(f"  cache: {cache}")
            print(f"    shape={df_cache.shape}, index={df_cache.index.names}, cols={list(df_cache.columns)[:6]}")
            print("    " + describe_v(df_cache, "cache"))
        else:
            print(f"  cache: MISSING ({cache})")

        # Layer 1: sample()
        try:
            c = ll.Country(name)
            samp = c.sample()
            print("  " + describe_v(samp, "sample()"))
            print(f"    sample shape={samp.shape}, index={samp.index.names}")
        except Exception as e:
            print(f"  sample(): FAIL {type(e).__name__}: {e}")
            continue

        # Layer 2: household_roster() (the fix should make v populated here)
        try:
            ros = c.household_roster()
            print("  " + describe_v(ros, "household_roster()"))
            print(f"    roster shape={ros.shape}, index={ros.index.names}")
            print(f"    roster cols={list(ros.columns)}")
        except Exception as e:
            print(f"  household_roster(): FAIL {type(e).__name__}: {e}")
            import traceback; traceback.print_exc()
            continue

        # Layer 3: household_characteristics()
        try:
            hc = c.household_characteristics()
            print(f"  household_characteristics(): shape={hc.shape}, index={hc.index.names}")
        except Exception as e:
            print(f"  household_characteristics(): FAIL {type(e).__name__}: {e}")
            import traceback; traceback.print_exc()


if __name__ == "__main__":
    main()
