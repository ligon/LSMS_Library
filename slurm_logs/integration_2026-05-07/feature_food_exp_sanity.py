#!/usr/bin/env python
"""Sanity-check Feature('food_expenditures')() — cross-country dataframe."""
import sys
import time
import warnings

import numpy as np
import pandas as pd

import lsms_library as ll


def main():
    print(f"=== Feature('food_expenditures')() sanity check {time.strftime('%H:%M:%S')} ===")
    print(f"lsms_library: {ll.__file__}")
    print()

    feat = ll.Feature("food_expenditures")
    t0 = time.time()
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        df = feat()
    elapsed = time.time() - t0
    print(f"feat() built in {elapsed:.1f}s")
    print(f"warnings raised: {len(ws)}")
    for w in ws[:20]:
        print(f"  - {w.category.__name__}: {str(w.message)[:120]}")
    if len(ws) > 20:
        print(f"  ... and {len(ws) - 20} more")
    print()

    # Basic shape
    print("=== shape / index / columns ===")
    print(f"  shape: {df.shape}")
    print(f"  index names: {df.index.names}")
    print(f"  index nlevels: {df.index.nlevels}")
    print(f"  columns: {list(df.columns)}")
    print(f"  dtypes:\n{df.dtypes}")
    print()

    # Per-country coverage
    print("=== per-country coverage ===")
    if "country" in df.index.names:
        per_country = df.groupby(level="country").agg(
            n_rows=("country", lambda x: len(x)) if "country" in df.columns else (df.columns[0], "size"),
        )
        per_country = df.groupby(level="country").size().to_frame("n_rows")
        # Distinct waves per country
        if "t" in df.index.names:
            waves_per = (
                df.reset_index()
                .groupby("country")["t"]
                .nunique()
                .to_frame("n_waves")
            )
            per_country = per_country.join(waves_per)
        # Distinct items (j) per country
        if "j" in df.index.names:
            items_per = (
                df.reset_index().groupby("country")["j"].nunique().to_frame("n_items")
            )
            per_country = per_country.join(items_per)
        # Distinct households (i) per country
        if "i" in df.index.names:
            hhs_per = (
                df.reset_index().groupby("country")["i"].nunique().to_frame("n_hhs")
            )
            per_country = per_country.join(hhs_per)
        print(per_country.to_string())
    else:
        print("  ! 'country' not in index names — Feature() may have not prepended it")
    print()

    # Numeric value sanity per country
    print("=== numeric column distributions per country ===")
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not num_cols:
        print("  ! no numeric columns")
    else:
        col = num_cols[0]
        print(f"  examining column: {col}")
        per_country_stats = df.groupby(level="country")[col].agg(
            ["count", "mean", "std", "min", "median", "max"]
        )
        per_country_stats["n_nan"] = df.groupby(level="country")[col].apply(
            lambda s: s.isna().sum()
        )
        per_country_stats["n_neg"] = df.groupby(level="country")[col].apply(
            lambda s: (s < 0).sum()
        )
        per_country_stats["n_zero"] = df.groupby(level="country")[col].apply(
            lambda s: (s == 0).sum()
        )
        with pd.option_context("display.width", 200, "display.max_columns", 20):
            print(per_country_stats.to_string())
    print()

    # Top food items by total expenditure (across countries)
    print("=== top food items (j) by total expenditure ===")
    if "j" in df.index.names and num_cols:
        col = num_cols[0]
        top_j = (
            df[col]
            .groupby(level="j")
            .sum()
            .sort_values(ascending=False)
            .head(20)
        )
        print(top_j.to_string())
    print()

    # Quick sample of head and tail
    print("=== head(3) ===")
    with pd.option_context("display.width", 200, "display.max_columns", 10):
        print(df.head(3))
    print()
    print("=== tail(3) ===")
    with pd.option_context("display.width", 200, "display.max_columns", 10):
        print(df.tail(3))


if __name__ == "__main__":
    main()
