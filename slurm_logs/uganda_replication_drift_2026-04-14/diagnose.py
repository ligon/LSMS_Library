"""
One-shot diagnostic: for each failing feature from the 2026-04-14 API-vs-
replication run, pull the rows where API and replication disagree and dump
them so the scrum master can design the fix.

Run from the repo root:

    .venv/bin/python slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py \
        2>&1 | tee slurm_logs/uganda_replication_drift_2026-04-14/diagnose.out

Features investigated (7 failed; fct already bumped):

  - household_roster        Sex dtype/encoding (all 35,493 rows)
  - household_characteristics  log HSize (1 row NaN/real mismatch)
  - shocks                  Shock (6 rows NaN/real mismatch)
  - food_prices             market (4 rows NaN/real mismatch)
  - food_quantities         quantity_home (10 rows NaN/real mismatch)
  - nutrition               Energy (2 rows |Δ| = 8200 outliers)

Each block prints:
  1. summary (counts, dtypes)
  2. the offending rows with API vs replication values side-by-side
  3. any per-feature hypothesis check
"""
from __future__ import annotations

import os
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

pd.set_option("display.max_columns", 80)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 60)

REPL_DIR = Path.home() / (
    "Projects/RiskSharing_Replication/external_data/"
    "LSMS_Library/lsms_library/countries/Uganda/var"
)


def _call_api(name: str, **kwargs) -> pd.DataFrame:
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return getattr(ll.Country("Uganda"), name)(**kwargs)


def _load_repl(name: str) -> pd.DataFrame:
    return pd.read_parquet(REPL_DIR / f"{name}.parquet", engine="pyarrow")


def _merged(api: pd.DataFrame, repl: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    common = [lev for lev in repl.index.names if lev in api.index.names]
    if not common:
        raise SystemExit(f"no common index levels: api={list(api.index.names)} "
                         f"repl={list(repl.index.names)}")
    m = api.reset_index().merge(
        repl.reset_index(), on=common, suffixes=("_api", "_repl")
    )
    return m, common


def banner(s: str) -> None:
    print("\n" + "=" * 78)
    print("  " + s)
    print("=" * 78)


# ------------------------------------------------------------- household_roster

def diagnose_household_roster() -> None:
    banner("household_roster: Sex mismatch on all 35,493 rows")
    api = _call_api("household_roster")
    repl = _load_repl("household_roster")

    # Apply the same transform the test uses
    api = api.drop(columns=["Generation", "Distance", "Affinity"], errors="ignore")
    api = api.rename(columns={"Relationship": "Relation"})
    if "v" in api.index.names:
        api = api.reset_index("v", drop=True)

    print(f"API  dtypes on shared cols:")
    print(api.dtypes.to_string())
    print(f"\nREPL dtypes on shared cols:")
    print(repl.dtypes.to_string())

    m, common = _merged(api, repl)
    print(f"\nMerged: {len(m)} rows on {common}")

    for c in ("Sex",):
        if f"{c}_api" not in m.columns:
            continue
        api_s, repl_s = m[f"{c}_api"], m[f"{c}_repl"]
        print(f"\n--- {c!r}")
        print(f"  api  dtype={api_s.dtype}  sample head: {list(api_s.head(5))}")
        print(f"  repl dtype={repl_s.dtype}  sample head: {list(repl_s.head(5))}")
        print(f"  api.astype(str).unique():  {sorted(set(api_s.astype(str)))[:10]}")
        print(f"  repl.astype(str).unique(): {sorted(set(repl_s.astype(str)))[:10]}")
        mismatch = (api_s.astype(str) != repl_s.astype(str)).sum()
        print(f"  {mismatch}/{len(m)} rows differ under .astype(str)")


# ---------------------------------------------------- household_characteristics

def diagnose_household_characteristics() -> None:
    banner("household_characteristics: log HSize 1-row NaN mismatch")
    api = _call_api("household_characteristics", market="Region")
    repl = _load_repl("household_characteristics")

    # Test applies column rename from female buckets etc; irrelevant for log HSize
    m, common = _merged(api, repl)
    api_s, repl_s = m["log HSize_api"], m["log HSize_repl"]
    bad = m[(api_s.isna() ^ repl_s.isna())]
    print(f"Total merged: {len(m)}   NaN-mismatched: {len(bad)}")
    if len(bad):
        cols = common + ["log HSize_api", "log HSize_repl"]
        if "HSize_api" in m.columns:
            cols += ["HSize_api", "HSize_repl"]
        print(bad[cols].to_string(max_rows=20))


# ------------------------------------------------------------------- shocks

def diagnose_shocks() -> None:
    banner("shocks: Shock 6-row NaN mismatch")
    api = _call_api("shocks", market="Region")
    repl = _load_repl("shocks")
    if "Shock" in api.index.names:
        api = api.reset_index("Shock")

    print("API index:", list(api.index.names), "cols:", list(api.columns))
    print("REPL index:", list(repl.index.names), "cols:", list(repl.columns))

    m, common = _merged(api, repl)
    print(f"Merged {len(m)} rows on {common}")
    api_s, repl_s = m["Shock_api"], m["Shock_repl"]
    bad = m[api_s.isna() ^ repl_s.isna()]
    print(f"NaN-mismatched: {len(bad)}")
    if len(bad):
        cols = common + ["Shock_api", "Shock_repl"]
        print(bad[cols].to_string(max_rows=20))

    print("\nAPI  Shock vc (top 15):")
    print(api["Shock"].astype(str).value_counts().head(15).to_string())
    print("\nREPL Shock vc (top 15):")
    print(repl["Shock"].astype(str).value_counts().head(15).to_string())

    api_set = set(api["Shock"].dropna().astype(str))
    repl_set = set(repl["Shock"].dropna().astype(str))
    print(f"\nIn API only: {sorted(api_set - repl_set)[:20]}")
    print(f"In REPL only: {sorted(repl_set - api_set)[:20]}")


# ----------------------------------------------------------------- food_prices

def diagnose_food_prices() -> None:
    banner("food_prices: market 4-row NaN mismatch")
    api = _call_api("food_prices", market="Region")
    repl = _load_repl("food_prices")

    m, common = _merged(api, repl)
    api_s, repl_s = m["market_api"], m["market_repl"]
    bad = m[api_s.isna() ^ repl_s.isna()]
    print(f"Merged {len(m)}   NaN-mismatched {len(bad)}")
    if len(bad):
        cols = common + ["market_api", "market_repl"]
        print(bad[cols].drop_duplicates().to_string(max_rows=40))
        print("\nDistinct bad (t,i,v):")
        for lev in ("t", "i", "v"):
            if lev in bad.columns:
                print(f"  {lev}: {sorted(bad[lev].dropna().unique())[:20]}")


# -------------------------------------------------------------- food_quantities

def diagnose_food_quantities() -> None:
    banner("food_quantities: quantity_home 10-row NaN mismatch")
    api = _call_api("food_quantities", market="Region")
    repl = _load_repl("food_quantities")

    m, common = _merged(api, repl)
    api_s, repl_s = m["quantity_home_api"], m["quantity_home_repl"]
    bad = m[api_s.isna() ^ repl_s.isna()]
    print(f"Merged {len(m)}   NaN-mismatched {len(bad)}")
    if len(bad):
        cols = common + ["quantity_home_api", "quantity_home_repl"]
        for extra in ("quantity_purchased_api", "quantity_purchased_repl"):
            if extra in m.columns:
                cols.append(extra)
        print(bad[cols].to_string(max_rows=40))


# --------------------------------------------------------------------- nutrition

def diagnose_nutrition() -> None:
    banner("nutrition: Energy 2-row |Δ|=8200 outliers")
    api = _call_api("nutrition", market="Region")
    repl = _load_repl("nutrition")

    m, common = _merged(api, repl)
    api_s, repl_s = m["Energy_api"], m["Energy_repl"]
    both = ~(api_s.isna() | repl_s.isna())
    diff = (api_s.astype(float) - repl_s.astype(float)).abs()
    bad_mask = both & (diff > 1.0)  # anything >1 kcal off
    bad = m[bad_mask].assign(diff=diff[bad_mask]).sort_values("diff", ascending=False)
    print(f"Total common rows: {both.sum()}   |Δ|>1 kcal: {bad_mask.sum()}")
    print(f"|Δ|>10 kcal:   {((both) & (diff > 10)).sum()}")
    print(f"|Δ|>100 kcal:  {((both) & (diff > 100)).sum()}")
    print(f"|Δ|>1000 kcal: {((both) & (diff > 1000)).sum()}")
    if len(bad):
        cols = common + ["Energy_api", "Energy_repl", "diff"]
        for extra in ("Quantity_api", "Quantity_repl", "j_api", "j_repl"):
            if extra in bad.columns:
                cols.append(extra)
        print(bad[cols].head(20).to_string(max_rows=20))


# ------------------------------------------------------------------- driver

if __name__ == "__main__":
    # Order: cheapest (no market=Region) first
    diagnose_household_roster()
    diagnose_household_characteristics()
    diagnose_shocks()
    diagnose_food_prices()
    diagnose_food_quantities()
    diagnose_nutrition()
