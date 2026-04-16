"""
Diagnose the 1315-row `log HSize` drift in household_characteristics that the
test_uganda_api_vs_replication test surfaced after we moved past the 1-row
NaN asymmetry.

Run from the repo root:

    .venv/bin/python slurm_logs/uganda_replication_drift_2026-04-14/diagnose_hsize.py \
        2>&1 | tee slurm_logs/uganda_replication_drift_2026-04-14/diagnose_hsize.out

For the top |Δ| rows we dump:
  - HSize on both sides (so we see if the drift is in the raw household size
    or only in the log);
  - member counts from household_roster on both the API and replication side,
    bucketed by age-sentinel status, to test the hypothesis that the API's
    age_handler() is recovering members the replication dropped.
"""
from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

pd.set_option("display.max_columns", 80)
pd.set_option("display.width", 220)
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


def banner(s: str) -> None:
    print("\n" + "=" * 78)
    print("  " + s)
    print("=" * 78)


def main() -> None:
    banner("household_characteristics: log HSize outlier rows")

    api = _call_api("household_characteristics", market="Region")
    repl = _load_repl("household_characteristics")

    common = [lev for lev in repl.index.names if lev in api.index.names]
    print(f"API shape={api.shape}  index={list(api.index.names)}")
    print(f"REPL shape={repl.shape}  index={list(repl.index.names)}")
    print(f"Merge on: {common}")

    a = api.reset_index()[common + ["HSize", "log HSize"]]
    r = repl.reset_index()[common + ["HSize", "log HSize"]]
    m = a.merge(r, on=common, suffixes=("_api", "_repl"))
    print(f"Merged rows: {len(m)}")

    both = m["log HSize_api"].notna() & m["log HSize_repl"].notna()
    diff = (m["log HSize_api"].astype(float) - m["log HSize_repl"].astype(float)).abs()
    over = both & (diff > 0.02)
    print(f"\nlog HSize |Δ| > 0.02: {over.sum()} rows (of {both.sum()} numeric-compared)")

    # Distribution of drift magnitude
    for thr in (0.02, 0.1, 0.5, 1.0, 2.0, 3.0):
        n = int((both & (diff > thr)).sum())
        print(f"  |Δ| > {thr:>4}: {n:>6}")

    # HSize-level drift (not just log)
    hsize_diff = (m["HSize_api"].astype(float) - m["HSize_repl"].astype(float))
    print(f"\nHSize raw Δ (api - repl) distribution over outlier rows:")
    print(hsize_diff[over].describe().to_string())
    print(f"\nHSize Δ value counts (top 15):")
    print(hsize_diff[over].astype(int).value_counts().head(15).to_string())

    # Sign: is the API larger or smaller than replication?
    signed = (m["log HSize_api"].astype(float) - m["log HSize_repl"].astype(float))
    print(f"\nSigned log-HSize Δ over outlier rows:")
    print(f"  api > repl:  {int(((signed > 0) & over).sum())}")
    print(f"  api < repl:  {int(((signed < 0) & over).sum())}")

    # Top 15 by magnitude
    m["abs_diff"] = diff
    m["hsize_diff"] = hsize_diff
    top = m[over].nlargest(15, "abs_diff")[
        common + ["HSize_api", "HSize_repl", "hsize_diff",
                  "log HSize_api", "log HSize_repl", "abs_diff"]
    ]
    banner("Top 15 |Δ| log HSize rows")
    print(top.to_string(index=False))

    # Cross-check: for the top-5 drifted HHs, ask household_roster on both sides
    # how many members each side thinks the HH has, and whether any were age-null.
    banner("household_roster member counts for top 5 drifted HHs")
    api_ros = _call_api("household_roster").reset_index()
    try:
        repl_ros = _load_repl("household_roster").reset_index()
    except Exception as exc:
        print(f"(replication household_roster not loadable: {exc})")
        repl_ros = None

    top5 = m[over].nlargest(5, "abs_diff")
    for _, row in top5.iterrows():
        i, t = row["i"], row["t"]
        print(f"\n--- (i={i!r}, t={t!r})")
        print(f"    HSize api={row['HSize_api']}  repl={row['HSize_repl']}  "
              f"Δ log={row['abs_diff']:.4f}")
        a_ros = api_ros[(api_ros["i"] == i) & (api_ros["t"] == t)]
        print(f"    API roster rows: {len(a_ros)}  "
              f"age-na: {int(a_ros['Age'].isna().sum()) if 'Age' in a_ros.columns else 'n/a'}")
        if repl_ros is not None:
            r_ros = repl_ros[(repl_ros["i"] == i) & (repl_ros["t"] == t)]
            print(f"    REPL roster rows: {len(r_ros)}  "
                  f"age-na: {int(r_ros['Age'].isna().sum()) if 'Age' in r_ros.columns else 'n/a'}")

    # Per-wave breakdown of outlier counts
    banner("Outlier counts by wave (t)")
    print(m[over].groupby("t").size().to_string())


if __name__ == "__main__":
    main()
