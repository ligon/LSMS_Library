"""GH #857 -- the sweep QUANTITY_OUTLIER_K / the reference quantile were chosen from.

Read-only: every country is taken from its warm L2-country ``crop_production``
parquet under ``data_root()`` and any country whose parquet is absent is named
and skipped.  It never calls ``Country()`` and never builds.

Run it as ``python slurm_logs/gh857_quantity_screen/sensitivity_sweep.py`` from
any checkout; it prints which ``lsms_library`` it imported so a worktree/main
mix-up is visible rather than silent (CLAUDE.md, scrum-master addendum 3).

What to look at: the full ratio ranking, and the fact that it has NO
discontinuity.  That absence is why ``QUANTITY_OUTLIER_K`` is documented as a
stated tolerance rather than a measured boundary.
"""
import os
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
pd.set_option("display.width", 250)

import lsms_library
print("lsms_library:", lsms_library.__file__)
from lsms_library.paths import data_root

COUNTRIES = ["Tanzania", "Uganda", "Malawi", "Nigeria", "Ethiopia"]
MIN_N = 30


def frame(country):
    path = data_root() / country / "var" / "crop_production.parquet"
    if not path.exists():
        print(f"  {country}: no warm parquet at {path} -- skipped")
        return None
    f = pd.read_parquet(path).reset_index()
    f["country"] = country
    if "j" not in f.columns and "crop" in f.columns:
        f["j"] = f["crop"]
    if "u" not in f.columns:
        f["u"] = pd.NA
    if "plot" not in f.columns and "plot_id" in f.columns:
        f["plot"] = f["plot_id"]
    f["u"] = f["u"].astype(str)
    f["j"] = f["j"].astype(str)
    f["Quantity"] = pd.to_numeric(f["Quantity"], errors="coerce").astype("float64")
    return f[["country", "t", "i", "plot", "j", "u", "Quantity"]]


def reference(f, quantile, min_n):
    """The ladder, exactly as ``quantity_audit._cell_reference`` walks it."""
    ref = pd.Series(np.nan, index=f.index)
    rung = pd.Series(None, index=f.index, dtype=object)
    for name, keys in (("t,u,j", ["country", "t", "u", "j"]),
                       ("t,u", ["country", "t", "u"]),
                       ("t", ["country", "t"])):
        g = f.groupby(keys, dropna=False, observed=True)["Quantity"]
        cand = (g.transform("quantile", quantile)
                .where((g.transform("count") >= min_n)
                       & (g.transform("quantile", quantile) > 0)))
        take = ref.isna() & cand.notna()
        ref = ref.where(~take, cand)
        rung = rung.where(~take, name)
    return ref, rung


held = [x for x in (frame(c) for c in COUNTRIES) if x is not None]
f = pd.concat(held, ignore_index=True)
print(f"\n{len(f):,} rows over {f['country'].nunique()} countries: "
      f"{sorted(f['country'].unique())}\n")

for quantile in (0.90, 0.95, 0.99):
    ref, _ = reference(f, quantile, MIN_N)
    ratio = f["Quantity"] / ref
    for K in (10, 30, 50, 100, 150, 200, 300, 500):
        fire = (ratio > K).fillna(False)
        print(f"q={quantile} min_n={MIN_N} K={K}: {int(fire.sum())} rows "
              f"{f[fire].groupby('country').size().to_dict()}")
    print()

ref, rung = reference(f, 0.90, MIN_N)
f = f.assign(ref=ref, rung=rung, ratio=f["Quantity"] / ref)
d = f[f["ratio"].notna()].sort_values("ratio", ascending=False)
print("--- the ratio ranking under the SHIPPED rule (q=0.90, min_n=30) ---")
print(d.head(70).to_string(index=False))
print("\nratio quantiles -- look for a gap; there is none:")
print(d["ratio"].describe(percentiles=[.9, .99, .999, .9999]))
