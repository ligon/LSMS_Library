#!/usr/bin/env python3
"""Definitive answer: how many countries have CORRECT food_acquired in Feature('food_acquired').

Strategy:
  1. Parallel per-country build (cache ON => warms cache) of Country(c).food_acquired();
     classify built? / sane? / nrows / index names / has s-axis / columns.
  2. Build Feature('food_acquired')() from the now-warm cache; list the countries
     that actually survive into the assembled frame (catches modal exclusion).
A country is CORRECT iff: built, sane (no fail), has canonical s-axis + [Quantity,Expenditure],
AND present in the assembled Feature() index.
"""
import os, sys, json, warnings, traceback
from multiprocessing import get_context

import lsms_library as ll
from lsms_library import diagnostics

def probe_one(country):
    rec = {"country": country, "built": False, "sane": None, "nrows": None,
           "index": None, "has_s": None, "cols": None, "err": None}
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country(country).food_acquired()
        rec["built"] = True
        rec["nrows"] = int(len(df))
        rec["index"] = list(df.index.names)
        rec["has_s"] = "s" in df.index.names
        rec["cols"] = list(df.columns)
        try:
            res = diagnostics.is_this_feature_sane(df, country, "food_acquired")
            rec["sane"] = bool(getattr(res, "ok", None))
        except Exception as e:
            rec["sane"] = f"sanecheck-error: {e}"
    except Exception as e:
        rec["err"] = f"{type(e).__name__}: {e}"
    return rec

def main():
    feat = ll.Feature("food_acquired")
    declared = sorted(feat.countries)
    print(f"declared countries ({len(declared)}): {declared}", flush=True)

    jobs = int(os.environ.get("PROBE_JOBS", "14"))
    ctx = get_context("fork")
    with ctx.Pool(processes=min(jobs, len(declared))) as pool:
        recs = pool.map(probe_one, declared)

    print("\n=== per-country food_acquired build/sanity (cache warmed) ===", flush=True)
    for r in recs:
        print(json.dumps(r), flush=True)

    # Now assemble Feature() from warm cache and see who survives.
    print("\n=== Feature('food_acquired')() assembly ===", flush=True)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            big = feat()
        present = sorted(big.index.get_level_values("country").unique())
        print(f"assembled rows: {len(big)}", flush=True)
        print(f"countries PRESENT in Feature() ({len(present)}): {present}", flush=True)
        dropped = sorted(set(declared) - set(present))
        print(f"declared-but-ABSENT ({len(dropped)}): {dropped}", flush=True)
    except Exception as e:
        print("ASSEMBLY ERROR:", traceback.format_exc(), flush=True)
        present = []

    built = [r["country"] for r in recs if r["built"]]
    sane = [r["country"] for r in recs if r["sane"] is True]
    canon = [r["country"] for r in recs if r["built"] and r["has_s"]
             and r["cols"] == ["Quantity", "Expenditure"]]
    print("\n=== SUMMARY ===", flush=True)
    print(f"declared:                 {len(declared)}", flush=True)
    print(f"built without error:      {len(built)}  {built}", flush=True)
    print(f"sane (is_this_feature_sane.ok): {len(sane)}  {sane}", flush=True)
    print(f"canonical (s-axis + [Quantity,Expenditure]): {len(canon)}  {canon}", flush=True)
    print(f"present in Feature():      {len(present)}  {present}", flush=True)
    correct = sorted(set(built) & set(canon) & set(present))
    print(f"\nCORRECT (built & canonical & present): {len(correct)}\n  {correct}", flush=True)

if __name__ == "__main__":
    main()
