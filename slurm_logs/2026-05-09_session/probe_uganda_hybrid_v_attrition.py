"""
Trace HH attrition through the Uganda 2009-10 pipeline to locate the
31-HH shortfall flagged by #246 (C-2):

    test_food_expenditures_retains_hybrid_v_HH:
        retained only 2869 HH in 2009-10 -- expected >=2900

Sample-level HH count for 2009-10 is documented as 2975; the test
allows up to 22 legitimate drop-outs (no food-expenditure records),
so the threshold 2900 reserves a 53-HH cushion.  We're at 2869,
i.e. 84 HHs missing vs. the documented 2975 baseline.

This script counts unique HH (i level) for 2009-10 at each stage of
the pipeline, prints set-differences, and classifies the lost HHs
by whether they have synthetic (`@lat,lon`) v's.
"""
import os
import sys
import time
import warnings

warnings.simplefilter("ignore")

import lsms_library as ll

T0 = time.time()
print(f"=== Uganda 2009-10 hybrid-v attrition probe ===", flush=True)
print(f"python:        {sys.executable}", flush=True)
print(f"lsms_library:  {ll.__file__}", flush=True)
print(f"LSMS_NO_CACHE: {os.environ.get('LSMS_NO_CACHE')!r}", flush=True)
print(flush=True)

WAVE = "2009-10"

uga = ll.Country("Uganda")


def hhs_at(label, fn):
    """Return the set of HH IDs (`i`) at the WAVE for the given table."""
    t = time.time()
    try:
        df = fn()
    except Exception as e:
        dt = time.time() - t
        print(f"  ERROR {label} (took {dt:.1f}s): {type(e).__name__}: {e}", flush=True)
        return None, None
    dt = time.time() - t
    if "t" not in (df.index.names or []):
        # not a wave-indexed table; try to skip gracefully
        print(f"  {label}: no 't' level, skipping (shape={df.shape})", flush=True)
        return None, None
    sub = df.xs(WAVE, level="t") if WAVE in df.index.get_level_values("t").unique() else None
    if sub is None:
        print(f"  {label}: WAVE {WAVE} not in t-levels", flush=True)
        return None, None
    if "i" in sub.index.names:
        ids = set(sub.index.get_level_values("i").unique())
    elif "i" in sub.columns:
        ids = set(sub["i"].unique())
    else:
        print(f"  {label}: no 'i' level; index={sub.index.names}", flush=True)
        return None, None
    print(f"  {label:50s} HHs={len(ids):5d}  rows={sub.shape[0]:7d}  ({dt:.1f}s)", flush=True)
    return ids, sub


print("=== HH counts at each pipeline stage ===", flush=True)

# Stage 1: sample (full sampling-design HHs)
sample_ids, sample_sub = hhs_at("sample()",
                                lambda: uga.sample())

# Stage 2: roster (people-level)
roster_ids, roster_sub = hhs_at("household_roster()",
                                lambda: uga.household_roster())

# Stage 3: derived characteristics (after roster_to_characteristics filter)
hc_ids, _ = hhs_at("household_characteristics()",
                   lambda: uga.household_characteristics())

# Stage 4: characteristics with market='Region' (the test's hc fixture)
hcm_ids, _ = hhs_at("household_characteristics(market='Region')",
                    lambda: uga.household_characteristics(market="Region"))

# Stage 5: food_expenditures (no market)
fe_ids, _ = hhs_at("food_expenditures()",
                   lambda: uga.food_expenditures())

# Stage 6: food_expenditures(market='Region') -- the test's fe fixture
fem_ids, _ = hhs_at("food_expenditures(market='Region')",
                    lambda: uga.food_expenditures(market="Region"))

print(flush=True)


def diff_report(label_a, ids_a, label_b, ids_b):
    if ids_a is None or ids_b is None:
        return
    only_a = ids_a - ids_b
    only_b = ids_b - ids_a
    print(f"=== {label_a} \\ {label_b}: {len(only_a)} HHs (in {label_a} only) ===", flush=True)
    print(f"=== {label_b} \\ {label_a}: {len(only_b)} HHs (in {label_b} only) ===", flush=True)
    if only_a and len(only_a) <= 100:
        sample = sorted(only_a)[:5] + (["..."] if len(only_a) > 5 else [])
        print(f"  examples (a only): {sample}", flush=True)


diff_report("sample", sample_ids, "household_roster", roster_ids)
diff_report("household_roster", roster_ids, "household_characteristics", hc_ids)
diff_report("household_characteristics", hc_ids, "household_characteristics(m)", hcm_ids)
diff_report("household_characteristics(m)", hcm_ids, "food_expenditures(m)", fem_ids)
diff_report("food_expenditures", fe_ids, "food_expenditures(m)", fem_ids)
diff_report("sample", sample_ids, "food_expenditures(m)", fem_ids)
print(flush=True)


# Synthetic-v classification: how many of the lost HHs (vs sample) have
# `@`-prefixed synthetic v's, vs real cluster IDs?
print("=== Synthetic-v classification of HHs lost from sample to fe(m) ===", flush=True)
if sample_ids is not None and fem_ids is not None and sample_sub is not None:
    lost = sample_ids - fem_ids
    print(f"  total lost (sample - fe(m)): {len(lost)}", flush=True)
    if lost:
        # Look up v for each lost HH
        if "v" in sample_sub.columns:
            v_col = sample_sub["v"]
        elif "v" in sample_sub.index.names:
            v_col = sample_sub.index.get_level_values("v").to_series(index=sample_sub.index)
        else:
            v_col = None
        if v_col is not None:
            # sample_sub is xs'd on t; index is whatever's left.  Map i -> v.
            tmp = sample_sub.copy()
            if "i" not in tmp.index.names:
                # i may already be the only level, or in columns
                pass
            i_to_v = {}
            for idx_tuple, row in tmp.iterrows():
                # extract i from idx_tuple
                if isinstance(idx_tuple, tuple):
                    if "i" in tmp.index.names:
                        ii = idx_tuple[tmp.index.names.index("i")]
                    else:
                        ii = idx_tuple[0]
                else:
                    ii = idx_tuple
                if ii not in i_to_v:
                    if "v" in tmp.columns:
                        i_to_v[ii] = row["v"]
                    elif "v" in tmp.index.names:
                        i_to_v[ii] = idx_tuple[tmp.index.names.index("v")]
            lost_vs = [i_to_v.get(i) for i in lost if i in i_to_v]
            n_synthetic = sum(1 for v in lost_vs if isinstance(v, str) and v.startswith("@"))
            n_real = sum(1 for v in lost_vs if isinstance(v, str) and not v.startswith("@"))
            n_unknown = len(lost) - n_synthetic - n_real
            print(f"  with synthetic v (@-prefixed): {n_synthetic}", flush=True)
            print(f"  with real cluster v:           {n_real}", flush=True)
            print(f"  not found in sample/other:     {n_unknown}", flush=True)
            # Sample of synthetic-v lost HHs
            syn_examples = [(i, i_to_v[i]) for i in list(lost)[:10] if i in i_to_v and isinstance(i_to_v[i], str) and i_to_v[i].startswith("@")]
            real_examples = [(i, i_to_v[i]) for i in list(lost)[:10] if i in i_to_v and isinstance(i_to_v[i], str) and not i_to_v[i].startswith("@")]
            if syn_examples:
                print(f"  synthetic examples: {syn_examples[:3]}", flush=True)
            if real_examples:
                print(f"  real-v examples:    {real_examples[:3]}", flush=True)
print(flush=True)


# Stage-attrition summary table
print("=== Attrition table (2009-10, Uganda) ===", flush=True)
def safe_len(s):
    return len(s) if s is not None else "n/a"
print(f"  sample()                                  : {safe_len(sample_ids)}", flush=True)
print(f"  household_roster()  (HHs)                 : {safe_len(roster_ids)}", flush=True)
print(f"  household_characteristics()               : {safe_len(hc_ids)}", flush=True)
print(f"  household_characteristics(market='Region'): {safe_len(hcm_ids)}", flush=True)
print(f"  food_expenditures()                       : {safe_len(fe_ids)}", flush=True)
print(f"  food_expenditures(market='Region')        : {safe_len(fem_ids)}  [test target >=2900]", flush=True)
print(flush=True)

# Per-stage delta (positive = HHs lost crossing this stage)
def safe_delta(a, b):
    if a is None or b is None:
        return "n/a"
    return len(a) - len(b)
print("=== Per-stage attrition (HHs lost crossing) ===", flush=True)
print(f"  sample -> roster:                          {safe_delta(sample_ids, roster_ids)}", flush=True)
print(f"  roster -> characteristics:                 {safe_delta(roster_ids, hc_ids)}", flush=True)
print(f"  characteristics -> characteristics(m):     {safe_delta(hc_ids, hcm_ids)}", flush=True)
print(f"  characteristics(m) -> fe(m):               {safe_delta(hcm_ids, fem_ids)}", flush=True)
print(f"  fe -> fe(m):                               {safe_delta(fe_ids, fem_ids)}", flush=True)
print(f"  sample -> fe(m):                           {safe_delta(sample_ids, fem_ids)}", flush=True)

print(flush=True)
print(f"=== elapsed: {time.time()-T0:.1f}s ===", flush=True)
