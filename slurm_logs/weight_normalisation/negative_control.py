"""Negative control: doctor a raw parquet and prove the new tests FAIL.

A test that never fails is not a test.  Builds a scratch data root holding a
copy of two countries' sample parquets, injects the two miscodings the checks
claim to catch, and reports whether each assertion fires.

  A. weight wired to household size  -> per-wave raw mean ~5, inside the
     empty band between the self-weighting and expansion classes.
  B. one wave's weight off by 10x    -> raw expansion total jumps 10x.

Also checks the two must-NOT-fire cases:
  C. CotedIvoire untouched (mean 1.0000 x4 + 443.42) -> both pass/skip.
  D. an expansion wave left alone                    -> passes.
"""
import shutil, sys
from pathlib import Path
import numpy as np, pandas as pd

SRC = Path('/global/scratch/fsa/fc_jevons/ligon/agent_cache_weightnorm')
DST = Path(sys.argv[1])

# --- reproduce the two predicates exactly as tests/test_sample.py has them ---
_RAW_SELF_WEIGHTING_RTOL = 0.01
_RAW_EXPANSION_FLOOR = 10.0


def raw_stats(path):
    raw = pd.read_parquet(path)
    if "weight" not in raw.columns:
        return None
    if "t" in (raw.index.names or []):
        waves = raw.index.get_level_values("t")
    elif "t" in raw.columns:
        waves = raw["t"]
    else:
        return None
    w = pd.to_numeric(raw["weight"], errors="coerce")
    flat = pd.DataFrame({"t": pd.Index(waves).astype(str), "w": w.to_numpy()})
    out = (flat.dropna(subset=["w"]).groupby("t")["w"]
               .agg(n="size", mean="mean", sum="sum"))
    return out if len(out) else None


def check_classes(raw):
    bad = raw[~(((raw["mean"] - 1.0).abs() <= _RAW_SELF_WEIGHTING_RTOL)
                | (raw["mean"] >= _RAW_EXPANSION_FLOOR))]
    return bad


def check_stability(raw):
    exp = raw[raw["mean"] >= _RAW_EXPANSION_FLOOR].sort_index()
    if len(exp) < 2:
        return 'SKIP', None
    totals = exp["sum"]
    ratios = (totals / totals.shift(1)).dropna()
    bad = ratios[~((ratios > 0.2) & (ratios < 5.0))]
    return ('FAIL' if len(bad) else 'PASS'), bad


def doctor(country, fn, label):
    src = SRC / country / 'var' / 'sample.parquet'
    dst = DST / country / 'var' / 'sample.parquet'
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    raw = pd.read_parquet(dst)
    raw = fn(raw)
    raw.to_parquet(dst)
    st = raw_stats(dst)
    bad = check_classes(st)
    stab, sbad = check_stability(st)
    print(f'\n--- {label}  ({country})')
    print(st.to_string())
    print(f'  class-separation : {"FAIL (fires)" if len(bad) else "PASS (silent)"}'
          + (f'\n{bad.to_string()}' if len(bad) else ''))
    print(f'  total-stability  : {stab}'
          + (f'  offending ratios {dict(sbad.round(2))}' if sbad is not None and len(sbad) else ''))
    return len(bad) > 0, stab


def waves_of(raw):
    return (raw.index.get_level_values('t') if 't' in (raw.index.names or [])
            else raw['t'])


print('=' * 74)
print('A. MISCODED: weight wired to household size (Uganda 2018-19, mean -> ~5)')
def a(raw):
    t = waves_of(raw)
    m = np.asarray(pd.Index(t).astype(str) == '2018-19')
    w = pd.to_numeric(raw['weight'], errors='coerce').to_numpy(dtype='float64', copy=True)
    rng = np.random.default_rng(0)
    w[m] = rng.integers(1, 10, m.sum()).astype(float)   # household size
    raw = raw.copy(); raw['weight'] = w
    return raw
a_fires, _ = doctor('Uganda', a, 'weight = household size')

print('\n' + '=' * 74)
print('B. MISCODED: one wave off by 10x (GhanaLSS 2016-17)')
def b(raw):
    t = waves_of(raw)
    m = np.asarray(pd.Index(t).astype(str) == '2016-17')
    w = pd.to_numeric(raw['weight'], errors='coerce').to_numpy(dtype='float64', copy=True)
    w[m] = w[m] * 10.0
    raw = raw.copy(); raw['weight'] = w
    return raw
_, b_stab = doctor('GhanaLSS', b, 'one wave 10x')

print('\n' + '=' * 74)
print('C/D. CONTROL: untouched countries must NOT fire')
for c in ('CotedIvoire', 'Malawi', 'GhanaLSS', 'Uganda'):
    st = raw_stats(SRC / c / 'var' / 'sample.parquet')
    bad = check_classes(st)
    stab, sbad = check_stability(st)
    print(f'  {c:<14} classes {"FIRES <<<" if len(bad) else "silent"}   '
          f'stability {stab}')

print('\n' + '=' * 74)
print('VERDICT')
print(f'  A (household-size miscoding) caught by class check : {a_fires}')
print(f'  B (10x miscoding) caught by stability check        : {b_stab == "FAIL"}')
