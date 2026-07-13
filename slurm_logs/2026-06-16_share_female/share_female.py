#!/usr/bin/env python
"""Pooled + per-country-wave share female from household_roster.

Resilient per-country loop (one country failing doesn't abort the run).
Captures both unweighted counts and design-weighted sums (weight joined
from sample() at the individual grain -> the size-correct individual
estimand, per the methodology discussion).  Writes a tidy CSV.
"""
import os, sys, traceback
import warnings; warnings.filterwarnings('ignore')
import pandas as pd
import lsms_library as ll

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'share_female.csv')

countries = ll.Feature('household_roster').countries
print(f'{len(countries)} countries with household_roster', flush=True)

rows = []
for c in countries:
    try:
        C = ll.Country(c)
        r = C.household_roster()
        sex = r['Sex'].astype(str).str.strip().values
        df = pd.DataFrame({
            'i': r.index.get_level_values('i'),
            't': r.index.get_level_values('t'),
            'Sex': sex,
        })
        # design weight (cross-sectional, household-level) joined at individual grain
        try:
            w = C.sample()['weight']          # index (i, t)
            df = df.set_index(['i', 't'])
            df['w'] = w.reindex(df.index).values
            df = df.reset_index()
        except Exception as we:
            df['w'] = pd.NA
            print(f'  (no weights for {c}: {we})', flush=True)
        for t, g in df.groupby('t'):
            mf = g['Sex'].isin(['M', 'F'])
            wv = g[mf].dropna(subset=['w'])
            rows.append(dict(
                country=c, t=str(t),
                n_F=int((g['Sex'] == 'F').sum()),
                n_M=int((g['Sex'] == 'M').sum()),
                n_other=int((~mf).sum()),
                w_F=float(wv.loc[wv['Sex'] == 'F', 'w'].sum()),
                w_M=float(wv.loc[wv['Sex'] == 'M', 'w'].sum()),
                w_cov=float(len(wv) / mf.sum()) if mf.sum() else 0.0,
            ))
        pd.DataFrame(rows).to_csv(OUT, index=False)   # checkpoint each country
        print(f'OK   {c}: {len(r):>7,} records, {df["t"].nunique()} wave(s)', flush=True)
    except Exception as e:
        print(f'FAIL {c}: {e}', flush=True)
        traceback.print_exc()

res = pd.DataFrame(rows)
res.to_csv(OUT, index=False)
print('\n===== SUMMARY =====', flush=True)
tf, tm = res['n_F'].sum(), res['n_M'].sum()
print(f'countries done: {res.country.nunique()}/{len(countries)}; country-waves: {len(res)}')
print(f'UNWEIGHTED pooled: F={tf:,} M={tm:,}  share female = {tf/(tf+tm):.4f}')
# equal-weight across country-waves (each survey counts once) -- robust to weight scaling
res['unw_share'] = res['n_F'] / (res['n_F'] + res['n_M'])
print(f'mean of per-country-wave unweighted shares = {res.unw_share.mean():.4f}')
wok = res[res['w_F'] + res['w_M'] > 0].copy()
wok['w_share'] = wok['w_F'] / (wok['w_F'] + wok['w_M'])
print(f'weighted per-survey shares available for {len(wok)}/{len(res)} country-waves')
print(f'mean of per-country-wave WEIGHTED shares = {wok.w_share.mean():.4f}')
print('wrote', OUT, flush=True)
