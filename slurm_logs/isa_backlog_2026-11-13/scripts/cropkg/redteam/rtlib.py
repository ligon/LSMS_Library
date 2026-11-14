"""Red-team helpers for GH #1559 (cropkg): an INDEPENDENT price-implied estimator.

Written from the ruling and the candidate's docstring, NOT from its code: plain
pandas groupby medians / quantiles on string keys, my own stated-mass parser, my
own unit-usability rule.  Compared row by row with
quantity_audit.survey_kg_price_factors by the callers.
"""
from __future__ import annotations

import re

import numpy as np
import pandas as pd

MASS_WORDS = {
    'kg': 1.0, 'kgs': 1.0, 'kilo': 1.0, 'kilos': 1.0, 'kilogram': 1.0, 'kilograms': 1.0,
    'kilogramme': 1.0, 'kilogrammes': 1.0,
    'g': 1e-3, 'gm': 1e-3, 'gms': 1e-3, 'gram': 1e-3, 'grams': 1e-3, 'gramm': 1e-3,
    'gramme': 1e-3, 'grammes': 1e-3,
    'milligram': 1e-6, 'milligramme': 1e-6, 'milligrammes': 1e-6,
    'tonne': 1000.0, 'tonnes': 1000.0, 'pound': 0.453592, 'lbs': 0.453592,
}
_KG_RX = re.compile(r'(\d+(?:[.,]\d+)?)\s*(?:kgs?|kilos?|kilogrammes?)\b', re.I)
_G_RX = re.compile(r'(\d+(?:[.,]\d+)?)\s*(?:g|gr|grs|grammes?|grams?)\b', re.I)
UNUSABLE = {'', 'unknown', 'manquant', 'nan', 'none', 'na', '<na>', 'missing'}
CURRENCY = {'value', 'valeur', 'fcfa', 'cfa', 'naira', 'cedi', 'cedis', 'currency', 'montant'}


def my_mass(label):
    if label is None or (not isinstance(label, str) and pd.isna(label)):
        return np.nan
    s = str(label).strip().lower()
    if s in MASS_WORDS:
        return MASS_WORDS[s]
    m = _KG_RX.search(s)
    if m:
        return float(m.group(1).replace(',', '.'))
    m = _G_RX.search(s)
    if m:
        return float(m.group(1).replace(',', '.')) / 1000.0
    return np.nan


def my_usable(label):
    if label is None or (not isinstance(label, str) and pd.isna(label)):
        return False
    s = str(label).strip().lower()
    return s not in UNUSABLE and s not in CURRENCY


def _num(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')


def _fold(s, fold=True):
    s = s.astype('string').str.strip()
    return s.str.casefold() if fold else s


def flat(df, fold=True):
    d = df.reset_index()
    jcol = 'j' if 'j' in d.columns else 'crop'
    out = pd.DataFrame({'t': d['t'].astype(str), 'j': _fold(d[jcol])})
    out['u_raw'] = d['u'].astype('string') if 'u' in d.columns else pd.NA
    out['us_raw'] = d['Unit_sold'].astype('string') if 'Unit_sold' in d.columns else pd.NA
    out['u'] = _fold(out['u_raw'], fold)
    out['us'] = _fold(out['us_raw'], fold)
    out['cond'] = _fold(d['condition']) if 'condition' in d.columns else pd.NA
    out['cond_s'] = _fold(d['Condition_sold']) if 'Condition_sold' in d.columns else pd.NA
    out['q'] = _num(d['Quantity'])
    out['kg'] = _num(d['Quantity_kg']) if 'Quantity_kg' in d.columns else np.nan
    out['qs'] = _num(d['Quantity_sold']) if 'Quantity_sold' in d.columns else np.nan
    out['vs'] = _num(d['Value_sold']) if 'Value_sold' in d.columns else np.nan
    out['i'] = d['i'].astype(str)
    out['plot_id'] = d['plot_id'].astype(str) if 'plot_id' in d.columns else ''
    out['deriv'] = d['Derivation'].astype('string').fillna('') if 'Derivation' in d.columns else ''
    out['kgf_sold'] = _num(d['KgFactor_sold']) if 'KgFactor_sold' in d.columns else np.nan
    return out


def _spread(g):
    n = g.size()
    q90, q10 = g.quantile(0.9), g.quantile(0.1)
    mx, mn = g.max(), g.min()
    return n, g.median(), np.where(n >= 10, q90 / q10, mx / mn)


def _cells(frame, keys, val):
    g = frame.groupby(keys, dropna=False, sort=True)[val]
    n, med, spr = _spread(g)
    res = pd.DataFrame({'n': n, 'med': med})
    res['spread'] = np.asarray(spr, dtype='float64')
    res['hh'] = frame.groupby(keys, dropna=False, sort=True)['i'].nunique().reindex(res.index).to_numpy()
    return res


def estimate(df=None, f=None, *, state=True, gate_both=True, donor=None, recip=None,
             mass=my_mass, usable=my_usable, judge_all=False, min_reports=5,
             min_baseline=5, max_spread=10.0, band=(0.1, 10.0), min_households=None):
    """Per-row implied kg per unit on a crop_production frame (positional)."""
    f = flat(df) if f is None else f
    n = len(f)
    mass_u = f['u_raw'].map(mass).astype('float64')
    mass_s = f['us_raw'].map(mass).astype('float64')
    use_u = f['u_raw'].map(usable).astype(bool)
    use_s = f['us_raw'].map(usable).astype(bool)
    with np.errstate(divide='ignore', invalid='ignore'):
        price = f['vs'] / f['qs']
    sale = (f['qs'] > 0) & (f['vs'] > 0) & np.isfinite(price) & (price > 0) & use_s
    if donor is not None:
        sale &= np.asarray(donor, dtype=bool)
    by_mass = sale & mass_s.notna()
    by_unit = sale & mass_s.isna()
    f = f.assign(price=price, pkg=price / mass_s)
    if judge_all:
        judged = (f['q'] > 0) & use_u & mass_u.isna()
    else:
        judged = (f['kg'] > 0) & (f['q'] > 0) & np.isfinite(f['kg']) & use_u & mass_u.isna()
    if recip is not None:
        judged &= np.asarray(recip, dtype=bool)
    implied = pd.Series(np.nan, index=f.index)
    rung = pd.Series(None, index=f.index, dtype=object)
    n_u = pd.Series(np.nan, index=f.index)
    n_k = pd.Series(np.nan, index=f.index)
    hh_u = pd.Series(np.nan, index=f.index)
    hh_k = pd.Series(np.nan, index=f.index)
    rungs =[('t,j,state,u', True), ('t,j,u', False)] if state else [('t,j,u', False)]
    known_h = f['cond'].notna() & ~f['cond'].fillna('unknown').str.startswith('unknown')
    known_s = f['cond_s'].notna() & ~f['cond_s'].fillna('unknown').str.startswith('unknown')
    for name, ws in rungs:
        todo = judged & implied.isna()
        bm, bu = by_mass, by_unit
        if ws:
            todo &= known_h
            bm, bu = bm & known_s, bu & known_s
            sk_base, rk_base = ['t', 'j', 'cond_s'], ['t', 'j', 'cond']
        else:
            sk_base = rk_base = ['t', 'j']
        if not (todo.any() and bm.any() and bu.any()):
            continue
        mc = _cells(f[bm], sk_base, 'pkg')
        uc = _cells(f[bu], sk_base + ['us'], 'price')
        mc['ok'] = (mc['n'] >= min_baseline) & (mc['spread'] <= max_spread)
        uc['ok'] = (uc['n'] >= min_reports) & ((uc['spread'] <= max_spread) if gate_both else True)
        if min_households is not None:
            mc['ok'] &= mc['hh'] >= min_households
            uc['ok'] &= uc['hh'] >= min_households
        mc.index.names = rk_base
        uc.index.names = rk_base + ['u']
        r = f.loc[todo, rk_base + ['u']]
        jm = r.join(mc, on=rk_base, how='left')
        ju = r.join(uc, on=rk_base + ['u'], how='left', rsuffix='_u')
        ok = jm['ok'].fillna(False).astype(bool) & ju['ok'].fillna(False).astype(bool)
        idx = r.index[ok.to_numpy()]
        implied.loc[idx] = (ju.loc[idx, 'med'] / jm.loc[idx, 'med']).to_numpy()
        rung.loc[idx] = name
        n_u.loc[idx] = ju.loc[idx, 'n'].to_numpy()
        n_k.loc[idx] = jm.loc[idx, 'n'].to_numpy()
        hh_u.loc[idx] = ju.loc[idx, 'hh'].to_numpy()
        hh_k.loc[idx] = jm.loc[idx, 'hh'].to_numpy()
    with np.errstate(divide='ignore', invalid='ignore'):
        kg_survey = (f['kg'] / f['q']).where(judged)
        ratio = kg_survey / implied
    flag = pd.Series(None, index=f.index, dtype=object)
    flag[ratio < band[0]] = 'low'
    flag[ratio > band[1]] = 'high'
    return pd.DataFrame({'kg_survey': kg_survey, 'implied': implied, 'rung': rung,
                         'n_unit': n_u, 'n_kg': n_k, 'hh_unit': hh_u, 'hh_kg': hh_k,
                         'ratio': ratio, 'flag': flag,
                         'judged_any': judged})


def summarise(f, est, waves=None):
    """Per-wave judged / named counts and the median ratio (aggregates only)."""
    out = {}
    t = f['t']
    for w in sorted(set(t) if waves is None else waves):
        m = (t == w).to_numpy()
        e = est[m]
        j = e['implied'].notna() & e['kg_survey'].notna()
        out[w] = {'judged': int(j.sum()),
                  'low': int((e['flag'] == 'low').sum()),
                  'high': int((e['flag'] == 'high').sum()),
                  'median_ratio': (float(e.loc[j, 'ratio'].median()) if j.any() else None),
                  'within2x': (float(((e.loc[j, 'ratio'] >= 0.5) & (e.loc[j, 'ratio'] <= 2)).mean())
                               if j.any() else None)}
    return out
