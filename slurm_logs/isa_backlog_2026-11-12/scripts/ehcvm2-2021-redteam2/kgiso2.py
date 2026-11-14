"""Row-level breakdown of the Togo 2018 pooled-kg movement (decode vs 2021-22 change),
plus a count of withheld NA purchased rows on never-purchased (Jamais) source rows.

usage: kgiso2.py CLONE FRAMES OUT.json
Aggregates only.
"""
import json, os, re, sys, traceback, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env

clone, frames, out = sys.argv[1:4]
assert_env(clone)
warnings.simplefilter('ignore')
import numpy as np
import pandas as pd
import lsms_library as ll
from lsms_library.transformations import food_quantities_from_acquired, food_kg_factors
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

NEW = '2021-22'
res = {}


def dump():
    json.dump(res, open(out, 'w'), indent=1, default=str)


def tmask(df, old):
    t = df.index.get_level_values('t').astype(str)
    return (t != NEW) if old else (t == NEW)


def flat(q):
    f = q[tmask(q, True)].reset_index()
    keys = [c for c in q.index.names]
    for c in keys:
        f[c] = f[c].astype(str)
    return f.set_index(keys)['Quantity'].astype(float), keys


try:
    c = ll.Country('Togo')
    kw = dict(unit_kg=c._unit_kg_factors(), no_kg=c._no_kg_factor_cells())
    P = pd.read_parquet(os.path.join(frames, 'prev', 'Togo', 'fa_agg.parquet'))
    H = pd.read_parquet(os.path.join(frames, 'head', 'Togo', 'fa_agg.parquet'))
    mixes = {'A_prev': pd.concat([P[tmask(P, True)], P[tmask(P, False)]]),
             'B_head': pd.concat([H[tmask(H, True)], H[tmask(H, False)]]),
             'C_oldprev_newhead': pd.concat([P[tmask(P, True)], H[tmask(H, False)]]),
             'D_oldhead_newprev': pd.concat([H[tmask(H, True)], P[tmask(P, False)]])}
    q = {k: food_quantities_from_acquired(v, units='kgs', **kw) for k, v in mixes.items()}
    res['kg_2018'] = {k: float(v.loc[tmask(v, True) & (v.index.get_level_values('u').astype(str) == 'kg'), 'Quantity'].sum())
                      for k, v in q.items()}
    # Row-level: per-row kg factor on the 2018 source rows, from food_kg_factors on each mix,
    # aligned on a stringified index with the 2018 codes decoded on every side.
    PAT = re.compile(r'^(659|660|662)(?:\.0)?(?=$|\s|\()')
    DEC = {'659': 'Main', '660': 'Quartier', '662': 'Tête'}
    fac = {}
    for k, v in mixes.items():
        kf = food_kg_factors(v, **kw)
        res.setdefault('factor_rows', {})[k] = [int(len(kf)), int(len(v))]
        old = v[tmask(v, True)]
        kf = kf.loc[tmask(kf, True)] if 't' in kf.index.names else kf.iloc[:len(old)]
        f = kf.reset_index()
        names = [n for n in kf.index.names]
        for n in names:
            f[n] = f[n].astype(str)
        f['u_raw'] = f['u']
        f['u'] = [PAT.sub(lambda m: DEC[m.group(1)], x) for x in f['u']]
        f = f.set_index(names)
        o = old.reset_index()
        for n in old.index.names:
            o[n] = o[n].astype(str)
        o['u'] = [PAT.sub(lambda m: DEC[m.group(1)], x) for x in o['u']]
        oq = o.set_index(list(old.index.names))['Quantity'].astype(float)
        if list(old.index.names) == names and oq.index.is_unique:
            f['qty'] = oq.reindex(f.index).to_numpy()
        else:
            f['qty'] = old['Quantity'].astype(float).to_numpy() if len(old) == len(f) else np.nan
            res.setdefault('qty_fallback', []).append(k)
        fac[k] = f
    res['factor_index'] = list(fac['A_prev'].index.names)
    res['factor_columns'] = [c for c in fac['A_prev'].columns][:20]
    res['index_unique'] = {k: bool(v.index.is_unique) for k, v in fac.items()}
    A = fac['A_prev']
    decoded_s = pd.Series(A['u_raw'].astype(str)).str.match(r'^(659|660|662)(\.0)?(\s|$|\()')
    res['decoded_rows_2018'] = int(decoded_s.sum())
    for a, b in (('A_prev', 'B_head'), ('A_prev', 'C_oldprev_newhead'), ('A_prev', 'D_oldhead_newprev')):
        J = fac[a][['kg_per_unit', 'qty', 'u_raw']].join(fac[b][['kg_per_unit'] + (['KgFactorSource'] if 'KgFactorSource' in fac[b].columns else [])],
                                                       how='inner', rsuffix='_b')
        res.setdefault('joined_rows', {})[f'{a}->{b}'] = int(len(J))
        fa_, fb_ = J['kg_per_unit'].astype(float).to_numpy(), J['kg_per_unit_b'].astype(float).to_numpy()
        qty = J['qty'].astype(float).to_numpy()
        decoded = pd.Series(J['u_raw'].astype(str)).str.match(r'^(659|660|662)(\.0)?(\s|$|\()').to_numpy()
        moved = ~np.isclose(fa_, fb_, rtol=1e-9, atol=0, equal_nan=True)
        dkg = np.nan_to_num(fb_ * qty) - np.nan_to_num(fa_ * qty)
        ub = J.index.get_level_values('u').astype(str)
        srcb = J['KgFactorSource'].astype(str).to_numpy() if 'KgFactorSource' in J.columns else None
        rec = {'rows_moved': int(moved.sum()), 'decoded_rows_moved': int((moved & decoded).sum()),
               'other_rows_moved': int((moved & ~decoded).sum()),
               'delta_kg_total': float(dkg.sum()), 'delta_kg_decoded_rows': float(dkg[decoded].sum()),
               'delta_kg_other_rows': float(dkg[~decoded].sum()),
               'moved_by_u_top': pd.Series(np.asarray(ub)[moved]).value_counts().head(10).to_dict(),
               'delta_kg_by_u_top': pd.Series(dkg, index=np.asarray(ub)).groupby(level=0).sum().abs().sort_values(ascending=False).head(10).to_dict()}
        if srcb is not None:
            rec['moved_by_source_in_b'] = pd.Series(srcb[moved]).value_counts().to_dict()
        res[f'{a}->{b}'] = rec
        dump()
except Exception as exc:
    res['kg_error'] = f'{type(exc).__name__}: {exc}'[:800]
    res['kg_tb'] = traceback.format_exc()[-2000:]
    dump()

# withheld NA purchased rows on never-purchased source rows (Togo 2021-22)
try:
    T21 = countries_root() / 'Togo' / '2021-22' / 'Data'
    s07 = get_dataframe(str(T21 / 's07b_me_tgo2021.dta'), convert_categoricals=False)
    cov = get_dataframe(str(T21 / 's00_me_tgo2021.dta'), convert_categoricals=False)[['grappe', 'menage']].copy()
    cov['region'] = get_dataframe(str(T21 / 's00_me_tgo2021.dta'), convert_categoricals=True)['s00q01'].astype('string').str.strip().to_numpy()
    s07 = s07.merge(cov, on=['grappe', 'menage'], how='left', validate='many_to_one')
    recs = ll.Country('Togo').derivations()
    classify = recs['Togo::food_acquired::s07bq03a-floored-count-withheld'].resolve()
    br = np.asarray(classify(s07['s07bq03a'], s07['s07bq03b'], s07['region'].to_numpy(), s07['s07bq04'], s07['s07bq05']), dtype=object)
    q3 = pd.to_numeric(s07['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    q4 = pd.to_numeric(s07['s07bq04'], errors='coerce').fillna(0).to_numpy()
    q5 = pd.to_numeric(s07['s07bq05'], errors='coerce').fillna(0).to_numpy()
    held = br == 'withheld'
    implied = held & (((q4 <= 0) & (q5 <= 0)) | (q3 - q4 - q5 > 1e-6))
    s06 = pd.to_numeric(s07['s07bq06'], errors='coerce').to_numpy()
    lome = s07['region'].to_numpy() == 'Grand Lomé'
    og = (q4 > 0) | (q5 > 0)
    res['withheld'] = {'held': int(held.sum()), 'implied': int(implied.sum()),
                       'implied_by_s07bq06': pd.Series(s06[implied]).value_counts().to_dict(),
                       'implied_with_own_or_gift': int((implied & og).sum()),
                       'implied_with_own_or_gift_jamais': int((implied & og & (s06 == 5)).sum()),
                       'implied_purchase_only_jamais': int((implied & ~og & (s06 == 5)).sum()),
                       'implied_lome_purchase_only': int((implied & lome & ~og).sum()),
                       'implied_q3_zero': int((implied & (q3 == 0)).sum()),
                       'held_not_implied': int((held & ~implied).sum())}
    dump()
except Exception as exc:
    res['withheld_error'] = f'{type(exc).__name__}: {exc}'[:800]
    res['withheld_tb'] = traceback.format_exc()[-2000:]
    dump()
dump()
print('KGISO2 DONE', flush=True)
