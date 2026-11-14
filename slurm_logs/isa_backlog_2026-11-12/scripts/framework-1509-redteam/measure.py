"""One arm of the base-vs-candidate measurement.

usage: measure.py OUTDIR COUNTRY [COUNTRY ...]
Writes, per country, OUTDIR/{C}.json plus parquets of the frames compared later.
"""
import os
import sys
import json
import time
import traceback
import warnings

sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa: F401,E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library import transformations as T  # noqa: E402

OUT = sys.argv[1]
COUNTRIES = sys.argv[2:]
ARM = os.environ['ARM']
SUBSET_WAVES = os.environ.get('SUBSET_WAVES', 'all')


def jdump(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=1, default=str)


def tvals(df):
    return np.asarray(df.index.get_level_values('t').astype(str))


def uvals(df):
    return np.asarray(df.index.get_level_values('u').astype(str))


def rows_by_t(df):
    return {str(k): int(v) for k, v in
            pd.Series(tvals(df)).value_counts().sort_index().items()}


def kg_by_t(fq):
    u = uvals(fq)
    m = u == 'kg'
    s = pd.Series(fq['Quantity'].to_numpy()[m]).groupby(tvals(fq)[m]).sum()
    return {str(k): float(v) for k, v in s.items()}


def call(label, rec, fn, *a, **k):
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            out = fn(*a, **k)
            err = None
        except Exception as exc:  # noqa: BLE001
            out = None
            err = f'{type(exc).__name__}: {exc}'[:2000]
    cats = {}
    for x in w:
        cats[x.category.__name__] = cats.get(x.category.__name__, 0) + 1
    rec.setdefault('calls', {})[label] = {
        'seconds': round(time.time() - t0, 1), 'error': err,
        'warnings': cats}
    print(f'[{ARM}] {label}: {round(time.time()-t0,1)}s err={err}', flush=True)
    return out


def save(df, path):
    if df is None:
        return
    df.reset_index().to_parquet(path)


def jsonable_attrs(df, keys):
    return {k: df.attrs.get(k) for k in keys if k in df.attrs}


ATTR_KEYS = ('kg_factor_sources', 'kg_factor_evidence', 'price_rows_dropped',
             'quantity_rows_dropped', 'kg_factor_baseline_gate',
             'kg_factor_size_pool', 'kg_factor_declared_none',
             'kg_factor_wave_spread')

for name in COUNTRIES:
    rec = {'country': name, 'arm': ARM}
    try:
        c = ll.Country(name)
        rec['waves'] = list(c.waves)
        fa = call('food_acquired', rec, c._aggregate_wave_data, None,
                  'food_acquired')
        rec['fa_rows'] = len(fa)
        rec['fa_rows_by_t'] = rows_by_t(fa)
        s = pd.Series(np.asarray(fa.index.get_level_values('s'), dtype=object))
        rec['fa_s'] = {str(k): int(v) for k, v in
                       s.value_counts(dropna=False).items()}
        save(fa, f'{OUT}/{name}_fa.parquet')

        unit_kg = c._unit_kg_factors()
        no_kg = c._no_kg_factor_cells()
        kgf = call('food_kg_factors', rec, T.food_kg_factors, fa,
                   unit_kg=unit_kg, no_kg=no_kg)
        assert kgf.index.equals(fa.index)
        kcols = [x for x in kgf.columns]
        kgf.reset_index(drop=True)[kcols].to_parquet(f'{OUT}/{name}_kgf.parquet')
        rec['kgf_attrs'] = jsonable_attrs(kgf, ATTR_KEYS)
        ct = pd.crosstab(tvals(kgf), kgf['KgFactorSource'].to_numpy())
        rec['kgf_sources_by_t'] = {str(t): {str(k): int(v) for k, v in r.items()}
                                   for t, r in ct.iterrows()}

        fq = call('food_quantities_kgs', rec, c.food_quantities, units='kgs')
        if fq is not None:
            rec['fq_kgs'] = {'rows': len(fq), 'rows_by_t': rows_by_t(fq),
                             'kg_total': float(sum(kg_by_t(fq).values())),
                             'kg_by_t': kg_by_t(fq),
                             'attrs': jsonable_attrs(fq, ATTR_KEYS)}
            save(fq, f'{OUT}/{name}_fq_kgs.parquet')
        fqu = call('food_quantities_units', rec, c.food_quantities, units='units')
        if fqu is not None:
            u = uvals(fqu)
            rec['fq_units'] = {'rows': len(fqu), 'rows_by_t': rows_by_t(fqu),
                               'u_unknown_rows': int(T._unit_sentinel_mask(fqu).sum()),
                               'u_value_rows': int(np.asarray(T._is_currency_denominated(
                                   pd.Index(u))).sum()),
                               'attrs': jsonable_attrs(fqu, ATTR_KEYS)}
            save(fqu, f'{OUT}/{name}_fq_units.parquet')
        frames = {}
        for lab, kw in (('food_prices_default', {}),
                        ('food_prices_kgprice', {'units': 'kgprice'}),
                        ('food_prices_unitvalue', {'units': 'unitvalue'}),
                        ('food_prices_unitprice', {'units': 'unitprice'})):
            fp = call(lab, rec, c.food_prices, **kw)
            if fp is None:
                continue
            frames[lab] = fp
            u = uvals(fp)
            price = pd.to_numeric(fp['Price'], errors='coerce').to_numpy()
            cur = np.asarray(T._is_currency_denominated(pd.Index(u)), dtype=bool)
            rec[lab] = {'rows': len(fp), 'rows_by_t': rows_by_t(fp),
                        'u_value_rows': int(cur.sum()),
                        'u_unknown_rows': int(T._unit_sentinel_mask(fp).sum()),
                        'price_eq_1_on_value': int((cur & (price == 1.0)).sum()),
                        'price_eq_1_total': int((price == 1.0).sum()),
                        'attrs': jsonable_attrs(fp, ATTR_KEYS)}
            if lab != 'food_prices_unitprice':
                save(fp, f'{OUT}/{name}_{lab}.parquet')

        # GH #1487: every wave subset is a slice of the all-waves result.
        waves = list(c.waves)
        fa_waves = sorted(set(tvals(fa)))
        sub_waves = fa_waves if SUBSET_WAVES == 'all' else fa_waves[:2]
        sub = {}
        for w in sub_waves:
            r = {}
            q = call(f'fq_kgs_wave_{w}', rec, c.food_quantities, units='kgs',
                     waves=[w])
            if q is not None and fq is not None:
                sl = fq[tvals(fq) == w]
                try:
                    pd.testing.assert_frame_equal(q.sort_index(), sl.sort_index(),
                                                  check_dtype=False)
                    r['kgs_equal'] = True
                except AssertionError as e:
                    r['kgs_equal'] = False
                    r['kgs_diff'] = str(e)[:600]
                    r['kgs_rows'] = [len(q), len(sl)]
                r['kgs_sources'] = q.attrs.get('kg_factor_sources')
                r['kgs_fallback'] = (q.attrs.get('kg_factor_evidence') or {}).get('fallback')
            p = call(f'fp_default_wave_{w}', rec, c.food_prices, waves=[w])
            full_fp = frames.get('food_prices_default')
            if p is not None and full_fp is not None:
                sl = full_fp[tvals(full_fp) == w]
                try:
                    pd.testing.assert_frame_equal(p.sort_index(), sl.sort_index(),
                                                  check_dtype=False)
                    r['kgvalue_equal'] = True
                except AssertionError as e:
                    r['kgvalue_equal'] = False
                    r['kgvalue_diff'] = str(e)[:600]
                    r['kgvalue_rows'] = [len(p), len(sl)]
            sub[w] = r
        rec['subset'] = sub

        if name == 'GhanaLSS':
            e = call('unitvalue_1987-88', rec, c.food_prices, units='unitvalue',
                     waves=['1987-88'])
            rec['unitvalue_1987_88'] = (None if e is None else
                                        {'rows': len(e),
                                         'index': list(e.index.names),
                                         'cols': list(e.columns),
                                         'attrs': jsonable_attrs(e, ATTR_KEYS)})
        if name in ('GhanaLSS', 'Uganda', 'Ethiopia'):
            n = call('nutrition', rec, c.nutrition)
            if n is not None:
                save(n, f'{OUT}/{name}_nutrition.parquet')
                rec['nutrition'] = {'rows': len(n), 'cols': list(n.columns)}
    except Exception:  # noqa: BLE001
        rec['fatal'] = traceback.format_exc()[-4000:]
        print(rec['fatal'], flush=True)
    jdump(rec, f'{OUT}/{name}.json')
    print(f'[{ARM}] DONE {name}', flush=True)
