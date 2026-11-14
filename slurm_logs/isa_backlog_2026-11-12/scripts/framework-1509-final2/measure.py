"""One arm of the #1509 final2 measurement (base / head / cand).

usage: measure.py OUTDIR COUNTRY [COUNTRY ...]
env:   ARM, SUBSET_COUNTRIES (space-separated; waves= invariance checked there),
       NUTRITION_COUNTRIES (space-separated).
Per country, cold in the arm's private LSMS_DATA_DIR: food_acquired (frame
hash), food_kg_factors with the Country kwargs (per-row columns saved
POSITIONALLY to <C>_rows.parquet), the derived tables in every mode, nutrition
where declared, and the waves= subset checks.  Writes OUTDIR/<C>.json.
"""
import hashlib
import json
import os
import sys
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
SUBSET = set(os.environ.get('SUBSET_COUNTRIES', '').split())
NUTRITION = set(os.environ.get('NUTRITION_COUNTRIES', '').split())
os.makedirs(OUT, exist_ok=True)

ATTR_KEYS = ('kg_factor_sources', 'kg_factor_evidence', 'price_rows_dropped',
             'quantity_rows_dropped', 'kg_factor_baseline_gate',
             'kg_factor_size_pool', 'kg_factor_declared_none',
             'kg_factor_wave_spread')
EXTRA = ('kg_item_unit', 'kg_unit', 'kg_item_unit_nonpurchase',
         'kg_unit_nonpurchase', 'kg_cell_has_purchase',
         'baseline_refused_spread', 'kg_size_pooled', 'kg_survey', 'kg_metric')


def jdump(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=1, default=str, sort_keys=True)


def tvals(df):
    return np.asarray(df.index.get_level_values('t').astype(str))


def uvals(df):
    return np.asarray(df.index.get_level_values('u').astype(str))


def rows_by_t(df):
    if df is None or 't' not in (df.index.names or []):
        return {}
    return {str(k): int(v) for k, v in
            pd.Series(tvals(df)).value_counts().sort_index().items()}


def kg_by_t(fq):
    m = uvals(fq) == 'kg'
    q = pd.to_numeric(fq['Quantity'], errors='coerce').to_numpy(dtype=float)
    s = pd.Series(q[m]).groupby(tvals(fq)[m]).sum()
    return {str(k): float(v) for k, v in s.items()}


def frame_hash(df):
    h = pd.util.hash_pandas_object(df.reset_index(), index=False).to_numpy()
    return hashlib.sha256(h.tobytes()).hexdigest()


def invariance_hash(df):
    """tests/test_uganda_invariance.py::_fingerprint's content hash."""
    row_hashes = pd.util.hash_pandas_object(df, index=True)
    return hashlib.sha256(row_hashes.values.tobytes()).hexdigest()


def lvl(df, name):
    if name in (df.index.names or []):
        return np.asarray(df.index.get_level_values(name), dtype=object)
    if name in df.columns:
        return np.asarray(df[name], dtype=object)
    return np.full(len(df), None, dtype=object)


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
        'seconds': round(time.time() - t0, 1), 'error': err, 'warnings': cats}
    print(f'[{ARM}] {label}: {round(time.time() - t0, 1)}s err={err}', flush=True)
    return out


def attrs_of(df, keys=ATTR_KEYS):
    return {k: df.attrs.get(k) for k in keys if k in df.attrs}


def slice_equal(sub, full, w):
    sl = full[tvals(full) == w]
    try:
        pd.testing.assert_frame_equal(sub.sort_index(), sl.sort_index(),
                                      check_dtype=False)
        return {'equal': True, 'rows': len(sub)}
    except AssertionError as e:
        return {'equal': False, 'rows': [len(sub), len(sl)], 'diff': str(e)[:600]}


for name in COUNTRIES:
    rec = {'country': name, 'arm': ARM, 'sha': os.environ['WANT_SHA']}
    try:
        c = ll.Country(name)
        rec['waves'] = list(c.waves)
        fa = call('food_acquired', rec, c._aggregate_wave_data, None,
                  'food_acquired')
        rec['fa_rows'] = len(fa)
        rec['fa_hash'] = frame_hash(fa)
        rec['fa_rows_by_t'] = rows_by_t(fa)
        rec['fa_s'] = {str(k): int(v) for k, v in pd.Series(
            lvl(fa, 's')).value_counts(dropna=False).items()}

        unit_kg = c._unit_kg_factors()
        no_kg = c._no_kg_factor_cells()
        kgf = call('food_kg_factors', rec, T.food_kg_factors, fa,
                   unit_kg=unit_kg, no_kg=no_kg)
        assert kgf.index.equals(fa.index)
        rec['layers'] = list(T.FOOD_KG_FACTOR_LAYERS)
        rec['kgf_attrs'] = attrs_of(kgf)
        ct = pd.crosstab(tvals(kgf), kgf['KgFactorSource'].to_numpy())
        rec['kgf_sources_by_t'] = {str(t): {str(k): int(v) for k, v in r.items()}
                                   for t, r in ct.iterrows()}
        q = pd.to_numeric(fa['Quantity'], errors='coerce').to_numpy(dtype=float)
        k = pd.to_numeric(kgf['kg_per_unit'], errors='coerce').to_numpy(dtype=float)
        rec['kgf_total_kg'] = float(np.nansum(q * k))
        rows = pd.DataFrame({
            't': tvals(fa), 'i': lvl(fa, 'i').astype(str),
            'j': lvl(fa, 'j').astype(str), 'u': lvl(fa, 'u').astype(str),
            's': lvl(fa, 's').astype(str), 'Quantity': q,
            'Expenditure': (pd.to_numeric(fa['Expenditure'], errors='coerce')
                            .to_numpy(dtype=float) if 'Expenditure' in fa.columns
                            else np.full(len(fa), np.nan)),
            'kg_per_unit': k,
            'KgFactorSource': kgf['KgFactorSource'].astype(str).to_numpy()})
        for col in EXTRA:
            if col in kgf.columns:
                rows[col] = kgf[col].to_numpy()
        rows.to_parquet(f'{OUT}/{name}_rows.parquet')
        # The purchase item rung and the item fallback are never both set.
        if {'kg_item_unit', 'kg_item_unit_nonpurchase'} <= set(kgf.columns):
            rec['item_and_np_both_set'] = int((kgf['kg_item_unit'].notna()
                                               & kgf['kg_item_unit_nonpurchase'].notna()).sum())
        del rows

        fq = call('food_quantities_kgs', rec, c.food_quantities, units='kgs')
        if fq is not None:
            byt = kg_by_t(fq)
            rec['fq_kgs'] = {'rows': len(fq), 'rows_by_t': rows_by_t(fq),
                             'kg_total': float(sum(byt.values())), 'kg_by_t': byt,
                             'attrs': attrs_of(fq)}
        fqu = call('food_quantities_units', rec, c.food_quantities, units='units')
        if fqu is not None:
            rec['fq_units'] = {'rows': len(fqu), 'rows_by_t': rows_by_t(fqu),
                               'attrs': attrs_of(fqu)}
            del fqu
        frames = {}
        for lab, kw in (('food_prices_default', {}),
                        ('food_prices_kgprice', {'units': 'kgprice'}),
                        ('food_prices_unitvalue', {'units': 'unitvalue'}),
                        ('food_prices_unitprice', {'units': 'unitprice'})):
            fp = call(lab, rec, c.food_prices, **kw)
            if fp is None:
                continue
            price = pd.to_numeric(fp['Price'], errors='coerce').to_numpy(dtype=float)
            rec[lab] = {'rows': len(fp), 'rows_by_t': rows_by_t(fp),
                        'price_median': float(np.nanmedian(price)) if len(fp) else None,
                        'price_hash': frame_hash(fp),
                        'attrs': attrs_of(fp)}
            if lab == 'food_prices_default':
                frames[lab] = fp
            else:
                del fp

        if name in SUBSET and fq is not None:
            sub = {}
            for w in sorted(set(tvals(fa))):
                if {str(x) for x in [w]} >= {str(x) for x in c.waves}:
                    continue
                r = {}
                q1 = call(f'fq_kgs_wave_{w}', rec, c.food_quantities, units='kgs',
                          waves=[w])
                if q1 is not None:
                    r['kgs'] = slice_equal(q1, fq, w)
                p1 = call(f'fp_default_wave_{w}', rec, c.food_prices, waves=[w])
                if p1 is not None and 'food_prices_default' in frames:
                    r['kgvalue'] = slice_equal(p1, frames['food_prices_default'], w)
                sub[w] = r
            rec['subset'] = sub
        del frames

        if name in NUTRITION:
            n = call('nutrition', rec, c.nutrition)
            if n is not None:
                n.to_parquet(f'{OUT}/{name}_nutrition.parquet')
                num = n.select_dtypes('number')
                rec['nutrition'] = {
                    'rows': len(n), 'shape': list(n.shape),
                    'cols': [str(x) for x in n.columns],
                    'index_names': list(n.index.names),
                    'totals': {str(k): float(v) for k, v in num.sum().items()},
                    'invariance_hash': invariance_hash(n),
                    'frame_hash': frame_hash(n)}
    except Exception:  # noqa: BLE001
        rec['fatal'] = traceback.format_exc()[-4000:]
        print(rec['fatal'], flush=True)
    jdump(rec, f'{OUT}/{name}.json')
    print(f'[{ARM}] DONE {name}', flush=True)
