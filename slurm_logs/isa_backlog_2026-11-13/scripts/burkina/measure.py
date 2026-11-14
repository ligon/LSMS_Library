"""Cold build of every Burkina_Faso table plus the variants this stream moves.

Usage: measure.py LABEL FRAMES_DIR OUT_JSON   (cwd = clone; PYTHONPATH = clone; private root)
Frames (flattened, coordinates reduced to md5) go to FRAMES_DIR, which must be node-local.
OUT_JSON holds counts, labels, dtypes and digests only.
"""
import hashlib
import json
import subprocess
import sys
import time
import traceback
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library.country import Country, grain_reports
from lsms_library.null_read_audit import null_read_reports
from lsms_library.null_index_audit import null_index_reports
from lsms_library.quantity_audit import quantity_reports
from lsms_library.datetime_parse import datetime_reports
from lsms_library import transformations as T

label, FR, JS = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
FR.mkdir(parents=True, exist_ok=True)
C = 'Burkina_Faso'
FAM = ('GrainCollapse', 'NullRead', 'NullIndex', 'ClusterIdentity', 'Quantity', 'Degraded',
       'Datetime', 'LabelPass', 'LegacyFallback', 'SampleUnavailable')
out = dict(label=label, head=subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True,
                                            text=True).stdout.strip(), calls={})


def save():
    with open(JS, 'w') as fh:
        json.dump(out, fh, indent=1, default=str, sort_keys=True)


def jsonable(a):
    try:
        return json.loads(json.dumps(a, default=str))
    except Exception:  # noqa: BLE001
        return str(a)[:2000]


ATTRS = ('kg_factor_sources', 'kg_factor_evidence', 'kg_factor_baseline_gate',
         'kg_factor_size_pool', 'kg_factor_wave_spread', 'kg_factor_declared_none',
         'price_rows_dropped', 'quantity_rows_dropped', 'valuation_sources',
         'valuation_geo_levels')


def digest(flat):
    d = flat[sorted(flat.columns)]
    h = pd.util.hash_pandas_object(d.astype(object).where(d.notna(), None).astype(str),
                                   index=False).to_numpy()
    return hashlib.md5(np.sort(h).tobytes()).hexdigest()


def run(name, fn, save_frame=True):
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df, err = fn(), None
        except Exception as e:  # noqa: BLE001
            df, err = None, f'{type(e).__name__}: {e}'[:800] + ' | ' + traceback.format_exc()[-1200:]
    cats, msgs = {}, []
    for w in caught:
        n = w.category.__name__
        cats[n] = cats.get(n, 0) + 1
        if any(f in n for f in FAM):
            msgs.append(f'{n}: {str(w.message)[:600]}')
    rec = dict(seconds=round(time.time() - t0, 1), err=err, warnings=cats, messages=msgs[:15])
    if isinstance(df, pd.DataFrame):
        flat = df.reset_index()
        rec.update(rows=int(len(df)), index=[str(n) for n in df.index.names],
                   columns=[str(c) for c in df.columns], unique=bool(df.index.is_unique),
                   attrs={k: jsonable(df.attrs[k]) for k in ATTRS if k in df.attrs},
                   dtypes={str(k): str(v) for k, v in flat.dtypes.items()})
        if 't' in flat:
            rec['rows_by_t'] = {str(k): int(n) for k, n in flat.groupby(flat['t'].astype(str)).size().items()}
        rec['digest'] = digest(flat)
        if save_frame:
            f2 = flat.copy()
            for c in [c for c in f2.columns if str(c).lower() in ('latitude', 'longitude')]:
                f2[c] = f2[c].map(lambda x: None if pd.isna(x) else hashlib.md5(
                    f'{float(x):.12f}'.encode()).hexdigest()[:16]).astype('string')
            for c in f2.columns:
                if f2[c].dtype == object:
                    f2[c] = f2[c].astype('string')
            try:
                f2.to_parquet(FR / f'{name}.parquet')
            except Exception as e:  # noqa: BLE001
                rec['save_err'] = repr(e)[:300]
    out['calls'][name] = rec
    save()
    print(f'{label} {name}: {rec.get("rows")} rows {rec["seconds"]}s err={(err or "")[:120]}', flush=True)
    return df


t00 = time.time()
names = [t for t in sorted(Country(C).data_scheme) if t not in ('panel_ids', 'updated_ids')]
out['tables'] = names
for t in names:
    run(t, lambda t=t: getattr(Country(C), t)())

VARIANTS = [
    ('food_expenditures_total', 'food_expenditures', dict(basis='total')),
    ('food_expenditures_valued', 'food_expenditures', dict(basis='total', valuation='median_price')),
    ('food_prices_unitvalue', 'food_prices', dict(units='unitvalue')),
    ('food_prices_kgprice', 'food_prices', dict(units='kgprice')),
    ('food_quantities_units', 'food_quantities', dict(units='units')),
    ('crop_production_2021', 'crop_production', dict(waves=['2021-22'])),
    ('crop_production_2018', 'crop_production', dict(waves=['2018-19'])),
    ('interview_date_2021', 'interview_date', dict(waves=['2021-22'])),
    ('plot_inputs_2021', 'plot_inputs', dict(waves=['2021-22'])),
]
for name, m, kw in VARIANTS:
    run(name, lambda m=m, kw=kw: getattr(Country(C), m)(**kw))

# kg factors over the served food_acquired (#1442 x #1509)
c = Country(C)
fa = c.food_acquired()
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    kg = T.food_kg_factors(fa, unit_kg=c._unit_kg_factors(), no_kg=c._no_kg_factor_cells())
out['kg_attrs'] = {k: jsonable(kg.attrs[k]) for k in ATTRS if k in kg.attrs}
keep = kg[[k for k in ('kg_per_unit', 'KgFactorSource', 'kg_item_unit', 'kg_unit',
                       'kg_item_unit_nonpurchase', 'kg_unit_nonpurchase') if k in kg.columns]].copy()
for col in ('Quantity', 'Expenditure'):
    keep[col] = fa[col].to_numpy()
keep = keep.reset_index()
for col in keep.columns:
    if keep[col].dtype == object:
        keep[col] = keep[col].astype('string')
keep.to_parquet(FR / 'kg_factors.parquet')
save()

# Agricultural transforms on the served frames
ag = {}
try:
    cp = c.crop_production()
    pf = c.plot_features()
    for nm, fn in (('harvest_kg', lambda: T.harvest_kg(cp)),
                   ('yield_kg_parcel', lambda: T.yield_kg(cp, pf, on='parcel')),
                   ('yield_kg_plot', lambda: T.yield_kg(cp, pf, on='plot'))):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                d = fn()
            f = d.reset_index()
            ag[nm] = {'rows': int(len(d)),
                      'rows_by_t': {str(k): int(v) for k, v in f.groupby(f['t'].astype(str)).size().items()},
                      'nonnull_by_t': {str(k): int(v) for k, v in
                                       f.groupby(f['t'].astype(str))[d.columns[0]].apply(lambda s: s.notna().sum()).items()},
                      'columns': [str(x) for x in d.columns]}
            d.reset_index().astype({k: 'string' for k in d.index.names if k}).to_parquet(FR / f'{nm}.parquet')
        except Exception as e:  # noqa: BLE001
            ag[nm] = f'ERR {type(e).__name__}: {e}'[:400]
    pi = c.plot_inputs()
    for nm, fn in (('nitrogen_kg', lambda: T.nitrogen_kg(pi)), ('seed_kg', lambda: T.seed_kg(pi))):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                d = fn()
            ag[nm] = {'rows': int(len(d)), 'digest': digest(d.reset_index()),
                      'sum': float(pd.to_numeric(d.iloc[:, 0], errors='coerce').sum())}
        except Exception as e:  # noqa: BLE001
            ag[nm] = f'ERR {type(e).__name__}: {e}'[:400]
except Exception as e:  # noqa: BLE001
    ag['err'] = traceback.format_exc()[-1500:]
out['ag'] = ag
save()

# crop_production detail (2021-22 driver) and the strict grain lever
crop = {}
try:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = Country(C).crop_production()
    f = cp.reset_index()
    f['t'] = f['t'].astype(str)
    crop['rows_by_t'] = {k: int(v) for k, v in f['t'].value_counts().items()}
    crop['index'] = [str(n) for n in cp.index.names]
    crop['unique'] = bool(cp.index.is_unique)
    w = f[f['t'] == '2021-22']
    if len(w):
        der = w['Derivation'].astype('string').fillna('')
        keys = {}
        for ks in der:
            for k in [x for x in ks.split('+') if x]:
                keys[k] = keys.get(k, 0) + 1
        crop['keys_2021'] = keys
        crop['farm_level_2021'] = int((w['plot_id'] == 'Farm-level').sum())
        crop['quantity_rows_2021'] = int(w['Quantity'].notna().sum())
        crop['quantity_sum_2021_by_key'] = {
            k: round(float(w.loc[der.str.contains(k, regex=False), 'Quantity'].sum()), 1)
            for k in keys}
        crop['derived_u_2021'] = {str(k): int(v) for k, v in w.loc[der.str.contains(
            'harvest-from-dispositions', regex=False), 'u'].value_counts().items()}
        crop['unit_sold_2021'] = {str(k): int(v) for k, v in w['Unit_sold'].value_counts().items()}
        crop['u_2021'] = {str(k): int(v) for k, v in w['u'].value_counts().items()}
        crop['condition_sold_2021'] = {str(k): int(v) for k, v in w['Condition_sold'].value_counts(dropna=False).items()} if 'Condition_sold' in w else None
        crop['crop_unknown_2021'] = int(w['crop'].astype(str).str.startswith('Unknown [').sum())
        crop['crops_from_text'] = {str(k): int(v) for k, v in w.loc[der.str.contains(
            'crop-code-from-roster-text', regex=False), 'crop'].value_counts().items()}
        if 'production_obs' in w:
            po = w['production_obs'].astype(str)
            crop['production_obs_2021'] = {'row_tokens': int(po.str.startswith('row:').sum()),
                                           'sentinel': int((po == 'unresolved_observation').sum())}
            no_obs = cp.xs('2021-22', level='t', drop_level=False).index.droplevel('production_obs')
            crop['repeat_rows_without_obs_2021'] = int(no_obs.duplicated(keep=False).sum())
    if 'production_obs' in f:
        crop['production_obs_2018'] = {str(k): int(v) for k, v in
                                       f.loc[f['t'] == '2018-19', 'production_obs'].value_counts().items()}
except Exception as e:  # noqa: BLE001
    crop['err'] = traceback.format_exc()[-1500:]
code_strict = ("import os, warnings, json\nos.environ['LSMS_GRAIN_STRICT']='1'\n"
               "import lsms_library as ll\nwarnings.simplefilter('ignore')\n"
               "res={}\n"
               "for kw in ({}, {'waves':['2021-22']}, {'waves':['2018-19']}):\n"
               "    try:\n"
               "        res[str(kw)]=len(ll.Country('Burkina_Faso').crop_production(**kw))\n"
               "    except Exception as e:\n"
               "        res[str(kw)]=type(e).__name__+': '+str(e)[:200]\n"
               "print('STRICT'+json.dumps(res))\n")
pr = subprocess.run([sys.executable, '-c', code_strict], capture_output=True, text=True)
line = [ln for ln in pr.stdout.splitlines() if ln.startswith('STRICT')]
crop['grain_strict'] = json.loads(line[0][6:]) if line else dict(rc=pr.returncode, err=pr.stderr[-800:])
out['crop'] = crop
save()

# Country vs Feature (one-country assemblies)
feat = {}
for tab in ('plot_inputs', 'crop_production', 'interview_date'):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            f = ll.Feature(tab)([C])
            cc = getattr(Country(C), tab)()
        fl, cl = f.reset_index(), cc.reset_index()
        rec = {'feature_rows': int(len(f)), 'country_rows': int(len(cc)), 'unique': bool(f.index.is_unique),
               'feature_index': [str(n) for n in f.index.names]}
        if 'u' in fl and 'u' in cl:
            rec['u_counts_equal'] = (fl['u'].astype(str).value_counts().sort_index().to_dict()
                                     == cl['u'].astype(str).value_counts().sort_index().to_dict())
        if 'Quantity' in fl and 'Quantity' in cl:
            rec['quantity_sum'] = [float(fl['Quantity'].sum()), float(cl['Quantity'].sum())]
        feat[tab] = rec
    except Exception as e:  # noqa: BLE001
        feat[tab] = f'ERR {type(e).__name__}: {e}'[:400]
out['feature'] = feat
save()


def rep(rs):
    return [{k: (v if isinstance(v, (int, float, str, bool, type(None))) else str(v)[:200])
             for k, v in dict(r).items() if k not in ('examples', 'offenders', 'columns_differing')}
            for r in rs]


out['reports'] = dict(grain=rep(grain_reports(country=C)), null_read=rep(null_read_reports(country=C)),
                      null_index=rep(null_index_reports(country=C)),
                      quantity=rep(quantity_reports(country=C)),
                      datetime=rep(datetime_reports(country=C)))
save()

# Warm fresh-process digests for the tables this stream moves
code = ("import lsms_library as ll, warnings, hashlib, numpy as np, pandas as pd, json\n"
        "warnings.simplefilter('ignore')\n"
        "res={}\n"
        "for t in ('interview_date','crop_production','plot_inputs','food_acquired','food_expenditures','food_prices','food_quantities'):\n"
        "    d=getattr(ll.Country('Burkina_Faso'),t)().reset_index(); d=d[sorted(d.columns)]\n"
        "    h=pd.util.hash_pandas_object(d.astype(object).where(d.notna(),None).astype(str),index=False).to_numpy()\n"
        "    res[t]=[int(len(d)),hashlib.md5(np.sort(h).tobytes()).hexdigest()]\n"
        "print('WARM'+json.dumps(res))\n")
pr = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True)
line = [ln for ln in pr.stdout.splitlines() if ln.startswith('WARM')]
out['warm_fresh'] = json.loads(line[0][4:]) if line else dict(rc=pr.returncode, err=pr.stderr[-1500:])
out['warm_equal_cold'] = {t: (out['warm_fresh'][t][1] == out['calls'].get(t, {}).get('digest'))
                          for t in (out['warm_fresh'] if line else {})}
out['seconds'] = round(time.time() - t00, 1)
save()
print('DONE', label, out['seconds'], flush=True)
