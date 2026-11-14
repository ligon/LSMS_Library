"""Red-team cold build of the Burkina tables this stream touches.

Usage: build.py LABEL FRAMES_DIR OUT.json   (cwd = clone, PYTHONPATH = clone, private empty root)
Frames go to FRAMES_DIR (node-local).  OUT.json holds counts, warnings, reports.
"""
import json
import subprocess
import sys
import time
import traceback
import warnings
from pathlib import Path

import pandas as pd

import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.country import Country, grain_reports
from lsms_library.datetime_parse import datetime_reports
from lsms_library.quantity_audit import quantity_reports
from lsms_library.null_index_audit import null_index_reports
from lsms_library.null_read_audit import null_read_reports

label, FR, JS = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
FR.mkdir(parents=True, exist_ok=True)
C = 'Burkina_Faso'
out = {'label': label, 'head': subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True,
                                              text=True).stdout.strip(), 'calls': {}}


def save():
    with open(JS, 'w') as fh:
        json.dump(out, fh, indent=1, default=str, sort_keys=True)


def jsonable(a):
    try:
        return json.loads(json.dumps(a, default=str))
    except Exception:  # noqa: BLE001
        return str(a)[:3000]


def store(name, df):
    flat = df.reset_index()
    for col in flat.columns:
        if flat[col].dtype == object or str(flat[col].dtype) == 'category':
            flat[col] = flat[col].astype('string')
    try:
        flat.to_parquet(FR / f'{name}.parquet')
    except Exception:  # noqa: BLE001
        flat = flat.copy()
        for col in flat.columns:
            if not pd.api.types.is_numeric_dtype(flat[col]) and not pd.api.types.is_bool_dtype(flat[col]) \
                    and not pd.api.types.is_datetime64_any_dtype(flat[col]):
                flat[col] = flat[col].astype('string')
        flat.to_parquet(FR / f'{name}.parquet')
    return flat


def run(name, fn):
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df, err = fn(), None
        except Exception as e:  # noqa: BLE001
            df, err = None, f'{type(e).__name__}: {e}'[:1500] + ' | ' + traceback.format_exc()[-1500:]
    cats, msgs = {}, []
    for w in caught:
        n = w.category.__name__
        cats[n] = cats.get(n, 0) + 1
        if n not in ('FutureWarning', 'DeprecationWarning', 'PerformanceWarning') and len(msgs) < 25:
            msgs.append(f'{n}: {str(w.message)[:700]}')
    rec = {'seconds': round(time.time() - t0, 1), 'err': err, 'warnings': cats, 'messages': msgs}
    if isinstance(df, pd.DataFrame):
        rec.update(rows=int(len(df)), index=[str(x) for x in df.index.names],
                   columns=[str(x) for x in df.columns], unique=bool(df.index.is_unique),
                   attrs={k: jsonable(v) for k, v in df.attrs.items() if k not in ('id_converted',)},
                   dtypes={str(k): str(v) for k, v in df.reset_index().dtypes.items()})
        try:
            flat = store(name, df)
            if 't' in flat:
                rec['rows_by_t'] = {str(k): int(v) for k, v in flat.groupby(flat['t'].astype(str)).size().items()}
        except Exception as e:  # noqa: BLE001
            rec['store_err'] = f'{type(e).__name__}: {str(e)[:500]}'
    out['calls'][name] = rec
    save()
    print(label, name, rec.get('rows'), rec['seconds'], (err or '')[:200], flush=True)
    return df


c = Country(C)
cp = run('crop_production', lambda: Country(C).crop_production())
run('crop_production_2021', lambda: Country(C).crop_production(waves=['2021-22']))
run('crop_production_2018', lambda: Country(C).crop_production(waves=['2018-19']))
pf = run('plot_features', lambda: Country(C).plot_features())
if cp is not None and pf is not None:
    run('harvest_kg', lambda: T.harvest_kg(cp))
    run('yield_kg_parcel', lambda: T.yield_kg(cp, pf, on='parcel'))
    run('yield_kg_plot', lambda: T.yield_kg(cp, pf, on='plot'))
if cp is not None:
    def _gcr():
        r = T.gross_crop_revenue(cp)
        return r.to_frame() if isinstance(r, pd.Series) else r
    run('gross_crop_revenue', _gcr)

    def _cdiv():
        r = T.crop_diversity(cp, weight='count')
        return r.to_frame() if isinstance(r, pd.Series) else r
    run('crop_diversity', _cdiv)

    def _mpv():
        r = T.median_price_valuation(cp, ['v'], item_keys=('crop',))
        return r.to_frame() if isinstance(r, pd.Series) else r
    run('median_price_valuation', _mpv)
out['grain_crop_production'] = jsonable(grain_reports(country=C, table='crop_production'))
save()

fa = run('food_acquired', lambda: Country(C).food_acquired())
run('food_expenditures', lambda: Country(C).food_expenditures())
run('food_expenditures_total', lambda: Country(C).food_expenditures(basis='total'))
run('food_expenditures_total_valued', lambda: Country(C).food_expenditures(basis='total', valuation='median_price'))
run('food_prices', lambda: Country(C).food_prices())
run('food_prices_unitvalue', lambda: Country(C).food_prices(units='unitvalue'))
run('food_quantities', lambda: Country(C).food_quantities())
run('food_quantities_units', lambda: Country(C).food_quantities(units='units'))
if fa is not None:
    def kgf():
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            kg = T.food_kg_factors(fa, unit_kg=c._unit_kg_factors(), no_kg=c._no_kg_factor_cells())
        keep = kg[[k for k in ('kg_per_unit', 'KgFactorSource') if k in kg.columns]].copy()
        keep['Quantity'] = fa['Quantity'].to_numpy()
        keep['Expenditure'] = fa['Expenditure'].to_numpy()
        out['kg_attrs'] = {k: jsonable(v) for k, v in kg.attrs.items()}
        return keep
    run('kg_factors', kgf)

pi = run('plot_inputs', lambda: Country(C).plot_inputs())
if pi is not None:
    run('nitrogen_kg', lambda: T.nitrogen_kg(pi))
    run('seed_kg', lambda: T.seed_kg(pi))
    out['plot_inputs_u_counts'] = pi.reset_index().groupby(['t', 'u']).size().astype(int).reset_index().astype(str).values.tolist()

run('interview_date', lambda: Country(C).interview_date())
out['datetime_reports'] = jsonable(datetime_reports(country=C))
save()

run('feature_crop_production', lambda: ll.Feature('crop_production')([C]))
run('feature_plot_inputs', lambda: ll.Feature('plot_inputs')([C]))

# derivation inputs (candidate keys; base raises KeyError, recorded)
for key in ('Burkina_Faso::crop_production::hh-crop-sale-single-row',
            'Burkina_Faso::crop_production::harvest-to-date'):
    run('inputs_' + key.rsplit('::', 1)[1], lambda key=key: Country(C).derivation_inputs(key, wave='2021-22'))

out['reports'] = {
    'grain': jsonable(grain_reports(country=C)),
    'quantity': jsonable([{k: r.get(k) for k in ('table', 'column', 'wave', 'n_implausible', 'rows', 'direction')}
                          for r in quantity_reports(country=C)]),
    'null_index': jsonable(null_index_reports(country=C)),
    'null_read_n': len(null_read_reports(country=C)),
}
save()
print('build complete', label, flush=True)
