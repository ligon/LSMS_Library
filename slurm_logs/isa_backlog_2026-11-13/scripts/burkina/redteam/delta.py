"""Delta review build (one arm).  Usage: delta.py LABEL FRAMES_DIR OUT.json  (cold private root)."""
import json
import subprocess
import sys
import time
import traceback
import warnings

import pandas as pd

import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.country import Country, grain_reports

label, FR, OJ = sys.argv[1:4]
C = 'Burkina_Faso'
res = {'label': label, 'head': subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True,
                                              text=True).stdout.strip(), 'calls': {}}


def _sk(o):
    if isinstance(o, dict):
        return {str(k): _sk(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_sk(v) for v in o]
    return o


def save():
    json.dump(_sk(res), open(OJ, 'w'), indent=1, default=str)


def store(name, df):
    f = (df.to_frame() if isinstance(df, pd.Series) else df).reset_index()
    for col in f.columns:
        if f[col].dtype == object or str(f[col].dtype) == 'category':
            f[col] = f[col].astype('string')
    f.to_parquet(f'{FR}/{name}.parquet')
    return f


def run(name, fn):
    t0 = time.time()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df, err = fn(), None
        except Exception as e:  # noqa: BLE001
            df, err = None, f'{type(e).__name__}: {str(e)[:1200]}'
    rec = {'s': round(time.time() - t0, 1), 'err': err,
           'warnings': sorted({w.category.__name__ for w in caught} - {'FutureWarning', 'DeprecationWarning'})}
    if isinstance(df, (pd.DataFrame, pd.Series)):
        try:
            f = store(name, df)
            rec.update(rows=len(f), index=[str(x) for x in df.index.names],
                       columns=[str(x) for x in (df.columns if isinstance(df, pd.DataFrame) else [df.name])],
                       unique=bool(df.index.is_unique))
            if 't' in f:
                rec['rows_by_t'] = f.groupby(f['t'].astype(str)).size().astype(int).to_dict()
        except Exception as e:  # noqa: BLE001
            rec['store_err'] = f'{type(e).__name__}: {e}'
    res['calls'][name] = rec
    save()
    print(label, name, rec.get('rows'), rec['s'], (err or '')[:160], flush=True)
    return df


cp = run('crop_production', lambda: Country(C).crop_production())
run('crop_production_2018', lambda: Country(C).crop_production(waves=['2018-19']))
run('crop_production_2021', lambda: Country(C).crop_production(waves=['2021-22']))
res['grain_crop'] = [{k: r.get(k) for k in ('wave', 'table')} for r in grain_reports(country=C, table='crop_production')]
pf = run('plot_features', lambda: Country(C).plot_features())
if cp is not None:
    run('harvest_kg', lambda: T.harvest_kg(cp))
    run('yield_kg_parcel', lambda: T.yield_kg(cp, pf, on='parcel'))
    run('gcr', lambda: T.gross_crop_revenue(cp))
    run('cdiv', lambda: T.crop_diversity(cp, weight='count'))
    run('mpv', lambda: T.median_price_valuation(cp, ['v'], item_keys=('crop',)))
run('feature_crop_bf', lambda: ll.Feature('crop_production')([C]))
run('feature_crop_bf_mali', lambda: ll.Feature('crop_production')([C, 'Mali']))
run('food_acquired', lambda: Country(C).food_acquired())
run('food_prices', lambda: Country(C).food_prices())
run('food_prices_unitvalue', lambda: Country(C).food_prices(units='unitvalue'))
run('food_quantities', lambda: Country(C).food_quantities())
run('interview_date', lambda: Country(C).interview_date())
run('plot_inputs', lambda: Country(C).plot_inputs())
for key in ('hh-crop-sale-single-row', 'harvest-to-date', 'harvest-from-dispositions',
            'crop-code-from-roster-text'):
    run('inputs_' + key, lambda key=key: Country(C).derivation_inputs(
        f'Burkina_Faso::crop_production::{key}', wave='2021-22'))
save()
print('delta build complete', label, flush=True)
