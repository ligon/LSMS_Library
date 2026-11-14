"""Fresh-process checks after a cold build.  Usage: strict.py MODE FRAMES_DIR OUT.json
MODE warm: warm reads equal the cold frames.  MODE grain / date: the table under the strict lever
the job script exported (LSMS_GRAIN_STRICT=1 / LSMS_DATE_STRICT=1)."""
import json
import os
import sys
import traceback
import warnings

import pandas as pd

from lsms_library.country import Country

mode, FR, OJ = sys.argv[1], sys.argv[2], sys.argv[3]
C = 'Burkina_Faso'
res = {'mode': mode, 'env': {k: v for k, v in os.environ.items() if k.startswith('LSMS_') and 'STRICT' in k}}


def flat(df):
    f = df.reset_index()
    for col in f.columns:
        if f[col].dtype == object:
            f[col] = f[col].astype('string')
    return f


def attempt(name, fn):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = fn()
            r = {'ok': True, 'rows': len(df)}
        except Exception as e:  # noqa: BLE001
            df, r = None, {'ok': False, 'err': f'{type(e).__name__}: {str(e)[:600]}',
                           'mro': [k.__name__ for k in type(e).__mro__][:6]}
    r['warnings'] = sorted({w.category.__name__ for w in caught})
    res[name] = r
    return df


if mode == 'warm':
    for name, fn in (('crop_production', lambda: Country(C).crop_production()),
                     ('food_acquired', lambda: Country(C).food_acquired()),
                     ('interview_date', lambda: Country(C).interview_date())):
        df = attempt(name, fn)
        if df is not None:
            cold = pd.read_parquet(f'{FR}/{name}.parquet')
            w = flat(df)
            cols = list(cold.columns)
            try:
                pd.testing.assert_frame_equal(
                    cold.sort_values(cols, na_position='last').reset_index(drop=True),
                    w[cols].sort_values(cols, na_position='last').reset_index(drop=True), check_dtype=True)
                res[name]['warm_equals_cold'] = True
            except Exception as e:  # noqa: BLE001
                res[name]['warm_equals_cold'] = f'{type(e).__name__}: {str(e)[:500]}'
elif mode == 'grain':
    attempt('crop_production', lambda: Country(C).crop_production())
    attempt('crop_production_2018', lambda: Country(C).crop_production(waves=['2018-19']))
elif mode == 'date':
    attempt('interview_date', lambda: Country(C).interview_date())
with open(OJ, 'w') as fh:
    json.dump(res, fh, indent=1, default=str, sort_keys=True)
print(json.dumps(res, default=str)[:3000])
