"""Cold single-wave crop_production in its own empty root, vs the all-waves slice.

build: single_wave.py build WAVE FRAMES_DIR OUT.json   (fresh empty LSMS_DATA_DIR)
cmp:   single_wave.py cmp ALLWAVES_FRAMES_DIR SINGLE_FRAMES_DIR WAVE OUT.json
"""
import json
import sys
import traceback
import warnings

import pandas as pd

mode = sys.argv[1]
if mode == 'build':
    from lsms_library.country import Country, grain_reports
    wave, FR, OJ = sys.argv[2], sys.argv[3], sys.argv[4]
    res = {'wave': wave}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = Country('Burkina_Faso').crop_production(waves=[wave])
            f = df.reset_index()
            for col in f.columns:
                if f[col].dtype == object or str(f[col].dtype) == 'category':
                    f[col] = f[col].astype('string')
            f.to_parquet(f'{FR}/crop_production_cold_{wave}.parquet')
            res.update(rows=len(df), columns=list(map(str, df.columns)), index=list(map(str, df.index.names)),
                       dtypes={str(k): str(v) for k, v in df.reset_index().dtypes.items()},
                       unique=bool(df.index.is_unique))
        except Exception:  # noqa: BLE001
            res['err'] = traceback.format_exc()[-2000:]
    res['warnings'] = sorted({w.category.__name__ for w in caught})
    res['grain'] = [{k: r.get(k) for k in ('wave', 'rows_destroyed', 'n_destroyed', 'groups') if k in r}
                    for r in grain_reports(country='Burkina_Faso')]
    json.dump(res, open(OJ, 'w'), indent=1, default=str)
    print(json.dumps(res, default=str)[:2000])
else:
    ALL, SING, wave, OJ = sys.argv[2:6]
    a = pd.read_parquet(f'{ALL}/crop_production.parquet')
    s = pd.read_parquet(f'{SING}/crop_production_cold_{wave}.parquet')
    a = a[a['t'].astype(str) == wave]
    res = {'wave': wave, 'rows': [len(a), len(s)],
           'cols_only_all': sorted(set(a.columns) - set(s.columns)),
           'cols_only_single': sorted(set(s.columns) - set(a.columns)),
           'dtype_diff': {k: [str(a[k].dtype), str(s[k].dtype)] for k in set(a.columns) & set(s.columns)
                          if str(a[k].dtype) != str(s[k].dtype)}}
    cols = [c for c in a.columns if c in s.columns]
    try:
        pd.testing.assert_frame_equal(a[cols].sort_values(cols, na_position='last').reset_index(drop=True),
                                      s[cols].sort_values(cols, na_position='last').reset_index(drop=True))
        res['equal_on_common'] = True
    except Exception as e:  # noqa: BLE001
        res['equal_on_common'] = f'{type(e).__name__}: {str(e)[:600]}'
    json.dump(res, open(OJ, 'w'), indent=1, default=str)
    print(json.dumps(res, default=str))
