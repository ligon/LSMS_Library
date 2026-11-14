"""Fresh-process warm reads; compare to the cold frames.  usage: warm.py <clone> <framesdir> <out.json>"""
import json
import sys
import traceback
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, fd, out = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
assert_env(clone)
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

W = '2024-25'
R = {}
c = ll.Country('Malawi')
for name, fn, cold in [
    ('sample', lambda **k: c.sample(**k), 'sample'),
    ('food_acquired', lambda **k: c.food_acquired(**k), 'food_acquired'),
    ('food_quantities_kgs', lambda **k: c.food_quantities(units='kgs', **k), 'food_quantities_kgs'),
]:
    cf = pd.read_parquet(fd / f'cand.{cold}.parquet')
    for label, kw in [('all', {}), ('w2024', {'waves': [W]}), ('w2019', {'waves': ['2019-20']})]:
        rec = {}
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            try:
                df = fn(**kw)
                df = df.copy(); df.attrs = {}
                rec['rows'] = len(df)
                t = cf.index.get_level_values('t').astype(str)
                ref = cf if label == 'all' else cf[t == kw['waves'][0]]
                try:
                    pd.testing.assert_frame_equal(df, ref, check_exact=True)
                    rec['equal_to_cold'] = True
                except AssertionError as e:
                    rec['equal_to_cold'] = False; rec['msg'] = str(e)[:800]
            except Exception as e:
                rec['error'] = f'{type(e).__name__}: {e}'; rec['tb'] = traceback.format_exc()[-2000:]
            cats = {}
            for x in w:
                cats[x.category.__name__] = cats.get(x.category.__name__, 0) + 1
            rec['warnings'] = cats
        R[f'{name}:{label}'] = rec
        print(name, label, rec, flush=True)
        out.write_text(json.dumps(R, indent=1, default=str))
print('WARM DONE', flush=True)
