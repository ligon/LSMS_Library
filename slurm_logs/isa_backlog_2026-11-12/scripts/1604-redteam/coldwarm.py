"""mode=cold: build and pickle API frames; mode=warm: fresh-process reads compared to the pickles.
usage: coldwarm.py <clone> <mode> <pkldir> <out.json>"""
import json
import sys
import traceback
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, mode, pk, out = sys.argv[1], sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4])
pk.mkdir(parents=True, exist_ok=True)
assert_env(clone)
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

c = ll.Country('Malawi')
CALLS = [
    ('food_acquired_all', lambda: c.food_acquired()),
    ('food_acquired_w2024', lambda: c.food_acquired(waves=['2024-25'])),
    ('sample_all', lambda: c.sample()),
    ('food_quantities_w2024', lambda: c.food_quantities(units='kgs', waves=['2024-25'])),
]
R = {}
for name, fn in CALLS:
    rec = {}
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            df = fn()
            rec['rows'] = len(df)
            rec['index_dtypes'] = [str(df.index.get_level_values(n).dtype) for n in df.index.names]
            if mode == 'cold':
                df.to_pickle(pk / f'{name}.pkl')
            else:
                ref = pd.read_pickle(pk / f'{name}.pkl')
                try:
                    pd.testing.assert_frame_equal(df, ref, check_exact=True)
                    rec['equal_to_cold'] = True
                except AssertionError as e:
                    rec['equal_to_cold'] = False
                    rec['msg'] = str(e)[:800]
                    try:
                        pd.testing.assert_frame_equal(df, ref, check_exact=True, check_index_type=False,
                                                      check_dtype=False)
                        rec['equal_ignoring_dtypes'] = True
                    except AssertionError as e2:
                        rec['equal_ignoring_dtypes'] = False
                        rec['msg2'] = str(e2)[:800]
                rec['attrs_equal_keys'] = sorted(df.attrs) == sorted(ref.attrs)
        except Exception as e:
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['tb'] = traceback.format_exc()[-2500:]
        cats = {}
        for x in w:
            cats[x.category.__name__] = cats.get(x.category.__name__, 0) + 1
        rec['warnings'] = cats
    R[name] = rec
    print(mode, name, {k: v for k, v in rec.items() if k != 'tb'}, flush=True)
    out.write_text(json.dumps(R, indent=1, default=str))
print('COLDWARM DONE', mode, flush=True)
