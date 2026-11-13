"""GH #1123 review follow-up: does a failing Niger wave guard surface?

Usage: python inject.py <checkout> <out.json>
Env: LSMS_COUNTRIES_ROOT (a scratch copy of the countries tree, possibly with
an injected seed-map collision), LSMS_DATA_DIR (fresh), optionally
LSMS_BUILD_BACKEND=make.  Records the outcome of Country('Niger').plot_inputs():
the exception (type + head of message) or the rows per wave, plus warning
categories.  Aggregates only.
"""
import collections
import json
import os
import sys
import warnings

checkout, out = sys.argv[1], sys.argv[2]
import lsms_library as ll  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
res = {'backend': os.environ.get('LSMS_BUILD_BACKEND', 'default'),
       'countries_root_is_scratch': not os.path.realpath(countries_root()).startswith(
           os.path.realpath(checkout) + os.sep)}
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    try:
        df = ll.Country('Niger').plot_inputs()
        res['error'] = None
        res['rows'] = len(df)
        res['rows_by_wave'] = {str(k): int(v) for k, v in
                               df.index.get_level_values('t').value_counts().sort_index().items()}
    except Exception as e:  # noqa: BLE001
        res['error'] = f'{type(e).__name__}: {str(e)[:240]}'
res['warning_categories'] = dict(collections.Counter(w.category.__name__ for w in caught))
res['warning_heads'] = sorted({str(w.message)[:110] for w in caught
                               if w.category.__name__ in ('UserWarning', 'RuntimeWarning')})
json.dump(res, open(out, 'w'), indent=1, sort_keys=True)
print(json.dumps(res, indent=1))
