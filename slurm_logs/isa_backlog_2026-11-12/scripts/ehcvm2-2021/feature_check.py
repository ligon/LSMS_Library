"""Feature('sample') and Feature('household_roster') over Togo, Benin, CotedIvoire,
Senegal; each country's slice must equal its own Country() frame row for row.
usage: python feature_check.py OUT_JSON"""
import json, os, sys, warnings, collections
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep)
import pandas as pd
import lsms_library as ll
COUNTRIES = ['Togo', 'Benin', 'CotedIvoire', 'Senegal']
out = {}
for feat in ('sample', 'household_roster'):
    rec = {}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = ll.Feature(feat)(COUNTRIES)
            rec['rows'] = len(df)
            rec['index'] = list(df.index.names)
            rec['dup_index'] = int(df.index.duplicated().sum())
            rec['by_country_wave'] = {f'{c}|{t}': int(n) for (c, t), n in
                                      df.groupby([df.index.get_level_values('country'),
                                                  df.index.get_level_values('t')]).size().items()}
            rec['attrs_keys'] = sorted(df.attrs)
            rec['population_countries'] = sorted((df.attrs.get('population') or {}).keys())
            eq = {}
            for c in COUNTRIES:
                own = getattr(ll.Country(c), feat)()
                sl = df.xs(c, level='country')
                common = [x for x in own.columns if x in sl.columns]
                sl = sl[common]
                own = own[common]
                try:
                    a = sl.reset_index().sort_values(list(sl.index.names)).reset_index(drop=True)
                    b = own.reset_index()[a.columns].sort_values(list(sl.index.names)).reset_index(drop=True)
                    pd.testing.assert_frame_equal(a, b, check_dtype=False, check_categorical=False)
                    eq[c] = 'equal'
                except Exception as exc:
                    eq[c] = f'{type(exc).__name__}: {str(exc)[:300]}'
            rec['slice_equals_country'] = eq
        except Exception as exc:
            rec['error'] = f'{type(exc).__name__}: {exc}'[:2000]
    rec['warnings'] = dict(collections.Counter(w.category.__name__ for w in caught))
    rec['warning_samples'] = [f'{w.category.__name__}: {str(w.message)[:300]}' for w in caught
                              if w.category.__name__ not in ('FutureWarning', 'DeprecationWarning')][:12]
    out[feat] = rec
    print(feat, rec.get('rows'), rec.get('error', '')[:200], flush=True)
json.dump(out, open(sys.argv[1], 'w'), indent=1, default=str)
