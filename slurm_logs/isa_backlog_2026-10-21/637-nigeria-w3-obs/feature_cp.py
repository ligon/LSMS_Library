import sys, json, warnings
import pandas as pd
import lsms_library as ll
from lsms_library.country import grain_reports
which = sys.argv[1]
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    df = ll.Feature('community_prices')(['Nigeria', 'Niger'])
cats = {}
for x in w:
    cats[type(x.message).__name__] = cats.get(type(x.message).__name__, 0) + 1
r = df.reset_index()
res = {'index': list(df.index.names), 'rows': len(df), 'unique': bool(df.index.is_unique),
       'by_country': r.groupby('country').size().to_dict(),
       'obs_dtype': str(df.index.get_level_values('obs').dtype),
       'visit_dtype': str(df.index.get_level_values('visit').dtype),
       'nigeria_by_t_obs': {f'{t}|{o}': int(n) for (t, o), n in r[r.country == 'Nigeria'].groupby(['t', 'obs']).size().items()},
       'nigeria_variety': sorted(set(r.loc[r.country == 'Nigeria', 'variety'])),
       'nigeria_visit': sorted(set(r.loc[r.country == 'Nigeria', 'visit'])),
       'warnings': cats,
       'grain_reports': len(grain_reports()),
       'user_warnings': [str(x.message)[:240] for x in w if type(x.message).__name__ == 'UserWarning'],
       'grain_warnings': [str(x.message)[:200] for x in w if 'Grain' in type(x.message).__name__]}
print(json.dumps(res, indent=1, default=str))
