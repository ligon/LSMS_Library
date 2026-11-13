"""GH #1123: Feature('plot_inputs') over the three countries, warm.

Usage: python feature.py <checkout> <out.json>
Records rows per country, Feature-level grain warnings, and whether each
country's Feature rows equal its Country() rows.  Aggregates only.
"""
import json
import os
import sys
import warnings

checkout, out = sys.argv[1], sys.argv[2]
import lsms_library as ll  # noqa: E402
from lsms_library import country as C  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
names = ['CotedIvoire', 'Niger', 'Senegal']
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    df = ll.Feature('plot_inputs')(names)
    per = {n: len(ll.Country(n).plot_inputs()) for n in names}
gw = [str(w.message)[:120] for w in caught if issubclass(w.category, C.GrainCollapseWarning)]
by = df.index.get_level_values('country').value_counts().to_dict()
res = {'rows': len(df), 'index_names': list(df.index.names), 'rows_by_country': by,
       'country_rows': per, 'feature_equals_country_rows': {n: by.get(n) == per[n] for n in names},
       'grain_collapse_warnings': len(gw),
       'grain_collapse_warning_heads': sorted({g.split(':')[0] for g in gw})}
json.dump(res, open(out, 'w'), indent=1, sort_keys=True, default=str)
print(json.dumps(res, default=str))
