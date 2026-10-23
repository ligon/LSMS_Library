import sys, os, importlib.util
import pandas as pd
from lsms_library.paths import countries_root
root = countries_root() / 'Nigeria'
sys.path.insert(0, str(root / '_'))
spec = importlib.util.spec_from_file_location('ngd', root / '_' / 'nigeria.py')
ng = importlib.util.module_from_spec(spec); spec.loader.exec_module(ng)
spec2 = importlib.util.spec_from_file_location('mpd', root / '2015-16' / '_' / 'mapping.py')
mp = importlib.util.module_from_spec(spec2); spec2.loader.exec_module(mp)
der = mp.inputs_community_prices('2015-16')
der['u'] = der.unit_label.map(ng._canon_community_unit)
print('rows', len(der), 'by obs', der.get('obs', pd.Series(dtype=int)).value_counts().to_dict() if 'obs' in der else 'n/a')
print('canonical u of the registry-derived W3 inputs:', der.u.value_counts(dropna=False).to_dict())
# builder's unit-aware parse
der['q_unit'] = [ng._c8_w3_size_text(x, uu) for x, uu in zip(der.size_label, der.u)]
print('builder unit-aware parse notna:', int(pd.Series(der.q_unit).notna().sum()),
      ' and u in g/cl:', int((pd.Series(der.q_unit).notna() & der.u.isin(['g','cl'])).sum()))
for w in ('2018-19', '2023-24'):
    from importlib import util
    s3 = util.spec_from_file_location('mp'+w, root / w / '_' / 'mapping.py'); m3 = util.module_from_spec(s3); s3.loader.exec_module(m3)
    d3 = m3.inputs_community_prices(w)
    d3['u'] = d3.unit_label.map(ng._canon_community_unit)
    print(w, 'registry-derived inputs', len(d3), 'canonical u top', d3.u.value_counts(dropna=False).head(6).to_dict())
