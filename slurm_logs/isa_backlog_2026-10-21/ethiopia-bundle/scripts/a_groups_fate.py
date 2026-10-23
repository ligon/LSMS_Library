import os, sys, warnings
checkout = sys.argv[1]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep)
warnings.simplefilter('ignore')
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe, format_id
R = f'{checkout}/lsms_library/countries/Ethiopia'
sys.path.insert(0, f'{R}/_'); import ethiopia as E
cm = E._eth_crop_label_map()
for w, f11, cc, v, fl, f9, cc9, q9, u9 in [('2013-14', 'sect11_ph_w2.dta', 'crop_code', 'ph_s11q04', 'ph_s11q01', 'sect9_ph_w2.dta', 'crop_code', 'ph_s9q04_a', 'ph_s9q04_b'),
                              ('2018-19', 'sect11_ph_w4.dta', 's11q01', 's11q12', 's11q07', 'sect9_ph_w4.dta', 's9q00b', 's9q05a', 's9q05b')]:
    s = get_dataframe(f'{R}/{w}/Data/{f11}', convert_categoricals=False)
    h = get_dataframe(f'{R}/{w}/Data/{f9}', convert_categoricals=False)
    L = pd.DataFrame({'holder': s['holder_id'].apply(format_id).values, 'code': pd.to_numeric(s[cc], errors='coerce').astype('Int64').values, 'v': pd.to_numeric(s[v], errors='coerce').values, 'no': (pd.to_numeric(s[fl], errors='coerce') == 2).values}).dropna(subset=['holder','code'])
    g = L.assign(b=L.v.isna() & ~L.no).groupby(['holder', 'code'])['b'].all()
    blank = set(g[g].index)
    H = pd.DataFrame({'holder': h['holder_id'].apply(format_id).values, 'code': pd.to_numeric(h[cc9], errors='coerce').astype('Int64').values,
                      'plot': (h['holder_id'].apply(format_id).astype(str)+'_'+h['parcel_id'].apply(format_id).astype(str)+'_'+h['field_id'].apply(format_id).astype(str)).values,
                      'q': pd.to_numeric(h[q9], errors='coerce').values, 'u': pd.to_numeric(h[u9], errors='coerce').values})
    npl = H.dropna(subset=['code']).groupby(['holder','code'])['plot'].nunique()
    tgt = [k for k in blank if npl.get(k, 0) == 1]
    T = H[H.set_index(['holder','code']).index.isin(tgt)].copy()
    T['j'] = T['code'].map(lambda c: cm.get(int(c)))
    print(w, 'groups', len(tgt), 'their §9 rows:', len(T), '| unit missing & qty missing:', int((T.u.isna() & T.q.isna()).sum()), '| j undecodable:', int(T.j.isna().sum()))
