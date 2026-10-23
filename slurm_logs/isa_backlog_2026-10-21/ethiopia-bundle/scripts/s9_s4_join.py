import os, sys, warnings
checkout = sys.argv[1]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe, format_id
sys.path.insert(0, f'{checkout}/lsms_library/countries/Ethiopia/_')
import ethiopia as E
P = E.PERENNIAL_CROP_CODES
R = f'{checkout}/lsms_library/countries/Ethiopia'
def pid(df):
    return (df['holder_id'].apply(format_id).astype(str) + '_' + df['parcel_id'].apply(format_id).astype(str) + '_' + df['field_id'].apply(format_id).astype(str))
for w, f9, f4 in [('2018-19', 'sect9_ph_w4.dta', 'sect4_pp_w4.dta'), ('2021-22', 'sect9_ph_w5.dta', 'sect4_pp_w5.dta')]:
    h = get_dataframe(f'{R}/{w}/Data/{f9}', convert_categoricals=False)
    p = get_dataframe(f'{R}/{w}/Data/{f4}', convert_categoricals=False)
    hk = pd.DataFrame({'plot_id': pid(h), '_code': pd.to_numeric(h['s9q00b'], errors='coerce').astype('Int64')})
    pk = pd.DataFrame({'plot_id': pid(p), '_code': pd.to_numeric(p['s4q01b'], errors='coerce').astype('Int64'),
                       'flag': pd.to_numeric(p['s4q15'], errors='coerce').astype('Int64')})
    dup = pk.duplicated(['plot_id', '_code'], keep=False)
    conflict = pk[dup].groupby(['plot_id', '_code'])['flag'].nunique(dropna=False)
    print('=====', w, '§9 rows', len(h), '§4 rows', len(p), 'dup §4 keys', int(dup.sum()), 'conflicting dup keys', int((conflict > 1).sum()))
    pk1 = pk.drop_duplicates(['plot_id', '_code'])
    m = hk.merge(pk1, on=['plot_id', '_code'], how='left', indicator=True)
    m['per'] = m['_code'].map(lambda c: int(c) in P if pd.notna(c) else None)
    m['fl'] = np.where(m['_merge'] == 'left_only', 'no §4 row', m['flag'].astype('string').fillna('NA'))
    print(pd.crosstab(m['per'], m['fl']))
    # holder-crop-level: §11 Q2 answered vs §4 flag via §9 single plot
