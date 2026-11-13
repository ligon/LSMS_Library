import sys
import pandas as pd
cov = pd.read_stata(sys.argv[1], convert_categoricals=False)
geo = pd.read_stata(sys.argv[2], convert_categoricals=False)
m = cov[['y2_hhid', 'ea_id', 'dist_to_IHS3location']].merge(geo[['y2_hhid', 'LAT_DD_MOD', 'LON_DD_MOD']], on='y2_hhid')
st = m[(m.dist_to_IHS3location <= 1) & m.LAT_DD_MOD.notna()]
fix = st.groupby('ea_id')[['LAT_DD_MOD', 'LON_DD_MOD']].first(); fix.index = fix.index.astype(str).str.replace(r'\.0$', '', regex=True)
cf = pd.read_parquet(sys.argv[3]).xs('2013-14', level='t'); cf.index = cf.index.astype(str)
common = fix.index.intersection(cf.index)
same = ((cf.loc[common, 'Latitude'] - fix.loc[common, 'LAT_DD_MOD']).abs() < 1e-6) & ((cf.loc[common, 'Longitude'] - fix.loc[common, 'LON_DD_MOD']).abs() < 1e-6)
print('2013-14: EAs with stayers', len(fix), 'of', m.ea_id.nunique(), '| matched to clusters', len(common), '| served coordinate is the stayer fix', int(same.sum()), '| not', int((~same).sum()))
