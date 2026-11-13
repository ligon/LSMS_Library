"""Round 4 (2013-14): source structure.  COUNTS ONLY -- no ids, no coordinates."""
import os, sys, re, subprocess
import numpy as np
import pandas as pd
import lsms_library as ll
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
from lsms_library.local_tools import get_dataframe, format_id
from lsms_library.paths import countries_root
R = countries_root() / 'Malawi'
norm = lambda s: s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

cov = get_dataframe(str(R / '2013-14/Data/HH_MOD_A_FILT_13.dta'))
geo = get_dataframe(str(R / '2013-14/Data/HouseholdGeovariables_IHPS_13.dta'))
print('cover shape', cov.shape, '| geo shape', geo.shape)
print('cover columns with dist/mov/ihs3/split/track:', [c for c in cov.columns if re.search(r'dist|mov|ihs3|split|track|baseline|y1|qx_type', c, re.I)])
print('geo columns with dist/ihs3:', [c for c in geo.columns if re.search(r'dist|ihs3|y1|mov', c, re.I)])
# variable label
try:
    import pyreadstat
    from lsms_library.local_tools import _ensure_dvc_pulled  # noqa
except Exception:
    pass
d = pd.to_numeric(cov['dist_to_IHS3location'], errors='coerce')
print('dist dtype', cov['dist_to_IHS3location'].dtype, '| non-null', int(d.notna().sum()), 'of', len(d),
      '| <=1', int((d <= 1).sum()), '| ==0', int((d == 0).sum()), '| <0', int((d < 0).sum()))
print('dist quantiles (0, .25,.5,.75,.9,1):', [round(float(x), 3) for x in d.quantile([0, .25, .5, .75, .9, 1])])
print('smallest positive values (5):', sorted(set(d[d > 0].round(3)))[:5])
# ids
ids = cov['y2_hhid'].map(lambda x: format_id(x))
print('y2_hhid unique', ids.nunique(), 'of', len(ids), '| matches NNNN-NNN', int(ids.astype(str).str.fullmatch(r'\d{4}-\d{3}').sum()))
print('geo y2_hhid unique', geo['y2_hhid'].nunique(), '| geo keys in cover', int(geo['y2_hhid'].astype(str).isin(set(cov['y2_hhid'].astype(str))).sum()),
      '| cover keys in geo', int(cov['y2_hhid'].astype(str).isin(set(geo['y2_hhid'].astype(str))).sum()))
v = norm(cov['ea_id'])
print('EAs', v.nunique(), '| households per EA min/median/max', cov.groupby(v).size().agg(['min', 'median', 'max']).tolist())
# panel structure vs later rounds
c16 = get_dataframe(str(R / '2016-17/Data/Panel/hh_mod_a_filt_16.dta'))
c19 = get_dataframe(str(R / '2019-20/Data/Panel/hh_mod_a_filt_19.dta'))
v16, v19 = set(norm(c16['ea_id'])), set(norm(c19['ea_id']))
V = set(v)
print('2013-14 EAs in 2016-17 panel', len(V & v16), '| in 2019-20 panel', len(V & v19), '| in both', len(V & v16 & v19),
      '| in neither', len(V - v16 - v19), '| 2016 panel EAs not in 2013', len(v16 - V), '| 2019 panel EAs not in 2013', len(v19 - V))
cont = V & v16
# other design columns
for c in ['qx_type', 'panelweight', 'hh_wgt', 'baseline_rural', 'reside', 'stratum', 'tracking', 'hhtype', 'split']:
    if c in cov.columns:
        print('column', c, 'present; value counts (top 6):', cov[c].astype(str).value_counts().head(6).to_dict() if cov[c].nunique() < 20 else f'{cov[c].nunique()} distinct')

m = cov[['y2_hhid', 'ea_id', 'dist_to_IHS3location']].merge(
    geo[['y2_hhid', 'LAT_DD_MOD', 'LON_DD_MOD']], on='y2_hhid', how='left')
m['v'] = norm(m['ea_id']); m['d'] = pd.to_numeric(m['dist_to_IHS3location'], errors='coerce')
print('coords non-null', int(m.LAT_DD_MOD.notna().sum()), 'of', len(m), '| lat/lon null pattern equal', bool(m.LAT_DD_MOD.isna().equals(m.LON_DD_MOD.isna())))
for lab, mask in [('strict <=1 km', m.d <= 1), ('fillna(0) <= 1 (existing test)', m.d.fillna(0) <= 1)]:
    st = m[mask & m.LAT_DD_MOD.notna()]
    g = st.groupby('v')[['LAT_DD_MOD', 'LON_DD_MOD']].nunique()
    sz = st.groupby('v').size()
    print(f'{lab}: stayers {len(st)} | EAs covered {st.v.nunique()} of {m.v.nunique()} | EAs with >1 coord {int(((g.LAT_DD_MOD > 1) | (g.LON_DD_MOD > 1)).sum())}'
          f' | per EA min {int(sz.min())} median {float(sz.median())} | continuing EAs covered {len(set(st.v) & cont)} of {len(cont)} | short-term-only covered {len(set(st.v) - cont)} of {len(V - cont)}')
for thr in [0.5, 2, 5]:
    st = m[(m.d <= thr) & m.LAT_DD_MOD.notna()]
    g = st.groupby('v')[['LAT_DD_MOD', 'LON_DD_MOD']].nunique()
    print(f'  <= {thr} km: stayers {len(st)} EAs {st.v.nunique()} | EAs with >1 coord {int(((g.LAT_DD_MOD > 1) | (g.LON_DD_MOD > 1)).sum())}')
st = m[(m.d <= 1) & m.LAT_DD_MOD.notna()]
fix = st.groupby('v')[['LAT_DD_MOD', 'LON_DD_MOD']].first()
mm = m.join(fix, on='v', rsuffix='_fix')
on = (mm.LAT_DD_MOD == mm.LAT_DD_MOD_fix) & (mm.LON_DD_MOD == mm.LON_DD_MOD_fix)
print('households ON their EA fix', int(on.sum()), '| stayers', int((on & (mm.d <= 1)).sum()), '| movers', int((on & (mm.d > 1)).sum()),
      '| unrecorded (NaN dist)', int((on & mm.d.isna()).sum()))
nan = mm[mm.d.isna()]
print('NaN-distance households', len(nan), '| with coordinate', int(nan.LAT_DD_MOD.notna().sum()), '| on fix', int((on & mm.d.isna()).sum()),
      '| off fix with coord', int((~on & mm.d.isna() & mm.LAT_DD_MOD.notna()).sum()), '| EAs', nan.v.nunique())
# is the distance what it says?  haversine from the household's coordinate to its EA's stayer fix
def hav(a1, o1, a2, o2):
    a1, o1, a2, o2 = map(np.radians, (a1, o1, a2, o2))
    h = np.sin((a2 - a1) / 2) ** 2 + np.cos(a1) * np.cos(a2) * np.sin((o2 - o1) / 2) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(h))
mm['km_fix'] = hav(mm.LAT_DD_MOD.astype(float), mm.LON_DD_MOD.astype(float), mm.LAT_DD_MOD_fix.astype(float), mm.LON_DD_MOD_fix.astype(float))
ok = mm.km_fix.notna() & mm.d.notna()
print('dist vs km to EA fix (displaced): spearman', round(float(mm.loc[ok, ['d', 'km_fix']].corr('spearman').iloc[0, 1]), 3),
      '| households with dist > 1: median km to fix', round(float(mm.loc[ok & (mm.d > 1), 'km_fix'].median()), 1),
      '| with dist <= 1: max km to fix', round(float(mm.loc[ok & (mm.d <= 1), 'km_fix'].max()), 3),
      '| dist > 20 and km_fix < 1:', int((ok & (mm.d > 20) & (mm.km_fix < 1)).sum()),
      '| dist <= 1 and km_fix > 20:', int((ok & (mm.d <= 1) & (mm.km_fix > 20)).sum()))
# district in ea_id vs district of household
if 'district' in cov.columns:
    pass
