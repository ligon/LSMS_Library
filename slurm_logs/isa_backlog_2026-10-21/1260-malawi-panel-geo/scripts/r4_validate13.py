"""Round 4 (2013-14): is dist_to_IHS3location what it says?  Cross-round fix check.
COUNTS ONLY -- no ids, no coordinates."""
import os, subprocess
import numpy as np
import pandas as pd
import lsms_library as ll
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
R = countries_root() / 'Malawi'
norm = lambda s: s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
G = lambda w, f: get_dataframe(str(R / w / 'Data' / f))

cov = G('2013-14', 'HH_MOD_A_FILT_13.dta'); geo = G('2013-14', 'HouseholdGeovariables_IHPS_13.dta')
m = cov[['y2_hhid', 'ea_id', 'dist_to_IHS3location', 'district', 'region', 'reside', 'baseline_rural', 'qx_type']].merge(
    geo[['y2_hhid', 'LAT_DD_MOD', 'LON_DD_MOD', 'distY1Y2']], on='y2_hhid')
m['v'] = norm(m.ea_id); m['d'] = m.dist_to_IHS3location.astype(float); m['d12'] = pd.to_numeric(m.distY1Y2, errors='coerce')
both = m.d.notna() & m.d12.notna()
print('distY1Y2 non-null', int(m.d12.notna().sum()), 'of', len(m), '| both', int(both.sum()),
      '| max |diff|', round(float((m.d - m.d12).abs()[both].max()), 4), '| <=1 agree', int(((m.d <= 1) == (m.d12 <= 1))[both].sum()))
print('qx_type x stayer:', pd.crosstab(m.qx_type.astype(str), m.d <= 1).to_dict())
# ea_id embeds the baseline district code (digits 2-3); does the household's current district code match?
st = m[m.d <= 1]
# 2010-11 served coordinate (IHS3 EA fix) per EA
c10 = G('2010-11', 'Full_Sample/Household/hh_mod_a_filt.dta')[['case_id', 'ea_id']]
g10 = G('2010-11', 'Full_Sample/Geovariables/HH_level/householdgeovariables.dta')[['case_id', 'lat_modified', 'lon_modified']]
x10 = c10.merge(g10, on='case_id'); x10['v'] = norm(x10.ea_id)
f10 = x10.dropna(subset=['lat_modified']).groupby('v')[['lat_modified', 'lon_modified']].agg(['nunique', 'first'])
print('2010-11: EAs', len(f10), '| EAs with >1 coord', int(((f10[('lat_modified', 'nunique')] > 1) | (f10[('lon_modified', 'nunique')] > 1)).sum()))
f10 = pd.DataFrame({'lat': f10[('lat_modified', 'first')].astype(float), 'lon': f10[('lon_modified', 'first')].astype(float)})
f13 = st.dropna(subset=['LAT_DD_MOD']).groupby('v')[['LAT_DD_MOD', 'LON_DD_MOD']].first().astype(float)
com = f13.index.intersection(f10.index)
dd = np.maximum((f13.loc[com, 'LAT_DD_MOD'] - f10.loc[com, 'lat']).abs(), (f13.loc[com, 'LON_DD_MOD'] - f10.loc[com, 'lon']).abs())
print(f'2013-14 stayer fix vs 2010-11 EA coordinate: {len(com)} of {len(f13)} EAs in 2010-11 | max |diff| deg {dd.max():.2e} | EAs differing > 2e-6: {int((dd > 2e-6).sum())}')
# continuing vs short-term-only
c16 = G('2016-17', 'Panel/hh_mod_a_filt_16.dta'); cont = set(norm(c16.ea_id))
for lab, sel in [('continuing', [v for v in com if v in cont]), ('short-term-only', [v for v in com if v not in cont])]:
    print(f'   {lab}: {len(sel)} EAs, max |diff| deg {dd.loc[sel].max():.2e}')
# every household: is its coordinate the 2010-11 coordinate of its own EA?  vs distance
mm = m.join(f10, on='v')
on10 = (mm.LAT_DD_MOD.astype(float) - mm.lat).abs().le(2e-6) & (mm.LON_DD_MOD.astype(float) - mm.lon).abs().le(2e-6)
print('households on their EA 2010-11 coordinate', int(on10.sum()), '| of which dist<=1', int((on10 & (mm.d <= 1)).sum()),
      '| dist>1', int((on10 & (mm.d > 1)).sum()), '| dist<=1 NOT on it', int((~on10 & (mm.d <= 1)).sum()))
# movers on the fix: distance distribution
mv = mm[on10 & (mm.d > 1)]
print('movers on the fix: dist quantiles', [round(float(q), 2) for q in mv.d.quantile([0, .25, .5, .75, .9, 1])],
      '| same district as EA modal stayer district?', None)
# district agreement: household district vs EA stayers' modal district
mode = st.groupby('v')['district'].agg(lambda s: s.astype(str).mode().iloc[0])
mm['dmode'] = mm.v.map(mode)
for lab, sel in [('stayers', mm.d <= 1), ('movers on fix', on10 & (mm.d > 1)), ('movers off fix', ~on10 & (mm.d > 1))]:
    x = mm[sel]
    print(f'   {lab}: {len(x)} hh | district == EA stayers\' modal district: {int((x.district.astype(str) == x.dmode).sum())}')
