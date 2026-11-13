"""Round 4 (2013-14): development (base) vs branch, cold builds.  COUNTS ONLY --
no ids, no coordinates.  Usage: r4_compare.py <base_out> <branch_out>"""
import os, sys, pickle, subprocess
import numpy as np
import pandas as pd
import lsms_library as ll
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
from lsms_library.local_tools import get_dataframe, format_id
from lsms_library.paths import countries_root
B, T = sys.argv[1], sys.argv[2]
P = lambda d, f: pd.read_parquet(f'{d}/{f}')
R = countries_root() / 'Malawi'
norm = lambda s: s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
def hav(a1, o1, a2, o2):
    a1, o1, a2, o2 = map(np.radians, (a1, o1, a2, o2))
    h = np.sin((a2 - a1) / 2) ** 2 + np.cos(a1) * np.cos(a2) * np.sin((o2 - o1) / 2) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(h))

cov = get_dataframe(str(R / '2013-14/Data/HH_MOD_A_FILT_13.dta'))
geo = get_dataframe(str(R / '2013-14/Data/HouseholdGeovariables_IHPS_13.dta'))
m = cov[['y2_hhid', 'ea_id', 'dist_to_IHS3location', 'baseline_rural']].merge(
    geo[['y2_hhid', 'LAT_DD_MOD', 'LON_DD_MOD', 'distY1Y2']], on='y2_hhid')
m['i'] = m.y2_hhid.map(format_id); m['v'] = norm(m.ea_id); m['d'] = m.dist_to_IHS3location.astype(float)
st = m[(m.d <= 1) & m.LAT_DD_MOD.notna()]
fix = st.groupby('v')[['LAT_DD_MOD', 'LON_DD_MOD']].agg(['nunique', 'first'])
assert ((fix[('LAT_DD_MOD', 'nunique')] == 1) & (fix[('LON_DD_MOD', 'nunique')] == 1)).all()
fix = pd.DataFrame({'lat': fix[('LAT_DD_MOD', 'first')].astype(float), 'lon': fix[('LON_DD_MOD', 'first')].astype(float)})
c16 = get_dataframe(str(R / '2016-17/Data/Panel/hh_mod_a_filt_16.dta')); cont = sorted(set(norm(c16.ea_id)) & set(fix.index))
short = sorted(set(fix.index) - set(cont))
print(f'2013-14 source: EAs {m.v.nunique()} | with stayers {len(fix)} | continuing {len(cont)} | short-term-only {len(short)}')

cf = {k: P(d, 'cluster_features.parquet') for k, d in [('dev', B), ('branch', T)]}
wf = {k: P(d, 'wave_2013-14.parquet').reset_index() for k, d in [('dev', B), ('branch', T)]}
print('cluster_features shapes', {k: v.shape for k, v in cf.items()}, '| index equal', cf['dev'].index.equals(cf['branch'].index))
c13 = {}
for k in cf:
    x = cf[k].xs('2013-14', level='t'); x.index = x.index.astype(str); c13[k] = x
print('\n== 2013-14')
for k in ['dev', 'branch']:
    x, w = c13[k], wf[k]
    s = x.loc[fix.index]
    at = ((s.Latitude.astype(float) - fix.lat).abs() < 1e-9) & ((s.Longitude.astype(float) - fix.lon).abs() < 1e-9)
    g = w.groupby('v')[['Latitude', 'Longitude']].nunique()
    km = hav(s.Latitude.astype(float), s.Longitude.astype(float), fix.lat, fix.lon)
    print(f'   {k}: wave rows {len(w)} | clusters {len(x)}, without GPS {int(x.Latitude.isna().sum())} | EAs serving the EA fix {int(at.sum())} of {len(fix)}'
          f' (continuing {int(at.loc[cont].sum())} of {len(cont)}, short-term-only {int(at.loc[short].sum())} of {len(short)})'
          f' | EAs with >1 coordinate {int(((g.Latitude > 1) | (g.Longitude > 1)).sum())}'
          f' | households keeping a coordinate {int(w.Latitude.notna().sum())} of {len(w)}'
          f' | served off the fix: max km {float(km.max()):.0f}, median km over those {float(km[~at].median()) if (~at).any() else 0:.0f}')
# blanked households = the geo file's relocated ones?
wb = wf['branch'].merge(m[['i', 'distY1Y2', 'd']], on='i', how='left')
print(f'   branch: blanked {int(wb.Latitude.isna().sum())} | of which distY1Y2 non-null {int((wb.Latitude.isna() & wb.distY1Y2.notna()).sum())}'
      f' | kept with distY1Y2 non-null {int((wb.Latitude.notna() & wb.distY1Y2.notna()).sum())} | kept movers (dist > 1) {int((wb.Latitude.notna() & (wb.d > 1)).sum())}')
# served Region/District/Rural vs stayers' modal value (wave-frame values of the source stayers)
stay_i = set(m.loc[m.d <= 1, 'i'])
ws = wf['branch'][wf['branch'].i.isin(stay_i)].copy()
# the API spells the wave frame's 'South' as 'Southern' (region table)
L = lambda s: s.astype(str).str.lower().replace({'south': 'southern'})
for c in ['Region', 'District', 'Rural']:
    ws[c] = L(ws[c])
mode = ws.groupby(ws.v.astype(str))[['Region', 'District', 'Rural']].agg(lambda s: s.mode().iloc[0])
tied = ws.groupby(ws.v.astype(str))[['Region', 'District', 'Rural']].agg(lambda s: int((s.value_counts() == s.value_counts().max()).sum() > 1))
print(f'   stayers\' modal value tied in EAs: {tied.sum().to_dict()}')
for col in ['Region', 'District', 'Rural']:
    d, b = L(c13['dev'][col]), L(c13['branch'][col])
    moved = d.index[(d != b).values]
    hh = int(wf['branch'].v.astype(str).isin(set(moved)).sum())
    to_mode = int((b.loc[moved] == mode.loc[moved, col]).sum()); from_mode = int((d.loc[moved] == mode.loc[moved, col]).sum())
    print(f'   {col}: moved {len(moved)} EAs ({hh} hh) | moved INTO the stayers\' mode {to_mode}, OUT of it {from_mode}'
          f' | served = stayers\' mode: dev {int((d.loc[mode.index] == mode[col]).sum())} -> branch {int((b.loc[mode.index] == mode[col]).sum())} of {len(mode)}')
# independent answer key: baseline_rural (the IHS3 EA's own urban/rural)
br = m.groupby('v')['baseline_rural'].agg(lambda s: s.astype(str).str.lower().mode().iloc[0])
brn = m.groupby('v')['baseline_rural'].nunique()
print(f'   baseline_rural: EAs where it varies within the EA {int((brn > 1).sum())} of {len(brn)}'
      f' | served Rural = baseline_rural: dev {int((L(c13["dev"].loc[br.index, "Rural"]) == br).sum())} -> branch {int((L(c13["branch"].loc[br.index, "Rural"]) == br).sum())} of {len(br)}')
# other waves, sample, housing
print('\nother waves identical dev vs branch:', {w: cf['dev'].xs(w, level='t').equals(cf['branch'].xs(w, level='t')) for w in ['2004-05', '2010-11', '2016-17', '2019-20']})
for w in ['2016-17', '2019-20']:
    print(f'   wave frame {w} identical:', P(B, f'wave_{w}.parquet').equals(P(T, f'wave_{w}.parquet')))
for f in ['sample.parquet', 'housing.parquet']:
    print(f, 'dev vs branch DataFrame.equals', P(B, f).equals(P(T, f)))
def summ(d):
    return {r.get('wave'): (r.get('destroyed'), r.get('conflicting_groups')) for r in pickle.load(open(f'{d}/grain_reports.pkl', 'rb')) if r.get('table') == 'cluster_features'}
sb, st_ = summ(B), summ(T)
print('\nSite 2 (destroyed, conflicting groups): dev -> branch')
for w in sorted(set(sb) | set(st_), key=str):
    print(' ', w, sb.get(w), '->', st_.get(w))
# cross-round: SERVED coordinate per EA, country table (branch)
x = cf['branch']
S = {w: x.xs(w, level='t') for w in ['2010-11', '2013-14', '2016-17', '2019-20']}
for w in S:
    S[w].index = S[w].index.astype(str)
def diff(a, b, vs):
    vs = [v for v in vs if v in S[a].index and v in S[b].index]
    dd = np.maximum((S[a].loc[vs, 'Latitude'].astype(float) - S[b].loc[vs, 'Latitude'].astype(float)).abs(),
                    (S[a].loc[vs, 'Longitude'].astype(float) - S[b].loc[vs, 'Longitude'].astype(float)).abs())
    return len(vs), float(dd.max()), int((dd > 2e-6).sum()), int(dd.isna().sum())
print('\ncross-round, SERVED coordinate (branch country table): EAs compared, max |diff| deg, EAs > 2e-6, NaN')
for a, b, vs, lab in [('2013-14', '2016-17', cont, 'continuing'), ('2013-14', '2019-20', cont, 'continuing'),
                      ('2016-17', '2019-20', cont, 'continuing'), ('2013-14', '2010-11', list(fix.index), 'all 204')]:
    n, mx, bad, na = diff(a, b, vs)
    print(f'   {a} vs {b} ({lab}): {n} EAs | max {mx:.2e} | > 2e-6: {bad} | NaN {na}')
x = cf['dev']; S = {w: x.xs(w, level='t') for w in ['2010-11', '2013-14', '2016-17', '2019-20']}
for w in S:
    S[w].index = S[w].index.astype(str)
n, mx, bad, na = diff('2013-14', '2016-17', cont)
print(f'   (dev) 2013-14 vs 2016-17 (continuing): {n} EAs | max {mx:.2e} | > 2e-6: {bad}')
n, mx, bad, na = diff('2013-14', '2010-11', list(fix.index))
print(f'   (dev) 2013-14 vs 2010-11 (all 204): {n} EAs | max {mx:.2e} | > 2e-6: {bad}')
