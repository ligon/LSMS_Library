import sys
import pandas as pd
cov = pd.read_stata(sys.argv[1], convert_categoricals=False)
geo = pd.read_stata(sys.argv[2], convert_categoricals=False)
wf = pd.read_parquet(sys.argv[3]).reset_index()
key, lat = sys.argv[4], sys.argv[5]
m = cov[[key, 'ea_id', 'dist_to_IHS3location']].merge(geo[[key, lat]], on=key)
m['i'] = m[key].astype(str).str.strip()
w = wf[~wf.i.astype(str).str.startswith('cs-')].set_index('i')
m = m.join(w[['Latitude']], on='i')
exp = (m.dist_to_IHS3location <= 1) & m[lat].notna()
got = m.Latitude.notna()
print('expected stayers with coords', int(exp.sum()), '| kept', int(got.sum()), '| expected-not-kept', int((exp & ~got).sum()), '| kept-not-expected', int((got & ~exp).sum()))
gap = m[exp & ~got]
print('gap rows: in shared EA', int((gap.ea_id.astype(str) == '30305580').sum()), '| dist values', gap.dist_to_IHS3location.round(3).value_counts().to_dict())
print('panel ids in wave frame matched', int(m.i.isin(w.index).sum()), 'of', len(m))
r1 = pd.read_parquet(sys.argv[6]).reset_index()
r1['i'] = r1.i.astype(str)
csv = set(r1.loc[r1.i.str.startswith('cs-'), 'v'].astype(str))
gap = gap.join(r1.set_index('i')[['v']], on='i')
print('non-shared gap rows: their v in CS v set', int(gap.v.astype(str).isin(csv).sum()), 'of', len(gap), '| ea_id == v', int((gap.ea_id.astype(str) == gap.v.astype(str)).sum()))
extra = m[got & ~exp].join(r1.set_index('i')[['v']], on='i')
print('kept-not-expected: dist', extra.dist_to_IHS3location.tolist(), '| geo lat null', int(extra[lat].isna().sum()))
