import sys, io, zipfile
import pandas as pd
cov = pd.read_stata(sys.argv[1], convert_categoricals=False)
zpath, member = sys.argv[2].split('::')
with zipfile.ZipFile(zpath) as zf:
    zc = pd.read_stata(io.BytesIO(zf.read(member)), convert_categoricals=False)
print('held case_id dtype', cov['case_id'].dtype, '| zip case_id dtype', zc['case_id'].dtype)
def norm(s):
    return s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
a = norm(zc.set_index('y3_hhid')['case_id']).reindex(cov['y3_hhid']).values
b = norm(cov.set_index('y3_hhid')['case_id']).reindex(cov['y3_hhid']).values
print('case_id differs after normalising:', int((a != b).sum()), 'of', len(b))
print('held case_id str lengths', norm(cov['case_id']).str.len().value_counts().to_dict(), '| zip', norm(zc['case_id']).str.len().value_counts().to_dict())
others = [c for c in cov.columns if c != 'case_id' and not zc[c].equals(cov[c])]
print('other differing columns:', others)
# mover=0 distinct coords
geo = pd.read_stata(sys.argv[3], convert_categoricals=False)
m = cov[['y3_hhid', 'ea_id', 'mover']].merge(geo[['y3_hhid', 'lat_modified', 'lon_modified']], on='y3_hhid').dropna(subset=['lat_modified'])
s = m[m.mover == 0]
g = s.groupby('ea_id')[['lat_modified', 'lon_modified']].nunique()
print('mover=0: distinct coords per EA value counts', g['lat_modified'].value_counts().sort_index().to_dict())
