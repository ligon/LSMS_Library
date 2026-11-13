"""Verify an extracted household geovariables file against the held panel cover.
Prints COUNTS ONLY -- no ids, no coordinates."""
import sys, io, zipfile
import pandas as pd
geo_path, cover_path, key, lat, lon = sys.argv[1:6]
zip_cover = sys.argv[6] if len(sys.argv) > 6 else None
geo = pd.read_stata(geo_path, convert_categoricals=False)
cov = pd.read_stata(cover_path, convert_categoricals=False)
print('geo shape', geo.shape, '| cover shape', cov.shape)
coordish = [c for c in geo.columns if any(s in c.lower() for s in ('lat', 'lon'))]
print('geo lat/lon-like columns:', coordish)
print('key in geo:', key in geo.columns, '| key in cover:', key in cov.columns,
      '| geo key dtype', geo[key].dtype, '| cover key dtype', cov[key].dtype)
print('geo has ea_id:', 'ea_id' in geo.columns, '| other id-like cols:',
      [c for c in geo.columns if 'id' in c.lower()])
for c in (lat, lon):
    print(f'{c}: present {c in geo.columns}, dtype {geo[c].dtype}, non-null {geo[c].notna().sum()} of {len(geo)}')
inrange = geo[lat].between(-17.2, -9.3) & geo[lon].between(32.6, 35.95)
print('coordinates inside Malawi bbox:', int(inrange.sum()), 'of', int((geo[lat].notna() & geo[lon].notna()).sum()), 'non-null pairs')
gk = geo[key].astype(str).str.strip(); ck = cov[key].astype(str).str.strip()
print('geo key unique:', gk.is_unique, gk.nunique(), '| cover key unique:', ck.is_unique, ck.nunique())
print('geo keys in cover:', int(gk.isin(set(ck)).sum()), 'of', len(gk),
      '| cover keys in geo:', int(ck.isin(set(gk)).sum()), 'of', len(ck))
m = cov[[key, 'ea_id']].assign(_k=ck).merge(geo.assign(_k=gk)[['_k', lat, lon] + (['ea_id'] if 'ea_id' in geo.columns else [])], on='_k', suffixes=('', '_geo'))
if 'ea_id_geo' in m.columns:
    print('ea_id agrees geo vs cover:', int((m['ea_id'].astype(str) == m['ea_id_geo'].astype(str)).sum()), 'of', len(m))
print('cover EAs:', cov['ea_id'].nunique())
g = m.dropna(subset=[lat, lon]).groupby('ea_id')[[lat, lon]].nunique()
multi = ((g[lat] > 1) | (g[lon] > 1))
print('EAs with GPS (any household):', m.dropna(subset=[lat, lon])['ea_id'].nunique(), '| EAs with >1 distinct coordinate:', int(multi.sum()))
if zip_cover:
    zpath, member = zip_cover.split('::')
    with zipfile.ZipFile(zpath) as zf:
        zc = pd.read_stata(io.BytesIO(zf.read(member)), convert_categoricals=False)
    same_cols = list(zc.columns) == list(cov.columns)
    print('zip cover vs held cover: shape', zc.shape, cov.shape, '| same columns', same_cols,
          '| DataFrame.equals', zc.equals(cov) if same_cols else 'n/a')
    if same_cols and not zc.equals(cov):
        neq = [c for c in cov.columns if not zc[c].equals(cov[c])]
        print('  columns differing:', neq)
