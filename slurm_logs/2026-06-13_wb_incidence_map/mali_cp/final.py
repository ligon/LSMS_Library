import warnings; warnings.filterwarnings('ignore')
import pandas as pd
from lsms_library import Country, diagnostics
c = Country('Mali')
cp = diagnostics.load_feature(c, 'community_prices')
r = cp.reset_index()
print("index:", cp.index.names, "shape:", cp.shape, "unique:", cp.index.is_unique)
print("u labels (all):", sorted(r['u'].dropna().unique()))
print("u count:", r['u'].nunique())

# u resolves to u-table Preferred Labels (no raw codes leaked)
import lsms_library.local_tools as tools
import os
os.chdir('/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/lsms_library/countries/Mali/_')
um = tools.get_categorical_mapping(tablename='u', idxvars='Code', **{'Preferred Label': 'Preferred Label'})
pls = set(um.values())
bad_u = sorted(set(r['u'].dropna().astype(str)) - pls)
print("u values NOT a u-table Preferred Label:", bad_u)

# j resolves to harmonize_food Preferred Labels
fm = tools.get_categorical_mapping(tablename='harmonize_food', idxvars='Code', **{'Preferred Label': 'Preferred Label'})
fpls = set(fm.values())
bad_j = sorted(set(r['j'].dropna().astype(str)) - fpls)
print("j values NOT a harmonize_food Preferred Label:", bad_j)

# v intersect sample
s = c.sample().reset_index()
sv = set(s[s['t'].astype(str) == '2014-15']['v'].dropna().astype(str))
cpv = set(r['v'].astype(str))
print(f"v intersect sample(): {len(cpv & sv)}/{len(cpv)}")
print("Price non-null:", r['Price'].notna().sum(), "| Quantity non-null:", r['Quantity'].notna().sum())
print("Price>0 all:", (r['Price'] > 0).all())
