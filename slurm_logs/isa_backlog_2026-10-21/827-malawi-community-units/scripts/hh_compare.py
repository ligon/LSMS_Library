"""Recovered community_prices rows vs household purchase unit values on matched (t, v, j, u).
argv: base cp parquet, cand cp parquet, out json.  Aggregates only."""
import os, sys, subprocess, json
import pandas as pd
import lsms_library
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
import lsms_library as ll
c = ll.Country('Malawi')
fp = c.food_prices(units='unitvalue')
print('food_prices', fp.shape, fp.index.names, list(fp.columns))
col = fp.columns[0] if fp.shape[1] == 1 else ('Price' if 'Price' in fp.columns else fp.columns[0])
f = fp.reset_index()
if 's' in f.columns:
    f = f[f['s'] == 'purchased']
hh = f.groupby(['t', 'v', 'j', 'u'])[col].median().rename('hh_unit_value').reset_index()
base = pd.read_parquet(sys.argv[1]).reset_index()
cand = pd.read_parquet(sys.argv[2]).reset_index()
mm = base.merge(cand, on=['t', 'v', 'j'], suffixes=('_b', '_c'))
rec = mm[(mm['u_b'] != mm['u_c']) & (mm['u_c'] != 'Unknown') & mm['Price_c'].notna()].copy()
rec['u'] = rec['u_c']
res = {'recovered_priced_rows_non_unknown': int(len(rec)),
       'recovered_j_in_household_j': sorted(set(rec['j']) & set(hh['j'])),
       'household_u_for_those_j': {j: sorted(set(hh.loc[hh['j'] == j, 'u']))[:40] for j in sorted(set(rec['j']) & set(hh['j']))}}
m = rec.merge(hh, on=['t', 'v', 'j', 'u'], how='inner')
res['matched_cells'] = int(len(m))
if len(m):
    r = m['Price_c'] / m['hh_unit_value']
    res['ratio_median'] = float(r.median()); res['ratio_q25'] = float(r.quantile(.25)); res['ratio_q75'] = float(r.quantile(.75))
    res['matched_by_t_j_u'] = {f'{t}/{j}/{u}': int(v) for (t, j, u), v in m.groupby(['t', 'j', 'u']).size().items()}
# reference: the already-united community rows on the same yardstick (base u present, NumberOfUnits>0)
ok = cand[(cand['u'] != 'Unknown') & cand['Price'].notna() & (cand['NumberOfUnits'] > 0)].copy()
ok['per_unit'] = ok['Price'] / ok['NumberOfUnits']
m2 = ok.merge(hh, on=['t', 'v', 'j', 'u'], how='inner')
r2 = m2['per_unit'] / m2['hh_unit_value']
res['reference_unit_bearing_matched_cells'] = int(len(m2))
res['reference_ratio_median'] = float(r2.median()) if len(m2) else None
json.dump(res, open(sys.argv[3], 'w'), indent=1)
print(json.dumps(res, indent=1))
