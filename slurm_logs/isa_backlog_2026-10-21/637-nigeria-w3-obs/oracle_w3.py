"""Raw-source oracle for Nigeria W3 community_prices (GH #637 defect C).

Expected rows built from sectc8a (obs 1) / sectc8b (obs 2) directly:
  keep a source row iff Price > 0, its item resolves to a j, its unit label
  canonicalises to a u, and it is NOT one of the registry's dropped rows
  (inputs_c8_unrecorded_size_drop); a registry-derived row
  (inputs_community_prices) is served at reported_price / size_quantity with
  the c8-metric-size key.  The registry inputs are matched on
  (v, obs, item_cd) -- which is what the new `obs` column on them is for.
Compared row for row with the BUILT Country('Nigeria').community_prices()
t == 2016Q1 (read from the scratch parquet the build script saved).
Prints aggregates only."""
import sys, os, importlib.util, json
import numpy as np, pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

which = sys.argv[1]
root = countries_root() / 'Nigeria'
spec = importlib.util.spec_from_file_location('ng_oracle', root / '_' / 'nigeria.py')
sys.path.insert(0, str(root / '_'))
ng = importlib.util.module_from_spec(spec); spec.loader.exec_module(ng)
spec2 = importlib.util.spec_from_file_location('ng_w3_mapping', root / '2015-16' / '_' / 'mapping.py')
mp = importlib.util.module_from_spec(spec2); spec2.loader.exec_module(mp)

_cwd = os.getcwd(); os.chdir(root / '2015-16' / '_')  # as the wave script runs
crop = ng._crop_labels()
os.chdir(_cwd)
rows = []
for obs, ab in ((1, 'a'), (2, 'b')):
    fn = str(root / '2015-16' / 'Data' / f'sectc8{ab}_harvestw3.dta')
    raw = get_dataframe(fn, convert_categoricals=False)
    dec = get_dataframe(fn, convert_categoricals=True)
    d = pd.DataFrame({
        'v': [ng.cluster_id(s, l, e) for s, l, e in zip(raw.state, raw.lga, raw.ea)],
        'obs': obs,
        'item_cd': pd.to_numeric(raw.item_cd).astype('int64'),
        'j': pd.to_numeric(raw.item_cd).astype('Int64').map(crop).astype('string'),
        'u': dec['c8q2'].map(ng._canon_community_unit).astype('string'),
        'Price': pd.to_numeric(raw.c8q3).astype('float64'),
    })
    rows.append(d)
src = pd.concat(rows, ignore_index=True)
n_src = len(src)
src = src[(src.Price > 0) & src.j.notna() & src.u.notna() & src.v.notna()]
has_obs = {}
drop = mp.inputs_c8_unrecorded_size_drop('2015-16')
der = mp.inputs_community_prices('2015-16')
has_obs['drop'] = 'obs' in drop.columns; has_obs['derived'] = 'obs' in der.columns
res = {'source_rows': n_src, 'priced_resolved_rows': len(src),
       'registry_dropped_rows': len(drop), 'registry_derived_rows': len(der),
       'inputs_carry_obs': has_obs}
if not all(has_obs.values()):
    print(json.dumps(res, indent=1)); print('inputs callables carry no obs: dev tree'); sys.exit(0)
k = ['v', 'obs', 'item_cd']
dk = pd.MultiIndex.from_frame(drop[k].astype({'obs': 'int64', 'item_cd': 'int64'}))
sk = pd.MultiIndex.from_frame(src[k])
res['dropped_keys_unique'] = bool(dk.is_unique)
res['dropped_matched_in_source'] = int(sk.isin(dk).sum())
exp = src[~sk.isin(dk)].copy()
exp['Derivation'] = pd.NA
dv = der.set_index(k)
ek = pd.MultiIndex.from_frame(exp[k])
# the documented rule: only a g/cl row is divided and re-united (data_scheme
# community_prices, GH #834); registry inputs on other units are served undivided
hit = ek.isin(dv.index) & exp['u'].isin(['g', 'cl']).to_numpy()
res['registry_derived_inputs_not_on_g_cl'] = int((ek.isin(dv.index) & ~exp['u'].isin(['g', 'cl']).to_numpy()).sum())
res['derived_matched'] = int(hit.sum())
q = dv.loc[ek[hit], 'size_quantity_g_or_cl'].to_numpy(dtype=float)
exp.loc[hit, 'Price'] = exp.loc[hit, 'Price'].to_numpy() / q
exp.loc[hit, 'Derivation'] = 'Nigeria::community_prices::c8-metric-size'
exp['t'] = '2016Q1'
exp = exp.set_index(['t', 'v', 'j', 'u', 'obs']).sort_index()
res['expected_rows'] = len(exp)
res['expected_unique'] = bool(exp.index.is_unique)
res['expected_by_obs'] = exp.groupby(level='obs').size().to_dict()

built = pd.read_parquet(f'/local/job40390255/data-private/637-ng/scratch/cp_{which}.parquet')
b = built[built.index.get_level_values('t') == '2016Q1']
res['built_rows'] = len(b)
res['built_index'] = list(b.index.names)
ib = b.index.reorder_levels(['t', 'v', 'j', 'u', 'obs'])
b = b.set_axis(ib)
res['missing_from_built'] = int((~exp.index.isin(b.index)).sum())
res['extra_in_built'] = int((~b.index.isin(exp.index)).sum())
common = exp.index.intersection(b.index)
pe = exp.loc[common, 'Price'].to_numpy(); pb = b.loc[common, 'Price'].to_numpy(dtype=float)
res['price_exact_equal'] = int((pe == pb).sum()); res['price_compared'] = len(common)
res['price_max_abs_diff'] = float(np.nanmax(np.abs(pe - pb))) if len(common) else None
de = exp.loc[common, 'Derivation'].astype('string').fillna('')
db = (b.loc[common, 'Derivation'].astype('string').fillna('') if 'Derivation' in b
      else pd.Series('', index=common))
res['derivation_equal'] = int((de.to_numpy() == db.to_numpy()).sum())
print(json.dumps(res, indent=1, default=str))
