"""community_prices 2019-20 EA accounting (aggregates only)."""
import os, sys, json, warnings
import pandas as pd, pyreadstat
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith('/local/job40390255/worktrees/637-mw/')
from lsms_library.country import Country
from lsms_library.data_access import get_data_file
from lsms_library.local_tools import format_id
from lsms_library.paths import data_root
c = Country('Malawi')
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    cp = c.community_prices()
    cf = c.cluster_features()
w = '2019-20'
api_v = set(cp.xs(w, level='t').index.get_level_values('v'))
cf_v = set(cf.xs(w, level='t').index.get_level_values('v'))
wave = pd.read_parquet(data_root() / 'Malawi' / w / '_' / 'community_prices.parquet').reset_index()
wave_v = set(wave['v'])
halves = {}
for half, rel in (('xs', 'Data/Cross_Sectional/com_ck.dta'), ('panel', 'Data/Panel/com_ck_19.dta')):
    ck, _ = pyreadstat.read_dta(str(get_data_file(f'Malawi/{w}/{rel}')), apply_value_formats=False)
    halves[half] = set(ck['ea_id'].apply(format_id))
gh = {}
for half, rel in (('xs', 'Data/Cross_Sectional/hh_mod_a_filt.dta'), ('panel', 'Data/Panel/hh_mod_a_filt_19.dta')):
    a, _ = pyreadstat.read_dta(str(get_data_file(f'Malawi/{w}/{rel}')), apply_value_formats=False, usecols=['ea_id'])
    gh[half] = set(a['ea_id'].apply(format_id))
dropped = wave_v - api_v
hollow = wave[wave['v'].isin(dropped)]
r = dict(cluster_features_v=len(cf_v), wave_parquet_v=len(wave_v), api_v=len(api_v),
         ck_ea_xs=len(halves['xs']), ck_ea_panel=len(halves['panel']),
         hh_ea_xs=len(gh['xs']), hh_ea_panel=len(gh['panel']),
         cf_xs=len(cf_v & gh['xs']), cf_panel=len(cf_v & gh['panel']),
         no_ck_record_xs=len((gh['xs'] & cf_v) - halves['xs']), no_ck_record_panel=len((gh['panel'] & cf_v) - halves['panel']),
         dropped_at_api=len(dropped), dropped_xs=len(dropped & halves['xs']), dropped_panel=len(dropped & halves['panel']),
         dropped_rows=int(len(hollow)),
         dropped_rows_any_price=int(pd.to_numeric(hollow['Price'], errors='coerce').notna().sum()),
         dropped_rows_cols=list(hollow.columns),
         served_panel=len(api_v & halves['panel']), served_xs=len(api_v & halves['xs']),
         missing_vs_cf=len(cf_v - api_v))
for k in ('Available', 'NumberOfUnits', 'u'):
    if k in hollow.columns:
        r[f'dropped_rows_{k}_notna'] = int(hollow[k].notna().sum())
print(json.dumps(r, indent=0))
json.dump(r, open(sys.argv[1], 'w'), indent=1)
