"""Small probes: Togo 101009 raw s16b answers; Senegal Feature-vs-Country *_unlisted cells on base and cand."""
import sys, json
import pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
out = {}
lab = get_dataframe(str(countries_root() / 'Togo/2018/Data1/s16b_me_tgo2018.dta'), convert_categoricals=True)
sel = lab[(lab['grappe'].astype(float) == 101) & (lab['menage'].astype(float) == 9)]
cols = ['s16bq01', 's16bq02', 's16bq03a', 's16bq03b', 's16bq04', 's16bq05', 's16bq06', 's16bq07a', 's16bq07b', 's16bq08', 's16bq09a', 's16bq09b', 's16bq09c']
out['togo_101009'] = sel[cols].astype(str).to_dict('records')
for d in sys.argv[1:]:
    k = pd.read_parquet(f'{d}/Senegal__plot_inputs.parquet').reset_index()
    f = pd.read_parquet(f'{d}/Senegal__feature_plot_inputs.parquet').xs('Senegal', level='country').reset_index()
    rec = out[d] = {}
    for col in ['u_unlisted', 'Used_unlisted', 'Purchased_unlisted']:
        rec[col] = {'country_dtype': str(k[col].dtype), 'feature_dtype': str(f[col].dtype),
                    'country_values': {str(x): int(n) for x, n in k[col].value_counts(dropna=False).items()},
                    'feature_values': {str(x): int(n) for x, n in f[col].value_counts(dropna=False).items()}}
print(json.dumps(out, indent=1, ensure_ascii=False, default=str))
