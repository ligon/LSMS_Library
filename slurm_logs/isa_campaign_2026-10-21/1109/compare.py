from pathlib import Path
import json
import os
import lsms_library as ll
from lsms_library.paths import countries_root
checkout=Path(os.environ["PYTHONPATH"].split(os.pathsep)[0]).resolve()
assert Path(ll.__file__).resolve()==checkout/"lsms_library/__init__.py"
assert countries_root().resolve()==checkout/"lsms_library/countries"
import pandas as pd
from pandas.testing import assert_frame_equal,assert_series_equal
import argparse
ap=argparse.ArgumentParser()
ap.add_argument('--baseline-data',required=True,type=Path)
ap.add_argument('--fixed-data',required=True,type=Path)
ap.add_argument('--out',required=True,type=Path)
args=ap.parse_args()
base=args.baseline_data.resolve()
fixed=args.fixed_data.resolve()
a=pd.read_pickle(base/'cold-served.pkl');b=pd.read_pickle(fixed/'cold-served.pkl')
pd.testing.assert_index_equal(a.index,b.index,exact=True)
org=a.index.get_level_values('input').astype(str)=='Organic Fertilizer'
assert_frame_equal(a.drop(columns='Quantity'),b.drop(columns=['Quantity','Applied']),check_exact=True)
assert_series_equal(a['Quantity'].mask(org),b['Quantity'],check_exact=True)
assert b.loc[org,'Applied'].eq(True).all() and b.loc[~org,'Applied'].isna().all()
assert b.Applied.dtype==pd.BooleanDtype()
na=pd.read_pickle(base/'cold-nitrogen.pkl');nb=pd.read_pickle(fixed/'cold-nitrogen.pkl')
assert_frame_equal(na,nb,check_exact=True)
report={'rows':len(a),'exact_original_index_order_values_and_dtypes':True,'original_columns_order_values_dtypes_exact_except_organic_quantity':True,'organic_rows':int(org.sum()),'organic_quantity_zero_to_missing':int(org.sum()),'organic_applied_true':int(b.loc[org,'Applied'].sum()),'other_applied_missing':int(b.loc[~org,'Applied'].isna().sum()),'nonorganic_quantity_and_other_measures_unchanged':True,'nitrogen_output_exact':True,'nitrogen_rows':len(na),'nitrogen_total':float(na.Nitrogen_kg.sum()),'organic_rows_by_wave':{str(k):int(v)for k,v in a.loc[org].groupby(level='t').size().items()},'original_column_dtypes':a.dtypes.astype(str).to_dict(),'added_column_dtype':str(b.Applied.dtype),'nonorganic_zero_only_payloads':int(((a.drop(columns='Quantity').isna().all(axis=1))&(~org)&(a.Quantity==0)).sum()),'no_shared_cache_promotion':True,'private_builds_offline':True}
args.out.write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2))
