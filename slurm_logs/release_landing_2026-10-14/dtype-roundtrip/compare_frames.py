from pathlib import Path
import pandas as pd,json,hashlib
p=Path('/local/job39919092/release-work/control/framework/dtype-roundtrip')
a=pd.read_pickle(p/'Mali-warm.pkl');b=pd.read_pickle(p/'Mali-actual-warm.pkl');cold=pd.read_pickle(p/'Mali-actual-cold.pkl')
def hashes(df):return hashlib.sha256(pd.util.hash_pandas_object(df,index=True).values.tobytes()).hexdigest()
result={'observed_rows':len(a),'rebuild_rows':len(b),'index_order_equal':a.index.equals(b.index),'index_sets_equal':set(a.index)==set(b.index),'index_dtypes_observed':[str(x.dtype)for x in a.index.levels],'index_dtypes_rebuild':[str(x.dtype)for x in b.index.levels],'dtypes_observed':{str(k):str(v)for k,v in a.dtypes.items()},'dtypes_rebuild':{str(k):str(v)for k,v in b.dtypes.items()},'column_order_equal':a.columns.equals(b.columns)}
a=a.sort_index();b=b.sort_index();result['column_differences']={}
for col in a:
 x=a[col];y=b[col];equal=(x.eq(y).fillna(False)|(x.isna()&y.isna()))
 result['column_differences'][col]={'value_mismatches':int((~equal).sum()),'null_masks_equal':x.isna().equals(y.isna())}
try:pd.testing.assert_frame_equal(a,b,check_exact=True)
except AssertionError as e:result['sorted_exact_equal']=False;result['assertion_heading']=str(e).splitlines()[0]
else:result['sorted_exact_equal']=True
result['sorted_hash_observed']=hashes(a);result['sorted_hash_rebuilt']=hashes(b)
normalized=cold.copy();normalized['MonthsSpent']=normalized['MonthsSpent'].astype('string[pyarrow]');result['actual_cold_stringified_matches_actual_warm_sorted']=hashes(normalized.sort_index())==hashes(b)
result['cold_numeric_months_total']=float(pd.to_numeric(cold.MonthsSpent).sum());result['warm_numeric_months_total']=float(pd.to_numeric(b.MonthsSpent).sum())
(p/'rebuild-comparison.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
