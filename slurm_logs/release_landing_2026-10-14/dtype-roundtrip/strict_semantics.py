from pathlib import Path
import pandas as pd,json,hashlib
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
repo=Path('/local/job39919092/release-work/framework/uganda-assets');assert Path(ll.__file__).resolve().parent==repo/'lsms_library';assert countries_root()==repo/'lsms_library/countries';assert 'dtype-' in str(data_root())
p=Path('/local/job39919092/release-work/control/framework/dtype-roundtrip')
def h(df):return hashlib.sha256(pd.util.hash_pandas_object(df,index=True).values.tobytes()).hexdigest()
result={}
for country,col in(('Liberia','Int_t'),('Mali','MonthsSpent')):
 original_warm=pd.read_pickle(p/f'{country}-warm.pkl');cold=pd.read_pickle(p/f'{country}-actual-cold.pkl');warm=pd.read_pickle(p/f'{country}-actual-warm.pkl')
 assert cold.index.is_unique and warm.index.is_unique and original_warm.index.is_unique
 assert set(cold.index)==set(warm.index)==set(original_warm.index)
 pd.testing.assert_index_equal(cold.sort_index().index,warm.sort_index().index,exact=True)
 pd.testing.assert_index_equal(cold.sort_index().index,original_warm.sort_index().index,exact=True)
 originalorder=cold.index.equals(original_warm.index)
 cold=cold.reindex(original_warm.index);warm=warm.reindex(original_warm.index)
 pd.testing.assert_frame_equal(warm,original_warm,check_exact=True)
 pd.testing.assert_index_equal(cold.index,warm.index,exact=True)
 pd.testing.assert_frame_equal(cold.drop(columns=col),warm.drop(columns=col),check_exact=True)
 assert cold[col].isna().equals(warm[col].isna())
 counts={}
 for t,part in cold.groupby(level='t',sort=False):
  right=warm.loc[part.index,col];types=part[col].dropna().map(lambda x:type(x).__name__).value_counts().to_dict()
  counts[str(t)]={'rows':len(part),'nonnull':int(part[col].notna().sum()),'cold_non_null_types':types,'warm_non_null_types':right.dropna().map(lambda x:type(x).__name__).value_counts().to_dict()}
 baseline=Path('/local/job39919092/release-work/control/runtime/corpus')/country/'results.jsonl';table='interview_date'if country=='Liberia'else'household_roster';record=next(json.loads(l)['served']for l in baseline.read_text().splitlines()if json.loads(l)['table']==table)
 r={'raw_rebuild_row_order_matches_original':originalorder,'index_key_sets_exact':True,'sorted_index_values_names_and_dtypes_exact_before_reindex':True,'row_order_aligned_for_reconstruction_only':True,'aligned_cold_fingerprint':h(cold),'recorded_cold_fingerprint':record['content_hash'],'aligned_cold_matches_recorded':h(cold)==record['content_hash'],'warm_frame_exact_after_alignment':True,'other_columns_exact':True,'null_mask_exact':True,'wave_details':counts}
 if country=='Liberia':
  pd.testing.assert_series_equal(cold[col].astype('datetime64[ns]'),warm[col].astype('datetime64[ns]'),check_exact=True);r['timestamp_values_exact_at_ns']=True
 else:
  a=pd.to_numeric(cold[col],errors='raise').astype('Float64');b=pd.to_numeric(warm[col],errors='raise').astype('Float64');pd.testing.assert_series_equal(a,b,check_exact=True)
  r['numeric_month_values_exact']=True;r['numeric_months_sum']=float(a.sum());r['nonserialization_value_mismatches']=0
  normalized=cold.copy();mask=normalized[col].isna();normalized[col]=normalized[col].astype(str).astype('string[pyarrow]').mask(mask)
  pd.testing.assert_frame_equal(normalized,warm,check_exact=True);r['exact_writer_cast_reproduces_full_warm_frame']=True
 assert r['aligned_cold_matches_recorded']
 result[country]=r
(p/'strict-semantics.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
