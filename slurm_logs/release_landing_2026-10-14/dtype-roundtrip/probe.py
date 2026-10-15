from pathlib import Path
import sys,json,hashlib,importlib.util,subprocess,time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from lsms_library.local_tools import to_parquet
repo=Path('/local/job39919092/release-work/framework/uganda-assets');root=Path('/local/job39919092/release-work/framework/dtype-roundtrip-data');out=Path('/local/job39919092/release-work/control/framework/dtype-roundtrip')
assert Path(ll.__file__).resolve().parent==repo/'lsms_library';assert countries_root()==repo/'lsms_library/countries';assert data_root()==root
assert not subprocess.check_output(['git','diff','3914d8804','HEAD','--','lsms_library'],cwd=repo,text=True)
spec=importlib.util.spec_from_file_location('fp',repo/'tests/generate_baseline.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
base=Path('/local/job39919092/release-work/control/runtime')
def record(stage,country,table):return next(json.loads(x)for x in(base/stage/country/'results.jsonl').read_text().splitlines()if json.loads(x)['table']==table)
def fp(df):
 d=mod.fingerprint(df);d['null_counts']={str(c):int(df[c].isna().sum())for c in df};d['wave_rows']={str(k):int(v)for k,v in df.index.get_level_values('t').value_counts(dropna=False).items()};return d
results={'source':'515864c993050c73e8add567b0a36e22f55af3ef','runtime_identical_to':'3914d8804d7b2936d75041cea98bbc9ddb6d4c9f','versions':{'pandas':pd.__version__,'pyarrow':pa.__version__},'countries':{}}
for country,table,column,dtype in [('Liberia','interview_date','Int_t','datetime64[s]'),('Mali','household_roster','MonthsSpent','object')]:
 cold=record('corpus',country,table)['served'];warm=record('final-promotion/run',country,table)['served']
 start=time.monotonic();frame=getattr(ll.Country(country),table)();frame.to_pickle(out/f'{country}-warm.pkl')
 result={'table':table,'api_seconds':time.monotonic()-start,'warm_record':warm,'cold_record':cold,'private_warm':fp(frame)}
 assert result['private_warm']['content_hash']==warm['content_hash'];assert result['private_warm']['dtypes']==warm['dtypes']
 restored=frame.copy();restored[column]=restored[column].astype(dtype);restored.to_pickle(out/f'{country}-restored-cold.pkl')
 result['dtype_only_restoration']=fp(restored);result['restored_matches_cold_hash']=result['dtype_only_restoration']['content_hash']==cold['content_hash'];result['restored_matches_cold_schema']=all(result['dtype_only_restoration'][k]==cold[k]for k in('shape','columns','index_names','dtypes','null_counts','wave_rows'))
 pd.testing.assert_index_equal(frame.index,restored.index,exact=True);result['index_exactly_unchanged']=True
 pd.testing.assert_frame_equal(frame.drop(columns=column),restored.drop(columns=column),check_exact=True);result['other_columns_exactly_unchanged']=True
 assert frame[column].isna().equals(restored[column].isna());result['null_mask_unchanged']=True
 if country=='Liberia':
  pd.testing.assert_series_equal(frame[column].astype('datetime64[ns]'),restored[column].astype('datetime64[ns]'),check_exact=True);result['timestamps_exactly_equal_in_ns']=True
 else:
  pd.testing.assert_series_equal(frame[column].astype(object),restored[column],check_exact=True);result['non_null_python_type_counts']=restored[column].dropna().map(lambda x:type(x).__name__).value_counts().to_dict()
 writer_input=restored[[column]].reset_index(drop=True)
 path=out/f'{country}-writer-probe.parquet';returned=to_parquet(writer_input,path,absolute_path=True);loaded=pd.read_parquet(path)
 result['writer_input_dtype']=str(restored[column].dtype);result['writer_return_dtype']=str(returned[column].dtype);result['writer_reloaded_dtype']=str(loaded[column].dtype);result['arrow_physical_type']=str(pq.read_schema(path).field(column).type)
 normalized=loaded.copy();normalized[column]=normalized[column].astype(dtype);pd.testing.assert_frame_equal(normalized,writer_input,check_exact=True);result['writer_roundtrip_value_index_exact_after_dtype_only_restoration']=True
 pd.testing.assert_frame_equal(loaded,frame[[column]].reset_index(drop=True),check_exact=True);result['writer_reloaded_column_equals_warm_column']=True
 results['countries'][country]=result
 (out/'result.json').write_text(json.dumps(results,indent=2,default=str)+'\n')
print('Private warm/reconstructed cold and writer roundtrip checks complete')
