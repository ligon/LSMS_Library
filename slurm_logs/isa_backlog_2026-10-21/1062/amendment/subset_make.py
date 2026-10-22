import os,json,pathlib,time,hashlib,subprocess
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from lsms_library.local_tools import get_dataframe,read_parquet_cache_hash,read_parquet_cache_waves
out=pathlib.Path(os.environ['EVIDENCE_OUT']);out.mkdir(parents=True,exist_ok=True)
root=pathlib.Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert pathlib.Path(ll.__file__).resolve().is_relative_to(root)
assert countries_root().resolve()==root/'lsms_library/countries'
c=ll.Country('Uganda');var=data_root('Uganda')/'var';result={}
os.environ['LSMS_BUILD_BACKEND']='dvc'
for table in ['earnings','enterprise_income']:
 df=getattr(c,table)(waves=['2011-12']);p=var/f'{table}.parquet'
 assert read_parquet_cache_waves(p)==['2011-12']
 assert read_parquet_cache_hash(p)==c._table_cache_hash(table,c.waves)
 result[table]={'subset_shape':list(df.shape),'before_coverage':read_parquet_cache_waves(p),'before_bytes':hashlib.sha256(p.read_bytes()).hexdigest(),'valid_hash_before':True}
os.environ['LSMS_BUILD_BACKEND']='make'
start=time.monotonic();income=c.income()
refs=pathlib.Path(os.environ['REFERENCE_EVIDENCE'])
expected=get_dataframe(refs/'served-income.parquet')
result['income']={'shape':list(income.shape),'expected_shape':list(expected.shape),'waves':sorted(set(income.index.get_level_values('t'))),'expected_waves':sorted(set(expected.index.get_level_values('t'))),'seconds':round(time.monotonic()-start,3)}
for table in ['earnings','enterprise_income']:
 p=var/f'{table}.parquet';result[table]['after_coverage']=read_parquet_cache_waves(p);result[table]['hash_verified_after']=read_parquet_cache_hash(p)==c._table_cache_hash(table,c.waves)
 try:pd.testing.assert_frame_equal(get_dataframe(p),get_dataframe(refs/f'raw-{table}.parquet'),check_exact=True);result[table]['raw_exact_to_original']=True
 except AssertionError:result[table]['raw_exact_to_original']=False
try:pd.testing.assert_frame_equal(income,expected,check_exact=True);result['income']['served_exact_to_original']=True
except AssertionError:result['income']['served_exact_to_original']=False
try:pd.testing.assert_frame_equal(get_dataframe(var/'income.parquet'),get_dataframe(refs/'raw-income.parquet'),check_exact=True);result['income']['raw_exact_to_original']=True
except AssertionError:result['income']['raw_exact_to_original']=False
(out/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
assert result['income']['served_exact_to_original'] and result['income']['raw_exact_to_original'],'All-wave income built from partial-coverage sibling caches'
assert all(result[t]['raw_exact_to_original'] for t in ['earnings','enterprise_income'])
