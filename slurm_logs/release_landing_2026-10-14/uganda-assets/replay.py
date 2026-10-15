import os,sys,json,time,subprocess,importlib.util
from pathlib import Path
import pandas as pd
import pytest
import lsms_library as ll
from lsms_library.paths import data_root,countries_root
from lsms_library.local_tools import read_parquet_cache_hash,read_parquet_cache_waves

repo=Path('/local/job39919092/release-work/integration')
assert Path(ll.__file__).resolve().parent==repo/'lsms_library'
assert countries_root()==repo/'lsms_library/countries'
root=Path('/local/job39919092/release-work/framework/uganda-assets-diagnosis-data')
assert data_root()==root and data_root('Uganda')==root/'Uganda'
out=Path('/local/job39919092/release-work/control/framework/uganda-assets-diagnosis')
p=root/'Uganda/var/assets.parquet'
def describe(df):
 return {'rows':len(df),'columns':list(df.columns),'index':list(df.index.names),'duplicates':int(df.index.duplicated().sum()),'all_null_rows':int(df.isna().all(axis=1).sum()),'non_null_numeric':{c:int(df[c].notna().sum())for c in df.select_dtypes('number').columns}}
def persist():
 (out/'replay-result.json').write_text(json.dumps(result,indent=2)+'\n')
result={'source':'17308b3d4f35dc7d569ec42b89c24fa82271ecfe','package':ll.__file__,'private_root':str(root),'initial_canonical_cached':describe(pd.read_parquet(p))}
frames={q.parent.parent.name:pd.read_parquet(q)for q in sorted((root/'Uganda').glob('*/_/assets.parquet'))}
result['waves']={w:describe(df)for w,df in frames.items()}
result['naive_concat']=describe(pd.concat(frames,names=['t']))
persist()
started=time.monotonic()
subprocess.run([sys.executable,'assets.py'],cwd=countries_root()/'Uganda/_',check=True)
result['country_script_seconds']=time.monotonic()-started
result['country_script']=describe(pd.read_parquet(p))
result['country_script_hash']=read_parquet_cache_hash(p)
result['country_script_manifest']=read_parquet_cache_waves(p)
spec=importlib.util.spec_from_file_location('actual_structure_tests',repo/'tests/test_table_structure.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
result['actual_tests_before_api']={}
for cls,name in((m.TestTableStructure,'test_no_duplicate_rows'),(m.TestFeatureSanity,'test_feature_is_sane')):
 try:getattr(cls(),name)('Uganda','assets',p)
 except pytest.fail.Exception as e:result['actual_tests_before_api'][name]={'outcome':'failed','message':str(e)}
 else:result['actual_tests_before_api'][name]={'outcome':'passed'}
persist()
started=time.monotonic()
c=ll.Country('Uganda');df=c.assets()
result['canonical_api_seconds']=time.monotonic()-started
result['canonical_api']=describe(df)
result['canonical_cache_after']=describe(pd.read_parquet(p))
result['canonical_cache_hash']=read_parquet_cache_hash(p)
result['canonical_cache_manifest']=read_parquet_cache_waves(p)
result['actual_tests_after_api']={}
for cls,name in((m.TestTableStructure,'test_no_duplicate_rows'),(m.TestFeatureSanity,'test_feature_is_sane')):
 try:getattr(cls(),name)('Uganda','assets',p)
 except pytest.fail.Exception as e:result['actual_tests_after_api'][name]={'outcome':'failed','message':str(e)}
 else:result['actual_tests_after_api'][name]={'outcome':'passed'}
persist()
print('Private replay complete; aggregate JSON written')
