import sys,os,json,time,subprocess,importlib.util
from pathlib import Path
import pandas as pd
import lsms_library as ll
from lsms_library.paths import data_root,countries_root
from lsms_library.local_tools import read_parquet_cache_hash,read_parquet_cache_waves
repo=Path('/local/job39919092/release-work/framework/uganda-assets');out=Path('/local/job39919092/release-work/control/framework/uganda-assets-diagnosis')
assert Path(ll.__file__).resolve().parent==repo/'lsms_library';assert countries_root()==repo/'lsms_library/countries'
root=data_root();assert root==Path('/local/job39919092/release-work/framework/uganda-assets-fixed-data')
p=root/'Uganda/var/assets.parquet'
result={'source':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'package':ll.__file__,'root':str(root),'changed_assets_script':True,'comparison_check_exact':True}
def save(): (out/'fixed-replay-result.json').write_text(json.dumps(result,indent=2)+'\n')
def describe(df):return {'rows':len(df),'index':list(df.index.names),'columns':list(df.columns),'dtypes':{str(c):str(t)for c,t in df.dtypes.items()},'duplicates':int(df.index.duplicated().sum()),'null_values':int(df.isna().sum().sum()),'id_converted':df.attrs.get('id_converted')}
def load_test(name):
 spec=importlib.util.spec_from_file_location(name,repo/'tests'/f'{name}.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
before_cache=pd.read_pickle(out/'before_cache.pkl');before_api=pd.read_pickle(out/'before_api.pkl')
wave_before={q.parent.parent.name:pd.read_parquet(q)for q in (root/'Uganda').glob('*/_/assets.parquet')}
start=time.monotonic();subprocess.run(['make',str(p)],cwd=countries_root()/'Uganda/_',check=True);result['make_seconds']=time.monotonic()-start
cache=pd.read_parquet(p);result['make_cache']=describe(cache);result['make_hash']=read_parquet_cache_hash(p);result['make_manifest']=read_parquet_cache_waves(p)
pd.testing.assert_frame_equal(cache,before_cache,check_exact=True);assert cache.attrs==before_cache.attrs;result['make_equals_canonical_raw_exact']=True;cache.to_pickle(out/'fixed_make_cache.pkl')
result['unchanged_waves']={}
for w,frame in wave_before.items():
 pd.testing.assert_frame_equal(pd.read_parquet(root/'Uganda'/w/'_/assets.parquet'),frame,check_exact=True);result['unchanged_waves'][w]=True
s=load_test('test_table_structure');result['existing_raw_checks']={}
for cls,name in[(s.TestTableStructure,'test_no_duplicate_rows'),(s.TestFeatureSanity,'test_feature_is_sane')]:
 getattr(cls(),name)('Uganda','assets',p);result['existing_raw_checks'][name]='passed'
i=load_test('test_uganda_invariance');result['existing_asset_invariance']={}
for rel in i.BASELINE:
 if 'assets' in rel:
  try:i.test_parquet_matches_baseline(countries_root()/'Uganda',rel)
  except AssertionError as e:result['existing_asset_invariance'][rel]={'outcome':'failed','message':str(e)}
  else:result['existing_asset_invariance'][rel]={'outcome':'passed'}
save()
start=time.monotonic();api=ll.Country('Uganda').assets();result['api_seconds']=time.monotonic()-start
pd.testing.assert_frame_equal(api,before_api,check_exact=True);assert api.attrs==before_api.attrs;result['api_equals_prior_exact']=True;api.to_pickle(out/'fixed_api.pkl');result['served_api']=describe(api)
after=pd.read_parquet(p);pd.testing.assert_frame_equal(after,before_cache,check_exact=True);result['canonical_cache_equals_prior_exact']=True;result['api_cache_hash']=read_parquet_cache_hash(p);result['api_cache_manifest']=read_parquet_cache_waves(p)
result['in_tree_parquet_count']=len(list((repo/'lsms_library/countries').rglob('*.parquet')));assert result['in_tree_parquet_count']==0
save();print('Fixed make and canonical replay complete')
