import os,sys,runpy,types,json,io,builtins
from pathlib import Path
from unittest.mock import patch
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root
import lsms_library.local_tools as lt
repo=Path('/local/job39919092/release-work/integration')
assert Path(ll.__file__).resolve().parent==repo/'lsms_library'
assert countries_root()==repo/'lsms_library/countries'
sys.path.insert(0,str(countries_root()/'Uganda/_'))
import uganda as actual_uganda
script=countries_root()/'Uganda/_/assets.py'
def frame(keys,values):
 return pd.DataFrame({'Value':values},index=pd.MultiIndex.from_tuples(keys,names=['i','j']))
def desc(df):
 return {'rows':len(df),'index':list(df.index.names),'columns':list(df.columns),'duplicates':int(df.index.duplicated().sum()),'non_null_values':int(df.select_dtypes('number').notna().sum().sum()),'null_assets':int(df['assets'].isna().sum())if 'assets'in df else None}
result={}
for name,frames in {
 'disjoint_two_rows_each':{'a':frame([('h1','chair'),('h1','table')],[10.,20.]),'b':frame([('h2','bed'),('h2','radio')],[30.,40.])},
 'singleton_wave':{'a':frame([('h1','chair')],[10.]),'b':frame([('h2','bed'),('h2','radio')],[30.,40.])},
}.items():
 captured=[]
 stub=types.ModuleType('uganda');stub.Waves={w:{}for w in frames};stub.id_walk=actual_uganda.id_walk
 original_open=builtins.open
 def fake_open(path,*args,**kwargs):
  if str(path)=='updated_ids.json':return io.StringIO('{}')
  return original_open(path,*args,**kwargs)
 def get(path,*args,**kwargs):return frames[Path(path).parts[1]].copy()
 with patch.dict(sys.modules,{'uganda':stub}),patch.object(lt,'get_dataframe',side_effect=get),patch.object(lt,'to_parquet',side_effect=lambda df,*a,**k:captured.append(df.copy())),patch('builtins.open',side_effect=fake_open):
  runpy.run_path(str(script),run_name='__main__')
 actual=captured[0]
 reference=actual_uganda.id_walk(pd.concat(frames,names=['t']),{})
 result[name]={'input_rows':sum(map(len,frames.values())),'old_script':desc(actual),'concat_reference':desc(reference),'old_preserves_axis':actual.index.names==['t','i','j'],'old_preserves_schema':list(actual.columns)==['Value'],'old_preserves_row_count':len(actual)==len(reference),'singleton_broadcast':name=='singleton_wave'and int(actual['assets'].eq(10.).sum())!=1}
 assert reference.index.is_unique and len(reference)==sum(map(len,frames.values()))
 assert not result[name]['old_preserves_axis'] and not result[name]['old_preserves_schema'] and not result[name]['old_preserves_row_count']
Path('/local/job39919092/release-work/control/framework/uganda-assets-diagnosis/synthetic-result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
