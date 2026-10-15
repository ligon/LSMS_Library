import sys,json,hashlib,importlib.util,time
from pathlib import Path
import pandas as pd
import lsms_library as ll
import lsms_library.country as cm
from lsms_library.paths import countries_root,data_root
repo=Path('/local/job39919092/release-work/framework/uganda-assets');root=Path('/local/job39919092/release-work/framework/dtype-cold-data');out=Path('/local/job39919092/release-work/control/framework/dtype-roundtrip')
assert Path(ll.__file__).resolve().parent==repo/'lsms_library';assert countries_root()==repo/'lsms_library/countries';assert data_root()==root
spec=importlib.util.spec_from_file_location('fp',repo/'tests/generate_baseline.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
def desc(df,col):
 d=mod.fingerprint(df);d['null_count']=int(df[col].isna().sum())if col in df else None;d['non_null_python_type_counts']=df[col].dropna().map(lambda x:type(x).__name__).value_counts().to_dict()if col in df else {};return d
original_writer=cm.to_parquet;events=[];current=None

def writer(df,fn,*a,**k):
 path=Path(fn)
 target,col=current
 if path.name==target+'.parquet':
  rec={'path':str(path),'before':desc(df,col)}
  if path.parent.name=='var':df.to_pickle(out/f'{path.parent.parent.name}-pre-country-write.pkl')
  returned=original_writer(df,fn,*a,**k);rec['returned']=desc(returned,col);rec['stored']=desc(pd.read_parquet(path),col);events.append(rec);return returned
 return original_writer(df,fn,*a,**k)
cm.to_parquet=writer
result={'source':'515864c993050c73e8add567b0a36e22f55af3ef','countries':{}}
def save():
 (out/'cold-result.json').write_text(json.dumps(result,indent=2,default=str)+'\n');(out/'writer-events.json').write_text(json.dumps(events,indent=2,default=str)+'\n')
for country,table,col in(('Liberia','interview_date','Int_t'),('Mali','household_roster','MonthsSpent')):
 current=(table,col);co=ll.Country(country);start=time.monotonic();cold=getattr(co,table)();cold.to_pickle(out/f'{country}-actual-cold.pkl')
 coldsecs=time.monotonic()-start;warm=getattr(ll.Country(country),table)();warm.to_pickle(out/f'{country}-actual-warm.pkl')
 origbase=Path('/local/job39919092/release-work/control/runtime/corpus')/country/'results.jsonl';orig=next(json.loads(l)['served']for l in origbase.read_text().splitlines()if json.loads(l)['table']==table)
 warmbase=Path('/local/job39919092/release-work/control/runtime/final-promotion/run')/country/'results.jsonl';oldwarm=next(json.loads(l)['served']for l in warmbase.read_text().splitlines()if json.loads(l)['table']==table)
 result['countries'][country]={'cold_seconds':coldsecs,'cold':desc(cold,col),'warm':desc(warm,col),'actual_cold_matches_original_fingerprint':mod.fingerprint(cold)['content_hash']==orig['content_hash'],'actual_warm_matches_final_record':mod.fingerprint(warm)['content_hash']==oldwarm['content_hash']}
 save()
print('Actual private cold/warm and writer trace complete')
