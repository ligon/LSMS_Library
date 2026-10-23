from identity import *
import warnings,sys,hashlib,stat,dataclasses
import pandas as pd
from lsms_library import local_tools as lt
assert json.loads((P/'PREPARATION.json').read_text())['terminal_success']
stage=sys.argv[1];parts=SIDE.split('-');backend=parts[1];case=parts[-1];populated='populated'in parts
requests={'all':None,'valid':['2014-15'],'2017':['2017-18'],'2018':['2018-19'],'2021':['2021-22'],'unsupported':['2017-18','2018-19','2021-22']}
waves=requests[case]
from exact import typed,equal
def receipt(p):
 s=p.stat();return {'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'size':s.st_size,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns}
def caches():return {str(p.relative_to(DATA)):receipt(p)for p in DATA.rglob('*.parquet')}
before=caches();mali_before={k:v for k,v in before.items()if k.startswith('Mali/')}
if stage=='cold'and not populated:assert not mali_before
if populated:assert mali_before and (OUT/'POPULATED.json').exists()
calls=[];original_run=subprocess.run
def observed_run(*a,**k):
 calls.append({'args':[str(x)for x in(a[0]if a else k.get('args',[]))],'cwd':str(k.get('cwd',''))})
 (OUT/(stage+'-subprocess-private.json')).write_text(json.dumps(calls,indent=2)+'\n')
 return original_run(*a,**k)
subprocess.run=observed_run
c=ll.Country('Mali',preload_panel_ids=False);assert c._has_country_builder('community_prices')is False
with warnings.catch_warnings(record=True)as ws:
 warnings.simplefilter('always')
 result=c.community_prices(**({}if waves is None else{'waves':waves}))
result.to_pickle(OUT/(stage+'-public-private.pkl'))
assert list(result.index.names)==['t','v','j','u','variety','visit','obs']
if case in ['all','valid']:
 equal(result,pd.read_pickle(P/'references/default-all-warm-public.pkl'))
 assert len(result)==155931 and result.attrs['id_converted']is True
 assert result.groupby(level='obs').size().to_dict()=={1:66699,2:49014,3:40218}
 assert result.groupby(level='visit').size().to_dict()=={1:66159,2:89772}
 assert result.index.is_unique and not result.index.to_frame(index=False).isna().any().any()
else:
 assert result.empty and list(result.columns)==['Price','Quantity','Description']
 assert result.Price.dtype==result.Quantity.dtype==pd.Float64Dtype()
 assert not any(Path(row['args'][0]).name=='make'and '-n'not in row['args']and any('community_prices.parquet'in word for word in row['args'])for row in calls)
if stage=='warm':equal(result,pd.read_pickle(OUT/'cold-public-private.pkl'))
after=caches();mali_after={k:v for k,v in after.items()if k.startswith('Mali/')}
if case not in ['all','valid']:assert mali_after==mali_before
if stage=='warm'and backend=='default':assert after==before
layers={}
for path in (DATA/'Mali').rglob('*.parquet'):
 raw=lt.get_dataframe(str(path));layer='country'if 'var'in path.parts else'native'
 raw.to_pickle(OUT/(stage+'-'+layer+'-private.pkl'))
 if layer=='native':equal(raw,pd.read_pickle(P/'references/default-all-cold-native.pkl'))
 else:equal(raw,pd.read_pickle(P/'references/default-all-cold-country.pkl'))
 meta={'rows':len(raw),'index_names':list(raw.index.names),'hash':lt.read_parquet_cache_hash(path),'coverage':lt.read_parquet_cache_waves(path),'receipt':receipt(path)}
 if layer=='country':
  assert meta['hash']==c._table_cache_hash('community_prices',c.waves)
  assert meta['coverage']==(c.waves if case=='all'or populated else['2014-15'])
 else:assert meta['hash']is None and meta['coverage']is None
 layers[layer]=meta
if backend=='make'and not populated:assert 'country'not in layers
if case in ['all','valid']:
 assert 'native'in layers
 if backend=='default':assert 'country'in layers
if 'Niger/var/community_prices.parquet'in before:assert after['Niger/var/community_prices.parquet']==before['Niger/var/community_prices.parquet']
(OUT/(stage+'-warnings-private.json')).write_text(json.dumps([{'category':w.category.__name__,'message':str(w.message),'filename':w.filename,'lineno':w.lineno}for w in ws],indent=2)+'\n')
(OUT/(stage+'-subprocess-private.json')).write_text(json.dumps(calls,indent=2)+'\n')
(OUT/(stage+'-receipts-private.json')).write_text(json.dumps(after,indent=2)+'\n')
r={'side':SIDE,'stage':stage,'head':HEAD,'backend':backend,'request':waves,'populated':populated,'rows':len(result),'warnings':len(ws),'full_values_axes_order_dtypes_nulls_attrs_exact':True,'all_supported_obs_source_rows_exact':case in ['all','valid'],'no_unsupported_source_build_or_unrelated_rows':case not in ['all','valid'],'cache_files_changed':[k for k,v in after.items()if before.get(k)!=v],'cache_files_removed':[k for k in before if k not in after],'layers':layers,'Make_new_country_cache':backend=='make'and not populated and'country'in layers,'source_acquisition':False}
(OUT/(stage+'-summary.json')).write_text(json.dumps(r,indent=2)+'\n');print(json.dumps({k:r[k]for k in ['side','stage','rows','warnings','cache_files_changed','cache_files_removed']}))
