from pathlib import Path
import collections,dataclasses,datetime,hashlib,json,math,os,stat,traceback
import builtins,io,socket
assert builtins.open.__module__=='sitecustomize' and io.open.__module__=='sitecustomize' and os.open.__module__=='sitecustomize'
assert socket.socket.connect.__module__=='sitecustomize' and socket.socket.connect_ex.__module__=='sitecustomize'
assert Path(os.environ['LSMS_CONFIG_DIR']).resolve()==Path(__file__).resolve().parent/'artifact-environment/config'
import numpy as np,pandas as pd,pyarrow as pa
from pandas.api.extensions import ExtensionArray,ExtensionDtype
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from lsms_library.config import _config_dir
P=Path(__file__).resolve().parent;O=P.parent/'review-1078';D=O.parent/'Uganda/1078';R=Path('/local/job40255479/worktrees/isa-review-1078-unit-attribution')
assert _config_dir().resolve()==P/'artifact-environment/config'
assert Path.cwd()==R and Path(ll.__file__).resolve()==R/'lsms_library/__init__.py' and countries_root().resolve()==R/'lsms_library/countries' and data_root().resolve()==P/'artifact-environment/data'
# Explicit exact numerical controls; no default numerical comparator is used.
ARGS={'check_exact':True,'rtol':0,'atol':0,'check_dtype':True,'check_index_type':True,'check_column_type':True,'check_names':True,'check_like':False,'check_categorical':True,'check_freq':True,'check_flags':True}
inputs={};checks=collections.Counter();attrs_types=collections.Counter();failures=[];exceptions=[]
def receipt(p):
 s=p.stat();return {'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':s.st_size,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns}
def track(p):
 p=Path(p).resolve()
 if str(p) not in inputs:inputs[str(p)]=receipt(p)
 return p
def load(p):
 p=track(p)
 if p.suffix=='.arrow':
  with pa.memory_map(str(p),'r') as f:return pa.ipc.open_file(f).read_all().to_pandas()
 if p.suffix=='.parquet':return pd.read_parquet(p)
 return pd.read_pickle(p)
def typed(a,b,path='attrs'):
 assert type(a) is type(b),(path,'type',type(a).__name__,type(b).__name__)
 attrs_types[type(a).__module__+'.'+type(a).__name__]+=1
 if a is None or a is pd.NA or a is pd.NaT:assert a is b,(path,'singleton');return
 if isinstance(a,pd.DataFrame):frame(a,b,path);return
 if isinstance(a,pd.Series):pd.testing.assert_series_equal(a,b,check_exact=True,rtol=0,atol=0,check_dtype=True,check_index_type=True,check_names=True,check_categorical=True,check_freq=True,check_flags=True);index(a.index,b.index,path+'.index');typed(a.attrs,b.attrs,path+'.attrs');return
 if isinstance(a,pd.Index):index(a,b,path);return
 if isinstance(a,ExtensionArray):pd.testing.assert_extension_array_equal(a,b,check_exact=True,rtol=0,atol=0,check_dtype=True);return
 if isinstance(a,ExtensionDtype):assert a==b,(path,'dtype');return
 if isinstance(a,np.ndarray):
  np.testing.assert_array_equal(a,b,strict=True)
  if a.dtype.kind=='O':
   for i,(x,y) in enumerate(zip(a.flat,b.flat)):typed(x,y,path+'['+str(i)+']')
  return
 if isinstance(a,dict):
  if hasattr(type(a),'_FIELDS'):
   typed(type(a)._FIELDS,type(b)._FIELDS,path+'.declared_fields')
   for name in type(a)._FIELDS:
    assert (name in a)==(name in b),(path,'field presence',name)
    typed(getattr(a,name),getattr(b,name),path+'.field.'+name)
  assert len(a)==len(b),(path,'mapping length')
  for key in a:
   matches=[]
   for other in b:
    if type(key) is not type(other):continue
    try:typed(key,other,path+'.key');matches.append(other)
    except AssertionError:pass
   assert len(matches)==1,(path,'typed mapping key');typed(a[key],b[matches[0]],path+'.value')
  return
 if isinstance(a,(list,tuple)):
  assert len(a)==len(b),(path,'sequence length')
  for i,(x,y) in enumerate(zip(a,b)):typed(x,y,path+'['+str(i)+']')
  return
 if isinstance(a,(set,frozenset)):
  assert len(a)==len(b),(path,'set length');remaining=list(b)
  for x in a:
   matched=False
   for i,y in enumerate(remaining):
    try:typed(x,y,path+'.member');matched=True;remaining.pop(i);break
    except AssertionError:pass
   assert matched,(path,'typed set member')
  return
 if dataclasses.is_dataclass(a):
  for f in dataclasses.fields(a):typed(getattr(a,f.name),getattr(b,f.name),path+'.'+f.name)
  return
 if isinstance(a,(float,np.floating)):
  assert (math.isnan(a) and math.isnan(b)) or a==b,(path,'exact float');return
 if isinstance(a,(complex,np.complexfloating)):
  typed(a.real,b.real,path+'.real');typed(a.imag,b.imag,path+'.imag');return
 if isinstance(a,(str,bytes,bool,int,np.integer,np.bool_,datetime.date,datetime.timedelta,pd.Timestamp,pd.Timedelta,np.datetime64,np.timedelta64)):
  if bool(pd.isna(a)) and bool(pd.isna(b)):return
  assert a==b,(path,'exact scalar');return
 raise AssertionError((path,'unsupported attrs type',type(a).__module__,type(a).__name__))
def dtype(a,b,label):
 assert type(a) is type(b) and a==b,(label,'dtype',repr(a),repr(b))
 if isinstance(a,pd.StringDtype):typed(a.storage,b.storage,label+'.storage');typed(a.na_value,b.na_value,label+'.na_value')
def index(a,b,label):
 assert type(a) is type(b),(label,'axis class')
 pd.testing.assert_index_equal(a,b,exact=True,check_exact=True,rtol=0,atol=0,check_names=True,check_categorical=True)
 typed(list(a.names),list(b.names),label+'.names');dtype(a.dtype,b.dtype,label+'.dtype')
 if isinstance(a,pd.MultiIndex):
  assert a.nlevels==b.nlevels;typed(a.sortorder,b.sortorder,label+'.sortorder')
  for i in range(a.nlevels):index(a.levels[i],b.levels[i],label+'.level'+str(i));np.testing.assert_array_equal(a.codes[i],b.codes[i],strict=True)
def frame(a,b,label):
 assert type(a) is type(b),(label,'frame class')
 pd.testing.assert_frame_equal(a,b,**ARGS);index(a.index,b.index,label+'.index');index(a.columns,b.columns,label+'.columns')
 np.testing.assert_array_equal(a.isna().to_numpy(),b.isna().to_numpy(),strict=True)
 for x,y in zip(a.dtypes,b.dtypes):dtype(x,y,label+'.column_dtype')
 typed(a.attrs,b.attrs,label+'.attrs')
def pair(a,b,label,group):
 try:frame(load(a),load(b),label);checks[group]+=1
 except Exception:
  failures.append({'label':label,'group':group,'traceback':traceback.format_exc()});raise
# Discriminating controls for the exact comparator itself; no source or API calls.
control_count=0
def must_reject(a,b,label):
 global control_count
 try:frame(a,b,'control/'+label)
 except AssertionError:control_count+=1
 else:raise AssertionError('Comparator accepted deliberate difference: '+label)
z=pd.DataFrame({'x':[1.0,np.nan]},index=pd.Index(['a','b'],name='row'))
y=z.copy(deep=True);y.loc['a','x']=np.nextafter(1.0,2.0);must_reject(z,y,'nextafter-payload')
y=z.copy(deep=True);y.loc['b','x']=0.0;must_reject(z,y,'null-mask')
y=z.copy(deep=True);y.index=y.index.rename('changed');must_reject(z,y,'axis-name')
y=z.copy(deep=True);y=y.iloc[::-1];must_reject(z,y,'row-order')
y=z.copy(deep=True);y['x']=pd.array(y['x'],dtype='Float64');must_reject(z,y,'extension-dtype')
x=z.copy(deep=True);y=z.copy(deep=True);x.attrs={'nested':{'x':1.0}};y.attrs={'nested':{'x':np.nextafter(1.0,2.0)}};must_reject(x,y,'nextafter-nested-attrs')
x=z.copy(deep=True);y=z.copy(deep=True);x.attrs={'x':1};y.attrs={'x':1.0};must_reject(x,y,'typed-attrs-scalar')
# Full matched-stage pairs and all cross-stage comparisons.
for stage in ('cold','warm'):
 for q in sorted((O/('baseline-frames-'+stage)).glob('*-private.pkl')):pair(q,O/('candidate-frames-'+stage)/q.name,stage+'/'+q.name,'matched_stage_'+stage)
for side in ('baseline','candidate'):
 for q in sorted((O/(side+'-frames-cold')).glob('*-private.pkl')):
  a=load(q);b=load(O/(side+'-frames-warm')/q.name);label=side+'/cross-stage/'+q.name
  if q.name!='Niger-public-food_acquired-private.pkl':frame(a,b,label);checks['cross_stage_full_exact']+=1;continue
  try:frame(a,b,label)
  except AssertionError as err:(P/(side+'-Niger-cross-stage-exact-dtype-failure-private.txt')).write_text(str(err))
  else:raise AssertionError('Expected pre-existing strict Niger u dtype failure was not observed')
  # Compare every payload/axis property exactly, with this sole declared dtype exception.
  index(a.columns,b.columns,label+'.columns');assert a.shape==b.shape;typed(a.attrs,b.attrs,label+'.attrs')
  np.testing.assert_array_equal(a.isna().to_numpy(),b.isna().to_numpy(),strict=True)
  for i in range(a.shape[1]):pd.testing.assert_series_equal(a.iloc[:,i],b.iloc[:,i],check_index=False,check_exact=True,rtol=0,atol=0,check_dtype=True,check_names=True,check_categorical=True,check_freq=True,check_flags=True)
  assert type(a.index) is type(b.index) is pd.MultiIndex;typed(list(a.index.names),list(b.index.names),label+'.names');typed(a.index.sortorder,b.index.sortorder,label+'.sortorder');assert a.index.nlevels==b.index.nlevels
  u=a.index.names.index('u')
  for i in range(a.index.nlevels):
   np.testing.assert_array_equal(a.index.codes[i],b.index.codes[i],strict=True)
   if i!=u:index(a.index.levels[i],b.index.levels[i],label+'.level'+str(i));continue
   x=a.index.levels[i];y=b.index.levels[i];assert type(x) is type(y) and type(x.array) is type(y.array);assert isinstance(x.dtype,pd.StringDtype) and isinstance(y.dtype,pd.StringDtype) and x.dtype.storage==y.dtype.storage=='pyarrow';assert x.dtype.na_value is pd.NA and type(y.dtype.na_value) is float and math.isnan(y.dtype.na_value)
   np.testing.assert_array_equal(x.isna(),y.isna(),strict=True);pd.testing.assert_extension_array_equal(x.array,y.array,check_dtype=False,check_exact=True,rtol=0,atol=0);typed(x.name,y.name,label+'.u.name')
  exceptions.append({'side':side,'frame':q.name,'strict_dtype_failure_preserved':True,'only_accepted_difference':'u StringDtype[pyarrow] pd.NA -> NaN missing sentinel metadata','payload_all_other_axis_dtypes_names_codes_masks_order_full_typed_attrs_exact':True});checks['cross_stage_sole_u_dtype_exception']+=1
# All twelve first-grid supporting caches.
scan=json.loads(track(O/'scanner-proof.json').read_text())
for rel in scan['new_cache_paths']:pair(O/'data-baseline-cold'/rel,O/'data-candidate-cold'/rel,rel,'added_cluster_caches')
rawref=O/'reference/probe-baseline-cold/raw-food-private.pkl';publicref=O/'reference/probe-baseline-cold/public-food-private.pkl'
for stage in ('cold','warm'):
 pair(O/('healing-'+stage+'/raw-food-private.pkl'),rawref,'healing-'+stage+'/raw','healing_vs_reference')
 pair(O/('healing-'+stage+'/public-food-private.pkl'),publicref,'healing-'+stage+'/public','healing_vs_reference')
for name in ('raw-food','public-food','feature-default'):pair(O/('healing-cold/'+name+'-private.pkl'),O/('healing-warm/'+name+'-private.pkl'),'healing/cross-stage/'+name,'healing_cross_stage')
pair(O/'data-candidate-healing/Uganda/var/food_acquired.parquet',rawref,'healing/final-cache','healing_final_cache')
# Actual captured Arrow inputs/outputs and final country cache on both heads.
badref=D/'data-baseline-timed-pair/Uganda/var/food_acquired.parquet'
for side in ('candidate','baseline'):
 T=O/('trace-'+side+'-timed-pair');events=[json.loads(l) for p in T.glob('*-events-private.jsonl') for l in track(p).read_text().splitlines()]
 for role in ('A','B'):
  writes=[e for e in events if e['kind']=='actual_pyarrow_write_input' and e['role']==role and (e.get('target') or '').endswith('/Uganda/var/food_acquired.parquet')];assert len(writes)==1;ident=writes[0]['ident'];expected=badref if side=='baseline' and role=='A' else rawref
  pair(T/(ident+'-input-private.arrow'),expected,side+'/'+role+'/input',side+'_actual_writers');pair(T/(ident+'-actual-output-private.parquet'),expected,side+'/'+role+'/output',side+'_actual_writers')
 pair(O/('data-'+side+'-timed-pair/Uganda/var/food_acquired.parquet'),badref if side=='baseline' else rawref,side+'/final-cache',side+'_final_cache')
 # Every actor's saved post-test/final frame is also covered.
 for role in ('A','B'):
  expected=badref if side=='baseline' and role=='A' else rawref
  for q in (O/('exercise-'+side+'-timed-pair-'+role)).glob('*-food-private.pkl'):pair(q,expected,side+'/'+role+'/'+q.name,side+'_actor_snapshots')
pair(O/'trace-candidate-timed-pair/A-real-read-wave-private.pkl',O/'candidate-frames-cold/Uganda-native-2011-12-private.pkl','candidate/real-native-wave','candidate_native_read')
# Counterfactual reference itself differs from the correct frame only in the nine specified NaNs.
bad=load(badref);expected=load(rawref).copy(deep=True);nine=load(O/'reference/nine-cells-private.pkl')
for cell in nine:expected.loc[tuple(cell['key']),cell['column']]=0.0
frame(bad,expected,'f8/exact-nine-reference');checks['exact_bad_reference_signature']+=1
for scenario in ('market-then-currency','currency-then-market'):
 for q in (O/('exercise-candidate-'+scenario+'-observer')).glob('*-food-private.pkl'):pair(q,rawref,scenario+'/'+q.name,'serial_saved_artifacts')
 pair(O/('data-candidate-'+scenario+'/Uganda/var/food_acquired.parquet'),rawref,scenario+'/final-cache','serial_final_caches')
# Before/after receipts cover every loaded old artifact, including full event sources.
assert all(receipt(Path(p))==r for p,r in inputs.items())
(P/'artifact-input-receipts-private.json').write_text(json.dumps(inputs,indent=2))
result={'status':'PASS','explicit_numerical_comparison':ARGS,'typed_recursive_attrs':True,'declared_record_fields_explicitly_checked':True,'exact_comparator_negative_controls_rejected':control_count,'default_pandas_testing_assert_equal_not_used':True,'pair_counts':dict(checks),'sole_cross_stage_dtype_exceptions':exceptions,'loaded_old_artifacts':len(inputs),'all_loaded_old_artifact_SHA_size_mode_mtime_before_after_exact':True,'attrs_type_counts':dict(attrs_types),'pre_artifact_read_package_config_data_cwd_socket_write_guard_identity_asserted':True,'no_source_or_API_rerun':True,'failures':[]}
(P/'exact-artifact-proof.json').write_text(json.dumps(result,indent=2));print(json.dumps({k:v for k,v in result.items() if k!='attrs_type_counts'},indent=2))
