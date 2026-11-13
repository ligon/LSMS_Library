# Exact comparator source copied for review.
# Only its function AST is used after guarded bootstrap; no old top-level checks.
# Required globals are the checked imports in exact_addendum.py.
ARGS={'check_exact':True,'rtol':0,'atol':0,'check_dtype':True,'check_index_type':True,'check_column_type':True,'check_names':True,'check_like':False,'check_categorical':True,'check_freq':True,'check_flags':True}

def receipt(p):
 s=p.stat();return {'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':s.st_size,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns}

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
