import os,sys,socket
from pathlib import Path
P=Path('/local/job40255479/control/isa-backlog-20261021/comparator-object-dtype-root')
assert sys.executable=='/local/job40255479/venv_img/bin/python'
assert [x.split(':')[0].strip() for x in Path('/proc/net/dev').read_text().splitlines()[2:]]==['lo']
def audit(event,args):
 if event in {'socket.connect','socket.connect_ex','socket.getaddrinfo'}:raise PermissionError('Synthetic proof is offline')
 if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
  mode,flags=args[1] or '',args[2]
  if any(c in mode for c in 'wax+') or flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND):
   q=Path(os.fsdecode(args[0])).absolute()
   if not q.is_relative_to(P) or not q.resolve().is_relative_to(P):raise PermissionError('Write outside new synthetic proof root')
 if event in {'os.remove','os.rmdir','os.mkdir','os.chmod','os.utime','os.truncate','os.rename','os.link','os.symlink'}:raise PermissionError('No mutation beyond new private files')
sys.addaudithook(audit)
def offline(*args,**kwargs):raise OSError('Synthetic proof is offline')
socket.socket.connect=offline;socket.socket.connect_ex=offline
import hashlib,json,decimal,importlib.metadata
assert all(os.environ[x]=='1' for x in ('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS'))
base=Path('/local/job40255479/control/isa-backlog-20261021/comparator-values-reassessment/integration17-release')
helpers={}
for name,filename,pin in [('slow','exact_strict.py','c42164e9d7e01027ac264b1442e5157d44aa0207655ab8c53d080659084f812d'),('fast','exact_strict_fast_v2.py','7b35e1126adb8c624b97714fe5b6c5baf20200413a5b373a0e1954287d2fe7b3')]:
 data=(base/filename).read_bytes();assert hashlib.sha256(data).hexdigest()==pin,(filename,'SHA mismatch')
 scope={'__name__':'root_synthetic_'+name};exec(compile(data,str(base/filename),'exec'),scope);helpers[name]=scope
np=helpers['fast']['np'];pd=helpers['fast']['pd']
def acceptance(callback):
 try:callback()
 except AssertionError:return False
 return True
cases=[('int-v-bool',1,True),('int-v-float',1,1.0),('numpy-v-python-int',np.int64(1),1),('decimal-v-int',decimal.Decimal('1'),1),('none-v-NA',None,pd.NA),('nan-v-None',float('nan'),None)]
rows=[]
for label,x,y in cases:
 a=pd.DataFrame({'x':pd.Series([x],dtype=object)});b=pd.DataFrame({'x':pd.Series([y],dtype=object)})
 dtype=a.x.array.dtype
 rows.append({'case':label,'scalar_types':[type(a.x.iloc[0]).__name__,type(b.x.iloc[0]).__name__],'dtype_type':type(dtype).__module__+'.'+type(dtype).__name__,'kind':dtype.kind,'is_numpy_dtype':isinstance(dtype,np.dtype),'plain_frame_accepts':acceptance(lambda:pd.testing.assert_frame_equal(a,b,check_exact=True,rtol=0,atol=0)),'slow_accepts':acceptance(lambda:helpers['slow']['frame'](a,b,label)),'fast_accepts':acceptance(lambda:helpers['fast']['frame'](a,b,label))})
result={'status':'SYNTHETIC_ONLY_COMPARATOR_TYPE_PROBE','packages':{n:importlib.metadata.version(n) for n in ('pandas','numpy','pyarrow')},'cases':rows,'LSMS_imports_or_API_calls':0,'existing_evidence_mutations':0,'signed':'Sue'}
with (P/'ROOT-SYNTHETIC-PROOF.json').open('x') as f:json.dump(result,f,indent=2);f.write('\n')
print(json.dumps(result))
