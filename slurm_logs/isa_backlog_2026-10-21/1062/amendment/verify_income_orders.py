import hashlib,json,os,pathlib,subprocess,time
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from lsms_library.local_tools import get_dataframe,read_parquet_cache_hash,read_parquet_cache_waves
TOP=pathlib.Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert pathlib.Path(ll.__file__).resolve().is_relative_to(TOP)
assert countries_root().resolve()==TOP/'lsms_library/countries'
OUT=pathlib.Path(os.environ['EVIDENCE_OUT']);OUT.mkdir(parents=True,exist_ok=True)
var=data_root('Uganda')/'var';refs=pathlib.Path(os.environ['REFERENCE_EVIDENCE'])
watched=['food_acquired','fct','nutrition','earnings','enterprise_income','income']
def snapshot():
 result={}
 for t in watched:
  p=var/f'{t}.parquet'
  if p.exists():result[t]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mtime_ns':p.stat().st_mtime_ns,'cache_hash':read_parquet_cache_hash(p),'waves':read_parquet_cache_waves(p)}
 return result
checks=[]
for n,(table,mode,force,subset) in enumerate([
 ('income','dvc',False,False),('income','make',False,False),('income','dvc',True,False),
 ('earnings','make',True,False),('income','make',True,False),('enterprise_income','make',True,False),
 ('income','dvc',False,False),('income','dvc',False,True)]):
 if n < int(os.environ.get('START_STEP', '0')):
  continue
 if force:
  p=var/f'{table}.parquet'
  if p.exists():p.rename(OUT/f'forced-{n}-{table}.parquet')
 before=snapshot();start=time.monotonic()
 call="ll.Country('Uganda')."+table+("(waves=['2011-12'])" if subset else "()")
 code="import lsms_library as ll; result="+call+"; result.to_parquet("+repr(str(OUT/f'served-{n}.parquet'))+")"
 p=subprocess.run([os.sys.executable,'-c',code],cwd=TOP,env={**os.environ,'LSMS_BUILD_BACKEND':mode},capture_output=True,text=True,timeout=180)
 (OUT/f'order-{n}.log').write_text(p.stdout+p.stderr)
 if p.returncode:raise RuntimeError(f'step {n} {table} failed; see private order log')
 after=snapshot();changed=[t for t in before if t!=table and before[t]!=after.get(t)]
 assert not changed,(n,table,changed)
 for source in ['earnings','enterprise_income','income']:
  pd.testing.assert_frame_equal(get_dataframe(var/f'{source}.parquet'),get_dataframe(refs/f'raw-{source}.parquet'),check_exact=True)
  if source != 'income':
   assert set(read_parquet_cache_waves(var/f'{source}.parquet') or [])==set(ll.Country('Uganda').waves)
 expected=get_dataframe(refs/f'served-{table}.parquet')
 if subset:expected=expected.loc[expected.index.get_level_values('t')=='2011-12']
 pd.testing.assert_frame_equal(pd.read_parquet(OUT/f'served-{n}.parquet'),expected,check_exact=True)
 check={'step':n,'requested':table,'backend':mode,'forced':force,'subset':subset,'seconds':round(time.monotonic()-start,3),'changed_siblings':changed,'raw_three_frames_exact':True,'served_exact':True}
 checks.append(check);(OUT/'summary.json').write_text(json.dumps(checks,indent=2)+'\n');print(check,flush=True)
