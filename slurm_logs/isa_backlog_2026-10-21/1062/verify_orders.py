import hashlib,json,os,pathlib,subprocess,time
import pandas as pd
import pyarrow.parquet as pq
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
TOP=pathlib.Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert pathlib.Path(ll.__file__).resolve().is_relative_to(TOP)
assert countries_root().resolve()==TOP/'lsms_library/countries'
OUT=pathlib.Path(os.environ['EVIDENCE_OUT']);OUT.mkdir(parents=True,exist_ok=True)
var=data_root('Uganda')/'var'
watched=['food_acquired','fct','nutrition','earnings','enterprise_income','income']
def snapshot():
 result={}
 for t in watched:
  p=var/f'{t}.parquet'
  if not p.exists():continue
  meta=pq.read_metadata(p).metadata or {}
  result[t]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mtime_ns':p.stat().st_mtime_ns,'cache_hash':meta.get(b'lsms_cache_hash',b'').decode(),'waves':meta.get(b'lsms_cache_waves',b'').decode()}
 return result
checks=[]
for n,(table,mode,force,subset) in enumerate([
 ('fct','dvc',False,False),('nutrition','dvc',False,False),
 ('fct','dvc',True,False),('nutrition','dvc',True,False),('fct','dvc',True,False),
 ('income','dvc',True,False),('earnings','dvc',True,False),('income','dvc',True,False),
 ('enterprise_income','dvc',True,False),('earnings','make',True,False),('enterprise_income','make',True,False),
 ('income','dvc',False,True),('nutrition','dvc',False,True)]):
 if force:
  p=var/f'{table}.parquet'
  if p.exists():p.rename(OUT/f'forced-{n}-{table}.parquet')
 before=snapshot();start=time.monotonic()
 code="import lsms_library as ll; ll.Country('Uganda')."+table+("(waves=['2011-12'])" if subset else "()")
 p=subprocess.run([os.sys.executable,'-c',code],cwd=TOP,env={**os.environ,'LSMS_BUILD_BACKEND':mode},capture_output=True,text=True,timeout=180)
 (OUT/f'order-{n}.log').write_text(p.stdout+p.stderr)
 if p.returncode:raise RuntimeError(f'step {n} {table} failed; see private order log')
 after=snapshot()
 changed=[t for t in before if t!=table and before[t]!=after.get(t)]
 check={'step':n,'requested':table,'backend':mode,'forced':force,'subset':subset,'seconds':round(time.monotonic()-start,3),'changed_siblings':changed,'requested_hash_present':bool(after[table]['cache_hash']),'requested_coverage_present':bool(after[table]['waves'])}
 checks.append(check);(OUT/'orders-summary.json').write_text(json.dumps(checks,indent=2)+'\n');print(check,flush=True)
 # This is a baseline/candidate experiment: do not stop the baseline after
 # its first known failure, so both revisions exercise the identical order.
