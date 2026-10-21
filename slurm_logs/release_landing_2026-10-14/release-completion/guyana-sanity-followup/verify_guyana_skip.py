"""Cover the one skipped sanity assertion using an independent physical cache copy."""
from pathlib import Path
import hashlib,json,os,shutil,subprocess,time,xml.etree.ElementTree as ET
out=Path(__file__).resolve().parent/'guyana-sanity-followup';out.mkdir(exist_ok=False)
source=Path('/local/job39919092/release-work/runtime/data-final-promotion/Guyana')
data=Path('/local/job39919092/release-work/release-guyana-sanity-data');data.mkdir(exist_ok=False)
(data/'dvc-cache').symlink_to('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache',target_is_directory=True)
shutil.copytree(source,data/'Guyana',copy_function=shutil.copy2)
def identity(root):
 return {str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mtime_ns':p.stat().st_mtime_ns,'device':p.stat().st_dev,'inode':p.stat().st_ino} for p in root.rglob('*') if p.is_file()}
original=identity(source);copied=identity(data/'Guyana');assert original.keys()==copied.keys()
for k,a in original.items():
 b=copied[k];assert a['bytes']==b['bytes'] and a['sha256']==b['sha256'] and (a['device'],a['inode'])!=(b['device'],b['inode'])
(out/'copy-proof.json').write_text(json.dumps({'source':str(source),'target':str(data/'Guyana'),'original':original,'copied':copied},indent=2)+'\n')
tree=Path('/local/job39919092/release-work/release-master')
env=os.environ.copy()
for k in ['LSMS_SKIP_AUTH','LSMS_NO_CACHE','LSMS_BUILD_BACKEND','PYTHONSAFEPATH','PYTHONOPTIMIZE','PYTEST_ADDOPTS']:
 env.pop(k,None)
env.update(PYTHONPATH=str(tree),LSMS_COUNTRIES_ROOT=str(tree/'lsms_library/countries'),LSMS_DATA_DIR=str(data),OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',LSMS_BUILD_WORKERS='1',LSMS_MAKE_JOBS='1')
python='/local/job39919092/venv_img/bin/python'
node='tests/test_table_structure.py::TestFeatureSanity::test_feature_is_sane[Guyana/interview_date]'
command=['taskset','-c','24-31',python,'-m','pytest','-q',node,'--timeout=600','--timeout-method=thread',f'--junitxml={out/"result.xml"}']
started=time.monotonic()
with (out/'run.log').open('x') as stream:r=subprocess.run(command,cwd=tree,env=env,stdout=stream,stderr=subprocess.STDOUT)
cases=list(ET.parse(out/'result.xml').getroot().iter('testcase'))
assert r.returncode==0 and len(cases)==1 and not any(c.find(k)is not None for c in cases for k in ['failure','error','skipped'])
assert identity(source)==original and identity(data/'Guyana')==copied
assert not subprocess.check_output(['git','status','--porcelain'],cwd=tree)
record={'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],cwd=tree,text=True).strip(),'node':node,'exit_code':r.returncode,'passed':1,'skipped':0,'failures':0,'seconds':time.monotonic()-started,'private_cache_files':len(copied),'original_and_copy_preserved':True,'original_skip_cause':'Guard catches either FileNotFoundError or ArrowInvalid; XML does not distinguish them. Concurrent cold-build interaction is plausible but not established.','country_documentation':'Household identity is ED-SN-HH; cluster identity is ED-SN. Materialize-make tables may use script fallback because no country Makefile is shipped.','scope':'Existing warm-cache sanity assertion only; no new scientific or cold-build claim.','log_sha256':hashlib.sha256((out/'run.log').read_bytes()).hexdigest(),'junit_sha256':hashlib.sha256((out/'result.xml').read_bytes()).hexdigest()}
assert record['source_sha']=='cb44e3522a2426ba03431ca9a218cc63fe03f6c9'
(out/'result.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record,indent=2))
