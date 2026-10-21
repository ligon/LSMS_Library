"""Run scanner, warm checks, reducers sequentially on one private farm root."""
import argparse,json,os,runpy,subprocess,sys,time
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--expected-sha',required=True)
p.add_argument('--out',type=Path,required=True)
p.add_argument('--mode',choices=['scan','warm','reducers','all'],default='all')
a=p.parse_args();repo=Path.cwd().resolve();out=a.out.resolve();out.mkdir(parents=True,exist_ok=True)
assert os.environ['PYTHONPATH']==str(repo)
assert os.environ['LSMS_EXPECTED_ROOT']==str(repo)
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==a.expected_sha
assert not subprocess.check_output(['git','diff','HEAD','--name-only'],text=True).strip()
for key in ['OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','LSMS_BUILD_WORKERS','LSMS_MAKE_JOBS']:assert os.environ[key]=='1',key
assert not os.environ.get('LSMS_NO_CACHE')
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
assert Path(ll.__file__).resolve().parent.parent==repo
assert countries_root().resolve()==repo/'lsms_library/countries'
assert data_root().resolve()==Path(os.environ['LSMS_DATA_DIR']).resolve()
assert data_root().resolve().is_relative_to(Path('/local'))
os.sched_setaffinity(0,{28,29,30,31})
os.environ['LSMS_ISSUES_LOG']=str(out/'issues-private.log')
identity={'head':a.expected_sha,'cwd':str(repo),'package':ll.__file__,'countries_root':str(countries_root()),'data_root':str(data_root()),'affinity':sorted(os.sched_getaffinity(0))}
(out/'identity.json').write_text(json.dumps(identity,indent=2)+'\n')
if a.mode=='scan':
 sys.argv=[str(repo/'bench/feature_audit/scan.py'),'--features','crop_production','plot_labor','plot_inputs','--phases','1,2','--jobs','3','--out',str(out/'farm-results-private.jsonl')]
 runpy.run_path(sys.argv[0],run_name='__main__')
elif a.mode=='warm':
 import pytest
 raise SystemExit(pytest.main(['-q','--timeout=600','--timeout-method=thread','--maxfail=1',f'--junitxml={out / "farm-warm-private.xml"}','tests/test_feature_canonical_index.py::test_warm_corpus_assembly_loses_no_rows','tests/test_feature_canonical_index.py::test_warm_corpus_plot_table_assembly_keeps_every_country_and_row']))
elif a.mode=='reducers':
 sys.argv=[str(Path(__file__).with_name('live_reducers.py')),'--expected-sha',a.expected_sha,'--out',str(out/'live-reducers.json')]
 runpy.run_path(sys.argv[0],run_name='__main__')
else:
 for mode in ['scan','warm','reducers']:
  with (out/f'{mode}-private.log').open('w') as log:
   result=subprocess.run([sys.executable,__file__,'--expected-sha',a.expected_sha,'--out',str(out),'--mode',mode],stdout=log,stderr=subprocess.STDOUT)
  if result.returncode:
   (out/'FAILURE.json').write_text(json.dumps({'phase':mode,'returncode':result.returncode})+'\n')
   raise SystemExit(result.returncode)
 (out/'completion.json').write_text(json.dumps({'source_commit':a.expected_sha,'state':'passed','phases':['scan','warm','reducers']})+'\n')
