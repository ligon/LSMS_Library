"""Read-only exact-master Linux data-job observer. Raw log remains private."""
import datetime as dt
import json
from pathlib import Path
import subprocess
import time

ROOT=Path(__file__).resolve().parent
RUN=39604418333
JOB=119236827295
EXPECTED='097283459f80016b0f00a51b814a0cb964a42789'
def now():return dt.datetime.now(dt.timezone.utc).isoformat()
def save(name,value):
    temp=ROOT/(name+'.tmp');temp.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n');temp.replace(ROOT/name)
def api(endpoint):
    return json.loads(subprocess.check_output(['gh','api',endpoint],stderr=subprocess.PIPE,timeout=90))
last=None;read_errors=0
while True:
    try:
        run=api(f'repos/ligon/LSMS_Library/actions/runs/{RUN}')
        assert run['id']==RUN and run['head_sha']==EXPECTED
        assert run['event']=='push' and run['head_branch']=='master'
        assert run['path']=='.github/workflows/ci.yml'
        assert run['head_repository']['full_name']=='ligon/LSMS_Library'
        jobs=api(f'repos/ligon/LSMS_Library/actions/runs/{RUN}/jobs?per_page=100')
        job=next(j for j in jobs['jobs']if j['id']==JOB)
        assert job['run_id']==RUN and job['name']=='data-tests'
        save('run-private.json',run);save('jobs-private.json',jobs);save('data-job-private.json',job)
        state={'observed_utc':now(),'job_status':job['status'],'job_conclusion':job['conclusion'],
               'run_status':run['status'],'run_conclusion':run['conclusion'],
               'steps':{s['name']:{'status':s['status'],'conclusion':s['conclusion']}for s in job['steps']if s['name']in['Run actions/checkout@v4','Configure S3 read credentials','Assert the requires_s3 gate is open','Build Uganda tables','Run full test suite']}}
        with(ROOT/'observations-private.jsonl').open('a')as f:f.write(json.dumps(state)+'\n')
        save('latest.json',state);key={k:v for k,v in state.items()if k!='observed_utc'}
        if key!=last:print(json.dumps(state),flush=True);last=key
        read_errors=0
        if job['status']=='completed':
            if job['conclusion']!='success':
                save('FAILURE.json',{'run':RUN,'job':JOB,'sha':EXPECTED,'job_conclusion':job['conclusion'],'observed_utc':now()})
                print('ALERT: exact master data job failed: '+str(job['conclusion']),flush=True)
            path=ROOT/'data-job-private.log'
            if not path.exists():
                fetched=subprocess.run(['gh','api','--allow-escape-sequences',f'repos/ligon/LSMS_Library/actions/jobs/{JOB}/logs'],capture_output=True,timeout=120)
                if fetched.returncode:
                    (ROOT/'log-fetch-private-error.txt').write_bytes(fetched.stderr)
                    print(json.dumps({'observed_utc':now(),'log_available':False,'gh_exit':fetched.returncode}),flush=True)
                    time.sleep(45);continue
                temp=ROOT/'data-job-private.log.tmp';temp.write_bytes(fetched.stdout);temp.chmod(0o600);temp.replace(path)
            completed=subprocess.run(['/local/job40255479/venv_img/bin/python',str(ROOT/'reconcile.py')],capture_output=True,text=True)
            (ROOT/'reconcile-private.log').write_text(completed.stdout+completed.stderr)
            if completed.returncode:
                save('PROOF-FAILURE.json',{'run':RUN,'job':JOB,'sha':EXPECTED,'error':'Completed log proof failed; inspect private reconciliation log','code':completed.returncode})
                print('ALERT: master data log proof failed; see PROOF-FAILURE.json',flush=True)
            else:print('MASTER_DATA_PROOF_READY '+str(ROOT/'verification.json'),flush=True)
            break
    except(AssertionError,KeyError,StopIteration)as error:
        save('PROOF-FAILURE.json',{'run':RUN,'job':JOB,'sha':EXPECTED,'error':str(error),'kind':type(error).__name__})
        print('ALERT: exact-run metadata identity/proof mismatch',flush=True);raise
    except(subprocess.SubprocessError,json.JSONDecodeError,OSError)as error:
        read_errors+=1
        save('metadata-read-error.json',{'observed_utc':now(),'consecutive_errors':read_errors,'kind':type(error).__name__})
        print('Metadata GET temporarily unavailable; attempt '+str(read_errors),flush=True)
        if read_errors>=5:raise
    time.sleep(45)
