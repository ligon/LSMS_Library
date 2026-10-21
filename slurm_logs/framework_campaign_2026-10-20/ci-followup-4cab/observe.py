"""Read-only remote CI observer. Never writes GitHub or prints job logs."""
import datetime as dt
import json
from pathlib import Path
import subprocess
import time

ROOT=Path('/local/job40255479/control/framework-campaign/followup-ci-review')
RUN=39531148624
JOB=118997075291
EXPECTED='4cab5f0db7d375ad543e652f61f43bd28fa94ecd'

def stamp():return dt.datetime.now(dt.timezone.utc).isoformat()
def api(endpoint):
    result=subprocess.run(['gh','api',endpoint],capture_output=True,check=True)
    return json.loads(result.stdout)
def save(name,value):
    temporary=ROOT/(name+'.tmp')
    temporary.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
    temporary.replace(ROOT/name)
last=None
while True:
    job=api(f'repos/ligon/LSMS_Library/actions/jobs/{JOB}')
    assert job['run_id']==RUN
    save('data-job.json',job)
    state={'status':job['status'],'conclusion':job['conclusion'],
           'full_suite':next(({'status':s['status'],'conclusion':s['conclusion']} for s in job['steps'] if s['name']=='Run full test suite'),None)}
    with (ROOT/'observations.jsonl').open('a') as stream:
        stream.write(json.dumps({'observed_at':stamp(),**state})+'\n')
    if state!=last:
        print(json.dumps({'observed_at':stamp(),'job':JOB,**state}),flush=True)
        last=state
    if job['status']=='completed':
        run=api(f'repos/ligon/LSMS_Library/actions/runs/{RUN}')
        assert run['head_sha']==EXPECTED and run['event']=='workflow_dispatch'
        save('run.json',run)
        save('jobs.json',api(f'repos/ligon/LSMS_Library/actions/runs/{RUN}/jobs?per_page=100'))
        log=ROOT/'data-job-private.log'
        if not log.exists():
            # Fetch once successfully available; a not-yet-ready response
            # writes no partial log and is retried after another observation.
            result=subprocess.run(['gh','api','--allow-escape-sequences',f'repos/ligon/LSMS_Library/actions/jobs/{JOB}/logs'],capture_output=True)
            if result.returncode:
                (ROOT/'log-fetch-private-error.txt').write_bytes(result.stderr)
                print(json.dumps({'observed_at':stamp(),'log_available':False,'gh_exit':result.returncode}),flush=True)
                time.sleep(45)
                continue
            temporary=ROOT/'data-job-private.log.tmp'
            temporary.write_bytes(result.stdout);temporary.chmod(0o600);temporary.replace(log)
        print(json.dumps({'observed_at':stamp(),'job':JOB,'conclusion':job['conclusion'],'private_log_saved':True,'bytes':log.stat().st_size}),flush=True)
        break
    time.sleep(45)
