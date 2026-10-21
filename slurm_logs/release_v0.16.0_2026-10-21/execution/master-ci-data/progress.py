"""Safe local progress projection; never performs GitHub API requests."""
import datetime as dt
import json
from pathlib import Path
import time

ROOT=Path(__file__).resolve().parent
EXPECTED='097283459f80016b0f00a51b814a0cb964a42789'
while True:
    now=dt.datetime.now(dt.timezone.utc)
    try:
        latest=json.loads((ROOT/'latest.json').read_text())
        job=json.loads((ROOT/'data-job-private.json').read_text())
        run=json.loads((ROOT/'run-private.json').read_text())
        assert run['head_sha']==EXPECTED
        started=dt.datetime.fromisoformat(job['started_at'].replace('Z','+00:00'))
        ready=(ROOT/'verification.json').exists()or(ROOT/'PROOF-FAILURE.json').exists()
        state={'run_id':39604418333,'job_id':119236827295,'source_sha':EXPECTED,
               'updated_utc':now.isoformat(),'last_remote_observation_utc':latest['observed_utc'],
               'elapsed_job_seconds':round((now-started).total_seconds()),
               'job_status':latest['job_status'],'job_conclusion':latest['job_conclusion'],
               'steps':latest['steps'],'terminal_proof_ready':ready,
               'failure_observed':(ROOT/'FAILURE.json').exists()or(ROOT/'PROOF-FAILURE.json').exists()}
        temp=ROOT/'progress.json.tmp';temp.write_text(json.dumps(state,indent=2,sort_keys=True)+'\n');temp.replace(ROOT/'progress.json')
        if ready:break
    except FileNotFoundError:pass
    time.sleep(45)
