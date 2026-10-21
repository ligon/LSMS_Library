"""Freeze the operator-stopped observer's status without claiming a test result."""
import datetime as dt
import hashlib
import json
from pathlib import Path

root=Path(__file__).resolve().parent
now=dt.datetime.now(dt.timezone.utc).isoformat()
latest=json.loads((root/'latest.json').read_text())
run=json.loads((root/'run-private.json').read_text())
job=json.loads((root/'data-job-private.json').read_text())
decision=json.loads((root.parent/'RELEASE_DECISION.json').read_text())
sha='097283459f80016b0f00a51b814a0cb964a42789'
assert run['id']==39604418333 and run['head_sha']==sha
assert job['id']==119236827295 and job['status']=='in_progress'
assert latest['job_status']=='in_progress' and latest['job_conclusion'] is None
assert not (root/'verification.json').exists()
assert not (root/'data-job-private.log').exists()
assert all(not Path(f'/proc/{pid}').exists() for pid in [3711698,3712489])
owned=[]
for p in Path('/proc').iterdir():
    if not p.name.isdigit():continue
    try:argv=(p/'cmdline').read_bytes().split(b'\0')
    except (OSError,ProcessLookupError):continue
    if len(argv)>1 and argv[1] in [str(root/'observe.py').encode(),str(root/'progress.py').encode()]:
        owned.append(int(p.name))
assert not owned
status={
    'verdict':'PENDING_TERMINAL_PROOF_UNAVAILABLE',
    'run_id':39604418333,'run_url':run['html_url'],
    'job_id':119236827295,'job_url':job['html_url'],
    'source_sha':sha,'source_identity_basis':'GitHub run metadata; full checkout-log extraction not performed',
    'last_remote_observation_utc':latest['observed_utc'],
    'job_status':latest['job_status'],'job_conclusion':latest['job_conclusion'],
    'workflow_status':latest['run_status'],'workflow_conclusion':latest['run_conclusion'],
    'steps':latest['steps'],'terminal_log_fetched':False,
    'terminal_pytest_counts':None,'nutrition_case_execution_proven':False,
    'all_87_rate_cases_execution_proven':False,
    'credential_failure_log_indicators_evaluated':False,
    'all_skip_reasons_enumerated':False,
    'observer_stopped_utc':now,'observer_stop_reason':'Parent requested final status-only handoff after publication',
    'owned_controller_pids':[3711698,3712489],
    'owned_controller_exit_codes':{'observe.py':143,'progress.py':143},
    'owned_controllers_remaining':owned,'wait_cell_386_terminated':True,
    'github_job_cancelled':False,'duplicate_run_is_release_gate':False,
    'prior_credentialed_evidence':{
        'run_id':39533863014,'run_url':'https://github.com/ligon/LSMS_Library/actions/runs/39533863014',
        'validated_source':decision['validated_source'],
        'counts':{'passed':6388,'skipped':2744,'xfailed':4,'xpassed':1,'failed':0,'errors':0},
        'basis':'Existing completed evidence reused by parent; not counts from the pending duplicate run',
        'exact_source_identity_evidence':'../master-source-identity.json',
        'release_decision':'../RELEASE_DECISION.json',
        'release_decision_sha256':hashlib.sha256((root.parent/'RELEASE_DECISION.json').read_bytes()).hexdigest(),
    },
    'private_snapshot_sha256':{name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ['run-private.json','jobs-private.json','data-job-private.json','latest.json','observations-private.jsonl']},
    'no_project_import_test_or_data_api':True,'no_source_cache_or_github_mutations':True,
}
(root/'status.json').write_text(json.dumps(status,indent=2,sort_keys=True)+'\n')
progress=json.loads((root/'progress.json').read_text())
progress.update({'observer_stopped':True,'observer_stopped_utc':now,'updated_utc':now,'last_remote_observation_utc':latest['observed_utc'],'job_status':latest['job_status'],'job_conclusion':latest['job_conclusion'],'steps':latest['steps'],'terminal_proof_ready':False,'duplicate_run_is_release_gate':False})
(root/'progress.json').write_text(json.dumps(progress,indent=2,sort_keys=True)+'\n')
report=f'''# Supplementary master Linux data CI: pending handoff

**PENDING; terminal proof unavailable.** The observer was stopped at the parent's
request after release publication. This is a status snapshot, not a passing or
failing test result.

[Run 39604418333]({run['html_url']}) / [data job 119236827295]({job['html_url']})
was still running at {latest['observed_utc']}, for full source
`{sha}` according to GitHub run metadata.
Checkout, S3 configuration, the credential gate and Uganda prerequisites had
successful step conclusions; the full test suite was in progress.
The terminal raw log was not fetched. Actual checkout-log SHA, pytest counts,
nutrition execution, all 87 rate-case execution and credential log indicators
therefore remain unproven for this duplicate run. No claim is made about all
skip reasons or the runner's actual BLAS kernel.

The parent retired this extra release barrier in `../RELEASE_DECISION.json`.
Existing completed credentialed run [39533863014](https://github.com/ligon/LSMS_Library/actions/runs/39533863014)
had 6388 passed, 2744 skipped, 4 xfailed and 1 xpassed, with zero failures/errors.
That evidence was at `532d58700fa46e52a0b390198604eca1dc4de00e`;
`../master-source-identity.json` records the parent's exact input-identity proof
for release source 097. These are existing results, not results of the pending
master duplicate. The local waiting-adapter stop and our two controller SIGTERM
exits are operator actions, not CI/test failures.

Both owned controllers stopped with exit 143; waiting cell 386 was terminated.
No owned controllers remain. No GitHub job was cancelled: the job continues
independently on GitHub and its terminal log can be fetched later using REPRO.md.
Raw metadata and observation history remain private; the safe snapshot includes
their SHA256 digests. Only COPYLIST.txt entries should be copied to reports.
No project import, test, data API, cache/source change or GitHub mutation occurred.
All writers for this handoff are stopped after manifest finalization.

Sue
'''
(root/'REPORT.md').write_text(report)
(root/'REPRO.md').write_text('''# Read-only later terminal proof

The stopped observer did not cancel or finish the GitHub job. The published
release does not wait for this duplicate run. Inspect its later state via the
linked job or use the following read-only commands in a NEW private directory.
Copy `observe.py`, `reconcile.py` and `progress.py` there first. Keep raw outputs
private. Use a repository cwd containing git object 097283459f80016b0f00a51b814a0cb964a42789
for the pinned-source `git show` checks; do not import or execute project code.

```sh
gh api repos/ligon/LSMS_Library/actions/runs/39604418333 > run-private.json
gh api 'repos/ligon/LSMS_Library/actions/runs/39604418333/jobs?per_page=100' > jobs-private.json
gh api repos/ligon/LSMS_Library/actions/jobs/119236827295 > data-job-private.json
# Only after the job is terminal:
gh api --allow-escape-sequences repos/ligon/LSMS_Library/actions/jobs/119236827295/logs > data-job-private.log
chmod 600 data-job-private.log
```

`reconcile.py` derives paths from its own directory. After terminal snapshots and
log are present there, run `/local/job40255479/venv_img/bin/python /absolute/private/path/reconcile.py`
with assertions enabled, from the repository cwd. The interpreter path is local
to this allocation; use a normal Python 3 interpreter if fetching later elsewhere.
The controller checks exact run/checkout identity and expected cohort counts;
any mismatch is a proof problem, not automatically a library defect.
It does not enumerate every skip reason or observe the BLAS kernel.
No dispatch, rerun, cancellation, test or data API is required.
''')
files=['REPORT.md','status.json','progress.json','REPRO.md','observe.py','reconcile.py','progress.py','finalize_pending.py']
(root/'COPYLIST.txt').write_text('\n'.join(files+['MANIFEST.json'])+'\n')
(root/'MANIFEST.json').write_text(json.dumps({'verdict':status['verdict'],'source_sha':sha,'raw_metadata_kept_private':True,'files':{name:{'bytes':(root/name).stat().st_size,'sha256':hashlib.sha256((root/name).read_bytes()).hexdigest()} for name in files}},indent=2,sort_keys=True)+'\n')
print(json.dumps({'verdict':status['verdict'],'last_remote_observation_utc':latest['observed_utc'],'controllers_remaining':owned,'safe_copy_files':len(files)+1}))
