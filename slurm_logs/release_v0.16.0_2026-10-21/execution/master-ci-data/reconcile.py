"""Aggregate completed master data-job evidence, without project imports."""
import ast
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import subprocess

if not __debug__:raise RuntimeError('Proof requires enabled assertions')
ROOT=Path(__file__).resolve().parent
RUN=39604418333;JOB=119236827295
EXPECTED='097283459f80016b0f00a51b814a0cb964a42789'
run=json.loads((ROOT/'run-private.json').read_text())
job=json.loads((ROOT/'data-job-private.json').read_text())
assert run['id']==RUN and run['head_sha']==EXPECTED and run['event']=='push'and run['head_branch']=='master'
assert job['id']==JOB and job['status']=='completed'
blob=(ROOT/'data-job-private.log').read_bytes();assert blob
ansi=re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
lines=[ansi.sub('',re.sub(r'^\S+Z\s?','',line)).rstrip()for line in blob.decode('utf-8',errors='replace').splitlines()]
text='\n'.join(lines)
checkout=[]
for i,line in enumerate(lines[:-1]):
    if line.startswith('[command]')and'git log -1 --format=%H'in line:
        value=lines[i+1].strip()
        if re.fullmatch('[0-9a-f]{40}',value):checkout.append(value)
summary=[line for line in lines if re.search(r'\b\d+ passed\b',line)and re.search(r'\bin [\d.]+s\b',line)]
assert summary,'No full pytest summary'
summary=summary[-1]
totals={(key+'s'if key in{'error','warning'}else key):int(value)for value,key in re.findall(r'(\d+) (failed|passed|skipped|xfailed|xpassed|errors?|warnings?)\b',summary)}
for key in ['failed','passed','skipped','xfailed','xpassed','errors','warnings']:totals.setdefault(key,0)
collected=[int(value)for value in re.findall(r'\bcollected (\d+) items\b',text)]
steps={s['name']:s for s in job['steps']}
required=['Run actions/checkout@v4','Configure S3 read credentials','Assert the requires_s3 gate is open','Build Uganda tables','Run full test suite']
step_conclusions={name:steps.get(name,{}).get('conclusion')for name in required}
terms=['UnmarkedS3TestWarning','reached a credential-less skip at RUN time','Skipped: no S3 credentials','needs S3 credentials to build country data','NoCredentialsError','PartialCredentialsError']
indicators={term:text.count(term)for term in terms}
def git(path):return subprocess.check_output(['git','show',EXPECTED+':'+path])
fixture=json.loads(git('tests/fixtures/uganda_baseline.json'))
ids=sorted(fixture)
assert len(ids)==53
# Read, never import, the exact pinned tests to bind marker interpretation.
invariance_source=git('tests/test_uganda_invariance.py')
identity_functions=[node for node in ast.parse(invariance_source).body if isinstance(node,ast.FunctionDef)and node.name=='_parquet_ids']
assert len(identity_functions)==1
assert any(isinstance(node,ast.Call)and isinstance(node.func,ast.Name)and node.func.id=='sorted'for node in ast.walk(identity_functions[0]))
rate_source=git('tests/test_gh1160_pooled_fertilizer_rate.py')
case_counts={}
for node in ast.parse(rate_source).body:
    if isinstance(node,ast.FunctionDef)and node.name.startswith('test_'):
        count=1
        for dec in node.decorator_list:
            if isinstance(dec,ast.Call)and isinstance(dec.func,ast.Attribute)and dec.func.attr=='parametrize':
                assert isinstance(dec.args[1],(ast.List,ast.Tuple)),node.name
                count*=len(dec.args[1].elts)
        case_counts[node.name]=count
assert sum(case_counts.values())==87

def blocks(module):
    prefix=module+' ';result=[]
    for i,line in enumerate(lines):
        if not line.startswith(prefix):continue
        candidate=line[len(prefix):];markers=[]
        while True:
            match=re.fullmatch(r'([.sFxXEP]+)\s*(?:\[\s*\d+%\])?',candidate.strip())
            if not match:break
            markers.extend(match.group(1));i+=1
            if i>=len(lines):break
            candidate=lines[i]
        result.append(''.join(markers))
    return result
nutrition_blocks=blocks('tests/test_uganda_invariance.py')
complete=[b for b in nutrition_blocks if len(b)==53]
position=ids.index('var/nutrition.parquet')
nutrition={'case_count':53,'nutrition_sorted_position_zero_based':position,'unique_complete_groups':len(complete),'observed_lengths':[len(b)for b in nutrition_blocks], 'marker':complete[0][position]if len(complete)==1 else None,'actually_executed_and_passed':len(complete)==1 and complete[0][position]=='.','basis':'Serial pytest ordered terminal markers, exact pinned sorted fixture/test source; not a generalized skip-reason or BLAS-kernel observation.'}
rate_blocks=blocks('tests/test_gh1160_pooled_fertilizer_rate.py')
rate_complete=[b for b in rate_blocks if len(b)==87]
rate={'expected_cases':87,'static_literal_parametrize_cases':case_counts,'unique_complete_groups':len(rate_complete),'observed_lengths':[len(b)for b in rate_blocks],'all_87_executed_and_passed':len(rate_complete)==1 and rate_complete[0]=='.'*87,'pass_markers':rate_complete[0].count('.')if len(rate_complete)==1 else None}
expected={'passed':6388,'skipped':2744,'xfailed':4,'xpassed':1,'failed':0,'errors':0}
problems=[]
if job['conclusion']!='success':problems.append('Data job did not succeed')
if checkout!=[EXPECTED]:problems.append('Actual checkout fullSHA does not match unique expected checkout')
if not collected or collected[-1]!=9137:problems.append('Collected count differs from9137')
if any(totals.get(k)!=v for k,v in expected.items()):problems.append('Pytest counts differ from exact532 baseline')
if any(value!='success'for value in step_conclusions.values()):problems.append('Required checkout/credential/Uganda/test step did not succeed')
if any(indicators.values()):problems.append('Credential failure/skip indicator observed')
if not nutrition['actually_executed_and_passed']:problems.append('Nutrition execution not uniquely proven')
if not rate['all_87_executed_and_passed']:problems.append('All87rate case execution not uniquely proven')
record={'verdict':'PASS'if not problems else'FAIL_OR_UNPROVEN','problems':problems,'run_id':RUN,'run_url':run['html_url'],'source_sha':EXPECTED,'event':run['event'],'head_branch':run['head_branch'],'job_id':JOB,'job_conclusion':job['conclusion'],'actual_checkout_shas':checkout,'step_conclusions':step_conclusions,'pytest_totals':totals,'collected_items':collected,'pytest_seconds':float(re.search(r'\bin ([\d.]+)s\b',summary).group(1)),'baseline_532_expected':expected,'credential_failure_skip_indicator_counts':indicators,'all_skip_reasons_enumerated':False,'all_2744_skips_individually_adjudicated':False,'nutrition_case':nutrition,'rate_cases':rate,'actual_runner_blas_kernel_observed':False,'private_log_bytes':len(blob),'private_log_sha256':hashlib.sha256(blob).hexdigest(),'workflow_status_at_data_fetch':run['status'],'workflow_conclusion_at_data_fetch':run['conclusion'],'workflow_overall_success_gate_owned_by_parent':True,'duplicate_run_is_release_gate':False,'retired_gate_decision':'../RELEASE_DECISION.json','analyzed_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'source_proof_sha256':{'fixture':hashlib.sha256(git('tests/fixtures/uganda_baseline.json')).hexdigest(),'invariance_test':hashlib.sha256(invariance_source).hexdigest(),'rate_test':hashlib.sha256(rate_source).hexdigest()}}
(ROOT/'verification.json').write_text(json.dumps(record,indent=2,sort_keys=True)+'\n')
report=f'''# Exact-master credentialed Linux data-job proof

**{record['verdict']}** at full source `{EXPECTED}`, run [{RUN}]({run['html_url']}), data job {JOB}.

Actual checkout matches the unique expected full SHA: {checkout==[EXPECTED]}.
Checkout, S3 configuration, credential gate, Uganda prerequisite and full-suite step conclusions: {step_conclusions}.
Collected {collected}; pytest totals {totals}, in {record['pytest_seconds']} seconds.
Expected comparison is the exact 532 implementation suite, not a full v0.15-to-v0.16 cold-corpus counterfactual.
Nutrition executed/passed: {nutrition['actually_executed_and_passed']}; ordered zero-based position {position}/53.
All 87 rate cases executed/passed: {rate['all_87_executed_and_passed']}.
Credential failure/skip indicators: {indicators}.

Raw log is private ({len(blob)} bytes), SHA256 `{record['private_log_sha256']}`.
No assertion that every one of 2744 skip reasons was enumerated or individually adjudicated; the console does not supply that proof.
No direct observation of the runner BLAS kernel is claimed.
Workflow status/conclusion when this data proof was fetched: {run['status']}/{run['conclusion']}.
This duplicate run is supplementary evidence. The parent retired its additional
release barrier in RELEASE_DECISION.json and reused the completed exact-code
credentialed run 39533863014. This report does not reinstate that retired barrier.
Other jobs are another observer's responsibility.

Problems: {problems}.
Observer and reconciliation only make read-only gh API GETs and inspect git objects/log metadata.
No project import/test, dispatch/rerun, source/data/cache write or Git/GitHub mutation occurs.
All observer/reconciliation processes stop when terminal proof is written.

Sue
'''
(ROOT/'REPORT.md').write_text(report)
files=['REPORT.md','verification.json','observe.py','reconcile.py','progress.py']
(ROOT/'COPYLIST.txt').write_text('\n'.join(files+['MANIFEST.json'])+'\n')
(ROOT/'MANIFEST.json').write_text(json.dumps({'run':RUN,'job':JOB,'source_sha':EXPECTED,'raw_log_kept_private':True,'files':{name:{'bytes':(ROOT/name).stat().st_size,'sha256':hashlib.sha256((ROOT/name).read_bytes()).hexdigest()}for name in files}},indent=2,sort_keys=True)+'\n')
print(json.dumps({'verdict':record['verdict'],'problems':problems,'totals':totals,'nutrition_pass':nutrition['actually_executed_and_passed'],'rate_87_pass':rate['all_87_executed_and_passed']}))
if problems:raise SystemExit(1)
