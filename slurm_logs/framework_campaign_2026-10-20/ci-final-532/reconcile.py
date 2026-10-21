"""Aggregate-only reconciliation of a completed private data-job log."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT=Path('/local/job40255479/control/framework-campaign/followup-ci-review/rate-532')
REPO=Path('/local/job40255479/worktrees/framework-rate')
EXPECTED='532d58700fa46e52a0b390198604eca1dc4de00e'
BASE='852ac2bfcd5f798533f56e7efa1f77db80c0138f'
run=json.loads((ROOT/'run.json').read_text())
job=json.loads((ROOT/'data-job.json').read_text())
assert run['head_sha']==EXPECTED and run['event']=='workflow_dispatch'
assert job['status']=='completed'
blob=(ROOT/'data-job-private.log').read_bytes()
assert blob
text=blob.decode('utf-8',errors='replace')
ansi=re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
lines=[ansi.sub('',re.sub(r'^\S+Z\s?','',line)).rstrip() for line in text.splitlines()]
text='\n'.join(lines)
checkout_shas=[]
for i,line in enumerate(lines[:-1]):
 if line.startswith('[command]') and 'git log -1 --format=%H' in line:
  candidate=lines[i+1].strip()
  if re.fullmatch('[0-9a-f]{40}',candidate):checkout_shas.append(candidate)
assert checkout_shas==[EXPECTED],checkout_shas
summary=[line for line in lines if re.search(r'\b\d+ passed\b',line) and re.search(r'\bin [\d.]+s\b',line)]
assert summary,'No complete pytest summary found'
summary=summary[-1]
totals={(key+'s' if key in {'error','warning'} else key):int(value) for value,key in re.findall(r'(\d+) (failed|passed|skipped|xfailed|xpassed|errors?|warnings?)\b',summary)}
for field in ['failed','passed','skipped','xfailed','xpassed','errors','warnings']:
 totals.setdefault(field,0)
seconds=re.search(r'\bin ([\d.]+)s\b',summary)
collected=[int(n) for n in re.findall(r'\bcollected (\d+) items\b',text)]
failed=[]
for line in lines:
 match=re.match(r'^FAILED (tests/\S+?)(?: - |$)',line)
 if match and match.group(1) not in failed:failed.append(match.group(1))
manifest=json.loads((REPO/'tests/fixtures/uganda_baseline.json').read_text())
ids=sorted(manifest)
marker_blocks=[]
for i,line in enumerate(lines):
 prefix='tests/test_uganda_invariance.py '
 if not line.startswith(prefix):continue
 markers=[]
 candidate=line[len(prefix):]
 while True:
  match=re.fullmatch(r'([.sFxXEP]+)\s*(?:\[\s*\d+%\])?',candidate.strip())
  if not match:break
  markers.extend(match.group(1))
  i+=1
  if i>=len(lines):break
  candidate=lines[i]
 marker_blocks.append(''.join(markers))
complete=[block for block in marker_blocks if len(block)==len(ids)]
nutrition={'manifest_case_count':len(ids),'sorted_case_position_zero_based':ids.index('var/nutrition.parquet'),'ordered_terminal_group_count':len(complete),'basis':'Serial pytest terminal status markers mapped to sorted _parquet_ids; fingerprint fixture keys unchanged; no collection-reordering hook in pinned source.'}
if len(complete)==1:
 nutrition['terminal_marker']=complete[0][nutrition['sorted_case_position_zero_based']]
 nutrition['actually_executed_and_passed']=nutrition['terminal_marker']=='.'
 nutrition['skipped']=nutrition['terminal_marker']=='s'
else:
 nutrition['terminal_marker']=None
 nutrition['actually_executed_and_passed']=False
 nutrition['mapping_limit']='No unique full ordered terminal group; do not infer a passing case.'
steps={s['name']:s for s in job['steps']}
credential_terms=['UnmarkedS3TestWarning','reached a credential-less skip at RUN time','Skipped: no S3 credentials','needs S3 credentials to build country data','NoCredentialsError','PartialCredentialsError']
credential={name:text.count(name) for name in credential_terms}
previous={'failed':1,'passed':6107,'skipped':2744,'xfailed':4,'xpassed':1,'collected':8857}
changes=subprocess.check_output(['git','diff','--name-status',BASE,EXPECTED,'--','tests'],cwd=REPO,text=True).splitlines()
result={'run_id':run['id'],'run_head':run['head_sha'],'actual_checkout_shas':checkout_shas,'event':run['event'],'job_id':job['id'],'job_conclusion':job['conclusion'],'run_status_at_fetch':run['status'],'run_conclusion_at_fetch':run['conclusion'],'pytest_totals':totals,'pytest_seconds':float(seconds.group(1)),'collected_items':collected,'failed_nodeids':failed,'previous_852':previous,'delta_vs_previous_852':{key:totals[key]-previous[key] for key in ['failed','passed','skipped','xfailed','xpassed']},'collected_delta_vs_852':collected[-1]-previous['collected'] if collected else None,'changed_test_assets_since_852':changes,'credential_gate_passed':steps['Assert the requires_s3 gate is open']['conclusion']=='success','uganda_build_passed':steps['Build Uganda tables']['conclusion']=='success','full_suite_step_conclusion':steps['Run full test suite']['conclusion'],'credential_skip_evidence_counts':credential,'all_skip_reasons_enumerated':False,'credential_evidence_limit':'Console does not enumerate all skip reasons. Gate success and absence of credential-skip warnings are observed; do not represent the entire skip population as individually adjudicated.','nutrition_case':nutrition,'actual_CI_blas_kernel_logged':False,'nutrition_kernel_cause_scope':'Exact local controlled counterfactual; no direct observation of CI hardware kernel.','private_log_bytes':len(blob),'private_log_sha256':hashlib.sha256(blob).hexdigest(),'raw_log_never_printed':True,'log_fetch_saved_once':True,'analyzed_at':dt.datetime.now(dt.timezone.utc).isoformat()}
(ROOT/'verification.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
print(json.dumps({'job_conclusion':result['job_conclusion'],'pytest_totals':totals,'seconds':result['pytest_seconds'],'collected_items':collected,'nutrition_case':nutrition,'credential_gate_passed':result['credential_gate_passed'],'credential_skip_evidence_counts':credential,'failed_node_count':len(failed)},sort_keys=True))
