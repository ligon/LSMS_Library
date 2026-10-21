"""Aggregate terminal master CI only; never copy raw survey logs into publication evidence."""
from pathlib import Path
import collections,hashlib,json,re,shutil,subprocess,sys
P=Path(__file__).resolve().parent
A=json.loads((P/'aggregate-results.json').read_text())
assert A['status']=='completed', 'Wait for terminal CI'
assert (P/'monitor-finished.json').exists(), 'Wait for the second terminal/master-tip snapshot'
subprocess.run([sys.executable,str(P/'render_report.py')],check=True,capture_output=True)
ANSI=re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
def markers(path):
 streams={};current=None
 for raw in path.read_text(errors='replace').splitlines():
  line=re.sub(r'^\d{4}-\d\d-\d\dT\S+\s*','',ANSI.sub('',raw))
  if line.startswith('=')and('warnings summary'in line or'FAILURES'in line):current=None
  match=re.fullmatch(r'(tests/\S+\.py) ([.sxXFEP]+)\s*(?:\[.*\])?',line)
  if match:current=match[1];streams.setdefault(current,'');streams[current]+=match[2];continue
  match=re.fullmatch(r'([.sxXFEP]+)\s*(?:\[.*\])?',line)
  if current and match:streams[current]+=match[1]
 return streams
for job in A['jobs']:
 if 'log_sha256'not in job:continue
 log=P/f"{A['run_id']}-{job['name']}.log"
 assert log.stat().st_size==job['log_bytes']and hashlib.sha256(log.read_bytes()).hexdigest()==job['log_sha256']and job['full_source_sha_present']
data=next(j for j in A['jobs']if j['name']=='data-tests')
assert 'log_sha256'in data and data['test_summaries'], 'Inspect incomplete terminal job evidence manually'
summary=data['test_summaries'][-1]['counts'];current=markers(P/f"{A['run_id']}-data-tests.log")
totals=collections.Counter(''.join(current.values()))
key={'.':'passed','s':'skipped','x':'xfailed','X':'xpassed','F':'failed','E':'error','P':'passed'}
assert all(totals[k]==summary.get(v,0)for k,v in key.items()if k!='P'),(totals,summary)
manual=json.loads((P.parent/'final-ci/semantic-review.json').read_text())
counts={module:dict(collections.Counter(stream))for module,stream in current.items()}
differences={module:{'validated_manual':manual['all_module_marker_counts'].get(module),'master':counts.get(module)}for module in set(counts)|set(manual['all_module_marker_counts'])if counts.get(module)!=manual['all_module_marker_counts'].get(module)}
prior=[]
for r in manual['resolved_previous_failures']:
 module=r['node_id'].split('::',1)[0];stream=current[module];assert len(stream)==r['module_items']
 marker=stream[r['zero_based_module_position']]
 prior.append({'node_id':r['node_id'],'zero_based_module_position':r['zero_based_module_position'],'module_items':len(stream),'master_marker':marker,'master_outcome':key[marker]})
semantic={'source_sha':A['source_sha'],'run_id':A['run_id'],'all_module_marker_counts':counts,'marker_totals':dict(totals),'comparison_manual_run':38746123119,'module_outcome_count_differences':differences,'previously_resolved_uganda_cases':prior,'skip_reason_limit':'Only actual module markers/counts are available; terminal output does not provide a per-node skip-reason ledger.','no_new_test_execution':True,'identity_proof':'source-identity.json establishes unchanged tests/config/workflow relative to validated3914; prior affected collection-order proof is in final-ci-publishable.'}
(P/'semantic-review.json').write_text(json.dumps(semantic,indent=2)+'\n')
report=(P/'REPORT.md').read_text()
report+='\n## Terminal data evidence\n\n'+data['test_summaries'][-1]['summary']+'. The marker stream reconciles exactly to the terminal counts.\n\n'
report+='The preceding implementation manual run38746123119 is compared by actual module outcome counts, without re-executing tests. Module-count differences: '+str(len(differences))+'. See semantic-review.json. Same totals do not establish individual skipped reasons, which were not printed.\n\n'
report+='| Previously failed Uganda case | Actual master outcome |\n|---|---|\n'+'\n'.join(f"| `{r['node_id']}` | {r['master_outcome']} |"for r in prior)+'\n\n'
report+='These node positions reuse the prior unchanged-source collection proof, calibrated against the original three failure positions. No pending/skipped test is relabeled as passed. Four expected failures and the existing unexpected pass, if present in the actual counts, retain their pytest categories.\n\n'
report+='| Completed job | Bytes | SHA256 |\n|---|---:|---|\n'+'\n'.join(f"| {j['name']} / {j['databaseId']} | {j['log_bytes']} | `{j['log_sha256']}` |"for j in A['jobs']if'log_sha256'in j)+'\n'
D=P.with_name('master-ci-publishable');D.mkdir(exist_ok=True)
for name in('aggregate-results.json','semantic-review.json','source-identity.json','native-validation.json','expected-native.json','docs-lock-observations.json','master-tip.json','master-tip-history.jsonl','monitor-process.json','monitor-finished.json','monitor.py','render_report.py','freeze_terminal.py'):
 if (P/name).exists():shutil.copy2(P/name,D/name)
if(P/'master-tip-change.json').exists():shutil.copy2(P/'master-tip-change.json',D/'master-tip-change.json')
(D/'REPORT.md').write_text(report)
files=[{'name':p.name,'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in sorted(D.iterdir())if p.name!='evidence-manifest.json']
(D/'evidence-manifest.json').write_text(json.dumps({'classification':'aggregate terminal outcomes and source-only tools; raw survey logs remain private','source_sha':A['source_sha'],'run_id':A['run_id'],'files':files},indent=2)+'\n')
for name in('REPORT.md','evidence-manifest.json'):
 p=D/name;print(name,hashlib.sha256(p.read_bytes()).hexdigest())
print(json.dumps({'summary':data['test_summaries'],'master_unchanged':A['live_master_tip']['matches_expected'],'module_count_differences':differences,'prior_uganda_cases':prior},indent=2))
