"""Render aggregate-only master CI state, including live branch identity."""
from pathlib import Path
import json,re
P=Path(__file__).resolve().parent
A=json.loads((P/'aggregate-results.json').read_text());E=json.loads((P/'expected-native.json').read_text());I=json.loads((P/'source-identity.json').read_text());T=A['live_master_tip']
assert A['source_sha']==E['source']==I['master_sha']
rows=[];native={}
for j in A['jobs']:
 summary='; '.join(x['summary']for x in j.get('test_summaries',[]))or'; '.join(j.get('native_results',[]))or('Skipped by condition'if j['conclusion']=='skipped'else'No pytest summary'if j['status']=='completed'else'Pending')
 rows.append(f"| [{j['name']}]({j['url']}) | {j['conclusion']or j['status']} | {summary} |")
 if j['name']=='windows-script-build'and'log_sha256'in j:
  native={'job_id':j['databaseId'],'source_sha':A['source_sha'],'conclusion':j['conclusion'],'expected':E['tables'],'actual':{}}
  for value in j.get('native_results',[]):
   m=re.match(r'(Uganda:\w+) rows=(\d+) cols=(\d+) waves=(\d+) digest=([0-9a-f]+)',value)
   if m:native['actual'][m[1]]={'rows':int(m[2]),'columns':int(m[3]),'waves':int(m[4]),'digest':m[5]}
  native['matches_expected']=all(all(native['actual'].get(k,{}).get(field)==value for field,value in fields.items())for k,fields in E['tables'].items())
  log=(P/f"{A['run_id']}-windows-script-build.log").read_text(errors='replace')
  native['installed_code_and_config_assertions_reached']='installed code and configs:'in log
  m=re.search(r'MAKEVARS Uganda parquet-list=(\d+) find-errors=(\d+)',log)
  native['make_prerequisite_check']={'parquet_list':int(m[1]),'find_errors':int(m[2])}if m else None
  native['mismatch_or_raised_diagnostic']=bool(re.search(r'(?m)^\d{4}-.*?\b(?:MISMATCH|RAISED) Uganda:',log))
if native:(P/'native-validation.json').write_text(json.dumps(native,indent=2)+'\n')
report=f'''SCOPE DEVIATIONS

None. Read-only Actions/master-ref monitoring and local source identity inspection. No dispatch, cancellation, GitHub/ref/source/cache mutation or publication action.

Master push CI [{A['run_id']}]({A['url']}) at exact source `{A['source_sha']}` is **{A['conclusion']or A['status']}** as observed {A['observed_utc']}. Pending/skipped jobs are not passes.

Live GitHub master at {T['observed_utc']}: `{T['master_sha']}`. Matches expected merge source: **{T['matches_expected']}**. Master tip ever changed during this monitor: **{A['master_tip_ever_changed']}**. A change is retained in master-tip-change.json/history and must be reviewed before release publication; success for an older CI source does not silently approve a new tip.

| Job | State | Actual terminal result |
|---|---|---|
{chr(10).join(rows)}

The source is the two-parent PR999 merge commit. Its full tree equals reviewed final-docs2e41feda3; runtime/config excluding CONTENTS, tests, workflow, dependencies and release recipe equal successfully validated3914d8804. See source-identity.json for exact parents/tree.

Master push additionally enables documentation and DVC-lock checks. Any lock-refresh/master advance is monitored explicitly. The local make-release process is owned by the parent and is separate; this CI report does not claim its result or a tag/publication.

Completed-job raw logs remain private and may contain survey warnings. Aggregate metadata records exact job/source identities, log sizes/SHA256 and actual summaries. No in-progress log fetches. The monitor waits for two terminal metadata snapshots, captures each completed log or records three failed attempts, then stops; inspect monitor-finished.json when present. Native expected-output comparison is recorded in native-validation.json when its terminal log is available.
'''
(P/'REPORT.md').write_text(report)
print(A['observed_utc'],A['status'],A['conclusion'],'master_matches',T['matches_expected'])
if native:print(json.dumps(native))
