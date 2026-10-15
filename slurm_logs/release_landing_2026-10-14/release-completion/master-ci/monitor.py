"""Read-only exact master CI monitor with live branch-tip tracking; not dispatched here."""
from pathlib import Path
import datetime,hashlib,json,re,subprocess,sys,time
OUT=Path(__file__).resolve().parent
RUN=sys.argv[1];EXPECTED=sys.argv[2];REPO='ligon/LSMS_Library'
ANSI=re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
failures=0
attempts={}
last_master_tip=None
terminal_observations=0
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def call(*args):return subprocess.run(['gh',*args],capture_output=True,timeout=120)
def parse(log):
 raw=log.read_bytes();content=ANSI.sub('',raw.decode(errors='replace'));summaries=[];failed=[];native=[]
 for line in content.splitlines():
  clean=re.sub(r'^\d{4}-\d\d-\d\dT\S+\s+','',line)
  if re.search(r'\b\d+ (?:passed|failed|skipped|xfailed|xpassed)\b.*\bin \d',clean):
   secs=re.search(r'\bin ([\d.]+)s',clean)
   summaries.append({'summary':clean.strip('= '),'counts':{k:int(n)for n,k in re.findall(r'(\d+) (passed|failed|skipped|xfailed|xpassed|errors?|warnings?)',clean)},'pytest_seconds':float(secs[1])if secs else None})
  match=re.match(r'(FAILED|ERROR) (tests/\S+)',clean)
  if match:failed.append({'outcome':match[1],'node_id':match[2]})
  if 'RESULT Uganda:' in clean:native.append(clean.split('RESULT ',1)[1])
 return {'log_bytes':len(raw),'log_sha256':hashlib.sha256(raw).hexdigest(),'full_source_sha_present':EXPECTED in content,'test_summaries':summaries,'failed_nodes':failed,'native_results':native}
while True:
 observed=now()
 try:
  response=call('run','view',RUN,'--repo',REPO,'--json','databaseId,headSha,headBranch,status,conclusion,createdAt,updatedAt,jobs,url')
  if response.returncode:raise RuntimeError(response.stderr.decode(errors='replace'))
  data=json.loads(response.stdout)
  if data['headSha']!=EXPECTED:raise ValueError('Run source SHA does not match requested exact head')
  (OUT/f'{RUN}.json').write_bytes(response.stdout);jobs=[]
  tip_response=call('api',f'repos/{REPO}/git/ref/heads/master')
  if tip_response.returncode:raise RuntimeError('Could not verify live master tip: '+tip_response.stderr.decode(errors='replace'))
  master_tip=json.loads(tip_response.stdout)['object']['sha']
  tip_record={'observed_utc':now(),'expected_source_sha':EXPECTED,'master_sha':master_tip,'matches_expected':master_tip==EXPECTED}
  (OUT/'master-tip.json').write_text(json.dumps(tip_record,indent=2)+'\n')
  if master_tip!=last_master_tip:
   with (OUT/'master-tip-history.jsonl').open('a')as stream:stream.write(json.dumps(tip_record)+'\n')
   if master_tip!=EXPECTED:
    (OUT/'master-tip-change.json').write_text(json.dumps(tip_record,indent=2)+'\n')
    comparison=call('api',f'repos/{REPO}/compare/{EXPECTED}...{master_tip}')
    if comparison.returncode==0:
     details=json.loads(comparison.stdout)
     concise={k:details.get(k)for k in('status','ahead_by','behind_by','total_commits','html_url')}
     concise['commits']=[{'sha':c['sha'],'message':c['commit']['message'],'html_url':c['html_url']}for c in details.get('commits',[])]
     (OUT/f'master-change-{master_tip}.json').write_text(json.dumps(concise,indent=2)+'\n')
    print('MASTER TIP CHANGED',json.dumps(tip_record),flush=True)
   last_master_tip=master_tip
  for job in data['jobs']:
   row={k:job[k]for k in('name','databaseId','status','conclusion','startedAt','completedAt','url')};row['source_sha']=EXPECTED
   row['active_steps']=[{'name':s['name'],'startedAt':s['startedAt']}for s in job['steps']if s['status']=='in_progress']
   log=OUT/f'{RUN}-{job["name"]}.log';ident=job['databaseId']
   if job['status']=='completed'and job['conclusion']!='skipped'and not log.exists()and attempts.get(ident,0)<3:
    attempts[ident]=attempts.get(ident,0)+1
    response=call('api','--allow-escape-sequences',f'repos/{REPO}/actions/jobs/{ident}/logs')
    (OUT/f'{ident}-fetch.json').write_text(json.dumps({'observed_utc':now(),'attempt':attempts[ident],'exit_code':response.returncode,'stderr':response.stderr.decode(errors='replace')},indent=2)+'\n')
    if response.returncode==0:log.write_bytes(response.stdout)
    elif attempts[ident]==3:
     details=call('api',f'repos/{REPO}/actions/jobs/{ident}')
     if details.returncode==0:
      jobdata=json.loads(details.stdout);(OUT/f'{ident}-terminal.json').write_bytes(details.stdout)
      url=jobdata.get('check_run_url')
      if url:
       annotations=call('api',url+'/annotations')
       if annotations.returncode==0:(OUT/f'{ident}-annotations.json').write_bytes(annotations.stdout)
   row['log_capture_attempts']=attempts.get(ident,0)
   if log.exists():row.update(parse(log))
   jobs.append(row)
  aggregate={'observed_utc':observed,'run_id':int(RUN),'source_sha':EXPECTED,'status':data['status'],'conclusion':data['conclusion'],'url':data['url'],'jobs':jobs,'live_master_tip':tip_record,'master_tip_ever_changed':(OUT/'master-tip-change.json').exists()}
  (OUT/'aggregate-results.json').write_text(json.dumps(aggregate,indent=2)+'\n')
  failures=0
  terminal=bool(jobs)and data['status']=='completed'and all(j['status']=='completed'for j in jobs)
  missing=[j for j in jobs if j['status']=='completed'and j['conclusion']!='skipped'and 'log_sha256'not in j]
  print(observed,[(j['name'],j['status'],j['conclusion'],'log_sha256'in j)for j in jobs],flush=True)
  terminal_observations=terminal_observations+1 if terminal else 0
  if terminal_observations>=2 and all(j['log_capture_attempts']>=3 for j in missing):
   (OUT/'monitor-finished.json').write_text(json.dumps({'finished_utc':now(),'run_id':int(RUN),'source_sha':EXPECTED,'reason':'all jobs terminal; each completed log captured or three attempts exhausted','missing_log_jobs':[j['databaseId']for j in missing],'semantic_review_required':True,'live_master_tip':tip_record,'master_tip_ever_changed':(OUT/'master-tip-change.json').exists(),'terminal_metadata_observations':terminal_observations},indent=2)+'\n');break
 except ValueError:
  raise
 except Exception as exc:
  failures+=1;(OUT/'capture-error.json').write_text(json.dumps({'observed_utc':now(),'consecutive_failures':failures,'error':str(exc)},indent=2)+'\n')
  if failures>=10:raise
 time.sleep(30)
