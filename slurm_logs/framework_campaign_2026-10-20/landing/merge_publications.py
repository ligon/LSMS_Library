from pathlib import Path
import subprocess,json,datetime,sys
root=Path('/local/job40255479/control/framework-campaign/landing')
items=json.loads((root/(sys.argv[1] if len(sys.argv)>1 else 'publication-updates.json')).read_text())
for r in items:
    n,head=r['pr'],r['head']
    before=json.loads(subprocess.check_output(['gh','pr','view',str(n),'--repo','ligon/LSMS_Library','--json','state,isDraft,baseRefName,headRefOid'],text=True))
    assert before=={'state':'OPEN','isDraft':False,'baseRefName':'development','headRefOid':head},before
    checks=json.loads(subprocess.check_output(['gh','api',f'repos/ligon/LSMS_Library/commits/{head}/check-runs?filter=latest&per_page=100'],text=True))['check_runs']
    latest={}
    for c in checks:
        if c['name'] in ['unit-tests','windows-smoke'] and (c['name'] not in latest or c['id']>latest[c['name']]['id']):latest[c['name']]=c
    assert len(latest)==2 and all(c['conclusion']=='success' for c in latest.values())
    subprocess.run(['gh','pr','merge',str(n),'--repo','ligon/LSMS_Library','--merge','--match-head-commit',head,'--body-file',str(root/'merge-body.txt')],check=True)
    after=json.loads(subprocess.check_output(['gh','pr','view',str(n),'--repo','ligon/LSMS_Library','--json','state,headRefOid,mergeCommit,mergedAt,url'],text=True))
    assert after['state']=='MERGED' and after['headRefOid']==head
    with (root/'merges.jsonl').open('a') as f:f.write(json.dumps({'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pr':n,**after})+'\n')
    print(json.dumps({'pr':n,'state':after['state'],'merge':after['mergeCommit']['oid']}),flush=True)
