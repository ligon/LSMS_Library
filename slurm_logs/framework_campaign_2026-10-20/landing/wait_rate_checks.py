import subprocess,json,time,sys,datetime
from pathlib import Path
root=Path('/local/job40255479/control/framework-campaign/landing')
items=json.loads((root/'rate-publication-update.json').read_text())
last=None
while True:
    result=[]
    for r in items:
        n,head=r['pr'],r['head']
        checks=json.loads(subprocess.check_output(['gh','api',f'repos/ligon/LSMS_Library/commits/{head}/check-runs?filter=latest&per_page=100'],text=True))['check_runs']
        latest={}
        for c in checks:
            if c['name'] in ['unit-tests','windows-smoke'] and (c['name'] not in latest or c['id']>latest[c['name']]['id']):latest[c['name']]=c
        states={k:{'status':v['status'],'conclusion':v['conclusion'],'id':v['id']} for k,v in latest.items()}
        result.append({'pr':n,'head':head,'checks':states,'ready':len(states)==2 and all(v['conclusion']=='success' for v in states.values())})
    encoded=json.dumps(result,sort_keys=True)
    if encoded!=last:print(encoded,flush=True);last=encoded
    if all(r['ready'] for r in result):
        (root/'rate-checks.json').write_text(json.dumps({'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'prs':result},indent=2)+'\n')
        break
    if any(v['conclusion'] in ['failure','timed_out','action_required'] for r in result for v in r['checks'].values()):sys.exit(1)
    time.sleep(45)
