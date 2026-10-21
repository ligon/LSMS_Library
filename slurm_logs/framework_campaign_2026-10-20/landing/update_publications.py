from pathlib import Path
import subprocess,json,datetime
ROOT=Path('/local/job40255479/control/framework-campaign/landing')
REPO=Path('/local/job40255479/mirrors/LSMS_Library')
BASE='852ac2bfcd5f798533f56e7efa1f77db80c0138f'
DEV='09a413ab4f8e8189980b4c83cdcf28037158e8b1'
ITEMS=[(1162,'review/778-cache-race-20261020','f7eb194d7a54176ad44dfdeb6529e6b4b1715aeb'),(1163,'review/1115-1128-provenance-20261020','44f6cd2e7d3665622d2119fe4725e5a7604a56fa'),(1164,'review/1159-pooled-reducers-20261020','45f80dcb50da189b7864034b1d92863dca68de09')]
def call(args,cwd=REPO):
    r=subprocess.run(args,cwd=cwd,text=True,capture_output=True)
    if r.returncode:raise RuntimeError(f'{args}: {r.stdout}\n{r.stderr}')
    return r.stdout
records=[]
for n,branch,old in ITEMS:
    state=json.loads(call(['gh','pr','view',str(n),'--repo','ligon/LSMS_Library','--json','headRefOid,headRefName,isDraft,state']))
    assert state['headRefOid']==old and state['headRefName']==branch and state['isDraft'] and state['state']=='OPEN'
    w=Path('/local/job40255479/worktrees')/f'land-{n}'
    if not w.exists():call(['git','worktree','add',str(w),branch])
    assert call(['git','status','--porcelain'],w)==''
    expected=call(['git','diff','--name-only',BASE,old],w).splitlines()
    current=call(['git','rev-parse','HEAD'],w).strip()
    if current==old:
        call(['git','merge','--no-ff','--no-commit',DEV],w)
        assert not call(['git','diff','--name-only','--diff-filter=U'],w)
        actual=call(['git','diff','--cached','--name-only',DEV],w).splitlines()
        prefix=':'
    else:
        assert call(['git','show','-s','--format=%P','HEAD'],w).strip().split()==[old,DEV]
        actual=call(['git','diff','--name-only',DEV,'HEAD'],w).splitlines()
        prefix='HEAD:'
    assert actual==expected,{'pr':n,'expected':expected,'actual':actual}
    blobs=[]
    for path in expected:
        want=call(['git','rev-parse',f'{old}:{path}'],w).strip()
        got=call(['git','rev-parse',f'{prefix}{path}'],w).strip()
        assert got==want,(n,path)
        blobs.append({'path':path,'reviewed_blob':want,'merged_blob':got})
    if current==old:
        message=ROOT/f'pr-{n}-merge-development.txt'
        message.write_text(f'Merge development into reviewed publication for PR #{n}\n\nRetain every reviewed task blob unchanged and establish development ancestry before retargeting. The maintainer authorized batch landing after exact532 validation.\n\nCo-Authored-By: GPT-6 <noreply@openai.com>\n')
        result=subprocess.run(['git','commit','-F',str(message)],cwd=w,text=True,capture_output=True,check=True)
        assert 'SUCODER: published' in result.stdout+result.stderr,result.stdout+result.stderr
    new=call(['git','rev-parse','HEAD'],w).strip()
    assert call(['git','ls-remote','origin',f'refs/heads/{branch}'],w).split()[0]==new
    call(['git','merge-base','--is-ancestor',DEV,new],w)
    call(['git','merge-base','--is-ancestor',old,new],w)
    call(['git','push','https://github.com/ligon/LSMS_Library.git',f'HEAD:{branch}'],w)
    call(['gh','pr','edit',str(n),'--repo','ligon/LSMS_Library','--base','development'])
    files=json.loads(call(['gh','api',f'repos/ligon/LSMS_Library/pulls/{n}/files?per_page=100']))
    assert sorted(f['filename'] for f in files)==sorted(expected)
    for f in files:assert f['sha']==call(['git','rev-parse',f'{new}:{f["filename"]}'],w).strip()
    call(['gh','pr','ready',str(n),'--repo','ligon/LSMS_Library'])
    record={'pr':n,'old_head':old,'head':new,'base':'development','development_ancestor':DEV,'worktree':str(w),'scope_count':len(expected),'all_reviewed_blobs_preserved':True,'github_diff_verified':True,'blobs':blobs,'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    records.append(record)
    (ROOT/'publication-updates.json').write_text(json.dumps(records,indent=2)+'\n')
    print(json.dumps({k:record[k] for k in ['pr','head','scope_count','all_reviewed_blobs_preserved','github_diff_verified']}),flush=True)
