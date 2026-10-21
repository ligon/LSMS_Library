"""Read-only GitHub reconciliation; writes only this private report directory."""
from pathlib import Path
import concurrent.futures,json,subprocess,datetime
import argparse
parser=argparse.ArgumentParser()
parser.add_argument('--out',type=Path,required=True)
args=parser.parse_args()
out=args.out.resolve()
assert out.is_relative_to(Path('/local')), out
out.mkdir(parents=True,exist_ok=True)
prior=Path('/local/job40255479/worktrees/framework-reliability/slurm_logs/framework_campaign_2026-10-20/reliability')
ready=json.loads((prior/'v016-readiness.json').read_text())
issues=sorted({r['issue'] for r in ready['release_issue_candidates']}|{1115,1128,1159,1160,778,932})
merged=json.loads((prior/'merged-prs.json').read_text())
recent=[r for r in merged if r['mergedAt']>'2026-10-15T15:37:21Z']
(out/'prior-merged-since-tag-time.json').write_text(json.dumps(recent,indent=2)+'\n')
prs=sorted({p for r in ready['release_issue_candidates'] for p in r['prs']}|{r['number'] for r in recent}|set(range(1154,1158))|set(range(1161,1166)))
def gh(path):
 return json.loads(subprocess.check_output(['gh','api',path],text=True))
def item_issue(n):
 data=gh(f'repos/ligon/LSMS_Library/issues/{n}')
 pages=json.loads(subprocess.check_output(['gh','api',f'repos/ligon/LSMS_Library/issues/{n}/comments','--paginate','--slurp'],text=True))
 comments=[c for page in pages for c in page]
 data['full_comments']=comments
 (out/f'issue-{n}.json').write_text(json.dumps(data,indent=2)+'\n')
 return n,len(comments)
def item_pr(n):
 data=gh(f'repos/ligon/LSMS_Library/pulls/{n}')
 (out/f'pr-{n}.json').write_text(json.dumps(data,indent=2)+'\n')
 return n,data['state'],data['merge_commit_sha']
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
 print('issues',list(pool.map(item_issue,issues)),flush=True)
 print('prs',list(pool.map(item_pr,prs)),flush=True)
for suffix in ['heads/development','heads/master','tags/v0.15.0']:
 d=gh('repos/ligon/LSMS_Library/git/ref/'+suffix)
 (out/('ref-'+suffix.replace('/','-')+'.json')).write_text(json.dumps(d,indent=2)+'\n')
 if suffix.startswith('tags') and d['object']['type']=='tag':
  tag=gh('repos/ligon/LSMS_Library/git/tags/'+d['object']['sha'])
  (out/'annotated-tag.json').write_text(json.dumps(tag,indent=2)+'\n')
for name,path in [('ruleset-development','repos/ligon/LSMS_Library/rulesets/25124452'),('pr-1153','repos/ligon/LSMS_Library/pulls/1153'),('candidate-ci','repos/ligon/LSMS_Library/actions/runs/39533863014')]:
 (out/(name+'.json')).write_text(json.dumps(gh(path),indent=2)+'\n')
(out/'observed.json').write_text(json.dumps({'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'issues':issues,'prs':prs,'recent_inventory_count':len(recent)},indent=2)+'\n')
