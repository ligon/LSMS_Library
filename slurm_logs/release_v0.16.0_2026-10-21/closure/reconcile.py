"""Read-only release closure analysis; imports no repository code."""
import ast
import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess

OUT=Path('/local/job40255479/control/release-v016/closure')
REPO='/local/job40255479/worktrees/framework-contracts'
DEV='acbd8eb34710e85d3bf66eb04f13dd0d971d1f3c'
CANDIDATE='532d58700fa46e52a0b390198604eca1dc4de00e'
BASE='cb44e3522a2426ba03431ca9a218cc63fe03f6c9'
PREFIX='slurm_logs/framework_campaign_2026-10-20/release-closure/'
def git(*args):return subprocess.check_output(['git',*args],cwd=REPO)
def ancestor(a,b):return subprocess.run(['git','merge-base','--is-ancestor',a,b],cwd=REPO,capture_output=True).returncode==0
def digest(s):return hashlib.sha256(s.encode()).hexdigest()
def save(name,obj): (OUT/name).write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n')
old=json.loads(git('show',DEV+':'+PREFIX+'closure-table.json'))
decisions=json.loads((OUT/'scope-decisions.json').read_text())
issues={int(p.stem.split('-')[1]):json.loads(p.read_text()) for p in (OUT/'private-issues').glob('*.json')}
prs={int(p.stem.split('-')[1]):json.loads(p.read_text()) for p in (OUT/'private-prs').glob('*.json')}
input_prs=json.loads(Path('/local/job40255479/control/release-v016/merged-prs.json').read_text())
actual=[p for p in input_prs if p.get('mergeCommit') and ancestor(p['mergeCommit']['oid'],DEV) and not ancestor(p['mergeCommit']['oid'],BASE)]
assert len(actual)==50 and set(prs)=={p['number'] for p in actual}
for p in actual:
 r=prs[p['number']]
 assert r['merged'] and r['state']=='closed' and r['merge_commit_sha']==p['mergeCommit']['oid']
 assert ancestor(r['merge_commit_sha'],DEV) and not ancestor(r['merge_commit_sha'],BASE)
assert git('rev-parse','v0.15.0^{}').decode().strip()==BASE and ancestor(BASE,DEV)
assert git('rev-parse','refs/remotes/origin/master').decode().strip()==BASE
assert git('rev-parse','refs/heads/development').decode().strip()==DEV

def evidence(n):
 d=issues[n];i=d['issue'];comments=d['comments']
 assert len(comments)==i['comments']
 return {'issue':n,'url':i['url'],'title':i['title'],'state':i['state'],
         'body_sha256':digest(i['body']),'comments_count':len(comments),
         'comment_evidence':[{'url':c.get('url',c.get('html_url')),'created_at':c.get('created_at'),'body_sha256':digest(c['body'])} for c in comments],
         'observed_at':d['observed_at'],'provider':d['fetch_provider']}
def prproof(n):
 p=prs[n]
 return {'number':n,'url':p['url'],'merged':p['merged'],'merged_at':p['merged_at'],
         'base':p['base'],'head_sha':p['head_sha'],'merge_commit_sha':p['merge_commit_sha'],
         'merge_in_dev':ancestor(p['merge_commit_sha'],DEV),'merge_in_v015':ancestor(p['merge_commit_sha'],BASE),
         'body_sha256':digest(p['body'])}
core=[]
for r in old['records']:
 n=r['issue'];e=evidence(n);assert e['state']=='open'
 e.update({'disposition':'safe closure under current ruled scope','scope':r['scope_and_evidence'],
           'supporting_prs':[prproof(p['number']) for p in r['supporting_prs']],
           'retained_residual_issues':r['retained_other_issue_residuals'],
           'body_unchanged_from_prior_audit':e['body_sha256']==r['current_body_sha256'],
           'comments_unchanged_from_prior_audit':{x['body_sha256'] for x in e['comment_evidence']}=={x['sha256'] for x in r['comment_evidence']},
           'prior_source_and_acceptance_audit':DEV+':'+PREFIX+'closure-table.json'})
 core.append(e)
assert len(core)==32 and all(x['comments_unchanged_from_prior_audit'] for x in core)
assert {x['issue'] for x in core if not x['body_unchanged_from_prior_audit']}=={1159,1160}

blob_cache={}
def source(path):
 if path not in blob_cache:
  data=git('show',DEV+':'+path)
  blob_cache[path]={'path':path,'blob_oid':git('rev-parse',DEV+':'+path).decode().strip(),
                    'sha256':hashlib.sha256(data).hexdigest(),'lines':len(data.splitlines())}
 return dict(blob_cache[path])
symbols={607:['food_unit_labels'],805:['_redecode_cp1252'],831:['_refuse_intree_source'],962:['_unit_sentinel_mask'],974:['_expand_kinship'],1008:['screen_8h'],1041:['food_acquired_for_wave'],1042:['_clean_freetext_unit','unrecorded_unit_to_count'],1047:['food_acquired_for_wave'],1113:['_coverage_skip_reason'],1140:['screen_8h']}
extra=[]
for key,spec in decisions['additional_proposals'].items():
 n=int(key);e=evidence(n);assert e['state']=='open';e.update(spec)
 e['disposition']='additional demonstrated closure proposal for parent review'
 e['supporting_pr']=prproof(spec['pr']);e['runtime_blob_proofs']=[source(p) for p in spec['runtime']]
 e['test_blob_proofs']=[source(p) for p in spec['tests']];e['test_anchors']=[];e['runtime_symbol_anchors']=[]
 for p in spec['tests']:
  for node in ast.walk(ast.parse(git('show',DEV+':'+p).decode())):
   if isinstance(node,ast.FunctionDef) and node.name.startswith('test_'):
    e['test_anchors'].append({'path':p,'line':node.lineno,'test':node.name})
 for p in spec['runtime']:
  if p.endswith('.py'):
   for node in ast.walk(ast.parse(git('show',DEV+':'+p).decode())):
    if isinstance(node,ast.FunctionDef) and node.name in symbols.get(n,[]):
     e['runtime_symbol_anchors'].append({'path':p,'line':node.lineno,'function':node.name})
 e['runtime_line_anchors']=list(e['runtime_symbol_anchors'])
 for p in spec['runtime']:
  needle={840:'community_prices:',1035:'Quantity_kg',1037:'to_datetime',1038:'##N/A##',1050:'coordonnes_gps__Latitude'}.get(n)
  if needle:
   matches=[i+1 for i,line in enumerate(git('show',DEV+':'+p).decode().splitlines()) if needle in line and not line.lstrip().startswith('#')]
   if matches:e['runtime_line_anchors'].append({'path':p,'line':matches[0],'selector':needle})
 e['source_reference_base']='https://github.com/ligon/LSMS_Library/blob/'+DEV+'/'
 e['validation_limit']='Static acceptance/source/test verification plus existing merged-PR and exact532 validation evidence; no fresh tests, survey reads or scientific-default reassessment.'
 extra.append(e)
assert len(extra)==16
safe=sorted(x['issue'] for x in core);proposed=sorted(x['issue'] for x in extra);combined=sorted(safe+proposed)
assert len(set(combined))==48
for name,ids in [('closing-keywords-32.txt',safe),('additional-closing-keywords-16.txt',proposed),('closing-keywords-48-proposed.txt',combined)]:
 (OUT/name).write_text('\n'.join('Closes #'+str(n) for n in ids)+'\n')
save('core-32.json',core);save('additional-16-proposals.json',extra)

closed=[evidence(n) for n in [953,958,960]];assert all(x['state']=='closed' for x in closed)
residuals=[]
for n,d in sorted(issues.items()):
 if n in combined or d['issue']['state']!='open' or '/pull/' in d['issue']['url']:continue
 e=evidence(n);e['remaining_scope']=decisions['primary_open_residuals'].get(str(n),d['issue']['title'])
 e['disposition']='retain OPEN; not a release closure';residuals.append(e)
assert {778,932,1044,1045,1066,1110,1149}.issubset({x['issue'] for x in residuals})
save('retained-residuals.json',residuals);save('already-closed.json',closed)

rx=re.compile(r'\b(close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s*:?\s+(?:(?:ligon/LSMS_Library)?#|https://github\.com/ligon/LSMS_Library/issues/)(\d+)\b',re.I)
raw=git('log',BASE+'..'+DEV,'--format=%H%x00%B%x00').decode().split('\x00');keywords=[]
for i in range(0,len(raw)-2,2):
 commit=raw[i].strip()
 for match in rx.finditer(raw[i+1]):
  n=int(match.group(2));keywords.append({'commit':commit,'number':n,'keyword':match.group(1),
    'classification':'verified closure' if n in combined else 'already merged pull request, not an issue' if n in [917,1028] else 'UNVERIFIED OUTSIDE CLOSURE LIST'})
assert all(x['classification']!='UNVERIFIED OUTSIDE CLOSURE LIST' for x in keywords)
save('commit-keyword-audit.json',{'commit_count':int(git('rev-list','--count',BASE+'..'+DEV)),
    'matches':keywords,'unverified_issue_keyword_count':0,'none_for_retained_778_932_1149_1110':True,
    'scope':'All full commit messages in exact v0.15.0..acbd ancestry, including merged side histories; conventional fix(#N) scopes are not closing keywords.'})

allprnums={p['number'] for p in input_prs};inventory=[]
for p in actual:
 n=p['number'];mentions=sorted(set(map(int,re.findall(r'#(\d+)',p['title']+'\n'+p['body'])))-allprnums)
 row=prproof(n);row['title']=p['title'];row['core_closures_supported']=[e['issue'] for e in core if any(x['number']==n for x in e['supporting_prs'])]
 row['additional_closures_proposed']=[e['issue'] for e in extra if e['pr']==n]
 row['mentions_not_promoted_to_closure']=[x for x in mentions if x not in combined]
 row['already_closed_related_issues']=[x for x in [953,958,960] if x in mentions]
 row['classification']=('scoped resolved implementation' if row['core_closures_supported'] or row['additional_closures_proposed'] else
                        'already-closed issue support' if row['already_closed_related_issues'] else
                        'CI fingerprint/evidence support, no separate issue closure' if n in [1153,1161] else
                        'release/evidence documentation only' if n in [1016,1158,1166] else 'partial implementation; umbrella/residual remains open')
 row['body_keyword_refs']=[int(m.group(2)) for m in rx.finditer(p['body'])]
 inventory.append(row)
git_pr_merges=[]
for line in git('log','--merges','--format=%H%x09%s',BASE+'..'+DEV).decode().splitlines():
 sha,title=line.split('\t',1);m=re.match(r'Merge pull request #(\d+)\b',title)
 if m:git_pr_merges.append({'number':int(m.group(1)),'merge_commit':sha})
assert {x['number'] for x in git_pr_merges}.issubset(set(prs)),git_pr_merges
nonstandard=sorted(set(prs)-{x['number'] for x in git_pr_merges})
assert nonstandard==[1153],nonstandard
save('actual-posttag-pr-inventory.json',{'count':50,'input_metadata_count':len(input_prs),
    'all_actual_posttag_git_PR_merge_numbers_covered':True,'git_PR_merge_records':git_pr_merges,
    'PRs_verified_by_GitHub_merge_SHA_ancestry_despite_nonstandard_merge_message':nonstandard,'records':inventory})

diff=git('diff','--name-status',CANDIDATE,DEV).decode().splitlines()
unexpected=[line for line in diff if not line.split('\t')[-1].startswith('slurm_logs/') and line.split('\t')[-1]!='.sucoder/handoff.org']
assert not unexpected,unexpected
save('static-identity.json',{'candidate':CANDIDATE,'development':DEV,'master_v015':BASE,
  'v015_is_development_ancestor':ancestor(BASE,DEV),'validated532_is_development_ancestor':ancestor(CANDIDATE,DEV),
  'all_common_runtime_package_config_test_script_tool_bin_bench_audit_workflow_build_dependency_blobs_modes_identical':True,
  'all_full_tree_differences':diff,'difference_path_count':len(diff),
  'only_changes_allowed':['slurm_logs/ evidence',' .sucoder/handoff.org'],
  'v015_tree':git('rev-parse',BASE+'^{tree}').decode().strip(),
  'development_tree':git('rev-parse',DEV+'^{tree}').decode().strip(),
  'scope':'Immutable Git objects only; no mutable source reads, repository imports, tests, data/cache APIs or build execution.'})
save('summary.json',{'status':'PASS scoped release closure audit','development':DEV,'master_v015':BASE,
    'prior_core32':safe,'additional16_proposed_for_parent_review':proposed,'combined48_if_parent_accepts_proposals':combined,
    'all32_currently_open':True,'unchanged_core_bodies':30,'updated_core_bodies':[1159,1160],
    'all32_comments_unchanged':True,'all_supporting_PRs_merged_posttag_in_development':True,
    'all_actual_posttag_PRs_covered':50,'fresh_issue_or_PR_records':len(issues),
    'connected_GitHub_issue_and_all_comment_fetches_complete':all(len(x['comments'])==x['issue']['comments'] for x in issues.values()),
    'retained_open_scope_count':len(residuals),'already_closed_no_new_keyword':[953,958,960],
    'unverified_commit_issue_closure_keywords':0,'runtime_identity_to_validated532':True,
    'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'actions':'Read-only connected GitHub/metadata GETs and immutable Git objects; only private report files written; no tests/build/data/catalog/cache/GitHub/repository mutation.'})
print(json.dumps({'status':'PASS','core':32,'additional_proposals':16,'combined_if_accepted':48,
                  'actual_posttag_PRs':50,'retained_open_scopes':len(residuals),
                  'runtime_identity_to532':True,'unverified_commit_issue_keywords':0},sort_keys=True))
