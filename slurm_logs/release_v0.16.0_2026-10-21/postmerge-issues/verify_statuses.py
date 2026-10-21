"""Read-only status/event GraphQL projection; no issue body/comment fields."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess

ROOT=Path('/local/job40255479/control/release-v016/postmerge-issues')
INPUT=Path('/local/job40255479/control/release-v016/closure')
EXPECTED_MASTER='097283459f80016b0f00a51b814a0cb964a42789'
RELEASE_URL='https://github.com/ligon/LSMS_Library/pull/1168'
accepted=sorted(int(line.split('#')[1]) for line in (INPUT/'closing-keywords-48.txt').read_text().splitlines())
retained=sorted(item['issue'] for item in json.loads((INPUT/'retained-residuals.json').read_text()))
assert len(accepted)==48 and len(retained)==77 and not set(accepted)&set(retained)
selection='number url state stateReason closedAt updatedAt timelineItems(last:5,itemTypes:[CLOSED_EVENT,REOPENED_EVENT]){nodes{__typename ... on ClosedEvent{createdAt closer{__typename ... on PullRequest{number url mergedAt mergeCommit{oid}} ... on Commit{oid url}}} ... on ReopenedEvent{createdAt}}}'
records=[];release=None;default=None
ids=sorted(accepted+retained)
for offset in range(0,len(ids),25):
    aliases='\n'.join('i'+str(n)+':issue(number:'+str(n)+'){'+selection+'}' for n in ids[offset:offset+25])
    header='release:pullRequest(number:1168){number url state mergedAt baseRefName headRefName mergeCommit{oid}} defaultBranchRef{name target{oid}}' if offset==0 else ''
    query='query{repository(owner:"ligon",name:"LSMS_Library"){'+header+'\n'+aliases+'}}'
    qfile=ROOT/f'query-{offset//25+1}.graphql';qfile.write_text(query+'\n')
    proc=subprocess.run(['gh','api','graphql','-F','query=@'+str(qfile)],capture_output=True)
    if proc.returncode:
        raise RuntimeError('Status-only GraphQL request failed; no body/comment fallback performed')
    response=json.loads(proc.stdout)
    if response.get('errors'):
        raise RuntimeError(json.dumps(response['errors']))
    repo=response['data']['repository']
    if offset==0:release=repo.pop('release');default=repo.pop('defaultBranchRef')
    for key,value in repo.items():
        assert value is not None,key
        n=value['number'];value['expected_state']='CLOSED' if n in accepted else 'OPEN'
        value['matches_expected']=value['state']==value['expected_state']
        closed=[event for event in value['timelineItems']['nodes'] if event['__typename']=='ClosedEvent']
        latest=closed[-1] if closed else None
        closer=latest.get('closer') if latest else None
        value['latest_close_event']=latest
        value['closure_linked_to_release1168']=bool(closer and (closer.get('number')==1168 or closer.get('oid')==EXPECTED_MASTER))
        records.append(value)
assert len(records)==125 and {r['number'] for r in records}==set(ids)
missing=[r['number'] for r in records if r['expected_state']=='CLOSED' and not r['matches_expected']]
unexpected=[r['number'] for r in records if r['expected_state']=='OPEN' and not r['matches_expected']]
assert release['state']=='MERGED' and release['mergeCommit']['oid']==EXPECTED_MASTER
assert default['name']=='master' and default['target']['oid']==EXPECTED_MASTER
closed_records=[r for r in records if r['expected_state']=='CLOSED']
summary={'status':'PASS' if not missing and not unexpected else 'MISMATCH',
         'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
         'release_pr':release,'default_branch':default,'accepted_expected_closed':48,
         'accepted_actually_closed':sum(r['state']=='CLOSED' for r in closed_records),
         'retained_expected_open':77,'retained_actually_open':sum(r['state']=='OPEN' for r in records if r['expected_state']=='OPEN'),
         'missing_closures':missing,'unexpected_retained_closures':unexpected,
         'accepted_with_release1168_close_link':sum(r['closure_linked_to_release1168'] for r in closed_records),
         'accepted_without_release1168_close_link':[r['number'] for r in closed_records if not r['closure_linked_to_release1168']],
         'closure_timestamps':sorted({r['closedAt'] for r in closed_records}),
         'fields_fetched':'Only issue status/reason/timestamps/URLs and close/reopen metadata; no bodies, titles or comments',
         'provider':'GET-equivalent read-only gh GraphQL query projection; connected issue tools cannot exclude bodies',
         'mutations':'Private report/query files only; no repository/GitHub/source/data/cache mutation',
         'limits':'Snapshot of issue states after release PR1168; CI/release/tag/publication are not assessed here'}
(ROOT/'verification.json').write_text(json.dumps({'summary':summary,'records':sorted(records,key=lambda r:r['number'])},indent=2,sort_keys=True)+'\n')
lines=['#+TITLE: Post-release-merge issue-state verification','#+AUTHOR: Sue','','* Result',summary['status']+' after release PR1168 merged to master ='+EXPECTED_MASTER+'=.','All 48 accepted issues are CLOSED; all 77 retained referenced issues are OPEN.' if summary['status']=='PASS' else 'A status mismatch requires parent inspection.','Missing closures: '+str(missing)+'. Unexpected retained closures: '+str(unexpected)+'.',
       'Accepted closures with an explicit closer link to PR1168/master merge: '+str(summary['accepted_with_release1168_close_link'])+'.',
       'Closure timestamps: ='+', '.join(summary['closure_timestamps'])+'=.',
       'Exact per-issue state, reason, closure timestamp, close/reopen event and closer URL are retained in verification.json.',
       '', '* Scope and reproduction',
       'The status-only GraphQL selections do not request body, title, comments or survey fields.',
       'The connected issue tools always fetch bodies, so this read-only field-projected CLI fallback honors the narrower task scope.',
       'Run verify_statuses.py with the mounted image Python; it reads the frozen closure exports and performs query-only GitHub requests.',
       'Later lifecycle changes need a separately named snapshot; do not overwrite this frozen observation.',
       'This result does not assess CI39604418333, create a release/tag, or establish publication status.',
       'No source, repository, GitHub, credentials or cache state was changed.', '', '-- Sue']
(ROOT/'REPORT.org').write_text('\n'.join(lines)+'\n')
files=['REPORT.org','verification.json','verify_statuses.py']+[f'query-{n}.graphql' for n in range(1,6)]+['COPYLIST.txt','MANIFEST.json']
(ROOT/'COPYLIST.txt').write_text('\n'.join(files)+'\n')
manifest={name:{'bytes':(ROOT/name).stat().st_size,'sha256':hashlib.sha256((ROOT/name).read_bytes()).hexdigest()} for name in files if name!='MANIFEST.json'}
(ROOT/'MANIFEST.json').write_text(json.dumps({'safe_status_only_files':manifest,'body_or_comment_fields_requested':False},indent=2,sort_keys=True)+'\n')
print(json.dumps(summary,sort_keys=True))
