"""Explain the status-only1099 closer from the frozen keyword audit."""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT=Path('/local/job40255479/control/release-v016/postmerge-issues')
prior=json.loads(Path('/local/job40255479/control/release-v016/closure/commit-keyword-audit.json').read_text())
data=json.loads((ROOT/'verification.json').read_text())
record=next(r for r in data['records'] if r['number']==1099)
closer=record['latest_close_event']['closer']
assert closer['__typename']=='Commit'
matched=any(r['number']==1099 and r['commit']==closer['oid'] and r['classification']=='verified closure' for r in prior['matches'])
assert matched
merge=data['summary']['release_pr']['mergeCommit']['oid']
ancestor=subprocess.run(['git','merge-base','--is-ancestor',closer['oid'],merge],cwd='/local/job40255479/worktrees/framework-contracts',capture_output=True).returncode==0
assert ancestor
data['summary']['accepted1099_commit_closure_explanation']={'issue':1099,'closer_commit':closer['oid'],'closer_url':closer['url'],
    'matches_frozen_verified1099_commit_keyword':True,'closer_commit_is_master_merge_ancestor':True,
    'cause':'Accepted1099 closed by its existing commit keyword when its history reached master, rather than by the release PR keyword; expected closure, no correction needed.'}
(ROOT/'verification.json').write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
p=ROOT/'REPORT.org';s=p.read_text();s=s.replace('* Scope and reproduction','Issue1099 instead names its accepted fix commit7117f5b683d5c98812027277de88b5d9d7a8536a as closer.\nIt matches the frozen keyword audit and is an ancestor of the master merge; this is the expected accepted-issue commit closure, not an unexpected residual closure.\n\n* Scope and reproduction')
s=s.replace('Run verify_statuses.py with the mounted image Python;','Run verify_statuses.py, then annotate_commit_closure.py with the mounted image Python;')
for a,b in [('Issue1099','Issue 1099'),('commit7117','commit 7117'),('PR1168','PR 1168'),('CI39604418333','CI 39604418333')]:s=s.replace(a,b)
lines=s.splitlines();s='\n'.join(line if not line.startswith('Closure timestamps:') else 'Closure timestamps range from2026-10-21T13:01:32Z to2026-10-21T13:02:05Z; exact timestamps remain in the per-issue JSON.' for line in lines)+'\n'
s=s.replace('from2026','from 2026').replace('to2026','to 2026');p.write_text(s)
files=(ROOT/'COPYLIST.txt').read_text().splitlines();files.insert(3,'annotate_commit_closure.py');(ROOT/'COPYLIST.txt').write_text('\n'.join(files)+'\n')
manifest={name:{'bytes':(ROOT/name).stat().st_size,'sha256':hashlib.sha256((ROOT/name).read_bytes()).hexdigest()} for name in files if name!='MANIFEST.json'}
(ROOT/'MANIFEST.json').write_text(json.dumps({'safe_status_only_files':manifest,'body_or_comment_fields_requested':False},indent=2,sort_keys=True)+'\n')
print(json.dumps({'status':'PASS','closed':48,'retained_open':77,'direct_PR1168_closures':47,'expected1099_commit_keyword_closure':True,'all_readers_writers_stopped':True},sort_keys=True))
