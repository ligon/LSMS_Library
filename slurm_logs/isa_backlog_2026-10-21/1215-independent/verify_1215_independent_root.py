"""Root's stdlib-only authentication; never decode frames or reopen six historical PKLs."""
from pathlib import Path
import collections
import hashlib
import json
import os
import stat
import subprocess
import sys
import time

assert sys.flags.no_site and sys.dont_write_bytecode
C=Path('/local/job40255479/control/isa-backlog-20261021')
P=C/'review-Uganda-1215';T=C/'review-Uganda-1215-terminal-records'
PIN={'READY.json':'dd8d1b6c7fc97cd01276abfaeb1e2c55b5505ea2f25b62b13f15af36e7e061b4',
 'final-private-manifest.json':'5b349ae255827c9c1a5e8d8f553acacd617b17d888fca9e492d0467ff62eff47',
 'safe-exports/ALLOWLIST.json':'544dc5d8491ea3a7b299077092951c7b6432e847628c38cba57ab636397e84fa'}
H={'slurm_logs/2026-06-16_ehcvm_interview_date/'+x+'.pkl' for x in ['malawi_baseline','malawi_retrofit','malawi_retrofit2']}
def load(q):return json.loads(Path(q).read_text())
def digest(q):
 h=hashlib.sha256()
 with Path(q).open('rb') as f:
  for b in iter(lambda:f.read(2**20),b''):h.update(b)
 return h.hexdigest()
def physical(q):
 s=Path(q).lstat();return {'kind':'symlink' if stat.S_ISLNK(s.st_mode) else 'directory' if stat.S_ISDIR(s.st_mode) else 'file',
 'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns,'nlink':s.st_nlink,'device':s.st_dev,'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid}
def receipt(q):
 q=Path(q);r=physical(q)
 if r['kind']=='file':assert r['nlink']==1;r.update(bytes=q.lstat().st_size,sha256=digest(q))
 elif r['kind']=='symlink':
  target=os.readlink(q);b=os.fsencode(target);r.update(target=target,bytes=len(b),sha256=hashlib.sha256(b).hexdigest(),hash_semantics='link-text')
 assert physical(q)=={k:v for k,v in r.items() if k in physical(q)}
 return r
for n,h in PIN.items():assert digest(P/n)==h,n
r=load(P/'READY.json');m=load(P/'final-private-manifest.json');counts=collections.Counter()
assert m['exact_control_exclusions']==['READY.json','final-private-manifest.json']
actual={'.'}
for root,dirs,files in os.walk(P,followlinks=False):
 for n in dirs+files:actual.add(str((Path(root)/n).relative_to(P)))
assert actual==set(m['entries'])|set(m['exact_control_exclusions'])
for n,row in m['entries'].items():
 assert n=='.' or (not Path(n).is_absolute() and '..' not in Path(n).parts)
 assert receipt(P/n)==row,('packet',n);counts[row['kind']]+=1
assert counts=={'file':1657,'symlink':46,'directory':1145}
assert sum(v.get('bytes',0) for v in m['entries'].values() if v['kind']=='file')==545526967
assert receipt(P/'final-private-manifest.json')==r['manifest_receipt']
allow=load(P/'safe-exports/ALLOWLIST.json');assert set(allow['files'])=={'REPORT.md','PROOF.json','QUALIFICATIONS.md','source.diff'}
for n,v in allow['files'].items():assert receipt(P/'safe-exports'/n)==v
assert allow['private_identifier_union_checked']==49
privacy=load(P/'safe-export-privacy-private.json');assert privacy['all_four_full_byte_scans'] and privacy['private_tokens']==49

src=load(P/'source-before.json');exception=load(P/'historical-tracked-binary-initial-source-snapshot-qualification-v3.json')
assert digest(P/'historical-tracked-binary-initial-source-snapshot-qualification-v3.json')=='31c7198f1fce6135748bb79aeeaba9ca339ddd80a9d83a0e282d2bedb1dfaf52'
fields=['st_dev','st_ino','st_size','st_mode','st_mtime_ns','st_nlink','st_uid','st_gid'];sc=collections.Counter()
env={**os.environ,'GIT_OPTIONAL_LOCKS':'0'}
def git(root,*args):return subprocess.check_output(['git','--no-optional-locks',*args],cwd=root,env=env,text=True)
for root,base in src['sources'].items():
 assert git(root,'rev-parse','HEAD').strip()==base['head']
 assert git(root,'status','--porcelain=v1','--untracked-files=all')==''
 assert set(git(root,'ls-files','-z').split('\0')[:-1])==set(base['tracked'])
 assert {n for n in base['tracked'] if Path(n).suffix=='.pkl'}==H
 for n,row in base['tracked'].items():
  q=Path(root)/n;s=q.lstat();got={k:getattr(s,k) for k in fields};got['kind']=row['kind']
  if row['kind']=='symlink':got['target']=os.readlink(q);sc['links']+=1
  else:
   assert stat.S_ISREG(s.st_mode) and s.st_nlink==1
   if n in H:
    sc['historical_metadata_only']+=1
    assert next(v for v in exception['per_head'][root]['rows'] if v['relative']==n)['saved_original_full_receipt']==row
   else:got['sha256']=digest(q);sc['regular_fresh_hashes']+=1
  assert got=={k:v for k,v in row.items() if n not in H or k!='sha256'},('source',root,n)
 blobs=git(root,'ls-files','-s','--',*sorted(H))
 for line in blobs.splitlines():
  descriptor,n=line.split('\t',1);mode,blob,stage=descriptor.split()
  assert {'mode':mode,'blob':blob,'stage':stage}==next(v for v in exception['per_head'][root]['rows'] if v['relative']==n)['current_git_index_identity']
assert sc=={'links':94,'historical_metadata_only':6,'regular_fresh_hashes':29727}
staged=load(P/'genuine-staged-copies.json');seen=set()
for row in staged['providers']+staged['copies']:
 q=Path(row.get('source',row.get('path')));v=row['receipt'];s=q.lstat();assert not q.is_symlink() and stat.S_ISREG(s.st_mode) and s.st_nlink==1
 for a,b in [('bytes','st_size'),('mode',None),('mtime_ns','st_mtime_ns'),('nlink','st_nlink'),('device','st_dev'),('inode','st_ino')]:assert (stat.S_IMODE(s.st_mode) if b is None else getattr(s,b))==v[a]
 assert digest(q)==v['sha256'];md=hashlib.md5()
 with q.open('rb') as f:
  first=f.read(32);md.update(first)
  for b in iter(lambda:f.read(2**20),b''):md.update(b)
 assert md.hexdigest()==v['md5'] and (first.startswith(b'<stata_dta>') or first[0] in (104,105,108,110,111,113,114,115))
 assert (s.st_dev,s.st_ino) not in seen;seen.add((s.st_dev,s.st_ino))
 assert physical(q)['mtime_ns']==s.st_mtime_ns
assert len(staged['providers'])==37 and len(staged['copies'])==148 and len(seen)==185 and staged['copy_bytes']==177769212
proof=load(P/'actors/genuine-strict-proof/strict-proof-summary.json')
assert len(proof['results'])==92 and proof['counts']=={'public':{'FAIL':10,'PASS':54},'native':{'PASS':28}}
assert collections.Counter(v['status'] for v in proof['results'])=={'PASS':82,'FAIL':10}
scan=[]
for role in ['candidate','parent']:
 q=P/('actors/scanner-'+role+'-default/scanner-private.jsonl');b=q.read_bytes();rows=[json.loads(x) for x in b.splitlines()]
 assert len(rows)==22 and all(x['status']=='warn' for x in rows);scan.append(b)
assert scan[0]==scan[1] and hashlib.sha256(scan[0]).hexdigest()=='8a67501840d3fd1b379f03f129b9a35abafef068737293ef937c98acc1299a2c'
prot=load(P/'protection-private.json');assert prot['unclassified_guard_events']==0
assert prot['guard_counts']=={'original-calibration-seed':40,'denied-source-associated-urllib3-IPv6-capability-bind':101,'denied-existing-Make-country-directory-noop':20}
assert digest(P/'protection-private.json')==m['full_protection_sha256']
sdk=load(T/'original-sealer-postexit-SDK-tool-records.json');assert digest(T/'original-sealer-postexit-SDK-tool-records.json')=='4ebae9573647e8aa5d94a6d62c3e6810ae93c94b01af4a00359ce6f3654f18e6'
sdkvalues={x['key']:x['value'] for x in sdk['records']}
for stage,pid,ppid in [('seal',262707,262213),('postexit',263300,262806)]:
 pre=load(T/stage/'launch-before.json');pop=load(T/stage/'original-Popen.json');term=load(T/stage/'terminal.json');out=load(T/stage/'stdout.txt')
 assert term['actual_exit_code']==0 and term['original_Popen_pid']==pop['original_Popen_pid']==pid and term['Popen_parent_pid']==pop['Popen_parent_pid']==pre['launcher_pid']==ppid
 assert pre['command']==pop['command'] and pre['time_ns_directly_before_Popen']<=pop['time_ns_directly_after_Popen']<=term['time_ns_after_wait']
 assert out['literal_pid']==pid and out['literal_parent_pid']==ppid and out['READY_sha256']==PIN['READY.json'] and out['manifest_sha256']==PIN['final-private-manifest.json']
 for stream in ['stdout','stderr']:
  b=(T/stage/(stream+'.txt')).read_bytes();assert len(b)==term[stream]['bytes'] and hashlib.sha256(b).hexdigest()==term[stream]['sha256'] and b.decode()==term['exact_child_'+stream+'_UTF8']
 assert any(v.get('exit_code')==0 and json.loads(v.get('output','{}'))==term for v in sdkvalues.values() if isinstance(v,dict) and v.get('exit_code')==0)
report={'status':'ROOT_ACCEPTED_ORDER_ONLY_1215_WITH_RETAINED_FAILURES_AND_HISTORY','candidate':r['candidate'],'actual_parent':r['actual_parent'],'pins':PIN,'packet_entries':2848,'file_bytes':545526967,'sources':dict(sc),'providers':37,'copies':148,'copy_bytes':177769212,'strict':{'PASS':82,'FAIL':10},'scanners_each_WARN':22,'source_historical_exception':'Initial independent snapshot SHA-read six files; root did not reopen them. Untraced Git internals qualified.','original_sealer_and_separate_postexit_actual_exit':0,'literal_harness_recovery':'Separate root recovery pending; exact SDK values and process records verified.','new_root_interval_ns':time.time_ns(),'root_metadata_stop':'Earlier source inspection requested nonexistent FINAL-PROTECTION.json; actual protection-private.json read without product replay.','signed':'Sue (GPT-6)'}
q=C/'1215-INDEPENDENT-ROOT-ACCEPTANCE.json'
with os.fdopen(os.open(q,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f:json.dump(report,f,indent=2);f.write('\n')
print(json.dumps({'status':report['status'],'receipt_sha256':digest(q),'packet_entries':2848,'strict_PASS':82,'strict_FAIL':10,'identifiers_printed':0}))
