"""Root metadata/source-only runtime-release pin gate. Sue."""
from pathlib import Path
import hashlib,json,os,stat,subprocess,sys,time
assert sys.flags.no_site and sys.dont_write_bytecode
C=Path('/local/job40255479/control/isa-backlog-20261021');P=C/'framework1218-empty-types'
def receipt(q):
 q=Path(q);s=q.lstat();assert stat.S_ISREG(s.st_mode) and s.st_nlink==1
 d={k:getattr(s,k) for k in ['st_ctime_ns','st_dev','st_gid','st_ino','st_mode','st_mtime_ns','st_nlink','st_size','st_uid']}
 d['sha256']=hashlib.sha256(q.read_bytes()).hexdigest();assert all(getattr(q.lstat(),k)==v for k,v in d.items() if k!='sha256');return d
q=P/'RUNTIME-REVIEW-PLAN-v2.json';assert receipt(q)['sha256']=='67ec88613a1586253e4af9143745d1886059fe6295c3cdf648031d721f439d33'
d=json.loads(q.read_text());checked={}
for group in ['controllers','unchanged_shared_bindings','original14_controller_receipts']:
 for name,pin in d[group].items():assert receipt(P/name)==pin,(group,name);checked[group+'/'+name]=pin
assert len(d['controllers'])==15 and len(d['unchanged_shared_bindings'])==2 and len(d['original14_controller_receipts'])==14
for group in ['original_unexecuted_plan','versioned_source_diff']:
 v=d[group];assert receipt(v['path'])==v['receipt'];checked[group]=v
env={**os.environ,'GIT_OPTIONAL_LOCKS':'0'}
for role,v in d['literal_identities'].items():
 root=Path(v['root'])
 assert subprocess.check_output(['git','--no-optional-locks','rev-parse','HEAD'],cwd=root,text=True,env=env).strip()==v['head']
 assert subprocess.check_output(['git','--no-optional-locks','status','--porcelain=v1','--untracked-files=all'],cwd=root,text=True,env=env)==''
 assert str((root/'.venv').resolve())==v['venv']
 assert str(Path(v['DVCtmp_link']).resolve())==v['DVCtmp_target']
for n,pin in d['production_paths'].items():assert receipt(Path(d['literal_identities']['candidate']['root'])/n)==pin,n
for n,v in d['independent_v1_review'].items():assert receipt(C/'review1218-runtime-gates'/n)==v
assert receipt(C/'1218-PHASE0-ROOT-AUTHENTICATION.json')==d['root_phase0_authentication']
assert receipt(P/'ROOT-FRAMEWORK-PARENT-AND-BF-CONFIG-PREPARATION.json')==d['root_source_preparation']
report={'status':'ROOT_SOURCE_PIN_GATE_PASS_NO_RUNTIME','plan_sha256':receipt(q)['sha256'],'checked_controller_groups':{k:len(d[k]) for k in ['controllers','unchanged_shared_bindings','original14_controller_receipts']},'three_HEADs':{k:v['head'] for k,v in d['literal_identities'].items()},'production_paths':list(d['production_paths']),'full_physical_and_SHA_receipts':checked,'historical_payloads_directly_opened':0,'untraced_Git_internals_qualified':True,'at_ns':time.time_ns(),'signed':'Sue (GPT-6)'}
out=C/'FRAMEWORK1218-V2-ROOT-PIN-GATE.json'
with os.fdopen(os.open(out,os.O_WRONLY|os.O_EXCL|os.O_CREAT,0o600),'w') as f:json.dump(report,f,indent=2);f.write('\n')
print(json.dumps({'status':report['status'],'receipt_sha256':receipt(out)['sha256'],'genuine_operations':0}))
