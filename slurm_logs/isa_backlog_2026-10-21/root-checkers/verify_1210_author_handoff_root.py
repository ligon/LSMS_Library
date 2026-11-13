"""Authenticate sealed author handoff before independent runtime review."""
from pathlib import Path
import ast,datetime,hashlib,json,os,re,stat,subprocess
C=Path('/local/job40255479/control/isa-backlog-20261021');P=C/'framework1210-fix';R=Path('/local/job40255479/worktrees/isa-invalid-wave-1210')
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def load(p):return json.loads(p.read_text())
def check(p,e,hash_content=True):
 s=p.lstat();a={'bytes':s.st_size,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns,'nlink':s.st_nlink,'device':s.st_dev,'inode':s.st_ino,'symlink':p.is_symlink(),'link':os.readlink(p) if p.is_symlink() else None}
 for k,v in a.items():assert e[k]==v,(k,str(p))
 if hash_content:assert e['sha256']==(hashlib.sha256(os.readlink(p).encode()).hexdigest() if p.is_symlink() else sha(p)),('sha',str(p))
assert sha(P/'READY.json')=='e0d3ff8fb9767a073822554a5eff758dc71a0e9fe3741366850cb66db7f731f3'
r=load(P/'READY.json');assert r['source_head']=='128cc095a62cbd210e30d2256359acfe53d4a2c5' and r['actual_parent']=='f8d236694c26fe9e709c5548f02c70f74ea72c33'
assert sha(P/'final-private-manifest.json')==r['manifest_sha256']=='bf77ea989db4da51aeb2d21385f40716a1a9757b8bea2ad3f4562ed097ff86f6'
assert sha(P/'safe-exports/ALLOWLIST.json')==r['safe_export_allowlist_sha256']=='1d06b3beaffcde3296596c48ba42962d1c69e6a33c1e536429e0a47b51cdb4f2'
assert sha(P/'freeze-proof-private.json')==r['freeze_proof_sha256']
m=load(P/'final-private-manifest.json');assert len(m['entries'])==r['manifest_entries']==1159
assert {str(q.relative_to(P)) for q in P.rglob('*') if q.is_file() or q.is_symlink()}==set(m['entries'])|set(m['excluded_paths'])
assert [str(q.relative_to(P)) for q in sorted(P.rglob('*')) if q.is_dir() and not q.is_symlink()]==m['directories']
skipped=[]
for rel,e in m['entries'].items():
 q=P/rel;raw='dvc-cache' in q.parts and re.fullmatch('[0-9a-f]{30}',q.name) is not None
 check(q,e,not raw)
 if raw:skipped.append(rel)
assert len(skipped)==228
allow=load(P/'safe-exports/ALLOWLIST.json');files=allow.get('files',allow)
assert set(files)=={'REPORT.org','RESIDUALS.md','commands.json','pins.json','protection.json','source.diff','validation.json'}
for name,e in files.items():check(P/'safe-exports'/name,e)
protection=load(P/'final-protection-private.json');safe=load(P/'safe-exports/protection.json')
assert protection['status']==safe['status']=='FINAL_PROTECTION_QUALIFIED_METADATA_ONLY'
assert protection['actual_v1']['session_id']==4319 and protection['actual_v1']['observed_exit_code']==1 and protection['actual_v1']['last_assertion_line']==63
assert protection['actual_v1']['preceding_full_hash_assertions_completed']
for name,e in protection['actual_v1']['bindings'].items():check(P/name,e)
log=P/'guard-private.jsonl';check(log,protection['guard_log']);assert sha(log)=='6ab8bdea5387bafde436a30cfc753d9000ee95815ed0e1801fa2e1bee89c380c'
rows=[json.loads(line) for line in log.read_text().splitlines()];assert rows==protection['guard_rows'] and len(rows)==safe['write_denials_recorded']==6 and log.stat().st_size==285
assert all(set(row)=={'event','path'} and row['event'].startswith('DENIED-os.') for row in rows)
tree=ast.parse((P/'protect.py').read_text());node=next(n for n in ast.walk(tree) if isinstance(n,ast.Assert) and n.lineno==63)
assert 'guard-private.jsonl' in ast.unparse(node)
assert not any('protect-v2.py' in p.name and p.suffix=='.log' for p in P.iterdir())
expected=['.coder/ledger/1210-invalid-food-waves.md','lsms_library/countries/Malawi/_/CONTENTS.org','lsms_library/country.py','tests/test_gh1210_invalid_food_waves.py']
assert subprocess.check_output(['git','-C',str(R),'diff','--name-only',r['actual_parent'],r['source_head']],text=True).splitlines()==expected
for root,head in [(R,r['source_head']),(Path(str(R)+'-base'),r['actual_parent'])]:
 assert subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True).strip()==head
 assert subprocess.check_output(['git','-C',str(root),'status','--porcelain'])==b''
assert sha(R/'lsms_library/country.py')=='5b0602d0fdda974445fa2ec6a446a74589e9f3940684af3a00180087dbb6eade'
assert sha(R/'tests/test_gh1210_invalid_food_waves.py')=='c0e837401eb7b26ba597710a7f2586f2167404781875fb61ab9b1155a51d900d'
assert sha(P/'seal-v1-private.log')=='2e56b909f77f78e79073937e1bf0d2cbb8ab5ae8b6e473a12c7e16ee51b86c46'
out={'status':'ROOT_AUTHENTICATED_SEALED_AUTHOR_HANDOFF_FOR_INDEPENDENT_REVIEW','at_UTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_head':r['source_head'],'parent':r['actual_parent'],'READY_sha256':sha(P/'READY.json'),'manifest_sha256':r['manifest_sha256'],'manifest_entries_physical_metadata_checked':1159,'non_L1_entries_content_rehashed_by_root':1159-len(skipped),'raw_L1_entries_full_hash_checked_by_observed_separate_postexit_not_rehashed_again_by_root':len(skipped),'observed_author_sealer_and_postexit_handles':[50484,7930],'observed_postexit_terminal':0,'six_denials_full_log_pinned_unresolved_dir_fd_timestamps_and_endpoint_history_retained':True,'protection_v1_terminal1_and_metadata_only_qualification_retained':True,'initial_parent_and_scope_metadata_diagnostics_retained':True,'author_handoff_not_production_acceptance':True,'no_project_import_API_data_or_test_replay_by_root':True,'root_checker_sha256':sha(Path(__file__)),'signed':'Sue'}
dest=C/'1210-AUTHOR-HANDOFF-ROOT.json';assert not dest.exists();dest.write_text(json.dumps(out,indent=2)+'\n');dest.chmod(0o600);print(json.dumps(out))
