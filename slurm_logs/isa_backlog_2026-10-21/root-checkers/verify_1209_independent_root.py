"""Root source/receipt/evidence acceptance; stdlib only, no data replay."""
from pathlib import Path
import ast,collections,datetime,hashlib,json,os,re,stat,subprocess
import xml.etree.ElementTree as ET
C=Path('/local/job40255479/control/isa-backlog-20261021');P=C/'review-1209-Malawi';W=Path('/local/job40255479/worktrees')
helper=C/'verify_849_218_root.py';assert hashlib.sha256(helper.read_bytes()).hexdigest()=='4800db0a62ba44a37be2c2b9c0a3318d38863fd18441e96d8347115a8e7810d2'
exec(compile(ast.Module(body=[n for n in ast.parse(helper.read_text()).body if isinstance(n,ast.FunctionDef)],type_ignores=[]),str(helper)+'[functions-only]','exec'))
def physical(q,e,content=True):
 s=q.lstat();a={'bytes':s.st_size,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns,'nlink':s.st_nlink,'device':s.st_dev,'inode':s.st_ino,'symlink':q.is_symlink(),'link':os.readlink(q) if q.is_symlink() else None}
 assert all(e[k]==v for k,v in a.items()),('physical',str(q))
 if content:assert digest(q)==e['sha256'],('sha',str(q))
assert digest(P/'READY.json')=='7ae6dff356f391ddbb3b07d604a97d0218c5d986bd9009b9ac3b4baaff7f3101'
r=load(P/'READY.json');candidate='47b3c5d76371acc457c5553ebf8adfb1859e115b';parent='d29a56783d08a8a647e2622150a4ee5fbdd7246a'
assert r['source_head']==candidate and r['actual_parent']==parent and r['verdict']=='PASS_INDEPENDENT_SCOPED_REVIEW'
assert digest(P/'final-private-manifest.json')==r['manifest_sha256']=='fdf2907b8bae4f4cda5de886ae9cc3fdf5bccd957dc439c9438848467c7c73cb'
assert digest(P/'safe-exports/ALLOWLIST.json')==r['safe_allowlist_sha256']=='142b5d8fd971469b74f3056ad18fd7a470d79ef967aa4cc94f7e71fee8f5577e'
m=load(P/'final-private-manifest.json');assert len(m['entries'])==r['manifest_entries']==906
assert {str(q.relative_to(P)) for q in P.rglob('*') if q.is_file() or q.is_symlink()}==set(m['entries'])|set(m['excluded_paths'])
assert [str(q.relative_to(P)) for q in sorted(P.rglob('*')) if q.is_dir() and not q.is_symlink()]==m['directories']
raw=[]
for rel,e in m['entries'].items():
 q=P/rel;skip='dvc-cache' in q.parts and re.fullmatch('[0-9a-f]{30}',q.name) is not None
 physical(q,e,not skip)
 if skip:raw.append(rel)
assert len(raw)==304
post=C/'review-1209-Malawi-postexit-stdout.json';terminal=C/'review-1209-Malawi-postexit-terminal.json'
assert digest(post)=='c835cf66f81dbbf4ab44dea0b49005d2ebfd95a298c0d9afeeecb292b945d83e' and digest(terminal)=='369a7c4d994980da408c2421f74ad7417485d9c478cdf581b129fea1b884b23a'
p=load(post);t=load(terminal);assert t['exact_returned_tool_record']['exit_code']==t['postexit_observed_exit_code']==t['sealer_observed_exit_code']==0 and t['session_id_requested']==99254
assert t['exact_returned_tool_record']['output'].encode()==post.read_bytes()
assert p['manifest_entries']==906 and p['READY_sha256']==digest(P/'READY.json') and p['manifest_sha256']==r['manifest_sha256'] and p['all_file_hashes_and_physical_receipts_exact'] and p['writes']==0 and p['live_actor_pids']==[]
physical(P/'seal-private.log',p['completed_sealer_stream'])
allow=load(P/'safe-exports/ALLOWLIST.json')['files'];assert set(allow)=={'REPORT.org','commands.json','pins.json','protection.json','source.diff','validation.json'}
for name,e in allow.items():physical(P/'safe-exports'/name,e)
baseline=load(P/'source-before-private.json');source_count=0
for role,head in [('candidate',candidate),('base',parent)]:
 s=baseline[role];assert s['head']==head and s['status']==''
 source_count+=verify_source(Path(s['root']),{'head':head,'files':s['tracked']},head)
assert source_count==29779
physical(Path('/local/job40255479/venv_img/bin/python').resolve(),baseline['runtime']);physical(Path('/local/job40255479/venv_img/VENV_GENERATION'),baseline['generation'])
R=Path(baseline['candidate']['root']);diff=git(R,'diff','--name-only',parent,candidate).decode().splitlines()
assert diff==['.coder/ledger/1209-malawi-food-builder.md','lsms_library/countries/Malawi/_/CONTENTS.org','lsms_library/countries/Malawi/_/Makefile','lsms_library/countries/Malawi/_/food_acquired.py','tests/test_gh1209_malawi_food_builder.py']
assert git(R,'diff','--name-only',parent,candidate,'--','lsms_library').decode().splitlines()==diff[1:4]
assert t['parent_pytest_identity_qualification']['agreement'] and t['parent_pytest_identity_qualification']['all_framework_tracked_blobs_equal']
parent_log=(P/'parent-tests-private.log').read_text();assert '../isa-review-Malawi-1209/lsms_library/country.py' in parent_log and str(W/'isa-review-Malawi-1209-base/lsms_library/countries/Malawi/_') in parent_log
assert (P/'test_candidate_1209.py').read_bytes()==(R/'tests/test_gh1209_malawi_food_builder.py').read_bytes()
junit(P/'pytest-candidate.xml',{'pass':6});junit(P/'pytest-parent.xml',{'pass':2,'fail':4});junit(P/'pytest-existing.xml',{'pass':13})
commands=load(P/'safe-exports/commands.json')['actors'];assert len(commands)==33
for row in commands:
 q=P/(row['tag']+'-terminal-private.json');v=load(q);assert v['exit_code']==(1 if row['tag']=='parent-tests' else 0)
 physical(Path(v['log']),v['log_receipt'])
 for name,e in v['script_pins'].items():physical(P/name,e)
country=load(P/'Country-comparisons-private.json');feature=load(P/'Feature-comparisons-private.json')
assert len(country['comparisons'])==56 and len(country['parent_Make_failures'])==16 and len(feature)==24
for row in country['comparisons']+feature:
 assert row['strict']=='PASS' and row['left']!=row['right']
 physical(Path(row['left']),row['left_receipt']);physical(Path(row['right']),row['right_receipt'])
native=load(P/'native-comparisons-private.json');assert len(native)==10
for row in native:
 assert row['strict']=='PASS';physical(Path(row['left']),row['left_receipt']);physical(Path(row['right']),row['right_receipt'])
footers=load(P/'cache-footers-private.json');assert len(footers)==98
for row in footers:physical(Path(row['path']),row['receipt'])
scope=load(P/'hash-scope-safe.json');inventories={side:load(P/(side+'-inventory-private.json')) for side in ['base','candidate']}
assert inventories['base']['head']==parent and inventories['candidate']['head']==candidate
assert inventories['base']['public_names']==inventories['candidate']['public_names'] and inventories['base']['Malawi']==inventories['candidate']['Malawi']
for name,total,count in [('country_hashes',549,26),('framework_hashes',39,0),('wave_hashes',1362,97)]:
 a,b=inventories['base'][name],inventories['candidate'][name];assert a.keys()==b.keys() and len(a)==total
 changed=[key for key in a if a[key]!=b[key]];assert changed==scope['changed'][name] and len(changed)==count and all(k.startswith('Malawi/') for k in changed)
assert digest(P/'exact_strict_fast_v3.py')=='68be90fadd95ef16d46a7911bee1b26e4d671662d669ccf681230b60a09c7e94'
cal=load(P/'calibration-safe.json');assert cal['status']=='PASS' and len(cal['negative_controls'])==cal['count']==49 and len(set(cal['negative_controls']))==49
v=load(P/'safe-exports/validation.json');assert v['Country']['strict_pass']==56 and v['Feature']['strict_pass']==24 and v['artifacts']['native_strict_pass']==10 and v['artifacts']['cache_footers']==98
assert v['artifacts']['scanner_statuses']=={'warn':158,'pass':3,'fail':2} and v['artifacts']['scanner_literal_order_equal'] and v['artifacts']['parent_food_candidate_grade_stale']==2
warnings=load(P/'warning-comparisons-private.json');assert len(warnings)==16 and all(not x['literal'] and x['exact_checkout_substitution'] for x in warnings)
direct=load(P/'candidate-make-one/direct-make-private.json');assert direct['exit_code']==0 and all(direct['after'][k]==e for k,e in direct['before'].items())
assert sum(k.endswith('/_/food_acquired.parquet') for k in direct['before'])==1 and sum(k.endswith('/_/food_acquired.parquet') for k in direct['after'])==5 and 'Malawi/var/food_acquired.parquet' not in direct['after']
prep=load(P/'preparation-private.json');assert len(prep['providers'])==19 and prep['physical_copies']==304
copy_inodes=set()
for root in prep['roots']:
 for row in load(P/root['label']/'preparation-private.json')['copies']:
  q=Path(row['copy']);physical(q,row['copy_receipt'],False);key=(q.stat().st_dev,q.stat().st_ino);assert key not in copy_inodes and q.stat().st_nlink==1 and not q.is_symlink();copy_inodes.add(key)
assert len(copy_inodes)==304
protection=load(P/'final-protection-private.json');assert protection['status']=='PASS' and protection['guard_denied_writes']==protection['network_attempts']==0 and protection['author_frozen_files_exact']==1085
out={'status':'ROOT_ACCEPTED_SCOPED_MALAWI1209_FIX','at_UTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':candidate,'parent':parent,'READY_sha256':digest(P/'READY.json'),'manifest_sha256':r['manifest_sha256'],'independent_tests':{'candidate_pass':6,'existing_pass':13,'parent_config_fail':4,'parent_config_pass':2},'parent_pytest_identity_qualification':t['parent_pytest_identity_qualification']['classification'],'parent_library_blobs_identical_and_actual_parent_config_Make_cwd_bound':True,'Country_strict_pairs':56,'Feature_strict_pairs':24,'native_strict_pairs':10,'calibration_rejects':49,'footers':98,'parent_country_footers_grade_stale':2,'scanner_records_each':163,'scanner_statuses':v['artifacts']['scanner_statuses'],'warning_pairs_checkout_path_only':16,'literal_warning_pairs':0,'country_hashes_changed':26,'framework_hashes_changed':0,'wave_hashes_changed':97,'tracked_source_receipts_rehashed_by_root':29779,'manifest_entries_physical_metadata_checked_by_root':906,'non_L1_entries_rehashed_by_root':602,'raw_L1_copies_full_hash_checked_by_independent_protection_and_postexit_not_rehashed_again_by_root':304,'observed_sealer_and_full_postexit_terminal_zero':[34361,99254],'author_old_diagnostics_and_metadata_utility_exceptions_retained':True,'no_global_zero_outside_write_or_science_or_corpus_claim':True,'root_read_actual_checkers_and_wrapper_source':True,'root_project_import_payload_decode_API_test_cache_replay':0,'root_checker_sha256':digest(Path(__file__)),'human_merge_required':True,'signed':'Sue'}
dest=C/'1209-INDEPENDENT-ROOT-ACCEPTANCE.json';assert not dest.exists();dest.write_text(json.dumps(out,indent=2)+'\n');dest.chmod(0o600);print(json.dumps(out))
