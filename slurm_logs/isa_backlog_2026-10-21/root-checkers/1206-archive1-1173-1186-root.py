"""Root byte/source/receipt review of two archive-backed scopes; no decode."""
from pathlib import Path
import ast,collections,datetime,hashlib,json,os,stat,subprocess
C=Path('/local/job40255479/control/isa-backlog-20261021');P=C/'comparator-values-archive1';L=P/'releases';R=Path('/local/job40255479/worktrees/isa-review-archive1-products')
def sha(p):
 h=hashlib.sha256()
 with Path(p).open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def load(p):return json.loads(Path(p).read_text())
def same(p,e):
 p=Path(p);s=p.lstat();a={'bytes':s.st_size,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'device':s.st_dev,'nlink':s.st_nlink,'symlink':p.is_symlink()}
 if p.is_symlink():a['link_target']=os.readlink(p)
 else:a['sha256']=sha(p)
 assert a==e,('physical receipt',str(p))
def git(*args):return subprocess.check_output(['git','-C',str(R),*args])
def pathset(root):return {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() or p.is_symlink()}
assert git('rev-parse','HEAD').decode().strip()=='f8d236694c26fe9e709c5548f02c70f74ea72c33' and git('status','--porcelain')==b''
source=load(P/'initial-source-private.json');assert source['head']==git('rev-parse','HEAD').decode().strip()
assert len(source['source_receipts'])==14880
for row in source['source_receipts']:
 e=dict(row);same(e.pop('path'),e)
ruling=C/'ROOT-ARCHIVE1-SAVED-CONTENT-AUTHORITY-RULING-private.json'
assert sha(ruling)==sha(P/'common'/ruling.name)=='08efdbda63d4175bacad59ae0a288831603791b9460aa463769d666ec27c405e'
authority=load(ruling);archive=load(P/'archive-authority-gate-private.json');same(archive['archive'],archive['receipt']);assert archive['receipt']==authority['archive_receipt']
oldcount=0
for old,pin in load(P/'old-frozen-root-pins-private.json').items():
 same(pin['manifest'],pin['receipt']);m=load(pin['manifest']);assert pathset(Path(old))==set(m)
 for rel,e in m.items():same(Path(old)/rel,e);oldcount+=1
assert oldcount==687
for head,mods in load(P/'class-module-Git-pins-private.json').items():
 for rel,blob in mods.items():assert git('rev-parse',head+':'+rel).decode().strip()==blob
assert sha(P/'exact_strict_fast_v3.py')=='68be90fadd95ef16d46a7911bee1b26e4d671662d669ccf681230b60a09c7e94'
cal=load(P/'common/own-calibration-safe.json');assert cal['negative_controls_total']==42 and len(cal['negative_controls'])==42 and cal['status']=='PASS_OWN_GUARDED_CALIBRATION'
out={}
for pr,rpin,mpin,count,products in [(1173,'96f0b03e641d7c28df572d8f7aca28edb3abf37322a19b6564f4a8e0bf9c7aa2','0a4e0bf82528ee7765b91d5f9349e62b523bfc8f934072b8b5a9dea66e1b0e76',96,13),(1186,'a624eea2fed5d7923b21519517cf653260233864bd69367a0848ab20dc83c38c','48dbdb007ea09ad28c09b9b7fef5b4c7bc7012f4e752dffbb57375f4975b09bc',76,12)]:
 stem=f'pr{pr}';D=P/stem;ready=L/(stem+'-READY-safe.json');assert sha(ready)==rpin;r=load(ready)
 assert r['runtime_terminal_exit']==r['packager_terminal_exit']==r['postexit_v2_terminal_exit']==0 and r['remaining_consumers']==0
 for rel,pin in r['pins'].items():assert sha(L/rel)==pin
 manifest=L/(stem+'-full-manifest-private.json');assert sha(manifest)==mpin;m=load(manifest);assert len(m)==count and pathset(D)==set(m)
 for rel,e in m.items():same(D/rel,e)
 for rel,e in load(L/(stem+'-safe-allowlist.json')).items():same(D/rel,e)
 assert sha(D/r['canonical_safe'])==r['canonical_safe_sha256']
 control=load(L/(stem+'-postexit-controller-v2-manifest-private.json'))
 same(control['canonical_controller'],control['canonical_controller_receipt']);same(control['canonical_postexit_proof'],control['canonical_postexit_proof_receipt']);same(control['v1_checker_source_retained_at'],control['v1_checker_source_receipt'])
 assert sha(control['canonical_controller'])==r['canonical_postexit_controller_sha256']=='5c80491a4974165380855585dbbc4d10a0ca7586a7ee25fc16c44ea9f165f9bc'
 post=load(control['canonical_postexit_proof']);assert post['manifest_sha256']==mpin and post['manifest_entries']==count and post['remaining_consumers']==post['private_data_payload_files']==post['guard_denial_events']==post['network_denial_events']==0
 prep=load(D/'preparation-private.json');assert len(prep['operands'])==products
 for row in prep['authority']:same(row['original'],row['receipt']);same(row['copy'],row['copy_receipt'])
 for key,row in prep['operands'].items():
  same(row['original'],row['original_current']);same(row['copy'],row['copy_receipt']);old=authority['entries'][key]
  assert old['PR']==pr and old['original']==row['original'] and old['NEW_current_physical_baseline']==row['original_current']
  assert row['historical']=={'sha256':old['historical_archive_content_sha256'],'bytes':old['historical_archive_bytes'],'mode':old['historical_archive_mode']}
  assert all(row['original_current'][k]==v for k,v in row['historical'].items()) and row['copy_receipt']['sha256']==row['historical']['sha256']
  a,b=row['original_current'],row['copy_receipt'];assert a['nlink']==b['nlink']==1 and not a['symlink'] and not b['symlink'] and b['mode']==0o600 and (a['device'],a['inode'])!=(b['device'],b['inode'])
 census=load(D/'complete-source-census-private.json')
 for row in census:
  for path,receipt in [('new_copy','new_copy_receipt'),('release_copy','release_receipt')]:same(row[path],row[receipt])
  if row['kind'].startswith('git-'):assert git('rev-parse',row['origin']).decode().strip()==row['git_blob']
  else:same(row['origin'],row['original_receipt'])
 for row in load(D/'support-originals-private.json'):same(row['original'],row['receipt']);same(row['copy'],row['copy_receipt'])
 pairs=load(D/'pair-assertion-map-private.json');actual=collections.Counter(row['status'] for row in pairs)
 assert actual==({'PASS':6,'FAIL':1} if pr==1173 else {'PASS':12})
 if pr==1173:
  diag=load(D/'axis-diagnostic-private-v2.json');bind=load(D/'attrs-stage-binding-private.json')
  assert [(v['name'],v['extra_filtered_allwave_domain_count'],v['code_dtype'],v['code_itemsize']) for v in diag['levels']]==[('i',3110,['int16','int16'],[2,2]),('t',7,['int8','int8'],[1,1])]
  assert len(diag['typed_attr_differences'])==bind['differences']==21
  assert bind['all_differences_are_extra_seven_outside_subset_wave_record_nodes_in_each_of_three_carriers'] and bind['no_shared_field_value_type_or_key_type_difference_observed']
  for fn in ['axis-construction-source-pins-private.json']:
   for row in load(D/fn):same(row['original'],row['receipt']);same(row['copy'],row['copy_receipt'])
  for row in bind['actual_original_record_module_pins']:same(row['original'],row['receipt']);same(row['copy'],row['copy_receipt'])
  for row in bind['actual_original_functions']:
   code=(R/'lsms_library'/(row['module']+'.py')).read_text();node=next(n for n in ast.parse(code).body if isinstance(n,ast.FunctionDef) and n.name==row['function']);assert ast.get_source_segment(code,node)==row['source']
  def finalizer(code):return next(n for n in ast.walk(ast.parse(code)) if isinstance(n,ast.FunctionDef) and n.name=='_finalize_result')
  assert ast.dump(finalizer((R/'lsms_library/country.py').read_text()),include_attributes=False)==ast.dump(finalizer(git('show','4ef24fb72421e0573841aeb63b8c19b93a4f97d8:lsms_library/country.py').decode()),include_attributes=False)
  classification={'NEW_consistency_pairs':7,'strict_pass':6,'strict_failure':1,'original_reference_pairs_recovered':0,'unused_i_domain_extras':3110,'unused_t_domain_extras':7,'code_width_changes':0,'independent_typed_attrs_failure_presence_nodes':21,'stage_bound_to_current_decoder_and_original_row_selection_and_per_frame_annotation':True,'full_axis_and_attrs_failures_retained_no_normalization_or_product_defect_inferred':True}
 else:classification={'original_public_pairs':12,'strict_pass':12,'strict_failure':0,'original_rename_projection_and_saved_scalar_predicates_pass':True}
 out[str(pr)]={'status':'ROOT_ACCEPTED_BOUNDED_ARCHIVE_BACKED_SAVED_EVIDENCE_WITH_ORIGINAL_GAPS',**classification,'READY_sha256':rpin,'manifest_sha256':mpin,'manifest_entries':count,'authorized_products':products,'original_historical_nanosecond_mtime_inode_nlink_continuity_not_claimed':True,'old_hold_packets_unchanged':True,'original_API_raw_source_Make_helper_and_fixture_memories_not_recovered':True}
dest=C/'1206-ARCHIVE1-1173-1186-ROOT-ACCEPTANCE.json';assert not dest.exists()
obj={'at_UTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'cohorts':out,'source_physical_entries':14880,'old_frozen_entries':687,'no_package_import_payload_decode_or_API_test_cache_replay_by_root':True,'root_read_actual_checkers_and_decoder_record_stage_binding':True,'all22_or_campaign_acceptance':False,'root_checker_sha256':sha(Path(__file__)),'signed':'Sue'}
dest.write_text(json.dumps(obj,indent=2)+'\n');dest.chmod(0o600);print(json.dumps(obj))
