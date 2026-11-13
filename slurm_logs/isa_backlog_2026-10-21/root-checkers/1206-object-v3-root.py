"""Stdlib-only root acceptance of separately frozen object-v3 saved evidence."""
from pathlib import Path
import ast,collections,datetime,hashlib,json,os,stat,subprocess
CONTROL=Path('/local/job40255479/control/isa-backlog-20261021');WORK=Path('/local/job40255479/worktrees')
source=CONTROL/'verify_849_218_root.py'
binding=json.loads((WORK/'isa-campaign/slurm_logs/isa_backlog_2026-10-21/root-checkers/849-218-checker-binding.json').read_text())
assert hashlib.sha256(source.read_bytes()).hexdigest()==binding['sha256']
tree=ast.parse(source.read_text());nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef)]
exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source)+'[functions-only]','exec'))
P=CONTROL/'comparator-values-reassessment';F=P/'corrected-object-v3';A=P/'corrected-object-v3-attestation'
specs=[(F,'09dec74118c5131863ce92b6c78214c6ecf87146a121aa758774ff892a5780b2','96b5c506e1955748ba62dca453260d773772d9ed9e1f261942500de45c13eeba',1202),(A,'afe74ed2c4677711f83862372d4978656c9db05c3a491bc532f7fb6d94e5c56c','1aa0b6cbcf0f0ff414e26ac83cd561cea4ae638356bf85559fbc2ea69c5a3fda',2)]
for root,rpin,mpin,count in specs:
 assert digest(root/'READY.json')==rpin;r=load(root/'READY.json');assert r['manifest_entries']==count and r['manifest_sha256']==mpin
 rows=verify_manifest(root,'FROZEN-MANIFEST.json',mpin,count)
 assert {str(q.relative_to(root)) for q in root.rglob('*') if q.is_file() or q.is_symlink()}==set(rows)|{'READY.json','FROZEN-MANIFEST.json'}
 for rel,rec in r['safe_export_allowlist'].items():verify_receipt(root/rel,rec)
gate=load(F/'preparation-safe.json');proof=load(F/'safe-exports/proof.json');results=load(F/'results-private.json');plan=load(F/'plan-private.json')
assert len(results)==len(plan)==1198
assert proof['positive_frame_groups']==1160 and proof['old_empty_expected_memory_gap_inspections']==38 and not proof['findings']
assert proof['extra18_direct_marked_single_country_Feature_memory_gaps_retained']==2 and not proof['all_campaign_result']
assert sum(r['status']=='PASS' for r in results)==1160
assert sum(r['right'] is None for r in results)==38
assert all(r['status'] in {'PASS','ACTUAL_EMPTY_NO_NUMERIC_ARRAY_VALUES; ORIGINAL_EXPECTED_MEMORY_NOT_SAVED'} for r in results)
for i,(r,p) in enumerate(zip(results,plan)):
 assert r['ordinal']==i and all(r[k]==v for k,v in p.items())
 assert r['right'] is None or r['left']!=r['right']
assert {k:v['positive_groups'] for k,v in proof['cohorts'].items()}=={'17':196,'18':265,'19':603,'1200':96}
assert sum(r['status']=='PASS' and bool(r.get('qualification')) for r in results)==8
cal=load(F/'safe-exports/calibration.json');assert cal['status']=='PASS' and cal['negative_controls_total']==42
assert len(cal['inherited_negative_controls_rejected'])==12 and len(cal['object_negative_controls_rejected'])==30
helper=F/'sources/exact_strict_fast_v3.py';assert digest(helper)==gate['new_helper_sha256']=='68be90fadd95ef16d46a7911bee1b26e4d671662d669ccf681230b60a09c7e94'
old=(F/'sources/exact_strict_fast_v2.py').read_text();new=helper.read_text();assert new==old.replace("if isinstance(a.dtype,np.dtype) and a.dtype.kind=='O':","if a.dtype.kind=='O' and not isinstance(a.dtype,pd.StringDtype):")
checks=load(F/'safe-exports/checker-sources.json')
for rel,e in checks.items():verify_receipt(F/'sources'/rel,e)
original_roots={'17':(CONTROL/'integration/framework-seventeen-review','private-final-manifest.json'),'18':(CONTROL/'integration/mali-eighteen-review','private-final-manifest.json'),'19':(CONTROL/'integration/niger-nineteen-docs-review','private-final-manifest.json'),'1200':(CONTROL/'review-Niger-1091','final-review-manifest-private.json')}
original_manifests={}
for key,(root,mf) in original_roots.items():
 g=gate['source_gates'][key];original_manifests[key]=verify_manifest(root,mf,g['manifest'],g['entries'])
R=WORK/'isa-review-comparator-values';assert git(R,'rev-parse','HEAD').decode().strip()=='f8d236694c26fe9e709c5548f02c70f74ea72c33' and git(R,'status','--porcelain')==b''
for key,g in gate['source_gates'].items():
 for module,blob in g['record_class_blobs'].items():
  rel='lsms_library/'+module+'.py';assert git(R,'rev-parse','HEAD:'+rel).decode().strip()==blob
  for head in g['heads']:assert git(R,'rev-parse',head+':'+rel).decode().strip()==blob
copies=load(F/'copy-receipts-private.json');assert len(copies)==1154
for path,e in copies.items():
 source=Path(path);copy=F/e['copy_relative'];verify_receipt(source,e['original']);verify_receipt(copy,e['copy'])
 assert not source.is_symlink() and not copy.is_symlink() and source.stat().st_nlink==copy.stat().st_nlink==1
 assert (source.stat().st_dev,source.stat().st_ino)!=(copy.stat().st_dev,copy.stat().st_ino)
 assert digest(source)==digest(copy)
for key,(folder,rpin,mpin) in gate['frozen_prior_releases'].items():
 T=P/folder;assert digest(T/'READY.json')==rpin;r=load(T/'READY.json');assert r['manifest_sha256']==mpin
 verify_manifest(T,'FROZEN-MANIFEST.json',mpin,r['manifest_entries'])
attest=load(A/'POSTEXIT-safe.json');assert attest['manifest_entries']==1202 and attest['zero_active_payload_analysis_consumers'] and attest['write_denials']==attest['network_denials']==0
assert proof['object_EA_checks']=={'object_EA_blocks':39,'object_EA_cell_pairs':32,'dtype_class:pandas.core.dtypes.dtypes.NumpyEADtype':39,'left_scalar_class:builtins.str':32,'right_scalar_class:builtins.str':32}
live_consumers([F,A])
result=dict(status='ROOT_ACCEPT_BOUNDED_CORRECTED_OBJECT_V3_SUPPLEMENT',at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),READY_sha256=specs[0][1],manifest_sha256=specs[0][2],all_new_manifest_entries=1204,all_old_manifest_entries=sum(g['entries'] for g in gate['source_gates'].values()),physical_selected_operands=1154,positive_groups=1160,original_stage_qualified_groups=8,original_empty_memory_gaps=38,additional18_Feature_memory_gaps=2,negative_calibrations=42,cohorts={k:v['positive_groups'] for k,v in proof['cohorts'].items()},object_EA_blocks=39,actual_object_cell_pairs=32,actual_object_cell_types='builtins.str',no_country_mismatch_found=True,no_all_campaign_verdict=True,no_API_payload_decode_source_or_test_replay=True,root_read_actual_checker_helper_diff_selection_and_postexit_sources=True,signed='Sue')
out=CONTROL/'1206-object-v3-ROOT-ACCEPTANCE.json'
with out.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
out.chmod(0o600);print(json.dumps(result))
