"""Joint root acceptance of independent1210 runtime and additive metadata; stdlib only."""
from pathlib import Path
import collections, datetime, hashlib, json, os, stat, subprocess
import xml.etree.ElementTree as ET

C=Path('/local/job40255479/control/isa-backlog-20261021')
P=C/'review-invalid-wave-1210'
S=C/'review-invalid-wave-1210-supplement-v2'
HEAD='128cc095a62cbd210e30d2256359acfe53d4a2c5'
BASE='f8d236694c26fe9e709c5548f02c70f74ea72c33'

def load(p): return json.loads(p.read_text())
def digest(p):
    with p.open('rb') as f: return hashlib.file_digest(f,'sha256').hexdigest()
def check(p,e,content=True):
    s=p.lstat();link=os.readlink(p) if p.is_symlink() else None
    actual=dict(bytes=s.st_size,mode=stat.S_IMODE(s.st_mode),mtime_ns=s.st_mtime_ns,
                nlink=s.st_nlink,device=s.st_dev,inode=s.st_ino,symlink=link is not None,link=link)
    assert all(actual[k]==v for k,v in e.items() if k!='sha256'),str(p)
    if content:
        h=hashlib.sha256(link.encode()).hexdigest() if link is not None else digest(p)
        assert h==e['sha256'],str(p)
def git(root,*args):
    return subprocess.check_output(['git','--no-optional-locks',*args],cwd=root,
                                  env=dict(os.environ,GIT_OPTIONAL_LOCKS='0'))

assert digest(P/'READY.json')=='b1878041c931bf71426f5f865ea2d9ebb01b6e279ede5fd2085a08eb17616765'
assert digest(P/'FINAL-MANIFEST-private.json')=='5f7364326678e13989091e887e6dd2a0d0044c9e28028f89ff2a9dcca5da99de'
ready=load(P/'READY.json');m=load(P/'FINAL-MANIFEST-private.json')
assert ready['entries']==len(m['entries'])==359
runtime=[Path(x) for x in m['excluded_runtime_prefixes']]
def included(q):
    rel=q.relative_to(P)
    return rel.as_posix() not in m['excluded_exact'] and not any(rel==x or x in rel.parents for x in runtime)
assert {str(q.relative_to(P)) for q in P.rglob('*') if (q.is_file() or q.is_symlink()) and included(q)}==set(m['entries'])
for rel,e in m['entries'].items():check(P/rel,e)
assert digest(S/'READY.json')=='5c1550fa983140b4b183235c5001207a0b52a95b28a0b83adc01bc381faabb29'
assert digest(S/'FINAL-MANIFEST-private.json')=='bbd87d432c35d8c2ea1887dceb848d2691829469cd0521e1ff8ed704a96a0b51'
sr=load(S/'READY.json');sm=load(S/'FINAL-MANIFEST-private.json')
assert sr['entries']==len(sm['entries'])==27
assert {q.name for q in S.iterdir() if q.is_file() and q.name not in sm['excluded_exact']}==set(sm['entries'])
for rel,e in sm['entries'].items():check(S/rel,e)
assert digest(S/'POSTEXIT-safe.json')=='02901d35ae08aea5c6158ae839929afb039037a5cb1e5611fcb9bfcdf1e1cda8'
assert digest(S/'SAFE-ALLOWLIST.json')==sr['safe_allowlist_sha256']=='e3f802110a167bc4ef007b054da5a4377fdc195adddfd724639be70dc5bf7bf5'
allow=load(S/'SAFE-ALLOWLIST.json')['files'];assert set(allow)=={'SAFE-SUMMARY.json'}
for rel,e in allow.items():check(S/rel,e)
assert digest(S/'SAFE-SUMMARY.json')=='7f3578124cce3dd1520e0dc680475f57b828baba10eba77834f1ef3e86e58ada'
sp=load(S/'POSTEXIT-safe.json');assert sp['status']=='POSTEXIT_PASS_ADDITIVE_METADATA' and sp['zero_actual_consumers'] and sp['manifest_entries']==27
for rel,e in load(S/'v1-reference-pins-private.json').items():check(P/rel,e)
physical=load(S/'v1-physical-baseline-private.json')
assert len(physical)==2065
assert {'.'}|{str(q.relative_to(P)) for q in P.rglob('*')}==set(physical)
for rel,e in physical.items():
    q=P if rel=='.' else P/rel;s=q.lstat()
    actual=dict(mode=stat.S_IMODE(s.st_mode),bytes=s.st_size,mtime_ns=s.st_mtime_ns,nlink=s.st_nlink,
                device=s.st_dev,inode=s.st_ino,symlink=q.is_symlink(),link=os.readlink(q) if q.is_symlink() else None,directory=q.is_dir())
    assert actual==e,rel
initial=load(P/'initial-private.json');sources=0
for role,head in [('candidate',HEAD),('base',BASE)]:
    r=initial['sources'][role];root=Path(r['root'])
    assert r['head']==head and r['status']=='' and git(root,'rev-parse','HEAD').decode().strip()==head
    assert git(root,'status','--porcelain')==b''
    assert {x for x in git(root,'ls-files','-z').decode().split('\0') if x}==set(r['tracked'])
    for rel,e in r['tracked'].items():check(root/rel,e);sources+=1
assert sources==29762
R=Path(initial['sources']['candidate']['root'])
assert git(R,'diff','--name-only',BASE,HEAD).decode().splitlines()==[
 '.coder/ledger/1210-invalid-food-waves.md','lsms_library/countries/Malawi/_/CONTENTS.org',
 'lsms_library/country.py','tests/test_gh1210_invalid_food_waves.py']
check(Path('/local/job40255479/venv_img/bin/python').resolve(),initial['runtime'])
check(Path('/local/job40255479/venv_img/VENV_GENERATION'),initial['generation'])
for label,root in [('pytest-candidate-v2',R),('pytest-base-v2',Path(initial['sources']['base']['root']))]:
    boots=[q for q in (P/'identities').glob(label+'-*-boot.json')]
    assert len(boots)==2
    workers=[]
    for q in boots:
        a=load(q);workers.append(a['worker']);assert a['root']==str(root) and a['package']==str(root/'lsms_library/__init__.py')
        assert a['countries']==str(root/'lsms_library/countries') and a['guard']
        pre=load(P/'identities'/(label+'-'+str(a['pid'])+'-precollection.json'))
        assert pre['precollection'] and pre['worker']==a['worker'] and pre['root']==str(root) and pre['package']==a['package']
    assert set(workers)=={None,'gw0'}
for label,total,fail in [('candidate-v2',28,0),('base-v2',28,17),('existing',21,0)]:
    cases=list(ET.parse(P/('pytest-'+label+'.xml')).getroot().iter('testcase'))
    assert len(cases)==total and sum(c.find('failure') is not None for c in cases)==fail
    assert not any(c.find('error') is not None or c.find('skipped') is not None for c in cases)
for side in ['base','candidate']:
    a=load(P/('meta-'+side+'-inventory-private.json'))
    if side=='base':old=a
    else:new=a
for name,n in [('country',549),('framework',39),('wave',1362)]:
    assert old[name].keys()==new[name].keys() and len(old[name])==n
    assert sum(old[name][k]!=new[name][k] for k in old[name])==n
for key in ['public_names','declaration_content','excluded_callables','excluded_constants']:
    assert old[key]==new[key],key
comparison=load(P/'comparisons-private.json');assert comparison['total']==comparison['strict_pass']==50
assert all(x['status']=='STRICT_PASS' for x in comparison['comparisons'])
assert len(comparison['warnings'])==24 and all(x['categories_and_order_exact'] and x['full_messages_exact'] for x in comparison['warnings'])
cal=load(P/'comparator-calibration-private.json');assert cal['status']=='PASS' and len(cal['rejected'])==12
metadata_cal=load(P/'metadata-calibration-private.json');assert metadata_cal['status']=='PASS' and metadata_cal['code_width_rejected'] and metadata_cal['record_field_presence_rejected']
assert digest(P/'exact_strict_fast_v3.py')=='68be90fadd95ef16d46a7911bee1b26e4d671662d669ccf681230b60a09c7e94'
for label in ['base-bad','candidate-bad','base-mixed','candidate-mixed']:
    for phase in ['cold','warm','invalid']:
        assert load(P/(label+'-grid-'+phase+'.terminal-private.json'))['exit_code']==0
        terminal=load(P/label/(phase+'-terminal-private.json'));assert terminal['status']=='PASS' and terminal['provider_gate_after']
        if phase=='warm':assert len(terminal['outcomes'])==8 and all(x['physical_unchanged'] for x in terminal['outcomes'])
    a=load(P/label/'invalid-safe.json');assert a['before_paths']==105 and a['error_class_args_exact'] and a['L1_country_exact'] and a['chain_types']==['builtins.KeyError']
    candidate=label.startswith('candidate');assert a['whole_tree_exact']==candidate
    assert (a['removed_native'],a['changed_dirs'],a['after_paths'])==((0,0,105) if candidate else (5,5,100))
for label in ['base-bad','candidate-bad']:assert load(P/(label+'-grid-scanner.terminal-private.json'))['exit_code']==0
left=P/'base-bad/scanner-private.jsonl';right=P/'candidate-bad/scanner-private.jsonl'
assert left.read_bytes()==right.read_bytes() and digest(left)=='9e19e6a16ec37b0ecf7c5f1823db372fc30fd48b6b850a1668c195691cd46cd1'
assert len(left.read_text().splitlines())==163
assert comparison['scanner']['status_counts']=={'warn':158,'pass':3,'fail':2}
proof=load(S/'pid-association-private.json');assert proof['unmatched_denials']==[] and proof['seeded_denials']==12 and proof['direct_startup_denials']==2
assert len(proof['calibrations'])==len(proof['startup_controllers'])==len(proof['logged_network_events'])==2
for row in proof['calibrations']:
    assert row['separate_original_pid_identity_record']=='NOT_CAPTURED' and len(row['event_lines_1based'])==6
    assert [e['event'] for e in row['events']]==['DENIED-os.remove','DENIED-os.rmdir','DENIED-os.open','DENIED-os.mkdir','DENIED-os.rename-source','DENIED-network']
    assert all(e['path']=='guard-never-created' for e in row['events'][:-1])
    assert row['selection']=='five exact source-literal filesystem seed operations'
for rel,e in proof['bindings'].items():check(P/rel,e)
assert load(S/'packaging-qualification-private.json')['status']=='ANTICIPATED_MODE_MISMATCH_NOT_PRESENT'
assert load(S/'canonical-current-safe-pins.json')==load(P/'safe-exports/pins.json')
post=load(P/'POSTEXIT-safe.json');assert post['status']=='POSTEXIT_PASS' and post['zero_actual_consumers']
assert post['originals_binary_exact']==19 and post['copies_binary_exact']==152 and post['manifest_entries']==359
records=load(S/'observed-terminal-records-private.json')
for role,session in [('v1_sealer',48820),('v1_postexit',30150)]:
    r=records[role];assert r['call']['session_id']==session and r['returned']['exit_code']==0
    q=Path(r['redirected_stdout']['path']);check(q,r['redirected_stdout']['receipt']);assert q.read_text()==r['redirected_stdout']['exact_text']
    q=Path(r['redirected_stderr']['path']);check(q,r['redirected_stderr']['receipt']);assert q.read_bytes()==b''
for rel in ['review.json','pins.json','reproduction.json']:
    check(P/'safe-exports'/rel,load(P/'safe-exports/ALLOWLIST.json')['files'][rel])
out=dict(status='ROOT_ACCEPTED_BOUNDED_INDEPENDENT1210_WITH_METADATA_QUALIFICATIONS',
 at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),candidate=HEAD,parent=BASE,
 runtime_READY_sha256=digest(P/'READY.json'),runtime_manifest_sha256=digest(P/'FINAL-MANIFEST-private.json'),
 runtime_entries=359,supplement_READY_sha256=digest(S/'READY.json'),supplement_manifest_sha256=digest(S/'FINAL-MANIFEST-private.json'),
 supplement_entries=27,tracked_source_receipts_rehashed=sources,v1_full_physical_paths_checked=2065,
 independent_tests=dict(candidate_pass=28,parent_fail=17,parent_pass=11,existing_pass=21),
 strict_pairs=50,public_pairs=40,native_pairs=10,comparator_faults_rejected=14,
 cold_roots=4,fresh_warm_roots=4,warm_outputs_each=8,warm_physical_unchanged=True,
 invalid_controls=dict(candidate_paths_preserved=105,parent_native_files_deleted=5,parent_directories_changed=5),
 literal_scanner_records=163,scanner_statuses=dict(warn=158,passed=3,fail=2),full_warning_pairs_exact=24,
 fingerprints_moved=dict(country=549,framework=39,wave=1362),
 original_nested_pins_all_exact=True,calibration_PID_strength='Source/path/order/time association, separate original PID records absent',
 stopped_controller_PID_strength='Direct original boot PID, precollection and exit3 stream binding',
 author_six_unattributed_relative_denials_retained=True,independent_two_precollection_fd12_denials_retained=True,
 no_whole_task_zero_network_or_outside_write_claim=True,iterator_and_earlier_derived_error_timing_changes_disclosed=True,
 raw_provider_copy_full_hashes_rely_on_observed_separate_independent_postexit_not_rehashed_by_root=True,
 no_corpus_or_full_suite_or_other_country_output_certification=True,
 root_no_project_import_payload_decode_API_test_or_protection_replay=True,
 root_checker_sha256=digest(Path(__file__)),root_initial_metadata_count_stop_preserved=True,
 comparator_fault_count_split=dict(frame_faults=12,metadata_faults=2),human_merge_required=True,signed='Sue')
dest=C/'1210-INDEPENDENT-ROOT-ACCEPTANCE.json';assert not dest.exists();dest.write_text(json.dumps(out,indent=2)+'\n');dest.chmod(0o600)
print(json.dumps(out))
