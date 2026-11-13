"""Authenticate independent GH1204 evidence as opaque bytes and metadata. Sue."""
from pathlib import Path
import datetime, hashlib, json, os, stat, subprocess, sys, xml.etree.ElementTree as ET
assert sys.flags.no_site and os.getuid() == 43292
C = Path('/local/job40255479/control/isa-backlog-20261021'); P = C/'review-Uganda-1204'
def load(p): return json.loads(Path(p).read_text())
def sha(p, algorithm='sha256'):
    with Path(p).open('rb') as f: return hashlib.file_digest(f,algorithm).hexdigest()
def rec(p, content=True):
    p=Path(p);s=p.lstat(); typ='symlink' if stat.S_ISLNK(s.st_mode) else 'file' if stat.S_ISREG(s.st_mode) else 'directory' if stat.S_ISDIR(s.st_mode) else 'other'
    assert typ!='other'
    r=dict(type=typ,mode=stat.S_IMODE(s.st_mode),mtime_ns=s.st_mtime_ns,nlink=s.st_nlink,device=s.st_dev,inode=s.st_ino,uid=s.st_uid,gid=s.st_gid,symlink=typ=='symlink',link=None)
    if typ=='symlink':
        b=os.fsencode(os.readlink(p));r.update(link=os.readlink(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest(),hash_semantics='link-text')
    elif typ=='file':
        r.update(bytes=s.st_size,hash_semantics='file-bytes')
        if content:r['sha256']=sha(p)
    after=p.lstat(); assert all(getattr(s,k)==getattr(after,k) for k in ('st_dev','st_ino','st_size','st_mode','st_mtime_ns','st_nlink','st_uid','st_gid'))
    return r
def check(p, expected, content=True):
    actual=rec(p,content)
    for k,v in expected.items(): assert actual[k]==v,(str(p),k)
    return actual
pins={'READY.json':'90150bdbef98d970e6ddc8868bbab8af0450cbd221193182ddb4f9d1f5c7f3b0',
      'final-private-manifest.json':'d1c74be6d05155eaa84829867dad5dfd1881af729bd32c4f6042df02153f1f24',
      'safe-exports/ALLOWLIST.json':'a62dc738a5dfbcd2c10bbba62754a3441cab645a1f278e63cbed0b33f01b6796'}
for n,s in pins.items():assert sha(P/n)==s
ready=load(P/'READY.json');m=load(P/'final-private-manifest.json');allow=load(P/'safe-exports/ALLOWLIST.json')['files']
assert ready['candidate']=='a417b69d71c460ab33f6a494b4df5d67cd792de5' and ready['actual_parent']=='491daf5654b9ee45332bec97e1296ae821ff46d5'
assert len(m['entries'])==ready['manifest_entries']==3539
actual={'.'}|{str(q.relative_to(P)) for q in P.rglob('*')}
assert actual==set(m['entries'])|set(m['exact_control_exclusions'])
for n,row in m['entries'].items():check(P/n,row)
check(P/'final-private-manifest.json',ready['manifest_receipt'])
assert sum(r['type']=='file' for r in m['entries'].values())==2166
assert sum(r.get('bytes',0) for r in m['entries'].values() if r['type']=='file')==1959358157
assert set(allow)=={'REPORT.md','commands.json','empty-order-residual.json','pins.json','protection.json','qualifications.json','source.diff','validation.json'}
for n,r in allow.items():check(P/'safe-exports'/n,r)
env=dict(os.environ,GIT_OPTIONAL_LOCKS='0'); source=load(P/'phase0-full-source-receipts.json')['roots']; count=0
for role,row in source.items():
    root=Path(row['root'])
    def git(*args):return subprocess.check_output(['git','--no-optional-locks',*args],cwd=root,env=env)
    assert git('rev-parse','HEAD').decode().strip()==row['head']
    assert git('status','--porcelain')==b''
    assert set(git('ls-files','-z').decode().rstrip('\0').split('\0'))==set(row['files'])
    for n,r in row['files'].items():check(root/n,r)
    count+=len(row['files'])
assert count==29825
candidate=Path(source['candidate']['root'])
assert subprocess.check_output(['git','--no-optional-locks','diff',ready['actual_parent'],ready['candidate']],cwd=candidate,env=env)==(P/'safe-exports/source.diff').read_bytes()
providers=load(P/'providers-and-copies-private.json'); assert len(providers['providers'])==37 and len(providers['copies'])==444 and providers['bytes']==533307636
unique=set()
for r in providers['providers']:
    now=check(r['source'],r['source_receipt']);assert not now['symlink'] and now['nlink']==1
    assert sha(r['source'])==r['sha256'] and sha(r['source'],'md5')==r['md5']
    assert sha(r['sidecar'])==r['sidecar_sha256']
    if r['declared_size'] is not None:assert now['bytes']==r['declared_size']
    with Path(r['source']).open('rb') as f:h=f.read(16)
    assert h.startswith(b'<stata_dta>') or (h and h[0] in range(104,119))
    unique.add((now['device'],now['inode']))
for r in providers['copies']:
    now=check(r['path'],r['receipt']);assert not now['symlink'] and now['nlink']==1 and now['mode']==0o600
    assert (now['device'],now['inode']) not in unique;unique.add((now['device'],now['inode']))
    assert sha(r['path'])==r['sha256'] and sha(r['path'],'md5')==r['md5']
for actor,tests,failed,code in [('tests-candidate-v2',8,0,0),('tests-base-v2',8,7,1),('tests-existing',170,0,0),('tests-complete-grain',2,0,0)]:
    a=P/'actors'/actor;term=load(a/'terminal.json');pop=load(a/'original-Popen.json')
    assert term['actual_exit_code']==code and term['original_Popen_pid']==pop['original_Popen_pid']
    for stream in ('stdout.txt','stderr.txt'):assert sha(a/stream)==term[stream]['sha256']
    cases=ET.parse(a/'junit.xml').getroot().findall('.//testcase')
    assert len(cases)==tests and sum(x.find('failure') is not None for x in cases)==failed
    assert all(x.find('error') is None and x.find('skipped') is None for x in cases)
    identities=list(a.glob('identity-pytest-session-before-collection-*.json'));assert len(identities)==2
    role='base' if actor=='tests-base-v2' else 'candidate';root=Path(source[role]['root'])
    for path in identities:
        i=load(path);assert Path(i['cwd'])==root and Path(i['package'])==root/'lsms_library/__init__.py' and Path(i['countries_root'])==root/'lsms_library/countries'
        assert i['startup_no_project_imports'] and i['net_namespace']!=i['original_parent_net_namespace'] and i['make_jobs_flag'] is None
        assert all(v=='1' for k,v in i['environment'].items() if k!='MAKEFLAGS') and i['environment']['MAKEFLAGS']=='-j1'
        dvc=load(a/('DVC-identity-pytest-session-before-collection-'+str(i['pid'])+'.json'))
        assert all(os.path.realpath(v).startswith(str(P)+os.sep) for v in dvc.values())
for side,n in [('base',29),('candidate',30)]:
    grid=load(P/('grid-'+side+'-terminal.json'));assert grid['status']=='ALL_PLANNED_ACTORS_TERMINAL0' and grid['count']==len(grid['accepted'])==n
    for name in grid['accepted']:
        a=P/'actors'/name;t=load(a/'terminal.json');o=load(a/'original-Popen.json');assert t['actual_exit_code']==0 and t['original_Popen_pid']==o['original_Popen_pid']
        for stream in ('stdout.txt','stderr.txt'):assert sha(a/stream)==t[stream]['sha256']
base=load(P/'actors/prebuild-base/inventory.json');cand=load(P/'actors/prebuild-candidate/inventory.json')
for field,total,changed in [('country_hashes',549,28),('framework_hashes',39,0),('wave_hashes',1362,171)]:
    b=base[field];c=cand[field];assert set(b)==set(c) and len(b)==total
    delta=[k for k in b if b[k]!=c[k]];assert len(delta)==changed and all(k.startswith('Uganda/') for k in delta)
assert base['public_names']==cand['public_names'] and base['materialization']==cand['materialization']
strict=load(P/'strict-comparison-private.json');v=load(P/'safe-exports/validation.json');art=load(P/'artifacts-private.json')
assert len(strict['pairs'])==strict['strict_pairs']==113 and strict['strict_pass']==109 and strict['strict_fail']==4
assert sum(r['status']=='PASS' for r in strict['pairs'])==109 and sum(r['status']=='FAIL' for r in strict['pairs'])==4
assert all(r['expected_empty_order'] and r['left_rows']==r['right_rows']==0 for r in strict['pairs'] if r['status']=='FAIL')
assert len(strict['native_footer_pairs'])==21 and all(r['full_footer_bytes_equal'] and r['exact_native_producer_source_equal'] for r in strict['native_footer_pairs'])
assert strict['scanner_literal_ordered_records_equal']==22 and strict['held_nonempty_crop_grain_ledgers']==0 and not strict['normalization_casting_reordering']
assert strict['original_pre_serialization_strict_FAIL_preserved'] and strict['native_oracle_uses_exact_existing_local_walk_and_actual_saved_native_operands']
assert v['candidate_targeted']['passed']==8 and v['actual_parent_same_targeted']['failed']==7 and v['supporting_tests']['passed']==170 and v['complete_nonempty_synthetic_grain_transport']['passed']==2
assert art['all_rows']==133761 and art['supported_rows']==15630 and art['null_v_rows']==928 and art['null_production_obs']==0
assert art['candidate_cold_barren_no_CROP_cache_writes'] and art['candidate_cold_barren_SAMPLE_cache_writes']==9
protection=load(P/'protection-private.json');assert protection['tracked_source_receipts_exact']==29825 and protection['copies_MD5_SHA_physical_exact']==444
assert protection['unexpected_guard_write_events']==0 and protection['guard_write_events']==86 and protection['guard_network_events']==283
assert protection['nonseed_unattributed_IPv6_loopback_binds']==273 and not protection['whole_task_zero_outside_prefix_write_or_network_attempt_claim']
assert not protection['live_consumers_before'] and not protection['live_consumers_after']
assert sha(P/'guard-private.jsonl')==protection['guard_log']['sha256']
record=C/'1204-independent-seal-postexit-tool-records.json';assert sha(record)=='9d333775e2288ad8c0b3d89d6d67f42f686e7d9ff8b0a304e17b5bbeee4379af'
tool=load(record);returns=tool['original_tool_returns'];inputs=tool['original_tool_inputs']
for stage,session in [('sealer',85807),('postexit',60718)]:
    assert inputs[stage+'_write_stdin']['session_id']==session
    assert returns['original_'+stage+'_terminal_return']['exit_code']==0
for stage,filename,pid in [('sealer','seal-independent.py',4128035),('separate_postexit','postexit-independent.py',4129550)]:
    row=tool['original_source_PID_UID_time_bindings_from_verbatim_stdout'][stage]
    assert row['literal_pid']==pid and row['literal_parent_pid']==3753746 and row['original_uid']==43292 and row['source_sha256']==sha(P/filename)
    assert row['READY_sha256']==pins['READY.json'] and row['manifest_sha256']==pins['final-private-manifest.json'] and row['live_consumers_before_after']==0
assert load(C/'1204-root-export-privacy-check.json')['matching_safe_export_files']==[]
for n,s in pins.items():assert sha(P/n)==s
out=dict(status='ROOT_ACCEPTED_BOUNDED_GH1204_REPAIR_WITH_KNOWN_GH1215_FAILURES',at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_head=ready['candidate'],actual_parent=ready['actual_parent'],READY_sha256=pins['READY.json'],manifest_sha256=pins['final-private-manifest.json'],allowlist_sha256=pins['safe-exports/ALLOWLIST.json'],manifest_entries=3539,payload_files=2166,tracked_source_receipts=29825,genuine_providers=37,distinct_binary_copies=444,copied_binary_bytes=533307636,targeted_pass=8,parent_fail=7,parent_pass=1,supporting_pass=170,complete_nonempty_synthetic_pass=2,original_grid_actors=59,strict_pairs=113,strict_pass=109,strict_new_empty_column_order_failures=4,residual_issue=1215,native_footer_source_pairs=21,held_real_nonempty_grain_records=0,original_history_privacy_scope_DVC_denial_network_and_mode_qualifications_retained=True,root_export_privacy_scan_sha256=sha(C/'1204-root-export-privacy-check.json'),full_corpus_or_science_certification=False,human_merge_required=True,root_no_payload_decode_API_test_or_controller_reexecution=True,checker_sha256=sha(__file__),signed='Sue')
q=C/'1204-INDEPENDENT-ROOT-ACCEPTANCE.json';fd=os.open(q,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f:json.dump(out,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
print(json.dumps(out))
