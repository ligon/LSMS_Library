"""Authenticate immutable Uganda1205 author handoff, not production acceptance."""
from pathlib import Path
import datetime, hashlib, json, os, re, stat, subprocess
C=Path('/local/job40255479/control/isa-backlog-20261021')
P=C/'uganda1205-diagnosis'
R=Path('/local/job40255479/worktrees/isa-Uganda-1205')
def sha(p,kind='sha256'):
    with p.open('rb') as f:return hashlib.file_digest(f,kind).hexdigest()
def load(p):return json.loads(p.read_text())
assert sha(P/'READY.json')=='08f1c4b622e7de3fb455580cd7455d712d2e54dd4be4bb5d489114bd96d81a1e'
assert sha(P/'manifest.json')=='06ce0a721a28472c0dacf696c3f7201c5f5e7697e1e6744bea27586ac414cdf4'
assert sha(P/'EXPORT-ALLOWLIST.json')=='8bbf8b49cc1e616ea2c17cc0ae181c944126ffed25ac2086bc6d3ee96a705f37'
ready=load(P/'READY.json');m=load(P/'manifest.json')
assert ready['source_head']=='884775a461d769fd4bb5181005ca3238513b5b01' and ready['initial_parent']=='491daf5654b9ee45332bec97e1296ae821ff46d5'
assert len(m['files'])==ready['manifest_files']==302 and (P/'manifest.json').stat().st_size==ready['manifest_bytes']==113871
assert {str(q.relative_to(P)) for q in P.rglob('*') if q.is_file() or q.is_symlink()}=={r['relative'] for r in m['files']}|set(m['omitted_self_files'])
raw=0
for row in m['files']:
    q=P/row['relative'];s=q.lstat();link=os.readlink(q) if q.is_symlink() else None
    assert [s.st_size,stat.S_IMODE(s.st_mode),s.st_mtime_ns,s.st_nlink,s.st_ino,s.st_dev,q.is_symlink(),link]==[row['stat_bytes'],row['mode'],row['mtime_ns'],row['nlink'],row['inode'],row['device'],row['symlink'],row['link_text']]
    digest=hashlib.sha256(link.encode()).hexdigest() if link is not None else sha(q)
    assert digest==row['sha256'] and (len(link.encode()) if link is not None else s.st_size)==row['bytes']
    if 'dvc-cache' in q.parts and re.fullmatch('[0-9a-f]{30}',q.name):raw+=1
assert raw==53
allow=load(P/'EXPORT-ALLOWLIST.json')['paths'];assert len(allow)==20
assert set(allow)<={r['relative'] for r in m['files']}|{'READY.json','EXPORT-ALLOWLIST.json'}
env=dict(os.environ,GIT_OPTIONAL_LOCKS='0')
def git(*args):return subprocess.check_output(['git','--no-optional-locks',*args],cwd=R,env=env)
assert git('rev-parse','HEAD').decode().strip()==ready['source_head'] and git('status','--porcelain')==b''
assert git('diff','--name-only',ready['initial_parent'],ready['source_head']).decode().splitlines()==[
 '.coder/ledger/1205-uganda-parcel-area.md','lsms_library/countries/Uganda/_/CONTENTS.org',
 'lsms_library/transformations.py','tests/test_gh1205_uganda_parcel_prefix.py']
assert sha(R/'lsms_library/transformations.py')==ready['runtime_transformations_sha256']=='6224948d6fe36295398fba224531e8e7bb199322bd4d66103e3e8682439e245e'
census=P/'provider-metadata-census-private.json';assert sha(census)=='f8604562167e802a8f84bb335dd632b121aaedff44a16d12fadb0f9cf2f38278'
sources=load(census)['selected'];assert len(sources)==53
for row in sources:
    q=Path(row['selected']['path']);s=q.lstat();e=row['selected']
    assert [s.st_size,stat.S_IMODE(s.st_mode),s.st_mtime_ns,s.st_nlink,s.st_dev,s.st_ino,q.is_symlink()]==[e['size'],e['mode'],e['mtime_ns'],e['nlink'],e['device'],e['inode'],e['symlink']]
    assert row['declared']['declared_binary_stata'] and sha(q,'md5')==row['declared']['declared_md5'] and sha(q)==row['expected_sha256']
term=C/'uganda1205-diagnosis-seal-postexit-records.json';records=load(term)
assert sha(term)=='87977d7321e63e7760e23f7e77781eb52b4486356cd873adaca2201d8777539d'
assert records['READY_sha256']==sha(P/'READY.json') and records['manifest_sha256']==sha(P/'manifest.json')
assert len(records['records'])==2
for row in records['records']:
    session=row['call']['session_id'];returned=row['returned'];text=returned['output'];encoded=text.encode()
    assert returned['exit_code']==0 and session in {32130,86007}
    assert len(encoded)==row['stdout_utf8_receipt']['bytes'] and hashlib.sha256(encoded).hexdigest()==row['stdout_utf8_receipt']['sha256']
    value=json.loads(text);assert value['READY_sha256']==sha(P/'READY.json') and value['manifest_sha256']==sha(P/'manifest.json')
    if session==86007:
        assert returned['chunk_id']=='c237ec' and value['status']=='POSTEXIT_READ_ONLY_VERIFIED'
        assert value['all_hashes_and_physical_receipts_exact'] and value['file_set_exact'] and value['live_guarded_actors']==[] and value['postexit_writes']==0
    else: assert returned['chunk_id']=='5f5dff' and value['source_head']==ready['source_head']
out=dict(status='ROOT_AUTHENTICATED_UGANDA1205_AUTHOR_HANDOFF_NOT_PRODUCTION_ACCEPTANCE',
 at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_head=ready['source_head'],parent=ready['initial_parent'],
 READY_sha256=sha(P/'READY.json'),manifest_sha256=sha(P/'manifest.json'),allowlist_sha256=sha(P/'EXPORT-ALLOWLIST.json'),
 complete_manifest_files_content_and_physical_checked=302,raw_private_L1_entries_rehashed=53,
 genuine_current_binary_providers_MD5_SHA_and_physical_checked=53,allowlisted_files=20,
 observed_original_seal_postexit_records_sha256=sha(term),
 author_tests_and_source_oracle_are_claims_for_independent_verification=True,
 diagnostic_stops_frequency_scope_and_private_output_exception_retained=True,
 root_no_project_import_payload_decode_API_test_or_data_build_replay=True,
 root_checker_sha256=sha(Path(__file__)),signed='Sue')
dest=C/'1205-AUTHOR-HANDOFF-ROOT.json';assert not dest.exists();dest.write_text(json.dumps(out,indent=2)+'\n');dest.chmod(0o600)
print(json.dumps(out))
