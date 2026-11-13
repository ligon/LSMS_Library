"""Independent root opaque archive31 member/source/terminal-chain check. Sue."""
from pathlib import Path
import datetime,decimal,hashlib,json,os,stat,sys,tarfile
assert sys.flags.no_site and os.getuid()==43292
C=Path('/local/job40255479/control/isa-backlog-20261021');A=C/'archive31-control';D=Path('/global/scratch/fsa/fc_jevons/ligon/isa-backlog-evidence/2026-10-21-job40255479');STEM='quiescent-followup-thirty-first'
def load(p):return json.loads(Path(p).read_text())
def sha(p):
    with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def rec(p,content=True,directory=False):
    p=Path(p);s=p.lstat();r=dict(bytes=s.st_size,mode=stat.S_IMODE(s.st_mode),mtime_ns=s.st_mtime_ns,inode=s.st_ino,device=s.st_dev,nlink=s.st_nlink,uid=s.st_uid,gid=s.st_gid,symlink=stat.S_ISLNK(s.st_mode))
    if directory:assert stat.S_ISDIR(s.st_mode) and not r['symlink'];r.pop('symlink')
    elif r['symlink']:r['link_target']=os.readlink(p)
    else:
        assert stat.S_ISREG(s.st_mode) and (s.st_nlink==1 or not content)
        if content:r['sha256']=sha(p)
    t=p.lstat();assert all(getattr(s,k)==getattr(t,k) for k in ('st_mode','st_size','st_mtime_ns','st_ino','st_dev','st_nlink','st_uid','st_gid'))
    return r
assert sha(A/'archive31-plan-v2-private.json')=='1c11f5ee3f2f43a6c57896bf828ff9910f600026341ed15f860c1ba0d43a41c9'
assert sha(A/'archive31-terminal-and-readback-receipt-private.json')=='ab5e4b3900a484b9af3667fc9d48a187110eb3db13f9847645fd5e589fc3da2e'
assert sha(C/'archive31-original-writer-postexit-tool-records.json')=='68ef885cc6ae93b4575687114531e57a9ada3bb0df2b687b8d47694e17400dd4'
p=load(A/'archive31-plan-v2-private.json');t=load(A/'archive31-terminal-and-readback-receipt-private.json');w=load(C/'archive31-original-writer-postexit-tool-records.json');approval=load(A/'ROOT-WRITE-APPROVAL.json')
assert approval['approved_plan_sha256']==t['approved_plan_sha256']==w['approved_plan_sha256']==sha(A/'archive31-plan-v2-private.json')
assert t['writer_session']==65825 and t['writer_terminal_chunk']=='a484a6' and t['writer_original_Popen_pid']==82005 and t['writer_exit_code']==t['child_verifier_exit_code']==0
assert t['postexit_session']==54540 and t['postexit_terminal_chunk']=='f250ac' and t['postexit_actual_pid']==90848 and t['postexit_exit_code']==0
for k,r in t['durable_artifacts'].items():
    assert rec(Path(p['destination_paths'][k]))==r and r['mode']==0o600 and r['uid']==43292 and r['nlink']==1
assert t['durable_artifacts']['archive']['sha256']=='1cc296cdc30b662eebe914d8e2362018d7bdeb11ca2f96bded131d996df2f115' and t['durable_artifacts']['archive']['bytes']==326181834
assert t['durable_artifacts']['manifest']['sha256']=='385744cfc8598ba5fc6d8e02e25fc3ac7d306bdb9b9a17def78e1e6a80c5a6d7'
m=load(D/(STEM+'.manifest.private.json'));audit=m['audit']
for k in ('files','excluded_entries','exact_native_operand_exceptions','source_directories','operand_counts','preserved_serialized_operand_counts','serialized_operand_classification'):assert m[k]==p[k],k
assert len(m['files'])==2386 and len(m['excluded_entries'])==692 and len(m['exact_native_operand_exceptions'])==241 and sum(m['preserved_serialized_operand_counts'].values())==585
assert sum(r['bytes'] for r in m['files'].values())==1684286201 and m['uncompressed_tar_bytes']==1688627200<p['cap_bytes']==8589934592
assert audit['approved_plan_source_text']==(A/'archive31-plan-v2-private.json').read_text() and audit['root_approval_source_text']==(A/'ROOT-WRITE-APPROVAL.json').read_text() and audit['configuration_source_text']==(A/'configuration-private.json').read_text()
for k,s in audit['controller_source_texts'].items():assert s==(A/('archive31-'+k+'.py')).read_text() and hashlib.sha256(s.encode()).hexdigest()==m['controller_receipts'][k]['sha256']
for q,r in audit['input_receipts'].items():assert rec(q)==r and Path(q).read_text()==audit['policy_and_qualification_source_texts'][Path(q).name]
assert audit['preparation_controller_source_text']==(A/'prepare_archive31.py').read_text()
names=[]
with tarfile.open(m['archive'],'r|gz') as tf:
    for item in tf:
        n=item.name;assert n in m['files'] and n not in names and not Path(n).is_absolute() and '..' not in Path(n).parts;names.append(n);r=m['files'][n]
        assert item.isfile() and not r['symlink'] and (item.size,item.mode,item.uid,item.gid)==(r['bytes'],r['mode'],r['uid'],r['gid'])
        assert int(decimal.Decimal(item.pax_headers['mtime'])*1000000000)==r['mtime_ns']
        f=tf.extractfile(item);assert f is not None and hashlib.file_digest(f,'sha256').hexdigest()==r['sha256']
assert names==list(m['files'])
for n,r in m['files'].items():assert rec(C/n)==r,n
for r in m['excluded_entries']:assert rec(C/r['path'],False)==r['physical']
for root,rows in m['source_directories'].items():
    q=C/root;actual={'.':rec(q,False,True),**{str(z.relative_to(q)):rec(z,False,True) for z in q.rglob('*') if z.is_dir() and not z.is_symlink()}};assert actual==rows
    expected={n for n in m['files'] if n.startswith(root+'/')}|{r['path'] for r in m['excluded_entries'] if r['path'].startswith(root+'/')}
    assert {str(z.relative_to(C)) for z in q.rglob('*') if z.is_file() or z.is_symlink()}==expected
calls={r['stage']:r for r in w['calls']};assert len(calls)==5
for row in calls.values():
    b=row['result']['output'].encode();assert len(b)==row['stdout_bytes'] and hashlib.sha256(b).hexdigest()==row['stdout_sha256']
assert calls['writer_initial']['result']['session_id']==calls['writer_poll1']['result']['session_id']==65825
assert calls['writer_terminal']['result']['exit_code']==calls['postexit_terminal']['result']['exit_code']==0
assert calls['writer_terminal']['result']['chunk_id']=='a484a6' and calls['postexit_terminal']['result']['chunk_id']=='f250ac'
for n,r in w['contemporaneous_private_records'].items():assert (A/n).read_text()==r['exact_text'] and sha(A/n)==r['sha256'] and load(A/n)==r['parsed_with_integer_preserving_python']
for n,r in w['new_controller_source_records'].items():assert (A/n).read_text()==r['exact_source_text'] and sha(A/n)==r['sha256']
launch=load(A/'archive31-writer-original-launch-private.json');term=load(A/'archive31-writer-original-terminal-private.json');child=load(A/'archive31-verifier-terminal-private.json')
assert launch['original_Popen_pid']==term['original_Popen_pid']==82005 and term['actual_returncode']==0 and launch['original_uid']==43292
assert child['exit_code']==0 and child['stderr']=='' and json.loads(child['stdout'])['files_verified']==2386
assert term['child_verifier_pid'] is None and t['child_verifier_PID_not_exposed']
assert hashlib.sha256(t['full_separate_postexit_stdout'].encode()).hexdigest()==t['postexit_stdout_sha256']
post=json.loads(t['full_separate_postexit_stdout']);assert post['actual_postexit_pid']==90848 and post['readonly_writes']==0 and not post['active_consumers']
dest=rec(D,False,True)
assert {k:dest[k] for k in p['destination_receipt']}==p['destination_receipt'] and dest['mode']==0o2700
out=dict(status='ROOT_ARCHIVE31_ACCEPTED',at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),archive_sha256=t['durable_artifacts']['archive']['sha256'],archive_bytes=326181834,manifest_sha256=t['durable_artifacts']['manifest']['sha256'],files=2386,excluded_entries=692,native_parquets=241,serialized_operands=585,included_source_bytes=1684286201,uncompressed_tar_bytes=1688627200,original_writer_exit=0,original_child_verifier_exit=0,separate_readonly_postexit_exit=0,root_full_opaque_member_and_source_after_hashes_exact=True,existing_private_directory_SGID02700_and_four_fresh_owner0600_preserved=True,raw_binary_sources_excluded_with_metadata=True,source_derived_raw_table_pickles_private_not_public=True,archival_not_new_product_or_science_acceptance=True,no_payload_decode_API_test_or_original_controller_execution=True,checker_sha256=sha(__file__),signed='Sue')
q=C/'ARCHIVE31-ROOT-ACCEPTANCE.json';fd=os.open(q,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f:json.dump(out,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
print(json.dumps(out))
