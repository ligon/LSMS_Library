"""Independent root archive30 opaque-byte acceptance. No product imports. Sue."""
from pathlib import Path
import datetime, decimal, hashlib, json, os, stat, sys, tarfile
assert sys.flags.no_site and os.getuid() == os.geteuid() == 43292
C = Path('/local/job40255479/control/isa-backlog-20261021')
A = C / 'archive30-control'
D = Path('/global/scratch/fsa/fc_jevons/ligon/isa-backlog-evidence/2026-10-21-job40255479')
STEM = 'quiescent-followup-thirtieth'
def read(p): return json.loads(Path(p).read_text())
def digest(p):
    with Path(p).open('rb') as f: return hashlib.file_digest(f, 'sha256').hexdigest()
def receipt(p, content=True):
    p = Path(p); s = p.lstat()
    r = dict(bytes=s.st_size, mode=stat.S_IMODE(s.st_mode), mtime_ns=s.st_mtime_ns,
             inode=s.st_ino, device=s.st_dev, nlink=s.st_nlink, uid=s.st_uid,
             gid=s.st_gid, symlink=p.is_symlink())
    if p.is_symlink(): r['link_target'] = os.readlink(p)
    else:
        assert stat.S_ISREG(s.st_mode) and s.st_nlink == 1
        if content: r['sha256'] = digest(p)
    t = p.lstat()
    assert all(getattr(s,k) == getattr(t,k) for k in ('st_mode','st_size','st_mtime_ns','st_ino','st_dev','st_nlink','st_uid','st_gid'))
    return r
def directory(p, full=False):
    p = Path(p); s = p.lstat(); assert stat.S_ISDIR(s.st_mode) and not p.is_symlink()
    r = dict(mode=stat.S_IMODE(s.st_mode), uid=s.st_uid, gid=s.st_gid, device=s.st_dev, inode=s.st_ino)
    if full: r.update(bytes=s.st_size, mtime_ns=s.st_mtime_ns, nlink=s.st_nlink)
    return r
assert digest(A/'archive30-plan-private.json') == '8e0ab9e10d1513966b37f04d50472d3bd3fb60ded9b193f2ef6d3a638d193186'
assert digest(A/'ROOT-WRITE-APPROVAL.json') == '7c1d3290e984b9af25e895e4e229ff4bc496bc1d7b968b6a0f9acb8b160d8b0e'
assert digest(A/'archive30-terminal-and-readback-receipt-private.json') == 'a74732468e7284d5e3bea68d7db63277c97f392c548c5e63ec52821370d25899'
p = read(A/'archive30-plan-private.json'); approval = read(A/'ROOT-WRITE-APPROVAL.json')
t = read(A/'archive30-terminal-and-readback-receipt-private.json')
assert directory(D) == p['destination_receipt'] == t['destination']
assert directory(D)['mode'] == 0o2700
for name,r in t['durable'].items():
    assert receipt(D/name) == r
    assert r['mode'] == 0o600 and r['uid'] == 43292
assert len(t['durable']) == 4
m = read(D/(STEM+'.manifest.private.json')); audit = m['audit']
assert json.loads(audit['approved_plan_source_text']) == p
assert json.loads(audit['root_approval_source_text']) == approval
assert audit['approved_plan_source_text'] == (A/'archive30-plan-private.json').read_text()
assert audit['root_approval_source_text'] == (A/'ROOT-WRITE-APPROVAL.json').read_text()
assert audit['configuration_source_text'] == (A/'configuration-private.json').read_text()
assert m['archive_receipt'] == t['durable'][STEM+'.tar.gz']
assert m['plan_sha256'] == approval['approved_plan_sha256']
for key in ('files','excluded_entries','exact_native_operand_exceptions','source_directories','operand_counts','preserved_serialized_operand_counts'):
    assert m[key] == p[key], key
for key,text in audit['controller_source_texts'].items():
    assert text == (A/('archive30-'+key+'.py')).read_text()
    assert hashlib.sha256(text.encode()).hexdigest() == m['controller_receipts'][key]['sha256']
for path,r in audit['input_receipts'].items():
    assert receipt(path) == r
    assert Path(path).read_text() == audit['policy_and_qualification_source_texts'][Path(path).name]
assert audit['preparation_controller_source_text'] == (A/'prepare_archive30.py').read_text()
assert len(m['files']) == 1830 and len(m['excluded_entries']) == 866
assert len(m['exact_native_operand_exceptions']) == 290
assert sum(m['preserved_serialized_operand_counts'].values()) == 166
assert sum(v['bytes'] for v in m['files'].values()) == 1656851816
assert m['uncompressed_tar_bytes'] == 1660190720 < p['cap_bytes'] == 8589934592
assert set(m['exact_native_operand_exceptions']) <= set(m['files'])
names = []
with tarfile.open(m['archive'], 'r|gz') as tf:
    for member in tf:
        n = member.name; assert n in m['files'] and n not in names
        assert not Path(n).is_absolute() and '..' not in Path(n).parts
        names.append(n); r = m['files'][n]
        assert member.isfile() and not r['symlink']
        assert (member.size,member.mode,member.uid,member.gid) == (r['bytes'],r['mode'],r['uid'],r['gid'])
        assert int(decimal.Decimal(member.pax_headers['mtime'])*1000000000) == r['mtime_ns']
        f = tf.extractfile(member); assert f is not None
        assert hashlib.file_digest(f,'sha256').hexdigest() == r['sha256']
assert names == list(m['files'])
for n,r in m['files'].items(): assert receipt(C/n) == r, n
for row in m['excluded_entries']: assert receipt(C/row['path'],False) == row['physical']
for root,rows in m['source_directories'].items():
    q = C/root
    current = {'.':directory(q,True), **{str(z.relative_to(q)):directory(z,True) for z in q.rglob('*') if z.is_dir() and not z.is_symlink()}}
    assert current == rows
    currentfiles = {str(z.relative_to(C)) for z in q.rglob('*') if z.is_file() or z.is_symlink()}
    expected = {n for n in m['files'] if n.startswith(root+'/')} | {v['path'] for v in m['excluded_entries'] if v['path'].startswith(root+'/')}
    assert currentfiles == expected
for name,row in t['streams'].items():
    assert receipt(A/name) == row['receipt']
    assert (A/name).read_text() == row['exact_utf8_text']
assert receipt(t['writer_tool_record']['path']) == t['writer_tool_record']['receipt']
assert t['writer_tool_records'] == read(t['writer_tool_record']['path'])
w = t['writer_tool_records']; assert w['initial_exec_return']['session_id'] == 52314
assert w['terminal_write_stdin_return']['exit_code'] == 0
assert w['terminal_write_stdin_return']['chunk_id'] == 'e3f324'
assert w['terminal_write_stdin_return']['output'] == ''
child = read(A/'archive30-verifier-terminal-private.json')
assert child['exit_code'] == 0 and child['stderr'] == ''
assert json.loads(child['stdout'])['files_verified'] == 1830
assert json.loads((A/'write-private.log').read_text()) == read(A/'archive30-safe.json') == t['safe_summary']
assert directory(D) == t['destination']
assert receipt(D/(STEM+'.tar.gz')) == m['archive_receipt']
out = dict(status='ROOT_ARCHIVE30_ACCEPTED', at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),
           archive_sha256=m['archive_receipt']['sha256'], archive_bytes=m['archive_receipt']['bytes'],
           manifest_sha256=digest(D/(STEM+'.manifest.private.json')), files=1830, native_parquets=290,
           serialized_operands=166, excluded_entries=866, included_bytes=1656851816, uncompressed_tar_bytes=1660190720,
           original_writer_exit=0, original_child_verifier_exit=0, root_full_opaque_members_and_source_after_exact=True,
           destination_SGID02700_and_four_owner0600_preserved=True, no_product_or_historical_controller_execution=True,
           checker_sha256=digest(__file__), signed='Sue')
q=C/'ARCHIVE30-ROOT-ACCEPTANCE.json'; fd=os.open(q,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f: json.dump(out,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
print(json.dumps(out))
