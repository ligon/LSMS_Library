"""Authenticate archive29 receipt and opaque compressed bytes, without replay."""
from pathlib import Path
import datetime
import hashlib
import json
import os
import stat

C = Path('/local/job40255479/control/isa-backlog-20261021')
A = C / 'archive29-control'
S = Path(__file__).resolve().parent.parent


def load(path):
    return json.loads(path.read_text())


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


safe = load(A / 'archive29-safe.json')
plan_path = A / 'archive29-plan-private.json'
assert sha(plan_path) == safe['plan_sha256'] == 'e0c8799dac9e639f414ad7b6188211c54f11ed8c36de8fd5c100b11748a20a56'
plan = load(plan_path)
paths = {k: Path(v) for k, v in plan['destination_paths'].items()}
assert sha(paths['archive']) == safe['archive_sha256'] == '06f48a8e4f0ca5365c55aa4a60dec68e581730a6dfca8e6532c39d1c1e8f3f12'
assert paths['archive'].stat().st_size == safe['archive_bytes'] == 163291151
assert sha(paths['manifest']) == safe['manifest_sha256'] == '1e951c78cf1611035ea429e97995d86e5b759fe607c2d1ad8e790fdf27fae5b3'
manifest = load(paths['manifest'])
assert len(manifest['files']) == len(plan['files']) == safe['files_verified'] == 416
for key in ['files', 'excluded_entries', 'exact_native_operand_exceptions']:
    assert manifest[key] == plan[key]
assert len(manifest['exact_native_operand_exceptions']) == 20
assert len(manifest['excluded_entries']) == 549
assert manifest['uncompressed_tar_bytes'] == safe['uncompressed_tar_bytes'] == 906414080 < plan['cap_bytes']
audit = manifest['audit']
assert audit['approved_plan_source_text'] == plan_path.read_text()
assert audit['root_approval_source_text'] == (A / 'ROOT-WRITE-APPROVAL.json').read_text()
assert sha(A / 'ROOT-WRITE-APPROVAL.json') == 'f2bcd15b68df5fb20703b7a31405df69ac53a3b9ad894ed265d16049595c48e7'
for kind in ['launcher', 'verifier']:
    assert sha(paths[kind]) == plan['controller_receipts'][kind]['sha256']
    assert paths[kind].read_text() == audit['controller_source_texts'][kind]
for path in paths.values():
    st = path.lstat()
    assert stat.S_ISREG(st.st_mode) and stat.S_IMODE(st.st_mode) == 0o600
    assert st.st_nlink == 1 and st.st_uid == os.getuid() == 43292
st = Path(plan['destination']).lstat()
actual = dict(mode=stat.S_IMODE(st.st_mode), uid=st.st_uid, gid=st.st_gid,
              inode=st.st_ino, device=st.st_dev)
assert actual == plan['destination_receipt'] and actual['mode'] == 0o2700
tool_path = A / 'archive29-write-tool-records.json'
assert sha(tool_path) == '285fab9e6ef5789cf673cbee4884cf6f5f9c6faed46ae45ea32146ba2943a38c'
tool = load(tool_path)
last = tool['records'][-1]
assert last['call']['session_id'] == 38595
assert last['returned']['exit_code'] == 0 and last['returned']['chunk_id'] == '09088e'
assert json.loads(last['returned']['output']) == safe
terminal = load(A / 'archive29-verifier-terminal-private.json')
verified = json.loads(terminal['stdout'])
assert terminal['terminal_exit'] == 0 and terminal['stderr'] == ''
assert verified['members_verified'] == 416 and verified['source_after_exact']
assert verified['archive_sha256'] == safe['archive_sha256']
post = load(A / 'archive29-postwrite-metadata-safe.json')
assert post['writer_actual_session'] == 38595 and post['writer_actual_exit'] == 0
assert post['active_consumers'] == [] and post['destination_permissions_identity_exact'] == actual
receipt = dict(safe, root_verified_at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),
               root_verified_archive_compressed_SHA=True,
               root_verified_exact_plan_approval_manifest_controllers_and_terminal_records=True,
               member_and_source_verification_authority='Observed new archive29 writer38595 terminal0 and its exact-member/source-after verifier terminal0; root checked opaque compressed bytes and metadata without replay.',
               root_checker_sha256=sha(Path(__file__)))
out = S / 'private-evidence-durability-twentyninth.json'
assert not out.exists()
out.write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
