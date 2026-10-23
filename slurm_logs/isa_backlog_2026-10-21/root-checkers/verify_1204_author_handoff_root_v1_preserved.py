"""Authenticate frozen Uganda1204 author evidence as opaque bytes only."""
from pathlib import Path
import datetime
import hashlib
import json
import os
import stat
import subprocess

C = Path('/local/job40255479/control/isa-backlog-20261021')
P = C / 'uganda1204-fix'


def sha(path, kind='sha256'):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, kind).hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def receipt(path):
    path = Path(path)
    before = path.lstat()
    link = os.readlink(path) if path.is_symlink() else None
    digest = hashlib.sha256(link.encode()).hexdigest() if link is not None else sha(path)
    after = path.lstat()
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mode', 'st_mtime_ns', 'st_nlink')
    assert all(getattr(before, k) == getattr(after, k) for k in fields)
    return dict(sha256=digest, bytes=after.st_size, mode=stat.S_IMODE(after.st_mode),
                mtime_ns=after.st_mtime_ns, nlink=after.st_nlink, device=after.st_dev,
                inode=after.st_ino, symlink=link is not None, link=link)


pins = {'READY.json': '5febb754720e182cc0834ca2f8b4f45fbb91dba1406b8b88e0dfe72497de9f08',
        'final-private-manifest.json': '920a0d87c1473651e0704b2988648178853a6981949e51840520e2814b610d54',
        'safe-exports/ALLOWLIST.json': '2eb8283f17b6f407faa14067e4b72a17d13db26fce1080fa65cc28be149b5d3a'}
for name, expected in pins.items():
    assert sha(P / name) == expected
ready = load(P / 'READY.json')
manifest = load(P / 'final-private-manifest.json')
allow = load(P / 'safe-exports/ALLOWLIST.json')['files']
assert len(manifest['entries']) == ready['manifest_entries'] == 1839
assert set(allow) == {'REPORT.org', 'validation.json', 'pins.json', 'protection.json',
                      'commands.json', 'source.diff', 'empty-order-residual.json', 'empty-order-issue.md'}
actual = {str(p.relative_to(P)) for p in P.rglob('*') if p.is_file() or p.is_symlink()}
assert actual == set(manifest['entries']) | set(manifest['excluded_paths'])
assert [str(p.relative_to(P)) for p in sorted(P.rglob('*')) if p.is_dir() and not p.is_symlink()] == manifest['directories']
copies = 0
for name, expected in manifest['entries'].items():
    assert receipt(P / name) == expected, ('MANIFEST_RECEIPT', name)
    if '/dvc-cache/' in '/' + name and len(Path(name).name) == 30:
        copies += 1
assert copies == 444
for name, expected in allow.items():
    assert receipt(P / 'safe-exports' / name) == expected

protection = load(P / 'final-protection-private.json')
env = dict(os.environ, GIT_OPTIONAL_LOCKS='0')
tracked_count = 0
for role, expected in protection['sources'].items():
    root = Path(expected['root'])
    def git(*args):
        return subprocess.check_output(['git', '--no-optional-locks', *args], cwd=root, env=env)
    assert git('rev-parse', 'HEAD').decode().strip() == expected['head']
    assert git('status', '--porcelain').decode() == expected['status'] == ''
    assert set(git('ls-files', '-z').decode().rstrip('\0').split('\0')) == set(expected['tracked'])
    for name, pin in expected['tracked'].items():
        path = root / name
        if pin == {'missing': True}:
            assert not path.exists() and not path.is_symlink()
        else:
            assert receipt(path) == pin, ('SOURCE_RECEIPT', role, name)
    tracked_count += len(expected['tracked'])
assert tracked_count == 29825
candidate = Path(protection['sources']['candidate']['root'])
def git_candidate(*args):
    return subprocess.check_output(['git', '--no-optional-locks', *args], cwd=candidate, env=env)
assert ready['source_head'] == 'a417b69d71c460ab33f6a494b4df5d67cd792de5'
assert ready['actual_parent'] == '491daf5654b9ee45332bec97e1296ae821ff46d5'
assert git_candidate('diff', '--name-only', ready['tested_clean_head'], ready['source_head']).decode().splitlines() == ['.coder/ledger/1204-uganda-crop-wave-coverage.md']
assert git_candidate('diff', '--name-only', ready['actual_parent'], ready['source_head']).decode().splitlines() == protection['safe']['approved_diff_paths']
assert git_candidate('diff', ready['actual_parent'], ready['source_head']) == (P / 'safe-exports/source.diff').read_bytes()
providers = load(P / 'provider-census-private.json')['available']
assert len(providers) == 37
for row in providers:
    for path_key, receipt_key in [('provider', 'provider_receipt'), ('sidecar', 'sidecar_receipt'), ('authority', 'authority_receipt')]:
        current = receipt(row[path_key])
        expected = row[receipt_key]
        assert current == {k: expected.get(k) for k in current}
    provider = Path(row['provider'])
    assert row['provider_receipt']['genuine_stata_binary_magic']
    assert provider.stat().st_size == row['declared_size']
    assert sha(provider, 'md5') == row['declared_md5']
    with provider.open('rb') as stream:
        header = stream.read(16)
    assert header.startswith(b'<stata_dta>') or (header and header[0] in range(104, 119))
for path, expected in protection['final_source_ignored_tmp_symlink_receipts'].items():
    assert receipt(path) == expected

records_path = C / 'uganda1204-seal-postexit-tool-records.json'
assert sha(records_path) == '2461c07020cf627e7773afbd1bccb5c230748a7957201ee3ce4d0c1331e6d579'
records = load(records_path)
assert records['known_hashes'] == pins
for name, session in [('sealer', 99339), ('postexit', 88691)]:
    row = records[name]
    assert row['terminal_input']['session_id'] == session
    assert row['terminal_result']['exit_code'] == row['actual_observed_tool_exit'] == 0
    assert row['terminal_result']['output'] == row['stdout']
    assert len(row['stdout'].encode()) == row['stdout_utf8_bytes']
    assert hashlib.sha256(row['stdout'].encode()).hexdigest() == row['stdout_sha256']
post = json.loads(records['postexit']['stdout'])
assert post['all_file_hashes_and_physical_receipts_exact'] and post['writes'] == 0
assert post['tracked_source_files'] == 29825 and post['private_physical_L1_copies'] == 444
assert not post['live_actor_pids'] and not post['live_descendant_pids'] and not post['source_cwd_numerical_or_build_pids']
assert receipt(P / 'seal-private.log') == post['completed_sealer_stream']
assert (P / 'seal-private.log').read_text() == records['sealer']['completed_redirected_stream']['stdout']
validation = load(P / 'safe-exports/validation.json')
assert validation['comparator']['strict_pass'] == 61 and validation['comparator']['pairs'] == 65
assert len(validation['comparator']['strict_findings']) == 4
assert validation['cache_artifacts']['held_endpoint_nonempty_crop_grain_ledgers'] == 0
for name, expected in pins.items():
    assert sha(P / name) == expected
out = dict(status='ROOT_AUTHENTICATED_UGANDA1204_AUTHOR_HANDOFF_NOT_PRODUCTION_ACCEPTANCE',
           at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),
           source_head=ready['source_head'], parent=ready['actual_parent'],
           READY_sha256=pins['READY.json'], manifest_sha256=pins['final-private-manifest.json'],
           allowlist_sha256=pins['safe-exports/ALLOWLIST.json'],
           complete_manifest_files_content_and_physical_checked=1839,
           complete_tracked_source_receipts_checked=29825,
           genuine_current_binary_providers_MD5_SHA_and_physical_checked=37,
           raw_private_L1_entries_rehashed=444, allowlisted_files=8,
           observed_original_seal_postexit_records_sha256=sha(records_path),
           four_new_empty_column_order_strict_failures_retained=True,
           nonempty_grain_audit_transport_is_synthetic_only=True,
           unattributed_denied_bind_and_IPC_noop_history_retained=True,
           root_initial_missing_manifest_and_allowlist_path_read_assumptions='Metadata-only failed reads; corrected to actual final-private-manifest.json and safe-exports/ALLOWLIST.json without source/data execution.',
           author_claims_require_independent_runtime_verification=True,
           root_no_project_import_payload_decode_API_test_or_data_build_replay=True,
           root_checker_sha256=sha(Path(__file__)), signed='Sue')
dest = C / '1204-AUTHOR-HANDOFF-ROOT.json'
with dest.open('x') as stream:
    json.dump(out, stream, indent=2)
    stream.write('\n')
dest.chmod(0o600)
print(json.dumps(out), flush=True)
