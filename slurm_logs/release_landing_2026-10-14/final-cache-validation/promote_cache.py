"""Manifest-pinned, journalled L2 promotion. No library imports or API calls.

Production CLI roots are fixed. snapshot/verify only read L2. stage writes a
new sibling transaction directory; apply/rollback are the only active-L2 writes.
An external quiescent window is required: this tool cannot lock other builders.
"""
from __future__ import annotations
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import time

SOURCE = Path('/local/job39919092/release-work/runtime/data-final-promotion')
SHARED = Path('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library')


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def mkdir_durable(path):
    missing = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    assert cursor.is_dir() and not cursor.is_symlink(), cursor
    for directory in reversed(missing):
        directory.mkdir()
        fsync_dir(directory.parent)


def save(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    fsync_dir(path.parent)


def journal(txn, event, **values):
    with (txn / 'journal.jsonl').open('a') as stream:
        stream.write(json.dumps({'time': time.time(), 'event': event, **values}) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def relative(value):
    assert isinstance(value, str) and value and '\\' not in value, value
    path = PurePosixPath(value)
    assert not path.is_absolute() and str(path) == value, value
    assert all(p not in {'', '.', '..', 'dvc-cache'} for p in path.parts), value
    assert path.suffix == '.parquet', value
    assert (len(path.parts) == 3 and path.parts[1] in {'var', '_'}) or (
        len(path.parts) == 4 and path.parts[2] == '_'), value
    return path


def safe_path(root, name):
    path = root / relative(name)
    assert not root.is_symlink() and root.resolve() == root, root
    for item in (path, *path.parents):
        if item == root:
            break
        assert not item.is_symlink(), item
    assert path.resolve().is_relative_to(root), path
    return path


def info(path):
    if not path.exists():
        return None
    assert path.is_file() and not path.is_symlink(), path
    before = path.stat()
    digest = sha(path)
    after = path.stat()
    assert (before.st_size, before.st_mtime_ns, before.st_dev, before.st_ino) == (
        after.st_size, after.st_mtime_ns, after.st_dev, after.st_ino), path
    return {'sha256': digest, 'bytes': before.st_size, 'mtime_ns': before.st_mtime_ns,
            'device': before.st_dev, 'inode': before.st_ino}


def same_bytes(actual, expected):
    if actual is None or expected is None:
        return actual is expected
    return (actual['sha256'], actual['bytes']) == (expected['sha256'], expected['bytes'])


def inventory(root):
    assert root.is_dir() and not root.is_symlink() and root.resolve() == root
    result = {}
    for child in sorted(root.iterdir()):
        if child.name == 'dvc-cache':
            continue
        assert not child.is_symlink(), child
        if not child.is_dir():
            continue
        for path in sorted(child.rglob('*')):
            assert not path.is_symlink(), path
            if path.is_file() and path.suffix == '.parquet':
                name = str(path.relative_to(root))
                relative(name)
                result[name] = info(path)
    return result


def load_manifest(path, expected_digest, final_sha, source=SOURCE, shared=SHARED):
    assert sha(path) == expected_digest, 'manifest digest does not match reviewed digest'
    plan = json.loads(path.read_text())
    assert plan['version'] == 1 and plan['final_sha'] == final_sha
    assert len(final_sha) == 40 and all(c in '0123456789abcdef' for c in final_sha)
    assert plan['source_root'] == str(source) and plan['shared_root'] == str(shared)
    assert plan['validation_status'] == 'reviewed_for_promotion'
    files = plan['files']
    assert files, 'empty promotion'
    for name, entry in files.items():
        relative(name)
        assert len(entry['sha256']) == 64 and all(c in '0123456789abcdef' for c in entry['sha256'])
        assert isinstance(entry['bytes'], int) and entry['bytes'] > 0
        assert entry['disposition'] == 'promote_verified'
        assert entry['classification'] in {
            'country_fresh_full_coverage', 'wave_fresh',
            'script_wave_legacy_verified_provenance', 'companion_verified_provenance'}
        if PurePosixPath(name).parts[1] == 'var':
            assert entry['classification'] == 'country_fresh_full_coverage'
            assert entry['stored_hash'] and entry['stored_hash'] == entry['expected_hash']
            assert entry['footer_waves'] is not None
            assert set(entry['declared_waves']) <= set(entry['footer_waves'])
    for name, entry in plan.get('retire', {}).items():
        relative(name)
        assert name not in files and entry['reason'] and entry['disposition'] == 'backup_then_retire'
    return plan


def snapshot(plan, manifest_digest, quiescence_note):
    assert quiescence_note.strip(), 'record the externally established quiescent window'
    return {'version': 1, 'manifest_sha256': manifest_digest,
            'final_sha': plan['final_sha'], 'shared_root': plan['shared_root'],
            'quiescence_note': quiescence_note, 'created': time.time(),
            'files': inventory(Path(plan['shared_root']))}


def copy_verified(source, target, expected):
    assert same_bytes(info(source), expected), source
    mkdir_durable(target.parent)
    assert not target.exists(), target
    shutil.copy2(source, target)
    with target.open('rb') as stream:
        os.fsync(stream.fileno())
    fsync_dir(target.parent)
    assert same_bytes(info(target), expected), target


def stage(plan, manifest_digest, baseline, txn):
    source, shared = Path(plan['source_root']), Path(plan['shared_root'])
    assert baseline['manifest_sha256'] == manifest_digest and baseline['final_sha'] == plan['final_sha']
    assert baseline['shared_root'] == str(shared) and baseline['quiescence_note']
    assert not txn.exists() and txn.parent == shared.parent and not txn.parent.is_symlink(), txn
    assert inventory(shared) == baseline['files'], 'shared L2 changed since snapshot'
    for name, entry in plan['files'].items():
        assert same_bytes(info(safe_path(source, name)), entry), name
    txn.mkdir()
    fsync_dir(txn.parent)
    assert txn.stat().st_dev == shared.stat().st_dev, 'staging must be on shared filesystem'
    (txn / 'staged').mkdir()
    (txn / 'backups').mkdir()
    fsync_dir(txn)
    save(txn / 'manifest.json', plan)
    save(txn / 'baseline.json', baseline)
    save(txn / 'state.json', {'phase': 'staging', 'manifest_sha256': manifest_digest})
    journal(txn, 'stage_started', manifest_sha256=manifest_digest, final_sha=plan['final_sha'])
    for name, entry in sorted(plan['files'].items()):
        copy_verified(safe_path(source, name), safe_path(txn / 'staged', name), entry)
    assert inventory(shared) == baseline['files'], 'shared L2 changed during staging'
    save(txn / 'state.json', {'phase': 'staged', 'manifest_sha256': manifest_digest})
    journal(txn, 'stage_complete', files=len(plan['files']))


@contextmanager
def transaction_lock(txn):
    with (txn / 'transaction.lock').open('a') as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def check_transaction(txn, manifest_digest):
    assert txn.is_dir() and not txn.is_symlink() and txn.resolve() == txn
    state = json.loads((txn / 'state.json').read_text())
    assert state['manifest_sha256'] == manifest_digest
    plan = json.loads((txn / 'manifest.json').read_text())
    baseline = json.loads((txn / 'baseline.json').read_text())
    assert baseline['manifest_sha256'] == manifest_digest
    return state, plan, baseline


def verify_final(plan, baseline):
    actual = inventory(Path(plan['shared_root']))
    expected_paths = (set(baseline['files']) | set(plan['files'])) - set(plan.get('retire', {}))
    assert set(actual) == expected_paths, 'shared parquet fileset differs from planned result'
    for name, entry in actual.items():
        if name in plan['files']:
            assert same_bytes(entry, plan['files'][name]), name
        else:
            assert entry == baseline['files'][name], (name, 'unlisted shared file changed')
    return {'files': len(actual), 'promoted': len(plan['files']),
            'retired': len(plan.get('retire', {})), 'all_planned_bytes_verified': True,
            'all_unlisted_parquets_unchanged': True}


def apply(txn, manifest_digest, quiescence_note):
    assert quiescence_note.strip()
    with transaction_lock(txn):
        state, plan, baseline = check_transaction(txn, manifest_digest)
        assert state['phase'] == 'staged', 'do not blindly resume a partial apply; inspect/rollback'
        shared = Path(plan['shared_root'])
        assert inventory(shared) == baseline['files'], 'shared L2 changed before apply'
        for name, entry in plan['files'].items():
            assert same_bytes(info(safe_path(txn / 'staged', name)), entry), name
        save(txn / 'state.json', {'phase': 'applying', 'manifest_sha256': manifest_digest})
        journal(txn, 'apply_started', quiescence_note=quiescence_note)
        actions = [(name, 'replace') for name in sorted(plan['files'])]
        actions += [(name, 'retire') for name in sorted(plan.get('retire', {}))]
        for name, action in actions:
            dest = safe_path(shared, name)
            previous = baseline['files'].get(name)
            assert info(dest) == previous, (name, 'concurrent change before replacement')
            if action == 'replace' and same_bytes(previous, plan['files'][name]):
                journal(txn, 'already_identical', path=name)
                continue
            if action == 'retire' and previous is None:
                journal(txn, 'already_absent', path=name)
                continue
            if previous is not None:
                copy_verified(dest, safe_path(txn / 'backups', name), previous)
                journal(txn, 'backup_ready', path=name, old_sha256=previous['sha256'])
            journal(txn, 'mutation_intent', path=name, action=action,
                    old=previous, new=plan['files'].get(name))
            # Recheck immediately before the atomic operation. External builders
            # must remain quiescent; this check is not a lock on other processes.
            assert info(dest) == previous, (name, 'concurrent change after backup')
            if action == 'replace':
                mkdir_durable(dest.parent)
                os.replace(safe_path(txn / 'staged', name), dest)
            else:
                dest.unlink()
            fsync_dir(dest.parent)
            journal(txn, 'mutation_done', path=name, action=action)
        result = verify_final(plan, baseline)
        save(txn / 'verification.json', result)
        save(txn / 'state.json', {'phase': 'applied', 'manifest_sha256': manifest_digest})
        journal(txn, 'apply_complete', **result)
        return result


def rollback(txn, manifest_digest, quiescence_note):
    assert quiescence_note.strip()
    with transaction_lock(txn):
        state, plan, baseline = check_transaction(txn, manifest_digest)
        assert state['phase'] in {'applying', 'applied', 'rolling_back', 'rolled_back'}
        shared = Path(plan['shared_root'])
        intents = [r for r in map(json.loads, (txn / 'journal.jsonl').read_text().splitlines())
                   if r['event'] == 'mutation_intent']
        save(txn / 'state.json', {'phase': 'rolling_back', 'manifest_sha256': manifest_digest})
        journal(txn, 'rollback_started', quiescence_note=quiescence_note)
        for event in reversed(intents):
            name, previous, new = event['path'], event['old'], event['new']
            dest = safe_path(shared, name)
            current = info(dest)
            if same_bytes(current, previous):
                continue
            expected_current = new if event['action'] == 'replace' else None
            assert same_bytes(current, expected_current), (name, 'concurrent change; do not overwrite')
            journal(txn, 'rollback_intent', path=name)
            if previous is None:
                dest.unlink()
            else:
                backup = safe_path(txn / 'backups', name)
                assert same_bytes(info(backup), previous), (name, 'invalid rollback backup')
                temp = dest.with_name(dest.name + '.promotion-rollback-tmp')
                copy_verified(backup, temp, previous)
                os.replace(temp, dest)
            fsync_dir(dest.parent)
            journal(txn, 'rollback_done', path=name)
        actual = inventory(shared)
        assert set(actual) == set(baseline['files'])
        assert all(same_bytes(actual[name], entry) for name, entry in baseline['files'].items())
        result = {'files': len(actual), 'baseline_bytes_restored': True,
                  'original_inodes_restored': False, 'backups_retained': True}
        save(txn / 'rollback-verification.json', result)
        save(txn / 'state.json', {'phase': 'rolled_back', 'manifest_sha256': manifest_digest})
        journal(txn, 'rollback_complete', **result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['snapshot', 'stage', 'apply', 'verify', 'rollback'])
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--final-sha', required=True)
    parser.add_argument('--snapshot', type=Path)
    parser.add_argument('--transaction', type=Path)
    parser.add_argument('--quiescence-note')
    args = parser.parse_args()
    plan = load_manifest(args.manifest, args.manifest_sha256, args.final_sha)
    if args.command == 'snapshot':
        assert args.snapshot and not args.snapshot.exists()
        assert args.snapshot.resolve().is_relative_to(Path('/local/job39919092/release-work/control/runtime/final-promotion'))
        result = snapshot(plan, args.manifest_sha256, args.quiescence_note or '')
        save(args.snapshot, result)
        print(json.dumps({'shared_files': len(result['files']), 'active_l2_writes': 0}))
        return
    assert args.transaction and args.transaction.is_absolute()
    assert args.transaction.parent == SHARED.parent
    assert args.transaction.name.startswith('.lsms-promotion-')
    if args.command != 'stage':
        _, saved_plan, _ = check_transaction(args.transaction, args.manifest_sha256)
        assert saved_plan == plan, 'transaction plan differs from reviewed manifest'
    if args.command == 'stage':
        assert args.snapshot
        stage(plan, args.manifest_sha256, json.loads(args.snapshot.read_text()), args.transaction)
        print(json.dumps({'phase': 'staged', 'active_l2_writes': 0}))
    elif args.command == 'apply':
        print(json.dumps(apply(args.transaction, args.manifest_sha256, args.quiescence_note or ''), indent=2))
    elif args.command == 'rollback':
        print(json.dumps(rollback(args.transaction, args.manifest_sha256, args.quiescence_note or ''), indent=2))
    else:
        state, saved_plan, baseline = check_transaction(args.transaction, args.manifest_sha256)
        assert state['phase'] == 'applied' and saved_plan == plan
        print(json.dumps(verify_final(plan, baseline), indent=2))


if __name__ == '__main__':
    main()
