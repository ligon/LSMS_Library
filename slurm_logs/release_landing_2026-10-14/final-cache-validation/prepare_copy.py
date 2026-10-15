"""Prepare a physical final-validation copy; never call a library API."""
from pathlib import Path
import collections
import hashlib
import json
import shutil
import subprocess
import time

CONTROL = Path(__file__).resolve().parent
BASE = CONTROL.parent / 'corpus'
SOURCE = Path('/local/job39919092/release-work/runtime/data-corpus')
TARGET = Path('/local/job39919092/release-work/runtime/data-final-promotion')
TREE = Path('/local/job39919092/release-work/runtime/corpus')


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def verify_original(snapshot):
    files = {str(p.relative_to(SOURCE)) for p in SOURCE.rglob('*')
             if p.is_file() and not p.is_symlink()}
    assert files == set(snapshot['files'])
    links = {str(p.relative_to(SOURCE)): str(p.readlink())
             for p in SOURCE.rglob('*') if p.is_symlink()}
    assert links == snapshot['symlinks']
    assert links == {'dvc-cache': '/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache'}
    for relative, expected in snapshot['files'].items():
        path = SOURCE / relative
        stat = path.stat()
        assert (stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino) == (
            expected['bytes'], expected['mtime_ns'], expected['device'], expected['inode']), relative
        assert sha(path) == expected['sha256'], relative
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=TREE, text=True).strip() == snapshot['source_sha']
    assert not subprocess.check_output(['git', 'status', '--short'], cwd=TREE, text=True)
    assert not list((TREE / 'lsms_library/countries').rglob('*.parquet'))


def main():
    started = time.perf_counter()
    snapshot = json.loads((BASE / 'snapshot-manifest.json').read_text())
    completion = json.loads((BASE / 'completion.json').read_text())
    assert snapshot['source'] == str(SOURCE)
    assert snapshot['source_sha'] == completion['source_sha']
    assert len(snapshot['files']) == 1519
    assert not completion['inventory_errors']
    assert len(completion['processes']) == 38
    assert {r['country']: r['exit_code'] for r in completion['processes'] if r['exit_code']} == {
        'Armenia': 1, 'Nepal': 1, 'Peru': 1}
    assert all(json.loads(p.read_text())['stage'] == 'complete'
               for p in BASE.glob('*/status.json'))
    assert not TARGET.exists(), TARGET
    verify_original(snapshot)
    shutil.copytree(SOURCE, TARGET, symlinks=True, copy_function=shutil.copy2)
    copied = {}
    for relative, expected in snapshot['files'].items():
        path = TARGET / relative
        stat = path.stat()
        assert (stat.st_dev, stat.st_ino) != (expected['device'], expected['inode']), relative
        assert stat.st_size == expected['bytes'] and sha(path) == expected['sha256'], relative
        copied[relative] = dict(expected, device=stat.st_dev, inode=stat.st_ino,
                                mtime_ns=stat.st_mtime_ns)
    actual = {str(p.relative_to(TARGET)) for p in TARGET.rglob('*')
              if p.is_file() and not p.is_symlink()}
    assert actual == set(copied)
    links = {str(p.relative_to(TARGET)): str(p.readlink())
             for p in TARGET.rglob('*') if p.is_symlink()}
    assert links == snapshot['symlinks']
    verify_original(snapshot)
    (CONTROL / 'copy-manifest.json').write_text(json.dumps({
        'source': str(SOURCE), 'target': str(TARGET),
        'source_sha': snapshot['source_sha'], 'files': copied, 'symlinks': links,
        'original_snapshot_sha256': sha(BASE / 'snapshot-manifest.json'),
    }, indent=2) + '\n')
    result = {
        'source': str(SOURCE), 'target': str(TARGET), 'source_sha': snapshot['source_sha'],
        'files': len(copied), 'bytes': sum(v['bytes'] for v in copied.values()),
        'extension_counts': dict(collections.Counter(Path(p).suffix for p in copied)),
        'all_sha256_equal': True, 'all_inodes_distinct': True,
        'original_verified_before_and_after': True, 'source_worktree_clean': True,
        'only_symlink': links, 'library_api_calls': 0, 'shared_l2_writes': 0,
        'final_sha_received': False, 'final_validation_started': False,
        'seconds': time.perf_counter() - started,
    }
    (CONTROL / 'copy-summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
