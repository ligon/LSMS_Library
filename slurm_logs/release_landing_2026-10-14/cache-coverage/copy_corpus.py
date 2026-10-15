"""Copy the completed corpus without sharing writable harmonized files."""
import hashlib
import json
from pathlib import Path
import shutil
import time

source = Path('/local/job39919092/release-work/runtime/data-corpus')
target = Path('/local/job39919092/release-work/cache-coverage-data')
control = Path('/local/job39919092/release-work/control/runtime/corpus')
completion = json.loads((control / 'completion.json').read_text())
progress = json.loads((control / 'progress.json').read_text())
assert progress['completed_pairs'] == progress['expected_pairs'] == 549
assert not progress['active'], progress['active']
assert completion['source_sha'] == progress['source_sha']
assert not target.exists(), 'Never overwrite a previous private validation root'

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def inventory(root):
    rows = {}
    for item in sorted(root.rglob('*')):
        rel = item.relative_to(root).as_posix()
        if item.is_symlink():
            assert rel == 'dvc-cache', f'Unexpected writable-tier symlink: {rel}'
        elif item.is_file():
            stat = item.stat()
            rows[rel] = {'bytes': stat.st_size, 'sha256': digest(item),
                         'mtime_ns': stat.st_mtime_ns, 'inode': stat.st_ino,
                         'device': stat.st_dev}
    return rows

before = inventory(source)
shutil.copytree(source, target, symlinks=True, copy_function=shutil.copy2)
copied = inventory(target)
after = inventory(source)
assert before == after, 'Original corpus changed during the copy'
assert before.keys() == copied.keys()
for key, original in before.items():
    replica = copied[key]
    assert (original['bytes'], original['sha256']) == (replica['bytes'], replica['sha256']), key
    assert (original['device'], original['inode']) != (replica['device'], replica['inode']), key
assert (target / 'dvc-cache').resolve() == (source / 'dvc-cache').resolve()
record = {'source_sha': completion['source_sha'], 'source': str(source),
          'target': str(target), 'copied_at_unix': time.time(),
          'regular_files': len(before), 'bytes': sum(r['bytes'] for r in before.values()),
          'all_file_hashes_equal': True, 'all_regular_inodes_distinct': True,
          'source_unchanged_during_copy': True, 'only_shared_symlink': 'dvc-cache'}
(target / 'CORPUS_COPY_COMPLETE.json').write_text(json.dumps(record, indent=2) + '\n')
(Path(__file__).parent / 'corpus-copy-manifest.json').write_text(json.dumps({'summary': record, 'files': copied}, indent=2) + '\n')
print(json.dumps(record), flush=True)
