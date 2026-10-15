"""Make an isolated physical copy for the final focused test selection."""
from pathlib import Path
import hashlib
import json
import shutil
import time

ROOT = Path(__file__).resolve().parent
SOURCE = Path('/local/job39919092/release-work/runtime/data-final-promotion')
TARGET = SOURCE.parent / 'data-final-targeted'


def inventory(root):
    result = {}
    for path in sorted(root.rglob('*.parquet')):
        assert not path.is_symlink()
        st = path.stat()
        result[str(path.relative_to(root))] = {
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'bytes': st.st_size,
            'mtime_ns': st.st_mtime_ns, 'device': st.st_dev, 'inode': st.st_ino}
    return result


assert (ROOT / 'run/completion.json').is_file()
assert (ROOT / 'run/trailing-uganda.json').is_file()
assert not TARGET.exists()
before = inventory(SOURCE)
assert len(before) == 1519
assert {str(p.relative_to(SOURCE)): str(p.readlink()) for p in SOURCE.rglob('*') if p.is_symlink()} == {
    'dvc-cache': '/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache'}
started = time.perf_counter()
TARGET.mkdir()
for relative in before:
    target = TARGET / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(SOURCE / relative, target)
(TARGET / 'dvc-cache').symlink_to('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache')
after = inventory(TARGET)
assert before.keys() == after.keys()
for relative, old in before.items():
    new = after[relative]
    assert new['sha256'] == old['sha256'] and new['bytes'] == old['bytes']
    assert (new['device'], new['inode']) != (old['device'], old['inode'])
assert before == inventory(SOURCE)
result = {'source': str(SOURCE), 'target': str(TARGET),
          'source_sha': '3914d8804d7b2936d75041cea98bbc9ddb6d4c9f',
          'source_files': before, 'target_files': after,
          'files': len(after), 'bytes': sum(r['bytes'] for r in after.values()),
          'source_unchanged': True, 'distinct_inodes': True,
          'seconds': time.perf_counter() - started}
(ROOT / 'targeted-copy-manifest.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: v for k, v in result.items() if k not in ('source_files', 'target_files')}, indent=2))
