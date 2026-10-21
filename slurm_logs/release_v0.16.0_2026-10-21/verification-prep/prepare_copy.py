"""Physically copy a quiescent L2 cache; never follow or copy shared L1."""
from pathlib import Path
import hashlib
import json
import os
import shutil

SOURCE = Path('/local/job40255479/followup-farm-data')
TARGET = Path('/local/job40255479/release-v016-seed-data')
OUT = Path(__file__).resolve().parent
ORIGINAL_MANIFEST = Path('/local/job40255479/control/framework-campaign/followup-suite-copy-manifest.json')
ORIGINAL_MANIFEST_SHA = 'c54c8efa4a64da4d7eca8d24d4b5f838b2cbb1d9c7b8413b794e427a27de2e1d'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(root):
    result = {}
    for current, dirs, names in os.walk(root, followlinks=False):
        for name in list(dirs) + names:
            path = Path(current) / name
            if path.is_symlink():
                assert path == root / 'dvc-cache', path
        dirs[:] = [name for name in dirs if not (Path(current) / name).is_symlink()]
        for name in names:
            path = Path(current) / name
            if path.is_symlink():
                continue
            st = path.stat()
            result[path.relative_to(root).as_posix()] = {
                'sha256': digest(path), 'bytes': st.st_size, 'device': st.st_dev,
                'inode': st.st_ino, 'mtime_ns': st.st_mtime_ns}
    return result


if __name__ == '__main__':
    assert SOURCE.is_dir() and not TARGET.exists()
    assert (SOURCE / 'dvc-cache').is_symlink()
    assert not (OUT / 'copy-summary.json').exists()
    before = snapshot(SOURCE)
    assert digest(ORIGINAL_MANIFEST) == ORIGINAL_MANIFEST_SHA
    original = json.loads(ORIGINAL_MANIFEST.read_text())
    assert len(before) == len(original) == 1519
    assert sum(record['bytes'] for record in before.values()) == 341221428
    assert before.keys() == original.keys()
    for name, record in before.items():
        assert record['sha256'] == original[name]['sha256'], name
    TARGET.mkdir(mode=0o700)
    for name in before:
        dest = TARGET / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(SOURCE / name, dest)
    (TARGET / 'dvc-cache').symlink_to((SOURCE / 'dvc-cache').resolve())
    copied = snapshot(TARGET)
    assert before == snapshot(SOURCE), 'Source changed during copy'
    assert before.keys() == copied.keys()
    for name, old in before.items():
        new = copied[name]
        assert old['sha256'] == new['sha256'] and old['bytes'] == new['bytes'], name
        assert (old['device'], old['inode']) != (new['device'], new['inode']), name
    (OUT / 'copy-manifest-private.json').write_text(json.dumps(copied, indent=2, sort_keys=True) + '\n')
    summary = {'source': str(SOURCE), 'target': str(TARGET), 'files': len(copied),
               'bytes': sum(v['bytes'] for v in copied.values()), 'bytes_equal': True,
               'all_l2_inodes_distinct': True, 'source_hash_stat_identity_unchanged': True,
               'only_symlink': 'dvc-cache', 'l1_target': str((TARGET / 'dvc-cache').resolve()),
               'manifest_sha256': digest(OUT / 'copy-manifest-private.json'),
               'original_532_start_manifest_sha256': ORIGINAL_MANIFEST_SHA,
               'matches_original_532_start_file_set_and_bytes': True,
               'state': 'prepared; no package import or API executed'}
    (OUT / 'copy-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary))
