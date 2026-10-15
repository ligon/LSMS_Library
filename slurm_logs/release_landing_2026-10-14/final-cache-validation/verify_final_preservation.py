"""Read-only preservation check after tests on a separate cache copy."""
from pathlib import Path
import json
import subprocess
import time

import prepare_copy as copy

ROOT = Path(__file__).resolve().parent
manifest = json.loads((ROOT / 'promotion-manifest.candidate.json').read_text())
data = Path(manifest['source_root'])
actual = {str(p.relative_to(data)) for p in data.rglob('*.parquet')}
all_private = {**manifest['files'], **manifest['excluded']}
assert actual == set(all_private)
for relative, entry in all_private.items():
    path = data / relative
    st = path.stat()
    assert {'bytes': st.st_size, 'mtime_ns': st.st_mtime_ns, 'device': st.st_dev, 'inode': st.st_ino} == {
        key: entry[key] for key in ('bytes', 'mtime_ns', 'device', 'inode')}, relative
    assert copy.sha(path) == entry['sha256'], relative
assert {str(p.relative_to(data)): str(p.readlink()) for p in data.rglob('*') if p.is_symlink()} == {
    'dvc-cache': '/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache'}
original = json.loads((ROOT.parent / 'corpus/snapshot-manifest.json').read_text())
copy.verify_original(original)
tree = Path('/local/job39919092/release-work/runtime/final-candidate')
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=tree, text=True).strip() == manifest['final_sha']
assert not subprocess.check_output(['git', 'status', '--short'], cwd=tree, text=True)
assert not list((tree / 'lsms_library/countries').rglob('*.parquet'))
result = {'source_sha': manifest['final_sha'], 'checked_at': time.time(),
          'candidate_manifest_sha256': copy.sha(ROOT / 'promotion-manifest.candidate.json'),
          'final_files': len(actual), 'final_sha_size_mtime_device_inode_unchanged': True,
          'original_corpus_unchanged': True, 'source_clean': True, 'in_tree_parquets': 0,
          'shared_l2_access': False,
          'scope': 'Known session final-cache builders completed; later tests use a separate physical root. This is no global lock on external shared writers.'}
(ROOT / 'preservation.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
