"""Read-only comparison of the preserved original to its completion snapshot."""
from pathlib import Path
import hashlib
import json
import subprocess
import time

CONTROL = Path(__file__).parent
snapshot = json.loads((CONTROL.parent/'corpus/snapshot-manifest.json').read_text())
source = Path(snapshot['source'])
tree = Path('/local/job39919092/release-work/runtime/corpus')
start = time.perf_counter()
actual = {str(p.relative_to(source)) for p in source.rglob('*') if p.is_file() and not p.is_symlink()}
assert actual == set(snapshot['files']), (actual-set(snapshot['files']), set(snapshot['files'])-actual)
for relative, expected in snapshot['files'].items():
    path = source/relative
    stat = path.stat()
    assert (stat.st_size,stat.st_mtime_ns,stat.st_dev,stat.st_ino) == (
        expected['bytes'],expected['mtime_ns'],expected['device'],expected['inode']), relative
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    assert digest.hexdigest() == expected['sha256'], relative
links = {str(p.relative_to(source)): str(p.readlink()) for p in source.rglob('*') if p.is_symlink()}
assert links == snapshot['symlinks'], links
sha = subprocess.check_output(['git','rev-parse','HEAD'],cwd=tree,text=True).strip()
assert sha == snapshot['source_sha'], sha
dirty = subprocess.check_output(['git','status','--short'],cwd=tree,text=True)
assert not dirty, dirty
in_tree = list((tree/'lsms_library/countries').rglob('*.parquet'))
assert not in_tree, in_tree
result = {'source':str(source),'source_sha':sha,'files_verified':len(actual),
          'all_sha256_unchanged':True,'all_metadata_unchanged':True,
          'only_original_symlink':links,'source_worktree_clean':True,
          'in_tree_parquets':0,'seconds':time.perf_counter()-start}
(CONTROL/'original-preservation.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
