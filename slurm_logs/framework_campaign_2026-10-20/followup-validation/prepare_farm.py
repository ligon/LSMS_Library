"""Physically copy untouched private suite seed to a separate farm cache.

Must run before the suite starts. Never touches original corpus roots.
"""
import argparse,hashlib,json,shutil
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--suite-data',type=Path,required=True)
p.add_argument('--farm-data',type=Path,required=True)
p.add_argument('--manifest',type=Path,required=True)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args();source=a.suite_data.resolve();target=a.farm_data.resolve()
assert source.is_relative_to(Path('/local')) and target.is_relative_to(Path('/local'))
assert source!=target and not target.exists()
manifest=json.loads(a.manifest.read_text())
source_stats={name:(source/name).stat() for name in manifest}
for name,record in manifest.items():
 assert hashlib.sha256((source/name).read_bytes()).hexdigest()==record['sha256'],name
shutil.copytree(source,target,symlinks=True)
links=[str(p.relative_to(target)) for p in target.rglob('*') if p.is_symlink()]
assert links==['dvc-cache'],links
for name,record in manifest.items():
 old=source_stats[name];new=(target/name).stat();now=(source/name).stat()
 assert hashlib.sha256((target/name).read_bytes()).hexdigest()==record['sha256'],name
 assert (new.st_dev,new.st_ino)!=(old.st_dev,old.st_ino),name
 assert (now.st_ino,now.st_size,now.st_mtime_ns)==(old.st_ino,old.st_size,old.st_mtime_ns),name
report={'source':str(source),'target':str(target),'files':len(manifest),'bytes':sum(s.st_size for s in source_stats.values()),'bytes_match_prepared_manifest':True,'all_l2_inodes_distinct':True,'source_size_mtime_inode_unchanged':True,'only_symlink':'dvc-cache','state':'prepared; no API executed'}
a.out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
