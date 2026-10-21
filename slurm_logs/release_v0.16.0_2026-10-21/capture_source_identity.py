"""Read exact Git tree objects; report whether final release inputs equal tested source."""
import argparse,hashlib,json,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--repo',required=True);p.add_argument('--sha',required=True);p.add_argument('--out',required=True);a=p.parse_args()
repo=Path(a.repo).resolve();expected='532d58700fa46e52a0b390198604eca1dc4de00e'
def git(*args):return subprocess.check_output(['git','-C',str(repo),*args])
assert git('rev-parse','HEAD').decode().strip()==a.sha
assert not git('status','--porcelain').strip()
def allowed(path):return path.startswith('slurm_logs/') or path in {'.sucoder/handoff.org','docs/releases/v0.16.0.md','docs/migration/v0.16.0.md'}
def tree(ref):
 result={}
 for row in git('ls-tree','-r','-z',ref).split(b'\0'):
  if row:
   meta,name=row.split(b'\t',1);result[name.decode()]=meta.decode()
 return result
before,after=tree(expected),tree(a.sha)
differences=[path for path in sorted(before.keys()|after.keys()) if before.get(path)!=after.get(path)]
assert all(allowed(path) for path in differences),[path for path in differences if not allowed(path)]
left={k:v for k,v in before.items() if not allowed(k)};right={k:v for k,v in after.items() if not allowed(k)};assert left==right
payload=json.dumps(right,sort_keys=True,separators=(',',':')).encode()
r={'candidate':expected,'release_commit':a.sha,'identical_non_report_paths':len(right),'identical_package_files':sum(k.startswith('lsms_library/') for k in right),'mode_type_blob_identity':True,'unexpected_differences':0,'documented_changed_paths':len(differences),'source_tree_sha256':hashlib.sha256(payload).hexdigest(),'allowed_deltas':['slurm_logs/**','.sucoder/handoff.org','docs/releases/v0.16.0.md','docs/migration/v0.16.0.md'],'scope':'All tracked non-report Git modes/types/blob IDs match the validated candidate exactly; no package imports or data reads.'}
Path(a.out).write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
