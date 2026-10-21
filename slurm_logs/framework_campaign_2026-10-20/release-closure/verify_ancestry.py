"""Reproduce read-only PR/tag inclusion and nonancestor path comparisons."""
import argparse
import json
from pathlib import Path
import subprocess

parser=argparse.ArgumentParser()
parser.add_argument('--snapshots',type=Path,required=True)
args=parser.parse_args()
root=args.snapshots.resolve()
assert root.is_relative_to(Path('/local'))
tag='cb44e3522a2426ba03431ca9a218cc63fe03f6c9'
candidate='532d58700fa46e52a0b390198604eca1dc4de00e'
development=json.loads((root/'ref-heads-development.json').read_text())['object']['sha']
def ancestor(a,b):
    status=subprocess.run(['git','merge-base','--is-ancestor',a,b],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode
    return True if status==0 else False if status==1 else None
rows=[]
for path in sorted(root.glob('pr-*.json')):
    if not path.stem[3:].isdigit():continue
    pr=json.loads(path.read_text());merge=pr['merge_commit_sha'] if pr['merged_at'] else None
    rows.append({'pr':pr['number'],'head':pr['head']['sha'],'merge_commit':merge,
                 'tag_contains_merge':ancestor(merge,tag)if merge else False,
                 'candidate_contains_merge':ancestor(merge,candidate)if merge else False,
                 'development_contains_merge':ancestor(merge,development)if merge else False,
                 'candidate_contains_head':ancestor(pr['head']['sha'],candidate)})
print(json.dumps(rows,indent=2))
