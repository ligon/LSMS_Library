"""Read-only git-object verification of the landed framework implementation."""
import hashlib,json,subprocess
from pathlib import Path
REPO=Path('/local/job40255479/mirrors/LSMS_Library')
BASE='532d58700fa46e52a0b390198604eca1dc4de00e'
PUBLICATION='23c1e2d52801bf206a15ec5900bca5d62e7f850f'
LANDED='1933434b84de82ac72b3921a7fd8fe5c00154754'
OUT=Path('/local/job40255479/control/framework-campaign/landing/source-equivalence.json')
def tree(sha):
    raw=subprocess.check_output(['git','ls-tree','-r','-z',sha],cwd=REPO)
    return {p.decode():meta.decode() for entry in raw.split(b'\0') if entry for meta,p in [entry.split(b'\t',1)]}
def digest(d):return hashlib.sha256(json.dumps(d,sort_keys=True,separators=(',',':')).encode()).hexdigest()
b,p,l=map(tree,[BASE,PUBLICATION,LANDED])
expected={'slurm_logs/framework_campaign_2026-10-20/1124-1125/'+n for n in ['1124-1125-review-r2.json','1124-1125-review-r2.org','1124-1125-review.json','1124-1125-review.org']}
assert p==l
assert set(b)<=set(l) and set(l)-set(b)==expected
assert all(l[path]==meta for path,meta in b.items())
result={'candidate':BASE,'publication':PUBLICATION,'landed_implementation':LANDED,'candidate_paths':len(b),'landed_paths':len(l),'all_candidate_modes_types_blobs_preserved':True,'publication_and_landed_full_trees_identical':True,'candidate_tree_inventory_sha256':digest(b),'landed_tree_inventory_sha256':digest(l),'only_added_paths':sorted(expected),'only_added_blobs':{x:l[x] for x in sorted(expected)},'runtime_config_package_tests_scripts_bin_tools_bench_workflows_build_dependencies_identical':True,'scope':'Read-only git objects; no package code, test or data/cache API executed.'}
OUT.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
