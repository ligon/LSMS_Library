"""Create aggregate-only archival evidence; retain raw diagnostic messages privately."""
from pathlib import Path
import argparse
import ast
import collections
import csv
import hashlib
import json
import platform
import re

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--evidence-root',type=Path,default=Path('/local/job39919092/release-work/control/runtime/feature-audit'),
                    help='Private Feature audit evidence directory; independent of this archived script location.')
ROOT=parser.parse_args().evidence_root.resolve()
TREE=Path('/local/job39919092/release-work/runtime/corpus')
OUT=ROOT/'publishable'
OUT.mkdir(exist_ok=True)
records=[json.loads(line) for line in (ROOT/'results.jsonl').read_text().splitlines()]
clusters=json.loads((ROOT/'clusters.json').read_text())
log=(ROOT/'scan.log').read_text()
assert '=== feature-audit scan summary ===' in log, 'scan is not complete'
inventory=json.loads((ROOT.parent/'corpus/inventory.json').read_text())
declared=collections.Counter(t for c in inventory['countries'] for t in c['tables'])
starts=re.findall(r'^\[(\d+)/(\d+) ([^]]+)\] building\.\.\.$',log,re.M)
done=re.findall(r'^    done in (\d+)s  \((\d+) countries, \+(\d+) records\)$',log,re.M)
assert len(starts)==len(done)==len(declared)==39
assert {r[2] for r in starts}==set(declared)
assert all(int(d[1])==declared[s[2]] for s,d in zip(starts,done))
assert sum(int(d[1]) for d in done)==549
assert not any(r['status']=='skip' for r in records)
scan_source=(TREE/'bench/feature_audit/scan.py').read_text()
grid_node=next(n for n in ast.parse(scan_source).body if isinstance(n,ast.AnnAssign)
               and getattr(n.target,'id',None)=='KWARG_GRID')
# The frozen trusted harness expression contains only literal grids, list
# concatenation and comprehensions over literal unit strings; no module import.
grid=eval(compile(ast.Expression(grid_node.value),'<frozen KWARG_GRID>','eval'),{'__builtins__':{}})
feature_rows=[]
for s,d in zip(starts,done):
    feature=s[2]
    feature_rows.append({'feature':feature,'declared_country_pairs':int(d[1]),
        'duration_seconds_rounded':int(d[0]),'emitted_records':int(d[2]),
        'phase1a_country_calls':int(d[1]),'phase1b_baseline_calls':int(feature!='panel_ids'),
        'phase2_variant_calls':len(grid.get(feature,[]))})
counts=collections.Counter((r['feature'],r['phase'],r.get('check','scope'),r['status']) for r in records)
caught=[]
for r in records:
    match=re.match(r'^Failed to load (\S+) for (.+?): ([A-Za-z_][A-Za-z_0-9]*): (.*)',r.get('detail',''),re.S)
    if match:
        feature,country,error_type,message=match.groups()
        category=('missing_source_or_unwired' if country in {'Armenia','Nepal','Peru'} else
                  'missing_cluster_dependency' if r.get('kwargs',{}).get('market')=='Region'
                  and message.startswith('cluster_features returned no data') else 'unclassified')
        caught.append((feature,r['phase'],country,error_type,category,json.dumps(r.get('kwargs',{}),sort_keys=True)))
hard=[]
for r in records:
    if r['status'] not in {'error','fail'}:continue
    error_type=r.get('detail','').split(':',1)[0]
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',error_type):error_type='nonexception_finding'
    hard.append((r['feature'],r['phase'],r.get('country') or 'ALL',r.get('check'),r['status'],error_type))
def tsv(name,header,rows):
    with (OUT/name).open('w') as f:
        w=csv.writer(f,delimiter='\t');w.writerow(header);w.writerows(rows)
tsv('feature-coverage.tsv',list(feature_rows[0]),[list(r.values()) for r in feature_rows])
tsv('check-counts.tsv',['feature','phase','check','status','count'],[(*k,v) for k,v in sorted(counts.items())])
tsv('caught-error-counts.tsv',['feature','phase','country','exception_type','category','kwargs','count'],
    [(*k,v) for k,v in sorted(collections.Counter(caught).items())])
tsv('hard-finding-counts.tsv',['feature','phase','country','check','status','exception_type','count'],
    [(*k,v) for k,v in sorted(collections.Counter(hard).items())])
tsv('cluster-counts.tsv',['cluster_id','check','severity','status','record_count','feature_count','country_count'],
    [(r['cluster_id'],r['check'],r['severity'],r['status'],r['count'],len(r['features']),len(r['countries'])) for r in clusters])
metadata=json.loads((ROOT/'run-metadata.json').read_text())
summary={'source_sha':metadata['source_sha'],'features_completed':39,'declared_country_pairs':549,
    'country_count':len(inventory['countries']),'phases_requested':[1,2],
    'phase1a_country_calls':sum(r['phase1a_country_calls'] for r in feature_rows),
    'phase1b_baseline_feature_calls':sum(r['phase1b_baseline_calls'] for r in feature_rows),
    'phase2_feature_variant_calls':sum(r['phase2_variant_calls'] for r in feature_rows),
    'phase2_grid':grid,'non_dataframe_property_note':'panel_ids is loaded in phase1a; existing harness intentionally does not assemble it as a DataFrame.',
    'scope_skips':0,'records':len(records),'status_counts':dict(collections.Counter(r['status'] for r in records)),
    'diagnostic_clusters':len(clusters),
    'cluster_status_counts':dict(collections.Counter(r['status'] for r in clusters)),
    'phase_counts':dict(collections.Counter(r['phase'] for r in records)),
    'feature_caught_exception_count':len(caught),
    'feature_caught_categories':dict(collections.Counter(r[4] for r in caught)),
    'wall_seconds_approx':(ROOT/'scan.log').stat().st_mtime-metadata['started_epoch_approx'],
    'sum_rounded_feature_seconds':sum(r['duration_seconds_rounded'] for r in feature_rows),
    'sequential_jobs':1,'cpu_affinity':'0-7','blas_threads':1,'lsms_build_workers':1,
    'timeout_limitation':metadata['timeout_note'],
    'authentication':'LSMS_SKIP_AUTH=1 suppresses import-time setup/unlock/prompt only. Existing credentials and shared L1 were available; normal lazy access remains enabled. This is NOT a credential-free audit (see #1001).',
    'raw_records_private':True,'raw_logs_private':True,
    'passing_checks_note':'Existing scanner omits most passing sanity/assembly checks. Emitted-record counts are diagnostic counts, not test pass totals.',
    'interpreter_version':platform.python_version()}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
def digest(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return {'bytes':p.stat().st_size,'sha256':h.hexdigest()}
provenance={'source_sha':metadata['source_sha'],'private_raw_artifacts':{},'frozen_source_files':{},
    'sanitization':'Only feature/phase/check/status counts, public country names, exception types/categories, configured kwargs, durations and coverage are published. Raw details, example values/keys, tracebacks and logs remain private.'}
for name in ['results.jsonl','scan.log','clusters.json','summary.json','ghana-food-loss.log']:
    provenance['private_raw_artifacts'][name]=digest(ROOT/name)
for name in ['bench/feature_audit/scan.py','bench/feature_audit/cluster.py','lsms_library/diagnostics.py','lsms_library/feature.py','lsms_library/country.py']:
    provenance['frozen_source_files'][name]=digest(TREE/name)
provenance['launcher']=digest(ROOT/'run.sh')
provenance['aggregate_generator']=digest(Path(__file__))
provenance['input_snapshot']=digest(ROOT/'snapshot.json')
provenance['original_corpus_manifest']=digest(ROOT.parent/'corpus/snapshot-manifest.json')
provenance['original_preservation_check']=digest(ROOT/'original-preservation.json')
provenance['run_metadata']=digest(ROOT/'run-metadata.json')
provenance['completion']=digest(ROOT/'completion.json')
provenance['tenure_history']=digest(ROOT/'tenure-history.json')
provenance['market_dependency_history']=digest(ROOT/'market-dependency-history.json')
provenance['replay_and_verification_generators']={name:digest(ROOT/name) for name in ['tenure_history.py','reconcile_ghana.py','verify_original.py']}
(OUT/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
print(json.dumps(summary,indent=2))
