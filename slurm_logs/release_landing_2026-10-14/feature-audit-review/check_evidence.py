"""Independently check saved Feature evidence. No library imports/cache access."""
import ast
from collections import Counter
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
from datetime import datetime, timezone

import yaml

CONTROL = Path('/local/job39919092/release-work/control')
ROOT = CONTROL / 'runtime/feature-audit'
OUT = CONTROL / 'countries/feature-audit-review'
TREE = Path('/local/job39919092/release-work/runtime/corpus')
PUB = ROOT / 'publishable'

def read(p): return json.loads(p.read_text())
def digest(p): return {'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
def git(*args): return subprocess.check_output(['git', '-C', str(TREE), *args], text=True)
def tsv(name):
    with (PUB/name).open() as f: return list(csv.DictReader(f, delimiter='\t'))

records = [json.loads(line) for line in (ROOT/'results.jsonl').read_text().splitlines()]
summary = read(PUB/'summary.json')
inventory = read(CONTROL/'runtime/corpus/inventory.json')['countries']
declared = Counter(t for c in inventory for t in c['tables'])
all_pairs = {(c['country'], t) for c in inventory for t in c['tables']}
assert len(all_pairs) == 549 and len(declared) == 39 and len(inventory) == 38
statuses = Counter(r['status'] for r in records)
phases = Counter(r['phase'] for r in records)
assert statuses == summary['status_counts'] == {'warn':3504, 'error':19, 'fail':5, 'pass':16}
assert phases == summary['phase_counts']
assert len(records) == summary['records'] == 3544
assert not any(r['status'] == 'skip' for r in records)
log = (ROOT/'scan.log').read_text()
assert '=== feature-audit scan summary ===' in log
starts = re.findall(r'^\[(\d+)/(\d+) ([^]]+)\] building\.\.\.$', log, re.M)
done = re.findall(r'^    done in (\d+)s  \((\d+) countries, \+(\d+) records\)$', log, re.M)
assert len(starts) == len(done) == 39
assert [int(s[0]) for s in starts] == list(range(1, 40))
assert {s[2] for s in starts} == set(declared)
coverage = tsv('feature-coverage.tsv')
for start, end, public in zip(starts, done, coverage):
    f = start[2]
    assert f == public['feature']
    assert int(end[1]) == declared[f] == int(public['declared_country_pairs'])
    assert int(end[2]) == sum(r['feature'] == f for r in records) == int(public['emitted_records'])
    assert int(end[0]) == int(public['duration_seconds_rounded'])
    assert int(public['phase1a_country_calls']) == declared[f]
    assert int(public['phase1b_baseline_calls']) == int(f != 'panel_ids')
    assert int(public['phase2_variant_calls']) == len(summary['phase2_grid'].get(f, []))
assert sum(int(c['phase2_variant_calls']) for c in coverage) == 17
assert sum(int(c['phase1b_baseline_calls']) for c in coverage) == 38
assert sum(int(c['phase1a_country_calls']) for c in coverage) == 549
checks = Counter((r['feature'], r['phase'], r['check'], r['status']) for r in records)
public_checks = {tuple(r[k] for k in ['feature','phase','check','status']):int(r['count'])
                 for r in tsv('check-counts.tsv')}
assert checks == public_checks
corpus = [json.loads(line) for p in (CONTROL/'runtime/corpus').glob('*/results.jsonl')
          for line in p.read_text().splitlines()]
corpus_by_pair = {(r['country'], r['table']):r for r in corpus}
errors = [r for r in records if r['status'] == 'error']
error_pairs = {(r['country'],r['feature']) for r in errors}
assert len(errors) == len(error_pairs) == 19
assert error_pairs == {(r['country'],r['table']) for r in corpus if r['status'] == 'error'}
assert Counter(c for c,t in error_pairs) == {'Armenia':2, 'Nepal':11, 'Peru':6}
assert all(r['phase'] == '1a-sanity' and r['check'] == 'build' for r in errors)
fails = [r for r in records if r['status'] == 'fail']
test_source = (TREE/'tests/test_declared_spellings.py').read_text()
known = next(n for n in ast.parse(test_source).body if isinstance(n, ast.AnnAssign)
             and getattr(n.target,'id',None) == 'KNOWN_UNHARMONIZED')
assert {(r['country'],r['feature']) for r in fails} == ast.literal_eval(known.value.args[0])
assert all(r['check'] == 'declared_spellings' for r in fails)
hard_counts = Counter((r['feature'],r['phase'],r['country'],r['check'],r['status'],
                       r['detail'].split(':',1)[0] if r['status']=='error' else 'nonexception_finding')
                      for r in errors+fails)
public_hard = {tuple(r[k] for k in ['feature','phase','country','check','status','exception_type']):int(r['count'])
               for r in tsv('hard-finding-counts.tsv')}
# The publisher permits a label-like first token for a nonexception finding.
assert sum(public_hard.values()) == 24
assert Counter(k[:5] for k in hard_counts.elements()) == Counter(
    k[:5] for k,v in public_hard.items() for _ in range(v))
caught = []
for r in records:
    m = re.match(r'^Failed to load (\S+) for (.+?): ([A-Za-z_][A-Za-z_0-9]*): (.*)', r['detail'], re.S)
    if not m: continue
    feature,country,exception,message = m.groups()
    category = ('missing_source_or_unwired' if (country,feature) in error_pairs else
                'missing_cluster_dependency' if country in ['GhanaSPS','Panama']
                and r['kwargs'] == {'market':'Region'}
                and message == 'cluster_features returned no data for any wave.' else 'UNKNOWN')
    assert category != 'UNKNOWN', (feature,country,exception)
    caught.append((feature,r['phase'],country,exception,category,json.dumps(r['kwargs'],sort_keys=True)))
assert Counter(caught) == {tuple(r[k] for k in ['feature','phase','country','exception_type','category','kwargs']):int(r['count'])
                           for r in tsv('caught-error-counts.tsv')}
assert Counter(r[4] for r in caught) == {'missing_source_or_unwired':52,'missing_cluster_dependency':8}
clusters = read(ROOT/'clusters.json')
assert len(clusters) == summary['diagnostic_clusters'] == 266
assert sum(r['count'] for r in clusters) == 3528
assert Counter(r['status'] for r in clusters) == summary['cluster_status_counts']
for r,p in zip(clusters,tsv('cluster-counts.tsv')):
    assert r['cluster_id'] == p['cluster_id'] and re.fullmatch(r'C\d+',p['cluster_id'])
    assert r['count'] == int(p['record_count'])
    assert len(r['features']) == int(p['feature_count'])
    assert len(r['countries']) == int(p['country_count'])
    assert all(r[k] == p[k] for k in ['check','severity','status'])

history = read(ROOT/'tenure-history.json')
for ref,expected in history['history'].items():
    for country,wanted in expected['country_tree_git_hashes'].items():
        assert git('rev-parse',f'{ref}:lsms_library/countries/{country}').strip() == wanted
    for path,symbol,key in [('lsms_library/diagnostics.py','_check_declared_spellings','declared_spellings_function_sha256'),
                            ('tests/test_declared_spellings.py','KNOWN_UNHARMONIZED','known_unharmonized_declaration_sha256')]:
        s = git('show',f'{ref}:{path}')
        n = next(n for n in ast.parse(s).body if getattr(n,'name',None)==symbol
                 or isinstance(n,ast.AnnAssign) and getattr(n.target,'id',None)==symbol)
        assert hashlib.sha256(ast.get_source_segment(s,n).encode()).hexdigest() == expected[key]
    schema = yaml.safe_load(git('show',f'{ref}:lsms_library/data_info.yml'))['Columns']['plot_features']
    assert hashlib.sha256(json.dumps(schema,sort_keys=True).encode()).hexdigest() == expected['plot_features_schema_sha256']
for country,paths in history['country_changes_v0_14_to_frozen'].items():
    assert git('diff','--name-only',history['refs'][0],history['refs'][-1],'--',f'lsms_library/countries/{country}').splitlines() == paths

conservation = []
for r in records:
    if r['check'] != 'reaggregate_conservation': continue
    f = r['feature']; col = r['metrics']['column']
    kept = ['Malawi','Nigeria','Uganda'] if f=='assets' else ['EthiopiaRHS','GhanaLSS','Malawi','Nigeria','Uganda']
    warn = next(x for x in records if x['feature']==f and x['kwargs']=={'labels':'Aggregate'}
                and "labels='Aggregate' unavailable for" in x['detail'])
    dropped = ast.literal_eval(re.search(r'country\(ies\) (\[.*?\]) --',warn['detail']).group(1))
    assert not set(kept) & set(dropped)
    assert f'(kept {len(kept)})' in warn['detail']
    values = [corpus_by_pair[c,f]['served']['numeric_sums'].get(col,0) for c in kept]
    total = math.fsum(values)
    got = r['metrics']['got']
    expected_loss = {'Quantity':16931.817543859648,'Expenditure':20272.567543859648}.get(col,0) if f=='food_acquired' else 0
    remainder = total-got-expected_loss
    assert abs(remainder) < 0.001
    conservation.append({'feature':f,'column':col,'kept_countries':kept,
                         'excluded_for_labels':len(dropped),'retained_country_fsum':total,
                         'feature_aggregate':got,'known_Ghana_raw_checksum_loss':expected_loss,
                         'floating_remainder':remainder})

# Inspect the proof without touching either original or active cache.
manifest = read(CONTROL/'runtime/corpus/snapshot-manifest.json')
preserved = read(ROOT/'original-preservation.json')
snapshot = read(ROOT/'snapshot.json')
assert len(manifest['files']) == preserved['files_verified'] == snapshot['files'] == 1519
assert sum(v['bytes'] for v in manifest['files'].values()) == snapshot['bytes'] == 340885785
assert preserved['all_sha256_unchanged'] and preserved['all_metadata_unchanged']
assert snapshot['all_files_sha256_equal'] and snapshot['all_file_inodes_distinct']
assert manifest['symlinks'] == preserved['only_original_symlink'] == {
    'dvc-cache':'/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache'}
assert preserved['source_worktree_clean'] and preserved['in_tree_parquets'] == 0
provenance = read(PUB/'provenance.json')
for name,wanted in provenance['private_raw_artifacts'].items(): assert digest(ROOT/name) == wanted
for name,wanted in provenance['frozen_source_files'].items(): assert digest(TREE/name) == wanted
archive = read(PUB/'MANIFEST.json')
assert set(archive['files']) == {p.name for p in PUB.iterdir() if p.is_file()} - {'MANIFEST.json'}
for name,wanted in archive['files'].items():
    assert digest(PUB/name) == wanted
    if name not in ['summary.json','provenance.json'] and (ROOT/name).is_file():
        assert (PUB/name).read_bytes() == (ROOT/name).read_bytes()

public_headers = {name:list(tsv(name)[0]) for name in [
    'feature-coverage.tsv','check-counts.tsv','caught-error-counts.tsv',
    'hard-finding-counts.tsv','cluster-counts.tsv']}
assert all(not any(k in fields for k in ['detail','example','traceback','index','household_id'])
           for fields in public_headers.values())
artifact_paths = [p for p in PUB.iterdir() if p.is_file()] + [ROOT/n for n in
    ['REPORT.md','TRIAGE.md','completion.json','original-preservation.json','snapshot.json',
     'tenure-history.json','market-dependency-history.json','publish_summary.py','verify_original.py']]
output = {'observed_utc':datetime.now(timezone.utc).isoformat(),'source_sha':summary['source_sha'],
          'scope':'saved aggregates/private diagnostics and source only; no APIs, builds or cache access',
          'counts':{'features':39,'countries':38,'country_pairs':549,'baseline_calls':38,'variants':17,
                    'records':len(records),'statuses':dict(statuses),'phases':dict(phases),'clusters':len(clusters)},
          'hard_errors_exactly_match_corpus':True,'error_pairs':sorted(error_pairs),
          'five_fails_match_existing_known_defect_set':True,
          'historical_source_hashes_independently_verified':True,
          'caught_exception_categories':dict(Counter(r[4] for r in caught)),
          'aggregate_checksum_reconciliation':conservation,
          'preservation_evidence_internally_consistent':True,
          'preservation_limit':'Verifier logic and recorded manifest/result inspected; original bytes not reread by reviewer',
          'public_tsv_headers':public_headers,
          'publishable_manifest_verified':True,
          'reviewed_artifacts':{str(p.relative_to(ROOT)):digest(p) for p in artifact_paths}}
(OUT/'validation.json').write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({k:output[k] for k in ['counts','hard_errors_exactly_match_corpus',
    'five_fails_match_existing_known_defect_set','caught_exception_categories',
    'aggregate_checksum_reconciliation','preservation_evidence_internally_consistent']},indent=2))
