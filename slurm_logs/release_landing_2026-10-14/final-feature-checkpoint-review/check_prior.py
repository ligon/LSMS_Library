"""Independent saved-record/metadata reconciliation; no library/cache access."""
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import runpy
import subprocess

HERE = Path(__file__).resolve().parent
AUDIT = Path('/local/job39919092/release-work/control/runtime/feature-audit')
PACKAGE = AUDIT/'prior-comparison'
TREE = Path('/local/job39919092/release-work/runtime/corpus')
CORPUS = AUDIT.parent/'corpus'
result = json.loads((PACKAGE/'comparison.json').read_text())

def digest(p):
    b = p.read_bytes()
    return {'bytes': len(b), 'sha256': hashlib.sha256(b).hexdigest()}

def read(p):
    return [json.loads(s) for s in p.read_text().splitlines() if s.strip()]

def git(*args):
    return subprocess.check_output(['git', '-C', str(TREE), *args], text=True)

current = read(AUDIT/'results.jsonl')
assert digest(AUDIT/'results.jsonl') == result['current_raw']
prior = []
for item in result['prior_snapshots']:
    p = Path(item['private_path'])
    assert digest(p) == {k: item[k] for k in ('bytes','sha256')}
    rows = read(p)
    assert dict(Counter(r.get('status') for r in rows)) == item['status_counts']
    assert dict(Counter(r.get('phase') for r in rows)) == item['phase_counts']
    assert sorted({r['feature'] for r in rows if r.get('feature')}) == item['features']
    prior.append(rows)
normalize = runpy.run_path(str(TREE/'bench/feature_audit/cluster.py'))['_normalize']
countries = {r['country'] for rows in [current,*prior] for r in rows if r.get('country')}
def key(r):
    return r.get('check'),r.get('severity'),r.get('status'),normalize(r.get('detail',''),countries)
old = {key(r) for rows in prior for r in rows if r.get('status') in {'warn','fail','error'}}
now = Counter(key(r) for r in current if r.get('status') == 'warn')
checks = {'current_warning_patterns':len(now),
          'current_patterns_seen_in_prior_union':sum(k in old for k in now),
          'current_patterns_unmatched_in_prior_union':sum(k not in old for k in now),
          'current_warning_records_in_matched_patterns':sum(v for k,v in now.items() if k in old)}
assert all(result[k] == v for k,v in checks.items())
generator_ast = ast.parse((PACKAGE/'compare_prior.py').read_text())
patterns = next(ast.literal_eval(n.value) for n in generator_ast.body
                if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='patterns' for t in n.targets))
categories = Counter()
for r in current:
    if r.get('status') != 'warn': continue
    cat = r['check'] if r['check'] != 'runtime_warning' else next((k for k,s in patterns if s in r.get('detail','')),'unclassified_runtime')
    categories[cat] += 1
assert dict(categories) == result['warning_category_record_counts']
assert sum(categories.values()) == 3504 and not categories['unclassified_runtime']
for ref, items in result['emitter_source_hashes'].items():
    for name, expected in items.items():
        if '::' not in name:
            actual = git('rev-parse',f'{ref}:{name}').strip()
        else:
            path, function = name.split('::')
            source = git('show',f'{ref}:{path}')
            node = next(n for n in ast.walk(ast.parse(source)) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==function)
            actual = hashlib.sha256(ast.get_source_segment(source,node).encode()).hexdigest()
        assert actual == expected, (ref,name)
refs = list(result['emitter_source_hashes'].values())
assert refs[0] == refs[1] == refs[2]
shapes = result['shape_exclusion_inputs']
for country, feature in shapes['equal_cached_index_name_pairs']:
    a=json.loads((CORPUS/country/'shared-baseline.json').read_text())['files']
    b=json.loads((CORPUS/country/'private-final-cache.json').read_text())['files']
    path='var/'+feature+'.parquet'
    assert a[path]['frame']['index_names'] == b[path]['frame']['index_names']
for row in shapes['missing_comparable_country_parquets']:
    a=json.loads((CORPUS/row['country']/'shared-baseline.json').read_text())['files']
    b=json.loads((CORPUS/row['country']/'private-final-cache.json').read_text())['files']
    path='var/'+row['feature']+'.parquet'
    assert (path in a) == row['shared_present'] and (path in b) == row['private_present']
assert len(shapes['equal_cached_index_name_pairs']) == 100
assert len(shapes['missing_comparable_country_parquets']) == 8
assert not shapes['changed_index_name_pairs']
manifest=json.loads((PACKAGE/'MANIFEST.json').read_text())
for name, expected in manifest['files'].items(): assert digest(PACKAGE/name)==expected
out={'scope':'saved records and metadata only; no library imports, API/cache access or source mutations',
     'counts':checks,'warnings':sum(categories.values()),'source_hashes_verified':sum(map(len,result['emitter_source_hashes'].values())),
     'all_three_refs_listed_emitters_identical':True,'index_name_pairs_equal':100,'index_name_comparison_gaps':8,
     'reviewed_files':{p.name:digest(p) for p in sorted(PACKAGE.iterdir()) if p.is_file()}}
(HERE/'prior-validation.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k!='reviewed_files'},indent=2))
