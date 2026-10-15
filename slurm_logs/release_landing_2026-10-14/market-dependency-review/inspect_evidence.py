"""Read-only source/evidence inspection; imports no lsms_library module."""
import ast
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml

SOURCE = Path('/local/job39919092/release-work/runtime/corpus')
CONTROL = Path('/local/job39919092/release-work/control')
OUT = CONTROL / 'countries/market-dependency-review'
REFS = ['v0.14.0', '452302adcfdbf3af538ef06375df1ac6c5aaa8a0',
        '7fa05179c8519f44ea329ba6387f5d6d6441a8ea']

def git(*args):
    return subprocess.check_output(['git', '-C', str(SOURCE), *args]).decode()

def sha(data):
    return hashlib.sha256(data if isinstance(data, bytes) else data.encode()).hexdigest()

record = {'observed_utc': datetime.now(timezone.utc).isoformat(),
          'scope': 'Git object reads and saved aggregate evidence only; no API or cache access',
          'refs': {}, 'corpus': {}}
recorded = json.loads((CONTROL / 'runtime/feature-audit/market-dependency-history.json').read_text())
for ref in REFS:
    code = git('show', f'{ref}:lsms_library/country.py')
    nodes = ast.parse(code)
    functions = {}
    for node in ast.walk(nodes):
        if isinstance(node, ast.FunctionDef) and node.name in ['_market_lookup', '_add_market_index']:
            functions[node.name] = sha(ast.get_source_segment(code, node))
            assert functions[node.name] == recorded[ref][node.name]
    entry = {'commit': git('rev-parse', f'{ref}^{{commit}}').strip(),
             'helpers_sha256_ast_source_segment': functions,
             'feature_blob': git('rev-parse', f'{ref}:lsms_library/feature.py').strip(),
             'countries': {}}
    for country in ['GhanaSPS', 'Panama']:
        prefix = f'lsms_library/countries/{country}'
        scheme_path = f'{prefix}/_/data_scheme.yml'
        scheme = yaml.load(git('show', f'{ref}:{scheme_path}'), Loader=yaml.BaseLoader)['Data Scheme']
        blob = git('rev-parse', f'{ref}:{scheme_path}').strip()
        assert blob == recorded[ref][country]['data_scheme_blob']
        assert 'cluster_features' not in scheme
        assert 'Region' not in scheme['sample']
        cfgs = {}
        paths = git('ls-tree', '-r', '--name-only', ref, prefix).splitlines()
        for path in paths:
            if path.endswith('/_/data_info.yml'):
                config = yaml.load(git('show', f'{ref}:{path}'), Loader=yaml.BaseLoader)
                cfgs[path] = {
                    'blob': git('rev-parse', f'{ref}:{path}').strip(),
                    'cluster_features_declared': 'cluster_features' in config,
                    'sample_myvars': config.get('sample', {}).get('myvars'),
                }
                assert 'cluster_features' not in config
        entry['countries'][country] = {
            'data_scheme_blob': blob, 'declared_tables': list(scheme),
            'sample_declared_columns': list(scheme['sample']), 'wave_configs': cfgs,
        }
    record['refs'][ref] = entry

for country in ['GhanaSPS', 'Panama']:
    data = (CONTROL / f'runtime/corpus/{country}/results.jsonl').read_bytes()
    rows = [json.loads(line) for line in data.splitlines()]
    sample = next(row for row in rows if row['table'] == 'sample')
    assert sample['status'] == 'success'
    assert sample['served']['null_counts']['v'] == 0
    assert 'Region' not in sample['served']['columns']
    assert all(row['status'] == 'success' for row in rows)
    record['corpus'][country] = {
        'results_sha256': sha(data), 'success_pairs': len(rows),
        'declared_waves': sample['declared_waves'], 'sample_served': sample['served'],
    }

# Snapshot only complete flushed JSON lines from the active scanner log.
data = (CONTROL / 'runtime/feature-audit/results.jsonl').read_bytes()
lines = data.splitlines(keepends=True)
if lines and not lines[-1].endswith(b'\n'):
    lines.pop()
rows = [json.loads(line) for line in lines]
selected = [row for row in rows
            if row.get('kwargs', {}).get('market') == 'Region'
            and 'cluster_features returned no data for any wave' in row.get('detail', '')
            and any(country in row['detail'] for country in ['GhanaSPS', 'Panama'])]
for feature in ['food_expenditures', 'food_prices', 'food_quantities']:
    for country in ['GhanaSPS', 'Panama']:
        assert sum(row['feature'] == feature and f'for {country}:' in row['detail']
                   for row in selected) == 1
record['audit_snapshot'] = {
    'complete_lines': len(lines), 'complete_prefix_sha256': sha(b''.join(lines)),
    'warnings': selected,
}
(OUT / 'evidence.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps({'refs_verified': len(record['refs']),
                  'recorded_helper_hashes_match': True,
                  'region_warnings': len(selected),
                  'sample_v_nulls': {c: v['sample_served']['null_counts']['v']
                                     for c, v in record['corpus'].items()}}, indent=2))
