"""Read-only source attribution and label-column comparison; no library API."""
from pathlib import Path
import ast
import hashlib
import json
import subprocess

import pandas as pd
import yaml

ROOT = Path(__file__).parent
TREE = Path('/local/job39919092/release-work/runtime/corpus')
SHARED = Path('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library')
PRIVATE = Path('/local/job39919092/release-work/runtime/data-corpus')
REFS = ['1528fa937bcf87f146b3af9ebfdbe18b8f912750',
        '452302adcfdbf3af538ef06375df1ac6c5aaa8a0',
        '7fa05179c8519f44ea329ba6387f5d6d6441a8ea']
COUNTRIES = ['Albania', 'Cambodia', 'Kosovo', 'Tajikistan', 'Timor-Leste']
def git(*args):
    return subprocess.check_output(['git', *args], cwd=TREE, text=True)
def text(ref, path):
    return git('show', f'{ref}:{path}')
def sha(s):
    return hashlib.sha256(s.encode()).hexdigest()
def symbol(source, name):
    node = next(n for n in ast.parse(source).body if
                getattr(n, 'name', None) == name or
                isinstance(n, ast.AnnAssign) and getattr(n.target, 'id', None) == name)
    return ast.get_source_segment(source, node)

history = {}
for ref in REFS:
    schema = yaml.safe_load(text(ref, 'lsms_library/data_info.yml'))
    history[ref] = {
        'country_tree_git_hashes': {c: git('rev-parse', f'{ref}:lsms_library/countries/{c}').strip() for c in COUNTRIES},
        'declared_spellings_function_sha256': sha(symbol(text(ref, 'lsms_library/diagnostics.py'), '_check_declared_spellings')),
        'known_unharmonized_declaration_sha256': sha(symbol(text(ref, 'tests/test_declared_spellings.py'), 'KNOWN_UNHARMONIZED')),
        'plot_features_schema_sha256': sha(json.dumps(schema['Columns']['plot_features'], sort_keys=True)),
    }
for key in ['declared_spellings_function_sha256', 'known_unharmonized_declaration_sha256', 'plot_features_schema_sha256']:
    assert len({h[key] for h in history.values()}) == 1, key
changed = {}
for c in COUNTRIES:
    paths = git('diff', '--name-only', REFS[0], REFS[-1], '--', f'lsms_library/countries/{c}').splitlines()
    assert paths == ([f'lsms_library/countries/{c}/_/Makefile'] if c == 'Cambodia' else []), (c, paths)
    changed[c] = paths
comparisons = {}
for c in COUNTRIES:
    old = SHARED/c/'var/plot_features.parquet'
    new = PRIVATE/c/'var/plot_features.parquet'
    before = [(p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ino) for p in [old, new]]
    left, right = pd.read_parquet(old), pd.read_parquet(new)
    columns = [col for col in ['Tenure', 'TenureSystem'] if col in right]
    pd.testing.assert_frame_equal(left[columns], right[columns])
    after = [(p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ino) for p in [old, new]]
    assert before == after, c
    comparisons[c] = {'tenure_columns': columns, 'rows': len(right),
                      'shared_and_cold_private_tenure_frames_exactly_equal': True,
                      'source_file_metadata_unchanged': True}
issues = json.loads((ROOT.parent.parent/'countries/release-all-issues-live.json').read_text())
issue = next(r for r in issues if r['number'] == 602)
result = {'refs': REFS, 'history': history, 'country_changes_v0_14_to_frozen': changed,
          'cambodia_only_diff': git('diff', REFS[0], REFS[-1], '--', 'lsms_library/countries/Cambodia/_/Makefile'),
          'read_only_label_comparisons': comparisons,
          'issue': {k: issue[k] for k in ['number', 'title', 'state', 'url']},
          'issue_body_sha256': sha(issue['body']),
          'interpretation': 'Five real unresolved tenure-vocabulary defects explicitly documented by the pre-existing test enumeration and ledger602 section7. Closed602 fixed enforcement, not these mappings. No historical cold rebuild or historical exact input-count claim. Existing shared cache is corroborating evidence, not a truth baseline.'}
(ROOT/'tenure-history.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps({'matching_check_and_vocabulary_source': True, 'matching_known_defect_set': True,
                  'country_changes': changed, 'read_only_label_comparisons': comparisons}, indent=2))
