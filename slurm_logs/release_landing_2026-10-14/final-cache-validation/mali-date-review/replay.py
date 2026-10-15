"""Read-only, aggregate-only replay of Mali's post-read date loss."""
from pathlib import Path
import ast
import collections
import hashlib
import importlib.util
import json
import subprocess

import pandas as pd
import lsms_library as ll
from lsms_library.country import (
    _normalize_dataframe_index, _enforce_declared_dtypes, _enforce_canonical_dtypes,
)

OUT = Path(__file__).resolve().parent
TREE = Path('/local/job39919092/release-work/runtime/final-candidate')
ROOT = Path('/local/job39919092/release-work/runtime/data-final-promotion')
BASE = OUT.parents[1] / 'corpus'
assert BASE == Path('/local/job39919092/release-work/control/runtime/corpus')
assert Path(ll.__file__).resolve() == TREE / 'lsms_library/__init__.py'
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=TREE, text=True).strip() == '3914d8804d7b2936d75041cea98bbc9ddb6d4c9f'
spec = importlib.util.spec_from_file_location('fingerprint_only', TREE / 'tests/generate_baseline.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def file_identity(path):
    stat = path.stat()
    return {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'bytes': stat.st_size, 'mtime_ns': stat.st_mtime_ns,
            'device': stat.st_dev, 'inode': stat.st_ino}


def wave_counts(frame):
    return {str(k): int(v) for k, v in frame.index.get_level_values('t').value_counts().items()}


def summary(frame):
    return {**module.fingerprint(frame), 'wave_rows': wave_counts(frame),
            'null_dates_by_wave': {str(w): int(part['Int_t'].isna().sum())
                                  for w, part in frame.groupby(level='t', sort=False)}}


path = ROOT / 'Mali/var/interview_date.parquet'
before = file_identity(path)
frame = pd.read_parquet(path)
scheme = {'index': '(t, v, i)', 'Int_t': 'datetime'}
normalized = _normalize_dataframe_index(frame.copy(), scheme, None, 'interview_date', country='Mali')
assert normalized.equals(frame)
coerced = normalized.reorder_levels(['i', 't', 'v'])
_enforce_declared_dtypes(coerced, scheme)
after_declared = summary(coerced)
_enforce_canonical_dtypes(coerced, 'interview_date')
after_canonical = summary(coerced)
dropped = coerced.dropna(how='all')
per_wave = frame.copy()
per_wave['Int_t'] = pd.concat([pd.to_datetime(part['Int_t'], errors='coerce')
    for _, part in frame.groupby(level='t', sort=False)]).reindex(frame.index)
per_wave = per_wave.reorder_levels(['i', 't', 'v'])
mixed = frame.copy()
mixed['Int_t'] = pd.to_datetime(mixed['Int_t'], errors='coerce', format='mixed')
mixed = mixed.reorder_levels(['i', 't', 'v'])
pd.testing.assert_frame_equal(mixed, per_wave)
old = {r['table']: r for r in map(json.loads, (BASE / 'Mali/results.jsonl').read_text().splitlines())}['interview_date']
new = {r['table']: r for r in map(json.loads, (OUT.parent / 'run/Mali/results.jsonl').read_text().splitlines())}['interview_date']
assert module.fingerprint(dropped)['content_hash'] == new['served']['content_hash']
assert module.fingerprint(mixed)['content_hash'] == old['served']['content_hash']
formats = {}
for wave, part in frame.groupby(level='t', sort=False):
    values = part['Int_t'].astype('string')
    formats[str(wave)] = {'rows': len(part),
                         'date_only': int(values.str.fullmatch(r'\d{4}-\d{2}-\d{2}').sum()),
                         'date_and_time': int(values.str.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}').sum()),
                         'raw_nulls': int(values.isna().sum())}
history = {}
for ref in ('v0.14.0', '452302adcfdbf3af538ef06375df1ac6c5aaa8a0',
            '7fa05179c8519f44ea329ba6387f5d6d6441a8ea', '3914d8804d7b2936d75041cea98bbc9ddb6d4c9f'):
    history[ref] = {}
    for file, names in (
        ('lsms_library/country.py', ('_enforce_declared_dtypes', '_enforce_canonical_dtypes', '_finalize_result')),
        ('lsms_library/local_tools.py', ('to_parquet',)),
    ):
        source = subprocess.check_output(['git', 'show', f'{ref}:{file}'], cwd=TREE, text=True)
        parsed = ast.parse(source)
        for node in ast.walk(parsed):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
                body = ast.get_source_segment(source, node)
                history[ref][file + ':' + node.name] = hashlib.sha256(body.encode()).hexdigest()
assert file_identity(path) == before
result = {'source_sha': '3914d8804d7b2936d75041cea98bbc9ddb6d4c9f',
          'input': str(path), 'input_identity_before_and_after': before,
          'package': ll.__file__, 'pandas': pd.__version__,
          'table_api_calls': 0, 'cache_writes': 0,
          'raw': summary(frame), 'normalization': summary(normalized),
          'raw_date_formats_by_wave': formats,
          'after_declared_dtype': after_declared, 'after_canonical_dtype': after_canonical,
          'after_dropna': summary(dropped), 'mixed_parse_counterfactual': summary(mixed),
          'per_wave_parse_equals_mixed_exactly': True,
          'default_parse_then_drop_matches_warm_digest_exactly': True,
          'mixed_parse_matches_cold_digest_exactly': True,
          'source_history_sha256': history}
(OUT / 'replay.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({key: result[key] for key in (
    'source_sha', 'pandas', 'table_api_calls', 'cache_writes', 'raw_date_formats_by_wave',
    'default_parse_then_drop_matches_warm_digest_exactly',
    'mixed_parse_matches_cold_digest_exactly')}, indent=2))
