"""Pure in-memory Tanzania date replay, using preloaded raw sample for v."""
from pathlib import Path
from types import SimpleNamespace
import hashlib
import importlib.util
import json
import subprocess

import pandas as pd
import lsms_library as ll
from lsms_library.country import Country, _normalize_dataframe_index, _enforce_declared_dtypes, _enforce_canonical_dtypes

OUT = Path(__file__).resolve().parent
TREE = Path('/local/job39919092/release-work/runtime/final-candidate')
ROOT = Path('/local/job39919092/release-work/runtime/data-final-promotion')
BASE = OUT.parents[1] / 'corpus'
assert Path(ll.__file__).resolve() == TREE / 'lsms_library/__init__.py'
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=TREE, text=True).strip() == '3914d8804d7b2936d75041cea98bbc9ddb6d4c9f'
spec = importlib.util.spec_from_file_location('fingerprint_only', TREE / 'tests/generate_baseline.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def identity(path):
    stat = path.stat()
    return {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'bytes': stat.st_size,
            'mtime_ns': stat.st_mtime_ns, 'device': stat.st_dev, 'inode': stat.st_ino}


def summary(frame):
    return {**module.fingerprint(frame),
            'wave_rows': {str(k): int(v) for k, v in frame.index.get_level_values('t').value_counts().items()},
            'null_dates_by_wave': {str(w): int(part['Int_t'].isna().sum())
                                  for w, part in frame.groupby(level='t', sort=False)}}


paths = {'dates': ROOT / 'Tanzania/var/interview_date.parquet',
         'sample': ROOT / 'Tanzania/var/sample.parquet'}
before = {name: identity(path) for name, path in paths.items()}
frame, sample = pd.read_parquet(paths['dates']), pd.read_parquet(paths['sample'])
scheme = {'index': '(t, i)', 'Int_t': 'datetime'}
normalized = _normalize_dataframe_index(frame.copy(), scheme, None, 'interview_date', country='Tanzania')
assert normalized.equals(frame)
# The existing helper consumes only this preloaded lookup. The stub has no
# sample() or Country table method, so it cannot make an API call.
stub = SimpleNamespace(name='Tanzania', _sample_v_cache=sample[['v']].copy())
joined = Country._join_v_from_sample(stub, normalized.copy()).reorder_levels(['i', 't', 'v'])
assert len(joined) == len(frame)
coerced = joined.copy()
_enforce_declared_dtypes(coerced, scheme)
after_declared = summary(coerced)
_enforce_canonical_dtypes(coerced, 'interview_date')
after_canonical = summary(coerced)
dropped = coerced.dropna(how='all')
mixed = joined.copy()
mixed['Int_t'] = pd.to_datetime(mixed['Int_t'], errors='coerce', format='mixed')
per_wave = joined.copy()
per_wave['Int_t'] = pd.concat([pd.to_datetime(part['Int_t'], errors='coerce')
    for _, part in joined.groupby(level='t', sort=False)]).reindex(joined.index)
pd.testing.assert_frame_equal(mixed, per_wave)
mixed_dropped = mixed.dropna(how='all')
old = {r['table']: r for r in map(json.loads, (BASE / 'Tanzania/results.jsonl').read_text().splitlines())}['interview_date']
new = {r['table']: r for r in map(json.loads, (OUT.parent / 'run/Tanzania/results.jsonl').read_text().splitlines())}['interview_date']
assert module.fingerprint(dropped)['content_hash'] == new['served']['content_hash']
assert module.fingerprint(mixed_dropped)['content_hash'] == old['served']['content_hash']
formats = {}
for wave, part in frame.groupby(level='t', sort=False):
    values = part['Int_t'].astype('string')
    formats[str(wave)] = {'rows': len(part), 'raw_nulls': int(values.isna().sum()),
                         'date_only': int(values.str.fullmatch(r'\d{4}-\d{2}-\d{2}').sum()),
                         'date_and_time': int(values.str.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}').sum()),
                         'timestamp_iso_t': int(values.str.match(r'\d{4}-\d{2}-\d{2}T').sum())}
assert all(identity(paths[name]) == metadata for name, metadata in before.items())
result = {'source_sha': '3914d8804d7b2936d75041cea98bbc9ddb6d4c9f',
          'package': ll.__file__, 'pandas': pd.__version__,
          'inputs_unchanged': before, 'table_api_calls': 0, 'cache_writes': 0,
          'raw': summary(frame), 'normalized': summary(normalized), 'after_v_join': summary(joined),
          'after_declared_dtype': after_declared, 'after_canonical_dtype': after_canonical,
          'after_dropna': summary(dropped), 'mixed_parse_after_dropna': summary(mixed_dropped),
          'raw_date_formats_by_wave': formats,
          'per_wave_parse_equals_mixed_exactly': True,
          'default_parse_then_drop_matches_warm_digest_exactly': True,
          'mixed_parse_then_drop_matches_cold_digest_exactly': True,
          'original_null_rows': int(frame['Int_t'].isna().sum())}
(OUT / 'replay.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: result[k] for k in ('source_sha', 'pandas', 'table_api_calls', 'cache_writes',
    'raw_date_formats_by_wave', 'original_null_rows',
    'default_parse_then_drop_matches_warm_digest_exactly',
    'mixed_parse_then_drop_matches_cold_digest_exactly')}, indent=2))
