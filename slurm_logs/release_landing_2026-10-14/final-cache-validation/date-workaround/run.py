"""Verify ordinary single-wave date APIs on a separate physical cache copy."""
from pathlib import Path
from types import SimpleNamespace
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import time

import pandas as pd
import lsms_library as ll
from lsms_library.country import Country, _normalize_dataframe_index

OUT = Path(__file__).resolve().parent
TREE = Path('/local/job39919092/release-work/runtime/final-candidate')
SOURCE = TREE.parent / 'data-final-promotion'
DATA = TREE.parent / 'data-date-workaround'
BASE = OUT.parents[1] / 'corpus'
SHA = '3914d8804d7b2936d75041cea98bbc9ddb6d4c9f'
assert Path(ll.__file__).resolve() == TREE / 'lsms_library/__init__.py'
assert os.environ['LSMS_DATA_DIR'] == str(DATA)
assert not os.environ.get('LSMS_NO_CACHE') and not os.environ.get('LSMS_BUILD_BACKEND')
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=TREE, text=True).strip() == SHA
spec = importlib.util.spec_from_file_location('fingerprint_only', TREE / 'tests/generate_baseline.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def identity(path):
    st = path.stat()
    return {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'bytes': st.st_size,
            'mtime_ns': st.st_mtime_ns, 'device': st.st_dev, 'inode': st.st_ino}


def inventory(root):
    return {str(p.relative_to(root)): identity(p) for country in ('Mali', 'Tanzania')
            for p in sorted((root / country).rglob('*.parquet'))}


before_source = inventory(SOURCE)
# Library import may initialize an empty root and empty L1 directory. Refuse
# any existing payload; remove only that empty directory, never cache content.
if DATA.exists():
    assert list(DATA.iterdir()) == [DATA / 'dvc-cache']
    (DATA / 'dvc-cache').rmdir()
else:
    DATA.mkdir()
(DATA / 'dvc-cache').symlink_to('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache')
for country in ('Mali', 'Tanzania'):
    assert not any(p.is_symlink() for p in (SOURCE / country).rglob('*'))
    shutil.copytree(SOURCE / country, DATA / country, copy_function=shutil.copy2)
before_copy = inventory(DATA)
assert before_source.keys() == before_copy.keys()
for relative, old in before_source.items():
    new = before_copy[relative]
    assert old['sha256'] == new['sha256'] and old['bytes'] == new['bytes']
    assert (old['device'], old['inode']) != (new['device'], new['inode'])
(OUT / 'copy-manifest.json').write_text(json.dumps({'source': before_source, 'copy': before_copy}, indent=2) + '\n')

results = {}
for country in ('Mali', 'Tanzania'):
    co = ll.Country(country, preload_panel_ids=False)
    parts, calls = [], []
    for wave in co.waves:
        started = time.perf_counter()
        frame = co.interview_date(waves=[wave])
        parts.append(frame)
        calls.append({'wave': wave, 'seconds': time.perf_counter() - started,
                      'served': module.fingerprint(frame),
                      'wave_rows': {str(k): int(v) for k, v in frame.index.get_level_values('t').value_counts().items()}})
    combined = pd.concat(parts)
    raw = pd.read_parquet(DATA / country / 'var/interview_date.parquet')
    scheme = {'index': '(t, v, i)' if country == 'Mali' else '(t, i)', 'Int_t': 'datetime'}
    expected = _normalize_dataframe_index(raw.copy(), scheme, None, 'interview_date', country=country)
    if country == 'Tanzania':
        sample = pd.read_parquet(DATA / country / 'var/sample.parquet')
        stub = SimpleNamespace(name=country, _sample_v_cache=sample[['v']].copy())
        expected = Country._join_v_from_sample(stub, expected)
    expected = expected.reorder_levels(['i', 't', 'v'])
    expected['Int_t'] = pd.to_datetime(expected['Int_t'], errors='coerce', format='mixed')
    expected = expected.dropna(how='all')
    old = {r['table']: r for r in map(json.loads, (BASE / country / 'results.jsonl').read_text().splitlines())}['interview_date']
    assert module.fingerprint(expected)['content_hash'] == old['served']['content_hash']
    # Concatenation groups waves; baseline row order can interleave waves.
    # Require exact values/index/dtypes after alignment, and separately record
    # whether original order was already equal. No numeric tolerance allowed.
    assert combined.index.is_unique and expected.index.is_unique
    assert set(combined.index) == set(expected.index)
    aligned = combined.reindex(expected.index)
    pd.testing.assert_frame_equal(aligned, expected, check_exact=True)
    assert module.fingerprint(aligned) == {k: old['served'][k] for k in module.fingerprint(aligned)}
    results[country] = {'calls': calls, 'combined': module.fingerprint(combined),
        'aligned_to_cold_order': module.fingerprint(aligned),
        'original_concat_order_equals_cold': combined.index.equals(expected.index),
        'exact_values_index_dtypes_equal_cold_after_alignment': True,
        'wave_rows': {str(k): int(v) for k, v in combined.index.get_level_values('t').value_counts().items()}}
assert inventory(SOURCE) == before_source
assert inventory(DATA) == before_copy
result = {'source_sha': SHA, 'package': ll.__file__, 'pandas': pd.__version__,
          'cache_root': str(DATA), 'country_results': results,
          'ordinary_single_wave_calls': sum(len(r['calls']) for r in results.values()),
          'physical_copy_files': len(before_copy), 'all_source_and_copy_parquets_unchanged': True,
          'bypasses': [], 'LSMS_SKIP_AUTH': 'setup skipped only; existing L1 credentials/cache environment retained'}
(OUT / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
