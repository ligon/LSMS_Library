"""Scratch release validation; reuses APIs and existing baseline fingerprint.
No writes outside OUT and the explicitly private DATA root; shared reads only.
"""
from __future__ import annotations
import argparse
import collections
import concurrent.futures
import gc
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
import warnings

TREE = Path('/local/job39919092/release-work/runtime/corpus')
DATA = Path('/local/job39919092/release-work/runtime/data-corpus')
SHARED = Path('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library')
OUT = Path('/local/job39919092/release-work/control/runtime/corpus')
SHA = '7fa05179c8519f44ea329ba6387f5d6d6441a8ea'


def dump(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + '\n')
    tmp.replace(path)


def append(path, value):
    with Path(path).open('a') as stream:
        stream.write(json.dumps(value, default=str, allow_nan=False) + '\n')
        stream.flush()


def setup():
    import lsms_library as ll
    from lsms_library.paths import countries_root
    from lsms_library.local_tools import data_root
    assert Path(ll.__file__).resolve() == TREE/'lsms_library/__init__.py', ll.__file__
    assert countries_root().resolve() == TREE/'lsms_library/countries'
    assert data_root().resolve() == DATA
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=TREE, text=True).strip() == SHA
    for key in ('LSMS_NO_CACHE', 'PYTHONSAFEPATH', 'LSMS_BUILD_BACKEND'):
        assert not os.environ.get(key), (key, os.environ.get(key))
    spec = importlib.util.spec_from_file_location('release_fingerprint', TREE/'tests/generate_baseline.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return ll, mod.fingerprint


def frame_summary(df, fingerprint):
    import numpy as np
    import pandas as pd
    rec = fingerprint(df)
    rec['index_unique'] = bool(df.index.is_unique)
    rec['null_counts'] = {str(c): int(df[c].isna().sum()) for c in df.columns}
    rec['numeric_sums'] = {}
    for col in df.select_dtypes(include='number').columns:
        val = df[col].sum(min_count=1)
        rec['numeric_sums'][str(col)] = None if pd.isna(val) else (float(val) if np.isfinite(val) else str(val))
    rec['wave_rows'] = {}
    if 't' in df.index.names:
        rec['wave_rows'] = {('<NULL>' if pd.isna(k) else str(k)): int(v)
                            for k, v in df.index.get_level_values('t').value_counts(dropna=False).items()}
    elif 't' in df.columns:
        rec['wave_rows'] = {('<NULL>' if pd.isna(k) else str(k)): int(v)
                            for k, v in df['t'].value_counts(dropna=False).items()}
    return rec


def parquet_info(path, expected=None, with_frame=False, fingerprint=None):
    import pandas as pd
    import pyarrow.parquet as pq
    from lsms_library.local_tools import cache_freshness, read_parquet_cache_hash, read_parquet_cache_waves
    before = path.stat()
    md = pq.read_metadata(path)
    rec = {'bytes': before.st_size, 'mtime_ns': before.st_mtime_ns,
           'rows': md.num_rows, 'physical_columns': md.num_columns,
           'stored_hash': read_parquet_cache_hash(path),
           'footer_waves': read_parquet_cache_waves(path),
           'expected_hash': expected, 'freshness': cache_freshness(path, expected)}
    if expected is not None and path.parent.name == 'var' and rec['stored_hash'] is None:
        rec['country_read_policy'] = 'stale'
    if with_frame:
        rec['frame'] = frame_summary(pd.read_parquet(path), fingerprint)
    after = path.stat()
    rec['stable_during_read'] = (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    return rec


def snapshot_country(country, root, co, fingerprint, destination):
    """Read parquets/JSON directly; never invoke API against shared cache."""
    records = {}
    for path in sorted((root/country).rglob('*')):
        if not path.is_file() or path.suffix not in {'.parquet', '.json'}:
            continue
        key = str(path.relative_to(root/country))
        try:
            if path.suffix == '.parquet':
                expected = co._table_cache_hash(path.stem, list(co.waves)) if path.parent.name == 'var' else None
                records[key] = parquet_info(path, expected, True, fingerprint)
            else:
                before = path.stat()
                blob = path.read_bytes()
                val = json.loads(blob)
                after = path.stat()
                records[key] = {'bytes': before.st_size, 'mtime_ns': before.st_mtime_ns,
                                'sha256': hashlib.sha256(blob).hexdigest(),
                                'type': type(val).__name__, 'entries': len(val) if hasattr(val, '__len__') else None,
                                'stable_during_read': (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)}
        except Exception as exc:
            records[key] = {'error': f'{type(exc).__name__}: {exc}', 'traceback': traceback.format_exc()}
    dump(destination, {'source_sha': SHA, 'root': str(root), 'country': country, 'files': records})


def inventory():
    ll, _ = setup()
    records = []
    for name in ll.countries():
        try:
            co = ll.Country(name, preload_panel_ids=False)
            records.append({'country': name, 'waves': list(co.waves), 'tables': list(co.data_scheme),
                            'contents': str(co.file_path/'_/CONTENTS.org'),
                            'contents_exists': (co.file_path/'_/CONTENTS.org').is_file()})
        except Exception as exc:
            records.append({'country': name, 'inventory_error': f'{type(exc).__name__}: {exc}',
                            'traceback': traceback.format_exc()})
    dump(OUT/'inventory.json', {'source_sha': SHA, 'worktree': str(TREE), 'private_root': str(DATA),
                              'shared_root': str(SHARED), 'countries': records,
                              'pairs': sum(len(r.get('tables', [])) for r in records)})
    return records


def worker(country):
    ll, fingerprint = setup()
    import pandas as pd
    from lsms_library.diagnostics import load_feature
    dest = OUT/country
    dest.mkdir(exist_ok=True)
    t0 = time.perf_counter()
    co = ll.Country(country, preload_panel_ids=False)
    tables = list(co.data_scheme)
    waves = list(co.waves)
    assert not list((DATA/country).rglob('*.parquet')), f'{country}: root must start empty'
    contents = co.file_path/'_/CONTENTS.org'
    # Retain a content identity so later findings cite the documentation read.
    dump(dest/'identity.json', {'source_sha': SHA, 'package': ll.__file__, 'countries_root': str(co.file_path.parent),
                              'waves': waves, 'tables': tables, 'start_time': time.time(),
                              'contents_sha256': hashlib.sha256(contents.read_bytes()).hexdigest() if contents.is_file() else None})
    dump(dest/'status.json', {'stage': 'shared_baseline_snapshot', 'country': country})
    snapshot_country(country, SHARED, co, fingerprint, dest/'shared-baseline.json')
    counts = collections.Counter()
    for table in tables:
        started = time.perf_counter()
        dump(dest/'status.json', {'stage': 'build', 'country': country, 'table': table,
                                 'completed': sum(counts.values()), 'total': len(tables), 'since': time.time()})
        rec = {'country': country, 'table': table, 'source_sha': SHA, 'declared_waves': waves}
        value = None
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always')
            try:
                value = load_feature(co, table)
                rec['return_type'] = type(value).__name__
                if isinstance(value, pd.DataFrame):
                    rec['status'] = 'empty' if value.empty else 'success'
                    try:
                        rec['served'] = frame_summary(value, fingerprint)
                    except Exception as exc:
                        rec['measurement_error'] = f'{type(exc).__name__}: {exc}'
                        rec['measurement_traceback'] = traceback.format_exc()
                elif value is None:
                    rec['status'] = 'empty'
                elif isinstance(value, dict):
                    rec['status'] = 'success' if value else 'empty'
                    rec['entries'] = len(value)
                    rec['content_hash'] = hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()
                else:
                    rec['status'] = 'success'
                    rec['entries'] = len(value) if hasattr(value, '__len__') else None
            except Exception as exc:
                rec['status'] = 'error'
                rec['error'] = f'{type(exc).__name__}: {exc}'
                rec['traceback'] = traceback.format_exc()
        rec['elapsed_seconds'] = time.perf_counter()-started
        rec['warning_counts'] = dict(collections.Counter(w.category.__name__ for w in captured))
        # Detailed diagnostics remain private scratch; aggregate report uses counts.
        dump(dest/(table+'-warnings.json'), [{'category': w.category.__name__, 'message': str(w.message)} for w in captured])
        try:
            expected = co._table_cache_hash(table, waves)
            rec['expected_hash'] = expected
            path = DATA/country/'var'/f'{table}.parquet'
            rec['country_cache'] = parquet_info(path, expected) if path.exists() else None
            rec['wave_caches'] = {}
            for wave in waves:
                wo = co[wave]
                folder = co.wave_folder_map.get(wave, wave)
                path = DATA/country/folder/'_'/f'{table}.parquet'
                if path.exists():
                    rec['wave_caches'][str(wave)] = {'folder': folder, **parquet_info(path, wo._input_hash(table))}
        except Exception as exc:
            rec['cache_measurement_error'] = f'{type(exc).__name__}: {exc}'
            rec['cache_measurement_traceback'] = traceback.format_exc()
        counts[rec['status']] += 1
        append(dest/'results.jsonl', rec)
        print(json.dumps({'country': country, 'table': table, 'status': rec['status'],
                          'seconds': round(rec['elapsed_seconds'], 2)}), flush=True)
        del value
        gc.collect()
    dump(dest/'status.json', {'stage': 'private_cache_snapshot', 'country': country, 'counts': dict(counts)})
    snapshot_country(country, DATA, co, fingerprint, dest/'private-final-cache.json')
    dirty = subprocess.check_output(['git', 'status', '--short'], cwd=TREE, text=True)
    intree = [str(p.relative_to(TREE)) for p in co.file_path.rglob('*.parquet')]
    dump(dest/'status.json', {'stage': 'complete', 'country': country, 'counts': dict(counts),
                             'elapsed_seconds': time.perf_counter()-t0, 'git_status': dirty, 'in_tree_parquets': intree})
    return 1 if counts['error'] else 0


def run():
    records = inventory()
    countries = [r['country'] for r in records if 'tables' in r]
    def one(name):
        with (OUT/(name+'.log')).open('w') as stream:
            process = subprocess.Popen([sys.executable, str(Path(__file__)), '--country', name], cwd=TREE,
                                       stdout=stream, stderr=subprocess.STDOUT)
            append(OUT/'processes.jsonl', {'country': name, 'pid': process.pid, 'event': 'start', 'time': time.time()})
            code = process.wait()
            append(OUT/'processes.jsonl', {'country': name, 'pid': process.pid, 'event': 'exit', 'code': code, 'time': time.time()})
            return {'country': name, 'exit_code': code}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(one, c) for c in countries]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    dump(OUT/'completion.json', {'source_sha': SHA, 'processes': results, 'inventory_errors': [r for r in records if 'inventory_error' in r]})
    return 1 if any(r['exit_code'] for r in results) else 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--country')
    parser.add_argument('--inventory-only', action='store_true')
    args = parser.parse_args()
    if args.inventory_only:
        inventory()
    elif args.country:
        raise SystemExit(worker(args.country))
    else:
        raise SystemExit(run())
