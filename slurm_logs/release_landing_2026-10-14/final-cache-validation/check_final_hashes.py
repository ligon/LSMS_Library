"""Final-source metadata preflight; run only after the coordinator supplies SHA."""
from pathlib import Path
import collections
import hashlib
import json
import subprocess
import time

import prepare_copy as copy
import run_final as run


def verify_copy(manifest):
    actual = {str(p.relative_to(run.DATA)) for p in run.DATA.rglob('*')
              if p.is_file() and not p.is_symlink()}
    assert actual == set(manifest['files'])
    for relative, expected in manifest['files'].items():
        path = run.DATA / relative
        stat = path.stat()
        assert (stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino) == (
            expected['bytes'], expected['mtime_ns'], expected['device'], expected['inode']), relative
        assert copy.sha(path) == expected['sha256'], relative
    links = {str(p.relative_to(run.DATA)): str(p.readlink())
             for p in run.DATA.rglob('*') if p.is_symlink()}
    assert links == manifest['symlinks']


def main():
    started = time.perf_counter()
    assert not run.OUT.exists(), 'preflight/run directory already exists; inspect before retrying'
    manifest = json.loads((copy.CONTROL / 'copy-manifest.json').read_text())
    original = json.loads((copy.BASE / 'snapshot-manifest.json').read_text())
    copy.verify_original(original)
    verify_copy(manifest)
    ll, _ = run.setup()
    run.OUT.mkdir()
    current = run.inventory()
    baseline = json.loads((copy.BASE / 'inventory.json').read_text())['countries']
    old_axes = {r['country']: (r['tables'], r['waves']) for r in baseline}
    new_axes = {r['country']: (r.get('tables'), r.get('waves')) for r in current}
    assert new_axes == old_axes, 'declaration/wave inventory changed; requires explicit review'
    before = {(r['country'], r['table']): r
              for path in copy.BASE.glob('*/results.jsonl')
              for r in map(json.loads, path.read_text().splitlines())}
    expected_pairs = {(r['country'], t) for r in current for t in r['tables']}
    assert len(expected_pairs) == 549 and set(before) == expected_pairs
    records = []
    for inv in current:
        co = ll.Country(inv['country'], preload_panel_ids=False)
        for table in inv['tables']:
            old = before[co.name, table]
            expected = co._table_cache_hash(table, list(co.waves))
            record = {
                'country': co.name, 'table': table,
                'previous_expected_hash': old.get('expected_hash'), 'expected_hash': expected,
                'country_hash_changed': expected != old.get('expected_hash'),
                'logical_wave_cache_observations': [],
            }
            path = run.DATA / co.name / 'var' / (table + '.parquet')
            record['country_cache'] = run.parquet_info(path, expected) if path.exists() else None
            for wave, old_cache in old.get('wave_caches', {}).items():
                folder = co.wave_folder_map.get(wave, wave)
                path = run.DATA / co.name / folder / '_' / (table + '.parquet')
                new_hash = co[wave]._input_hash(table)
                record['logical_wave_cache_observations'].append({
                    'wave': wave, 'folder': folder,
                    'previous_expected_hash': old_cache['expected_hash'],
                    'expected_hash': new_hash,
                    'hash_changed': old_cache['expected_hash'] != new_hash,
                    'cache': run.parquet_info(path, new_hash) if path.exists() else None,
                })
            records.append(record)
    verify_copy(manifest)
    copy.verify_original(original)
    changed = [r for r in records if r['country_hash_changed']]
    source_diff = subprocess.check_output([
        'git', 'diff', '--name-status', original['source_sha'], run.SHA,
    ], cwd=run.TREE, text=True)
    (run.OUT / 'source-diff.txt').write_text(source_diff)
    result = {
        'source_sha': run.SHA, 'baseline_source_sha': original['source_sha'],
        'countries': len(current), 'pairs': len(records), 'records': records,
        'country_hash_changes': len(changed),
        'country_hash_changes_by_country': dict(collections.Counter(r['country'] for r in changed)),
        'wave_observations': sum(len(r['logical_wave_cache_observations']) for r in records),
        'wave_hash_changes': sum(w['hash_changed'] for r in records
                                 for w in r['logical_wave_cache_observations']),
        'original_and_copy_unchanged': True, 'table_api_calls': 0,
        'elapsed_seconds': time.perf_counter() - started,
    }
    run.dump(run.OUT / 'final-hash-impact.json', result)
    run.dump(run.OUT / 'warm-start-approved.json', {
        'source_sha': run.SHA, 'baseline_source_sha': original['source_sha'],
        'pairs': len(records), 'private_copy_verified': True,
        'hash_impact_sha256': copy.sha(run.OUT / 'final-hash-impact.json'),
        'meaning': 'identity and cache-copy preconditions verified; no shared-write authorization',
    })
    print(json.dumps({k: v for k, v in result.items() if k != 'records'}, indent=2))


if __name__ == '__main__':
    main()
