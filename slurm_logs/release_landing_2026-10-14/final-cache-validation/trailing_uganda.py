"""Ordinary trailing reads after the full final-source country inventory."""
import json
import time
import warnings

import run_final as run


def main():
    ll, fingerprint = run.setup()
    from lsms_library.diagnostics import load_feature
    completion = json.loads((run.OUT / 'completion.json').read_text())
    assert completion['source_sha'] == run.SHA and not completion['inventory_errors']
    assert len(completion['processes']) == 38
    assert {r['country']: r['exit_code'] for r in completion['processes'] if r['exit_code']} == {
        'Armenia': 1, 'Nepal': 1, 'Peru': 1}
    assert not (run.OUT / 'trailing-uganda.json').exists(), 'refusing duplicate trailing sequence'
    old = {r['table']: r for r in map(json.loads,
           (run.BASE / 'Uganda/results.jsonl').read_text().splitlines())}
    co = ll.Country('Uganda', preload_panel_ids=False)
    results = []
    for repetition in (1, 2):
        for table in ('fct', 'earnings'):
            started = time.perf_counter()
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter('always')
                frame = load_feature(co, table)
            served = run.frame_summary(frame, fingerprint)
            path = run.DATA / 'Uganda/var' / (table + '.parquet')
            expected = co._table_cache_hash(table, list(co.waves))
            cache = run.parquet_info(path, expected, True, fingerprint)
            result = {'repetition': repetition, 'table': table,
                      'served': served, 'cache': cache,
                      'seconds': time.perf_counter() - started,
                      'warnings': [{'category': w.category.__name__, 'message': str(w.message)}
                                   for w in captured]}
            results.append(result)
            run.dump(run.OUT / 'trailing-uganda-progress.json', results)
            assert served['content_hash'] == old[table]['served']['content_hash'], (table, 'served hash changed')
            assert served['shape'] == old[table]['served']['shape'], (table, 'served shape changed')
            baseline_raw = json.loads((run.BASE / 'Uganda/private-final-cache.json').read_text())['files'][f'var/{table}.parquet']['frame']
            assert cache['frame']['content_hash'] == baseline_raw['content_hash'], (table, 'raw hash changed')
            assert cache['frame']['shape'] == baseline_raw['shape'], (table, 'raw shape changed')
            assert cache['freshness'] == 'fresh', (table, cache)
            assert cache['footer_waves'] is not None and set(co.waves) <= set(cache['footer_waves']), table
            if repetition == 2:
                first = next(r for r in results if r['repetition'] == 1 and r['table'] == table)
                assert (cache['mtime_ns'], cache['bytes']) == (
                    first['cache']['mtime_ns'], first['cache']['bytes']), (table, 'second read rewrote cache')
    # Re-read every Uganda file after both rounds; sibling rewrites cannot hide
    # behind the per-call observation. This is a snapshot, never a metadata write.
    run.snapshot_country('Uganda', run.DATA, co, fingerprint,
                         run.OUT / 'Uganda/private-final-cache.json')
    run.dump(run.OUT / 'trailing-uganda.json', {'source_sha': run.SHA, 'calls': results})
    print(json.dumps({'source_sha': run.SHA, 'calls': len(results),
                      'all_served_and_raw_fingerprints_match_corpus': True,
                      'both_call_rounds_fresh_with_full_coverage': True}, indent=2))


if __name__ == '__main__':
    main()
