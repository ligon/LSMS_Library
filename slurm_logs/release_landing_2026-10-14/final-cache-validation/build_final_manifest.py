"""Freeze final evidence/allowlist after the completed run and trailing reads.

Only metadata/hash methods and direct parquet reads are used. No table API,
cache writes, or shared-L2 access. The emitted manifest remains pending review.
"""
from pathlib import Path
import collections
import hashlib
import json
import time

import prepare_copy as copy
import run_final as run


def main():
    started = time.perf_counter()
    ll, fingerprint = run.setup()
    completion = json.loads((run.OUT / 'completion.json').read_text())
    trailing = json.loads((run.OUT / 'trailing-uganda.json').read_text())
    assert completion['source_sha'] == trailing['source_sha'] == run.SHA
    assert not completion['inventory_errors'] and len(completion['processes']) == 38
    assert all(json.loads(p.read_text())['stage'] == 'complete' for p in run.OUT.glob('*/status.json'))
    original = json.loads((run.BASE / 'snapshot-manifest.json').read_text())
    copy.verify_original(original)
    old_records = {(r['country'], r['table']): r for p in run.BASE.glob('*/results.jsonl')
                   for r in map(json.loads, p.read_text().splitlines())}
    records = [r for p in run.OUT.glob('*/results.jsonl')
               for r in map(json.loads, p.read_text().splitlines())]
    counts = collections.Counter((r['country'], r['table']) for r in records)
    assert set(counts) == set(old_records) and len(counts) == 549 and set(counts.values()) == {1}
    differences = []
    for r in records:
        before = old_records[r['country'], r['table']]
        for key in ('status', 'served', 'content_hash', 'entries', 'return_type'):
            if r.get(key) != before.get(key):
                differences.append({'country': r['country'], 'table': r['table'], 'field': key,
                                    'before': before.get(key), 'after': r.get(key)})
        if r['status'] == 'error' and r['error'].split(':', 1)[0] != before['error'].split(':', 1)[0]:
            differences.append({'country': r['country'], 'table': r['table'], 'field': 'error_type'})
        for key in ('measurement_error', 'cache_measurement_error'):
            if r.get(key):
                differences.append({'country': r['country'], 'table': r['table'], 'field': key, 'after': r[key]})
    exception_path = copy.CONTROL / 'served-exceptions.json'
    exceptions = json.loads(exception_path.read_text())
    assert exceptions['source_sha'] == run.SHA
    assert exceptions['baseline_sha'] == original['source_sha']
    for path, digest in exceptions['evidence_sha256'].items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, ('exception evidence changed', path)
    allowed = {(r['country'], r['table'], r['field']): r for r in exceptions['exceptions']}
    assert set(allowed) == {('Liberia', 'interview_date', 'served'),
                           ('Mali', 'household_roster', 'served'),
                           ('Mali', 'interview_date', 'served'),
                           ('Tanzania', 'interview_date', 'served')}
    unexpected = []
    for diff in differences:
        item = allowed.get((diff['country'], diff['table'], diff['field']))
        if item is None or diff != {k: item[k] for k in ('country', 'table', 'field', 'before', 'after')}:
            unexpected.append(diff)
        else:
            diff['classification'] = item['classification']
            diff['scope'] = item['scope']
    assert {(r['country'], r['table'], r['field']) for r in differences} == set(allowed)
    run.dump(run.OUT / 'served-comparison.json', {
        'baseline_sha': original['source_sha'], 'final_sha': run.SHA,
        'pairs': len(records), 'differences': differences,
        'unexplained_differences': unexpected, 'universal_cold_warm_api_equality': False,
        'exception_manifest_sha256': copy.sha(exception_path),
        'counts': dict(collections.Counter(r['status'] for r in records)),
        'error_pairs': [{'country': r['country'], 'table': r['table'],
                         'type': r['error'].split(':', 1)[0]} for r in records if r['status'] == 'error'],
    })
    assert not unexpected, 'unexplained served/status/measurement drift; inspect served-comparison.json'
    assert collections.Counter(r['status'] for r in records) == {'success': 530, 'error': 19}
    initial = json.loads((copy.CONTROL / 'copy-manifest.json').read_text())
    preflight = json.loads((run.OUT / 'final-hash-impact.json').read_text())
    expected_changes = {(r['country'], r['table']): r['country_hash_changed'] for r in preflight['records']}
    start_time_ns = int(min(r['time'] for r in map(json.loads,
        (run.OUT / 'processes.jsonl').read_text().splitlines()) if r['event'] == 'start') * 1e9)
    actual = {str(p.relative_to(run.DATA)) for p in run.DATA.rglob('*')
              if p.is_file() and not p.is_symlink()}
    assert actual == set(original['files']), 'physical fileset changed; explicit review required'
    links = {str(p.relative_to(run.DATA)): str(p.readlink()) for p in run.DATA.rglob('*') if p.is_symlink()}
    assert links == original['symlinks']
    countries = {}
    baseline_files = {}
    for country in {Path(p).parts[0] for p in actual}:
        countries[country] = ll.Country(country, preload_panel_ids=False)
        baseline_files[country] = json.loads((run.BASE / country / 'private-final-cache.json').read_text())['files']
    files, exclusions, raw_differences = {}, {}, []
    for relative in sorted(actual):
        path = run.DATA / relative
        parts = Path(relative).parts
        country, table = parts[0], path.stem
        co = countries[country]
        stat = path.stat()
        entry = {'sha256': copy.sha(path), 'bytes': stat.st_size, 'mtime_ns': stat.st_mtime_ns,
                 'device': stat.st_dev, 'inode': stat.st_ino, 'disposition': 'promote_verified'}
        assert (stat.st_dev, stat.st_ino) != (original['files'][relative]['device'], original['files'][relative]['inode'])
        country_expected = co._table_cache_hash(table, list(co.waves)) if parts[1] == 'var' else None
        meta = run.parquet_info(path, country_expected, True, fingerprint)
        assert meta['stable_during_read']
        raw = meta.pop('frame')
        entry.update(meta)
        entry['frame'] = raw
        previous = baseline_files[country]['/'.join(parts[1:])]
        if raw != previous['frame']:
            raw_differences.append({'path': relative, 'before': previous['frame'], 'after': raw})
        entry['raw_fingerprint_matches_cold_corpus'] = raw == previous['frame']
        entry['physical_sha_matches_initial_copy'] = entry['sha256'] == initial['files'][relative]['sha256']
        if parts[1] == 'var':
            entry['declared_waves'] = list(co.waves)
            if meta['freshness'] == 'fresh' and meta['footer_waves'] is not None and set(co.waves) <= set(meta['footer_waves']):
                entry['classification'] = 'country_fresh_full_coverage'
            else:
                exclusions[relative] = {'reason': 'country hash/coverage incomplete', **entry}
                continue
        elif parts[1] == '_':
            assert relative in {'Malawi/_/panel_ids.parquet', 'Nigeria/_/panel_ids.parquet'}, relative
            assert not expected_changes[country, 'panel_ids']
            assert entry['physical_sha_matches_initial_copy'] and entry['raw_fingerprint_matches_cold_corpus']
            entry['classification'] = 'companion_verified_provenance'
            entry['provenance'] = 'Design-B panel_ids companion built by completed cold load_json_cache; final input hash unchanged; final panel_ids API digest unchanged; bytes unchanged'
        else:
            folder = parts[1]
            waves = [folder] if folder in co.waves else [w for w in co.waves if co.wave_folder_map.get(w, w) == folder]
            assert waves, (relative, 'unresolved physical folder')
            expected = {w: co[w]._input_hash(table) for w in waves}
            entry['logical_wave_expected_hashes'] = expected
            if meta['stored_hash'] is not None:
                if set(expected.values()) == {meta['stored_hash']}:
                    entry['classification'] = 'wave_fresh'
                    entry['expected_hash'] = meta['stored_hash']
                    entry['freshness'] = 'fresh'
                else:
                    exclusions[relative] = {'reason': 'stamped wave hash mismatch/ambiguity', **entry}
                    continue
            else:
                scripts = [co.file_path / '_' / (table + '.py')]
                scripts += [co[w].file_path / '_' / (table + '.py') for w in waves]
                assert any(p.exists() for p in scripts), (relative, 'hashless wave without script provenance')
                changed = expected_changes.get((country, table))
                assert changed is not None, (relative, 'undeclared script output')
                if changed:
                    assert stat.st_mtime_ns >= start_time_ns, (relative, 'changed input but legacy output not rebuilt')
                    provenance = 'final ordinary stale-country rebuild; script wave rewritten during final run; raw fingerprint compared'
                else:
                    assert entry['physical_sha_matches_initial_copy'], (relative, 'unchanged input but unexpected script cache rewrite')
                    provenance = 'completed cold build; final inputs unchanged; copied bytes unchanged; raw fingerprint compared'
                entry['classification'] = 'script_wave_legacy_verified_provenance'
                entry['freshness'] = 'legacy_script_wave'
                entry['provenance'] = provenance
        files[relative] = entry
    run.dump(run.OUT / 'raw-cache-comparison.json', {'files': len(actual), 'differences': raw_differences})
    run.dump(run.OUT / 'promotion-exclusions.json', exclusions)
    assert not raw_differences, 'unexpected raw value drift; inspect raw-cache-comparison.json'
    # Explicit coordinator disposition for the observed #788/#809 sibling
    # cycle. Never stamp or promote this hashless country output. Preserve it
    # privately and back up/retire only its exact shared counterpart.
    assert set(exclusions) == {'Uganda/var/nutrition.parquet'}, 'unreviewed cache exclusion'
    nutrition = exclusions['Uganda/var/nutrition.parquet']
    assert nutrition['stored_hash'] is None and nutrition['footer_waves'] is None
    assert nutrition['raw_fingerprint_matches_cold_corpus']
    assert nutrition['frame']['shape'] == [23777, 15]
    assert nutrition['frame']['content_hash'] == '12d25f6ec6d069af7d3cff703b5091bc4770944eaafacf2e87bc2f058f450e3a'
    nutrition['disposition'] = 'exclude_hashless_country_output'
    nutrition['reason'] = 'Ordinary fct rebuild invokes shared nutrition.py rule and rewrites nutrition without country hash/coverage; raw values unchanged. Existing #788/#809 sibling-write cycle.'
    run.dump(run.OUT / 'promotion-exclusions.json', exclusions)
    retire = {'Uganda/var/nutrition.parquet': {
        'disposition': 'backup_then_retire',
        'reason': 'Exclude verified-value but hashless final nutrition sibling output; back up and retire the exact shared counterpart if present. Next ordinary nutrition() rebuilds via the normal gate, which may again leave fct hashless (#788/#809). No manual stamping or synthetic assembly.'}}
    copy.verify_original(original)
    manifest = {'version': 1, 'final_sha': run.SHA, 'baseline_sha': original['source_sha'],
                'source_root': str(run.DATA), 'shared_root': str(run.SHARED),
                'validation_status': 'pending_parent_review',
                'served_comparison_exceptions': exceptions,
                'files': files, 'retire': retire, 'excluded': exclusions,
                'evidence': {name: copy.sha(run.OUT / name) for name in (
                    'final-hash-impact.json', 'served-comparison.json', 'raw-cache-comparison.json',
                    'trailing-uganda.json', 'completion.json')},
                'original_preserved': True, 'created': time.time()}
    run.dump(copy.CONTROL / 'promotion-manifest.candidate.json', manifest)
    summary = {'final_sha': run.SHA, 'files': len(files),
               'final_private_files': len(actual), 'excluded_files': len(exclusions),
               'shared_retirement_paths': list(retire),
               'bytes': sum(r['bytes'] for r in files.values()),
               'classifications': dict(collections.Counter(r['classification'] for r in files.values())),
               'pairs_without_comparison_difference': len(records) - len(differences),
               'successful_pairs_exactly_equal': sum(r['status'] == 'success' for r in records) - len(differences),
               'bounded_serialization_exceptions': 2, 'existing_798_row_loss_exceptions': 2,
               'universal_cold_warm_api_equality': False,
               'raw_cache_fingerprints_equal': len(actual),
               'original_preserved': True, 'shared_l2_access': False,
               'candidate_manifest_sha256': copy.sha(copy.CONTROL / 'promotion-manifest.candidate.json'),
               'seconds': time.perf_counter() - started}
    run.dump(copy.CONTROL / 'final-validation-summary.json', summary)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
