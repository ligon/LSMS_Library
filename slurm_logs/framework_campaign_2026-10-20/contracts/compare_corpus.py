"""Compare completed corpus summaries without reading household frames.

Reuses the existing corpus driver's served fingerprint and aggregate fields.
Exit 2 denotes incomplete/invalid evidence. Differences require human review;
this program does not automatically whitelist an issue's whole country.
"""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def load(root: Path, expected_sha: str, pairs: int):
    inventory = json.loads((root / 'inventory.json').read_text())
    completion = json.loads((root / 'completion.json').read_text())
    assert inventory['source_sha'] == completion['source_sha'] == expected_sha
    expected = {(r['country'], t) for r in inventory['countries'] for t in r.get('tables', [])}
    assert len(expected) == pairs == inventory['pairs']
    assert not completion['inventory_errors']
    country_names = {r['country'] for r in inventory['countries']}
    assert {r['country'] for r in completion['processes']} == country_names
    records, digests = {}, {}
    for country in sorted(country_names):
        folder = root / country
        identity = json.loads((folder / 'identity.json').read_text())
        status = json.loads((folder / 'status.json').read_text())
        assert identity['source_sha'] == expected_sha
        assert Path(identity['package']).resolve() == Path(inventory['worktree']).resolve() / 'lsms_library/__init__.py'
        assert Path(identity['countries_root']).resolve() == Path(inventory['worktree']).resolve() / 'lsms_library/countries'
        assert status['stage'] == 'complete', (country, status['stage'])
        assert not status['in_tree_parquets'], country
        blob = (folder / 'results.jsonl').read_bytes()
        digests[country] = hashlib.sha256(blob).hexdigest()
        rows = [json.loads(line) for line in blob.splitlines()]
        assert len(rows) == len(identity['tables'])
        for rec in rows:
            key = (rec['country'], rec['table'])
            assert key not in records and key in expected
            assert rec['source_sha'] == expected_sha
            assert not rec.get('measurement_error'), key
            assert not rec.get('cache_measurement_error'), key
            if rec.get('return_type') == 'DataFrame':
                assert 'served' in rec, key
            records[key] = rec
        assert Counter(r['status'] for r in rows) == Counter(status['counts'])
        process = next(r for r in completion['processes'] if r['country'] == country)
        assert bool(process['exit_code']) == any(r['status'] == 'error' for r in rows)
    assert set(records) == expected
    return inventory, records, digests


def error_type(rec):
    return rec.get('error', '').split(':', 1)[0] or None


def normalized_error(rec, prefixes):
    message = rec.get('error')
    if message is None:
        return None
    for prefix, replacement in sorted(prefixes.items(), key=lambda item: -len(item[0])):
        message = message.replace(prefix, replacement)
    return message


def text_digest(value):
    return None if value is None else hashlib.sha256(value.encode()).hexdigest()


def compare(a, b, prefixes):
    changes = []
    expected_hashes, wave_hashes = [], []
    unchanged_errors = []
    cache_metadata_changes = []
    error_message_changes = []
    for key in sorted(a):
        x, y = a[key], b[key]
        fields = {}
        for field in ('status', 'return_type', 'declared_waves', 'entries', 'content_hash'):
            if x.get(field) != y.get(field):
                fields[field] = {'before': x.get(field), 'after': y.get(field)}
        if error_type(x) != error_type(y):
            fields['error_type'] = {'before': error_type(x), 'after': error_type(y)}
        ex, ey = normalized_error(x, prefixes), normalized_error(y, prefixes)
        if ex != ey:
            change = {'country': key[0], 'table': key[1],
                      'before_type': error_type(x), 'after_type': error_type(y),
                      'before_normalized_sha256': text_digest(ex),
                      'after_normalized_sha256': text_digest(ey)}
            error_message_changes.append(change)
            fields['normalized_error_message_sha256'] = {'before': text_digest(ex), 'after': text_digest(ey)}
        if x['status'] == y['status'] == 'error' and ex == ey:
            unchanged_errors.append({'country': key[0], 'table': key[1], 'error_type': error_type(x),
                                     'normalized_message_sha256': text_digest(ex)})
        sx, sy = x.get('served', {}), y.get('served', {})
        for field in sorted(set(sx) | set(sy)):
            if sx.get(field) != sy.get(field):
                # Aggregates/fingerprints only; never include a frame or values.
                fields['served.' + field] = {'before': sx.get(field), 'after': sy.get(field)}
        if fields:
            changes.append({'country': key[0], 'table': key[1], 'fields': fields})
        if x.get('expected_hash') != y.get('expected_hash'):
            expected_hashes.append({'country': key[0], 'table': key[1]})
        def cache_state(rec):
            country = rec.get('country_cache')
            def state(cache):
                return None if cache is None else {k: cache.get(k) for k in ('rows', 'freshness', 'footer_waves', 'stable_during_read')}
            return {'country': state(country),
                    'waves': {w: state(c) for w, c in rec.get('wave_caches', {}).items()}}
        if cache_state(x) != cache_state(y):
            cache_metadata_changes.append({'country': key[0], 'table': key[1],
                                           'before': cache_state(x), 'after': cache_state(y)})
        wx, wy = x.get('wave_caches', {}), y.get('wave_caches', {})
        for wave in sorted(set(wx) | set(wy)):
            if wx.get(wave, {}).get('expected_hash') != wy.get(wave, {}).get('expected_hash'):
                wave_hashes.append({'country': key[0], 'table': key[1], 'wave': wave,
                                    'presence_before': wave in wx, 'presence_after': wave in wy})
    return {
        'baseline_status_counts': dict(Counter(r['status'] for r in a.values())),
        'candidate_status_counts': dict(Counter(r['status'] for r in b.values())),
        'served_or_status_changed_cells': changes,
        'unchanged_error_cells': unchanged_errors,
        'normalized_error_message_changed_cells': error_message_changes,
        'country_expected_hash_changed_cells': expected_hashes,
        'cache_metadata_changed_cells': cache_metadata_changes,
        'observed_wave_expected_hash_changed_cells': wave_hashes,
        'warning_count_changed_cells': [
            {'country': k[0], 'table': k[1], 'before': a[k].get('warning_counts'), 'after': b[k].get('warning_counts')}
            for k in sorted(a) if a[k].get('warning_counts') != b[k].get('warning_counts')],
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline', type=Path, required=True)
    p.add_argument('--candidate', type=Path, required=True)
    p.add_argument('--baseline-sha', required=True)
    p.add_argument('--candidate-sha', required=True)
    p.add_argument('--pairs', type=int, default=549)
    p.add_argument('--out', type=Path, required=True)
    args = p.parse_args()
    try:
        ai, a, ad = load(args.baseline, args.baseline_sha, args.pairs)
        bi, b, bd = load(args.candidate, args.candidate_sha, args.pairs)
        assert set(a) == set(b)
        prefixes = {}
        for inventory in (ai, bi):
            prefixes[inventory['worktree']] = '<SOURCE_ROOT>'
            prefixes[inventory['private_root']] = '<DATA_ROOT>'
        # These are the exact fixed source checkouts used by this campaign.
        for fixed in ('/local/job40255479/worktrees/baseline-976fc840d',
                      '/local/job40255479/worktrees/framework-integration'):
            prefixes[fixed] = '<SOURCE_ROOT>'
        report = {'evidence_complete': True, 'pairs': len(a),
                  'error_normalization_prefixes': prefixes,
                  'baseline_sha': args.baseline_sha, 'candidate_sha': args.candidate_sha,
                  'baseline_results_sha256': ad, 'candidate_results_sha256': bd,
                  'limitations': ['No raw frames read by comparator.',
                      'Served fingerprint includes index values/order; index-level dtypes not recorded separately.',
                      'DataFrame attrs are not included in the corpus fingerprint.',
                      'Observed wave-cache hashes do not enumerate every theoretical wave hash.',
                      'Error messages are compared after replacing only the listed exact source/data root prefixes.',
                      'Matching normalized exception messages do not independently prove every upstream cause.'],
                  **compare(a, b, prefixes)}
    except (AssertionError, FileNotFoundError, json.JSONDecodeError) as exc:
        report = {'evidence_complete': False, 'failure': type(exc).__name__ + ': ' + str(exc)}
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k in ('evidence_complete', 'pairs', 'failure')}, sort_keys=True))
    return 0 if report['evidence_complete'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
