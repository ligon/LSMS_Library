"""Exercise byte-level promotion/recovery on synthetic local files only."""
from pathlib import Path
import json
import tempfile

import promote_cache as p

CONTROL = Path(__file__).resolve().parent


def fixture(root):
    source = root / 'synthetic-source'
    shared = root / 'synthetic-shared'
    source.mkdir()
    shared.mkdir()
    assert source != p.SOURCE and shared != p.SHARED
    assert source.is_relative_to(CONTROL) and shared.is_relative_to(CONTROL)
    for parent, name, value in [
        (source, 'Example/var/a.parquet', b'new-a'),
        (source, 'Example/var/b.parquet', b'new-b'),
        (shared, 'Example/var/a.parquet', b'old-a'),
        (shared, 'Example/var/retired.parquet', b'old-retired'),
        (shared, 'Example/var/retained.parquet', b'untouched'),
        (shared, 'dvc-cache/sentinel', b'L1 never touched'),
    ]:
        path = parent / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    plan = {'version': 1, 'final_sha': 'a' * 40,
            'source_root': str(source), 'shared_root': str(shared),
            'validation_status': 'reviewed_for_promotion',
            'files': {}, 'retire': {'Example/var/retired.parquet': {
                'disposition': 'backup_then_retire', 'reason': 'synthetic retirement'}}}
    for name in ('Example/var/a.parquet', 'Example/var/b.parquet'):
        plan['files'][name] = {**p.info(source / name), 'disposition': 'promote_verified',
                              'classification': 'country_fresh_full_coverage',
                              'stored_hash': 'f' * 64, 'expected_hash': 'f' * 64,
                              'footer_waves': ['2001'], 'declared_waves': ['2001']}
    manifest = root / 'manifest.json'
    manifest.write_text(json.dumps(plan))
    digest = p.sha(manifest)
    assert p.load_manifest(manifest, digest, plan['final_sha'], source, shared) == plan
    baseline = p.snapshot(plan, digest, 'synthetic isolated directory, no other writers')
    txn = root / '.lsms-promotion-synthetic'
    return source, shared, plan, digest, baseline, txn


def rejects(call):
    try:
        call()
    except AssertionError:
        return
    raise AssertionError('operation should have rejected its precondition')


def main():
    outcomes = {}
    with tempfile.TemporaryDirectory(prefix='promotion-synthetic-', dir=CONTROL) as temporary:
        root = Path(temporary)
        for case in ('roundtrip', 'concurrent_before_apply', 'interrupted_apply',
                     'source_changed', 'rollback_concurrent', 'paths'):
            case_root = root / case
            case_root.mkdir()
            source, shared, plan, digest, baseline, txn = fixture(case_root)
            if case == 'source_changed':
                (source / 'Example/var/a.parquet').write_bytes(b'changed after review')
                rejects(lambda: p.stage(plan, digest, baseline, txn))
                assert not txn.exists()
            elif case == 'paths':
                for value in ('../var/a.parquet', '/Example/var/a.parquet',
                              'dvc-cache/var/a.parquet', 'Example/var/a.json',
                              'Example//var/a.parquet'):
                    rejects(lambda value=value: p.relative(value))
                link = shared / 'Alias'
                link.symlink_to(source / 'Example', target_is_directory=True)
                rejects(lambda: p.safe_path(shared, 'Alias/var/a.parquet'))
                link.unlink()
            else:
                p.stage(plan, digest, baseline, txn)
                assert p.inventory(shared) == baseline['files']
                if case == 'concurrent_before_apply':
                    target = shared / 'Example/var/a.parquet'
                    target.write_bytes(b'other writer')
                    rejects(lambda: p.apply(txn, digest, 'synthetic'))
                    assert target.read_bytes() == b'other writer'
                    assert not (shared / 'Example/var/b.parquet').exists()
                elif case == 'interrupted_apply':
                    original_journal = p.journal
                    def interrupted(txn, event, **values):
                        if event == 'mutation_done':
                            raise RuntimeError('synthetic crash after replace, before done journal')
                        original_journal(txn, event, **values)
                    p.journal = interrupted
                    try:
                        p.apply(txn, digest, 'synthetic')
                    except RuntimeError as exc:
                        assert 'synthetic crash' in str(exc)
                    else:
                        raise AssertionError('missing synthetic crash')
                    finally:
                        p.journal = original_journal
                    assert (shared / 'Example/var/a.parquet').read_bytes() == b'new-a'
                    p.rollback(txn, digest, 'synthetic recovery')
                    assert all(p.same_bytes(p.info(shared / name), entry)
                               for name, entry in baseline['files'].items())
                else:
                    p.apply(txn, digest, 'synthetic')
                    assert p.verify_final(plan, baseline)['all_planned_bytes_verified']
                    if case == 'rollback_concurrent':
                        target = shared / 'Example/var/a.parquet'
                        target.write_bytes(b'post-promotion other writer')
                        rejects(lambda: p.rollback(txn, digest, 'synthetic'))
                        assert target.read_bytes() == b'post-promotion other writer'
                    else:
                        result = p.rollback(txn, digest, 'synthetic')
                        assert result['baseline_bytes_restored']
                        assert set(p.inventory(shared)) == set(baseline['files'])
                        assert (txn / 'backups/Example/var/a.parquet').read_bytes() == b'old-a'
                        assert p.rollback(txn, digest, 'synthetic retry')['baseline_bytes_restored']
            assert (shared / 'dvc-cache/sentinel').read_bytes() == b'L1 never touched'
            outcomes[case] = 'passed'
    result = {'scope': 'synthetic bytes only; no real cache or library APIs',
              'script_sha256': p.sha(CONTROL / 'promote_cache.py'),
              'cases': outcomes, 'real_shared_l2_writes': 0,
              'original_or_final_cache_access': 0, 'temporary_files_removed': True}
    (CONTROL / 'promotion-synthetic-checks.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
