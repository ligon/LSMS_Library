"""Independent hermetic controls prepared from the entry-local contract."""
import importlib.util
import os
from pathlib import Path
import subprocess
import urllib.request

import pandas as pd
import pytest
import lsms_library as ll
from lsms_library.paths import countries_root, data_root

TOP = Path('/local/job40255479/worktrees/uganda-nutrition-cpu-hash')

@pytest.fixture
def target(monkeypatch):
    assert Path.cwd().resolve() == TOP
    assert Path(ll.__file__).resolve() == TOP / 'lsms_library/__init__.py'
    assert countries_root().resolve() == TOP / 'lsms_library/countries'
    assert str(data_root()).startswith('/local/job40255479/nutrition-repair-review-')
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == os.environ['REVIEW_FROZEN_HEAD']
    def forbidden(*args, **kwargs):
        raise AssertionError('Independent variant review forbids network')
    monkeypatch.setattr(urllib.request, 'urlopen', forbidden)
    spec = importlib.util.spec_from_file_location('review_raw_uganda', TOP / 'tests/test_uganda_invariance.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

@pytest.fixture
def case(target, tmp_path, monkeypatch):
    index = pd.MultiIndex.from_tuples([('synthetic-household', '2020')], names=['i', 't'])
    primary = pd.DataFrame({'Metric': [1.0]}, index=index)
    alternate = pd.DataFrame({'Metric': [2.0]}, index=index)
    entry = target._fingerprint(primary)
    alt_hash = target._fingerprint(alternate)['content_hash']
    folder = tmp_path / 'Uganda'
    (folder / 'var').mkdir(parents=True)
    alternate.to_parquet(folder / 'var/nutrition.parquet')
    monkeypatch.setattr(target, 'BASELINE', {'var/nutrition.parquet': entry})
    def run(name='var/nutrition.parquet'):
        target.test_parquet_matches_baseline(folder, ['2020'], name)
    return entry, alt_hash, run, folder, alternate


def test_entry_local_exact_alternate_is_accepted(case):
    entry, alternate, run, *_ = case
    entry['content_hash_variants'] = [alternate]
    run()


def test_primary_still_passes_without_optional_field(case, target):
    entry, _, run, folder, _ = case
    index = pd.MultiIndex.from_tuples([('synthetic-household', '2020')], names=['i', 't'])
    pd.DataFrame({'Metric': [1.0]}, index=index).to_parquet(folder / 'var/nutrition.parquet')
    run()


@pytest.mark.parametrize('variants', [None, [], ['0' * 64]])
def test_unmeasured_hash_still_fails(case, variants):
    entry, _, run, *_ = case
    if variants is not None:
        entry['content_hash_variants'] = variants
    with pytest.raises(AssertionError, match='content hash mismatch'):
        run()


def test_other_entry_cannot_inherit_nutrition_alternate(case, target):
    entry, alternate, run, folder, frame = case
    entry['content_hash_variants'] = [alternate]
    frame.to_parquet(folder / 'var/other.parquet')
    target.BASELINE['var/other.parquet'] = {k:v for k,v in entry.items() if k != 'content_hash_variants'}
    with pytest.raises(AssertionError, match='content hash mismatch'):
        run('var/other.parquet')


@pytest.mark.parametrize('field,value,reason', [
    ('shape', [999, 1], 'shape mismatch'),
    ('columns', ['Wrong'], 'column mismatch'),
    ('index_names', ['wrong', 't'], 'index names mismatch'),
    ('dtypes', {'Metric': 'Int64'}, 'dtype mismatch'),
])
def test_allowed_variant_never_bypasses_structure(case, field, value, reason):
    entry, alternate, run, *_ = case
    entry['content_hash_variants'] = [alternate]
    entry[field] = value
    with pytest.raises(AssertionError, match=reason):
        run()


def test_null_primary_debt_remains_unpinned(case):
    entry, _, run, *_ = case
    entry['content_hash'] = None
    entry['content_hash_variants'] = ['0' * 64]
    run()


def test_null_primary_does_not_unpin_shape(case):
    entry, _, run, *_ = case
    entry['content_hash'] = None
    entry['content_hash_variants'] = []
    entry['shape'] = [999, 1]
    with pytest.raises(AssertionError, match='shape mismatch'):
        run()


def test_explicit_partial_coverage_still_skips(case, monkeypatch):
    from lsms_library import local_tools
    entry, alternate, run, *_ = case
    entry['content_hash_variants'] = [alternate]
    monkeypatch.setattr(local_tools, 'read_parquet_cache_waves', lambda path: ['different-wave'])
    with pytest.raises(pytest.skip.Exception, match='partial build'):
        run()


def test_error_debt_still_skips(case):
    entry, _, run, *_ = case
    entry['error'] = 'synthetic historical missing file'
    with pytest.raises(pytest.skip.Exception, match='Baseline recorded an error'):
        run()
