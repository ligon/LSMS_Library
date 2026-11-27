"""Declared L2 inputs invalidate country outputs, regardless of cache author."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from lsms_library import Country


ROOT = Path(__file__).resolve().parents[1]
UGANDA = ROOT / 'lsms_library/countries/Uganda'


def configured(graph):
    country = Country('Uganda', preload_panel_ids=False)
    country.__dict__['_resources_cache'] = {
        'Data Scheme': dict.fromkeys(
            ('income', 'earnings', 'enterprise_income', 'support',
             'panel_ids', 'food_prices'), {}),
        'Cache Dependencies': graph,
    }
    return country


@pytest.mark.parametrize('graph, detail', [
    (None, 'expected a mapping'),
    ([], 'expected a mapping'),
    ({'income': 'earnings'}, 'nonempty list'),
    ({'income': []}, 'nonempty list'),
    ({'income': [False]}, 'cached table names'),
    ({'income': ['unknown']}, 'cached table names'),
    ({'income': ['panel_ids']}, 'cached table names'),
    ({'income': ['food_prices']}, 'cached table names'),
    ({'food_prices': ['earnings']}, 'unknown cached table'),
    ({'typo': ['earnings']}, 'unknown cached table'),
    ({'income': ['earnings', 'earnings']}, 'duplicate dependency'),
    ({'income': ['income']}, 'cycle'),
    ({'income': ['earnings'], 'earnings': ['support'], 'support': ['income']}, 'cycle'),
])
def test_bad_declaration_fails_even_on_cold_cache(graph, detail, tmp_path, monkeypatch):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'cache'))
    country = configured(graph)

    def producer(*args, **kwargs):
        pytest.fail('bad dependency config must fail before any build')

    monkeypatch.setattr(subprocess, 'run', producer)
    with pytest.raises(ValueError, match=detail):
        country.income()
    assert not list(tmp_path.rglob('*.parquet'))


def test_diamond_memo_full_dependency_waves_and_named_hashes(monkeypatch):
    c = configured({
        'income': ['earnings', 'enterprise_income'],
        'earnings': ['support'],
        'enterprise_income': ['support'],
    })
    seen = []
    mutable = {'support': 'A'}

    def own(name, waves):
        seen.append((name, tuple(waves)))
        return mutable.get(name, name)

    monkeypatch.setattr(c, '_own_table_cache_hash', own)
    subset = c.waves[-1:]
    h = c._table_cache_hash('income', subset)
    assert seen.count(('support', tuple(c.waves))) == 1
    assert seen == [('income', tuple(subset)), ('earnings', tuple(c.waves)),
                    ('support', tuple(c.waves)), ('enterprise_income', tuple(c.waves))]
    assert c._table_cache_hash('enterprise_income', subset) != 'enterprise_income'
    assert c._table_cache_hash('support', subset) == 'A'
    mutable['support'] = 'B'
    assert c._table_cache_hash('income', subset) != h
    mutable['support'] = None
    with pytest.raises(ValueError, match='no cache fingerprint for .support.'):
        c._table_cache_hash('income', subset)


def test_valid_assumed_fresh_override_deliberately_skips_dependency_hashing(tmp_path, monkeypatch):
    import pandas as pd
    from lsms_library.local_tools import to_parquet

    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path))
    c = configured({'income': ['earnings']})
    c.assume_cache_fresh = True
    wave = c.waves[0]
    path = tmp_path / 'Uganda/var/income.parquet'
    path.parent.mkdir(parents=True)
    frame = pd.DataFrame({'income': [11.]},
                         index=pd.MultiIndex.from_tuples([(wave, 'h1')], names=['t', 'i']))
    to_parquet(frame, path, absolute_path=True, cache_hash='intentionally-old',
               cache_waves=[wave])

    def no_hash(*args):
        pytest.fail('assume_cache_fresh must still bypass dependency hashes')

    monkeypatch.setattr(c, '_own_table_cache_hash', no_hash)
    pd.testing.assert_frame_equal(c._read_assumed_fresh_cache('income', [wave]), frame)


def test_real_uganda_scope_is_two_inputs_not_output_columns():
    ug = Country('Uganda', preload_panel_ids=False)
    assert ug._country_cache_dependencies == {'income': ('earnings', 'enterprise_income')}
    assert list(ug._materialization_entry('income')) == ['materialize', 'index', 'income']
    assert ug._table_cache_hash('sample', ug.waves) == ug._own_table_cache_hash('sample', ug.waves)


@pytest.mark.parametrize('author', ['dvc', 'make'])
def test_real_api_replaces_stale_income_regardless_of_cache_author(tmp_path, author):
    if not shutil.which('make'):
        pytest.skip('make unavailable')
    config = tmp_path / 'countries'
    data = tmp_path / 'cache'
    scripts = config / 'Uganda/_'
    scripts.mkdir(parents=True)
    for name in ('income.py', 'Makefile'):
        shutil.copyfile(UGANDA / '_' / name, scripts / name)
    for name in ('panel_ids.json', 'updated_ids.json'):
        (scripts / name).write_text('{}')
    (scripts / 'data_scheme.yml').write_text("""Waves: ['2020-21', '2021-22']
Cache Dependencies:
  income: [enterprise_income, earnings]
Data Scheme:
  earnings: {materialize: make, index: '(t, i)', Earnings: float}
  enterprise_income: {materialize: make, index: '(t, i)', profits: float}
  income: {materialize: make, index: '(t, i)', income: float}
""")

    def put(wave, filename, column, value):
        location = config / 'Uganda' / wave / 'Data' / filename
        payload = f'hid,{column}\nh1,{value}\n'.encode()
        digest = hashlib.md5(payload, usedforsecurity=False).hexdigest()
        blob = data / 'dvc-cache' / digest[:2] / digest[2:]
        blob.parent.mkdir(parents=True, exist_ok=True)
        blob.write_bytes(payload)
        text = f'outs:\n- md5: {digest}\n  size: {len(payload)}\n  path: {filename}\n'
        assert not location.exists()
        return text

    for wave, wages, profits in [('2020-21', 10, 1), ('2021-22', 20, 2)]:
        folder = config / 'Uganda' / wave
        (folder / 'Data').mkdir(parents=True)
        (folder / '_').mkdir()
        (folder / '_/data_info.yml').write_text("""earnings:
  file: wages.csv
  idxvars: {i: hid}
  myvars: {Earnings: amount}
enterprise_income:
  file: firms.csv
  idxvars: {i: hid}
  myvars: {profits: profit}
""")
        (folder / 'Data/wages.csv.dvc').write_text(put(wave, 'wages.csv', 'amount', wages))
        (folder / 'Data/firms.csv.dvc').write_text(put(wave, 'firms.csv', 'profit', profits))
    new_wages = put('2020-21', 'wages.csv', 'amount', 30)

    script = """
import os, sys
from lsms_library import Country
from lsms_library.paths import data_root
from lsms_library.local_tools import (
    read_parquet_cache_hash, read_parquet_cache_waves, read_parquet_make_inputs,
)

author, wages = sys.argv[1:]
def country():
    return Country('Uganda', preload_panel_ids=False)
u = country()
cache = data_root('Uganda') / 'var/income.parquet'
def rows(c, waves=None):
    return c.income(waves=waves).sort_index(level='t')['income'].tolist()
def signature():
    return (cache.read_bytes(), cache.stat().st_mtime_ns)
def hashes():
    return {n:u._table_cache_hash(n,u.waves)
            for n in ('income', 'earnings', 'enterprise_income')}

os.environ['LSMS_BUILD_BACKEND'] = 'dvc'
if author == 'make':
    # Development's Make path can copy these valid, fully covered siblings.
    u.earnings()
    u.enterprise_income()
    os.environ['LSMS_BUILD_BACKEND'] = 'make'
assert not cache.exists()
assert rows(u) == [11.,22.]
before = hashes()
assert read_parquet_cache_hash(cache) == before['income']
assert read_parquet_cache_waves(cache) == u.waves
assert read_parquet_make_inputs(cache, _with_presence=True)[1] == (author == 'make')
old_signature = signature()
os.environ.pop('LSMS_BUILD_BACKEND', None)
assert rows(country(), ['2021-22']) == [22.]
assert signature() == old_signature

(u.file_path / '2020-21/Data/wages.csv.dvc').write_text(wages)
assert rows(u, ['2020-21']) == [31.], 'default returned income with an old earnings input'
curr = hashes()
assert {n for n in curr if curr[n]!=before[n]} == {'income','earnings'}
assert signature() != old_signature
assert read_parquet_cache_hash(cache) == curr['income']
assert read_parquet_cache_waves(cache) == u.waves
assert read_parquet_make_inputs(cache, _with_presence=True) == (None, False)
new_signature = signature()
os.environ['LSMS_BUILD_BACKEND'] = 'dvc'
assert rows(country(), ['2021-22']) == [22.]
assert rows(country()) == [31.,22.]
assert signature() == new_signature
"""
    env = {**os.environ, 'LSMS_COUNTRIES_ROOT': str(config), 'LSMS_DATA_DIR': str(data),
           'PYTHONPATH': str(ROOT), 'LSMS_SKIP_AUTH': '1',
           'LSMS_BUILD_WORKERS': '1', 'LSMS_MAKE_JOBS': '1'}
    for key in ('LSMS_NO_CACHE', 'LSMS_ALLOW_INTREE_SOURCES'):
        env.pop(key, None)
    result = subprocess.run([sys.executable, '-c', script, author, new_wages],
                            cwd=tmp_path, env=env, capture_output=True,
                            text=True, timeout=180)
    assert result.returncode == 0, result.stderr[-6000:]
