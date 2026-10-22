"""GH #1062: requested Uganda builds must preserve companion country caches.

Run the actual scripts with controlled IO: the numeric operations remain real,
while a writer records every output and the API return is deliberately distinct
from the raw cache consumed by income. See the task ledger sections 2 and 4.
"""
import sys
import shutil
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


UGANDA = Path(__file__).resolve().parents[1] / 'lsms_library' / 'countries' / 'Uganda' / '_'


def _exec_script(name):
    path = UGANDA / name
    exec(compile(path.read_text(), str(path), 'exec'), {'__name__': '__main__'})


@pytest.mark.parametrize('target', ['fct', 'nutrition', None])
def test_nutrition_writes_only_requested_target(target, monkeypatch):
    import lsms_library as ll
    import lsms_library.local_tools as lt

    # Multiple sources, one non-kg row, and a food absent from the FCT exercise
    # the original kg filter, duplicate sum and food intersection exactly.
    idx = pd.MultiIndex.from_tuples([
        ('2020', 'h1', 'Food A', 'kg', 'purchased'),
        ('2020', 'h1', 'Food A', 'Kg', 'produced'),
        ('2020', 'h1', 'Food B', 'kg', 'purchased'),
        ('2020', 'h1', 'Food A', 'piece', 'purchased'),
        ('2020', 'h2', 'Food C', 'kg', 'purchased'),
    ], names=['t', 'i', 'j', 'u', 's'])
    q = pd.DataFrame({'Quantity': [2., 3., 7., 100., 11.]}, index=idx)
    fct = pd.DataFrame({'i': ['Food A'], 'Protein': [13.], 'Energy': [17.]})
    usda = pd.DataFrame({'i': ['Food B'], 'Protein': [19.], 'Energy': [23.]})

    class Country:
        def __init__(self, name):
            assert name == 'Uganda'
        def food_quantities(self):
            return q.copy()

    monkeypatch.setattr(ll, 'Country', Country)
    monkeypatch.setattr(lt, 'df_from_orgfile', lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(pd, 'read_csv', lambda fn: (fct if fn == 'fct_uganda.csv' else usda).copy())
    written = {}
    monkeypatch.setattr(lt, 'to_parquet', lambda df, fn: written.__setitem__(fn, df.copy()))
    # The script's helper is imported from the country directory; it is not
    # executed, and no FDC request is needed when the pinned CSV exists.
    monkeypatch.syspath_prepend(str(UGANDA))
    monkeypatch.setattr(sys, 'argv', ['nutrition.py'] + ([target] if target else []))
    _exec_script('nutrition.py')

    requested = target or 'nutrition'
    assert list(written) == [f'../var/{requested}.parquet']
    if requested == 'fct':
        want = pd.DataFrame({'Food A': [13., 17.], 'Food B': [19., 23.]},
                            index=pd.Index(['Protein', 'Energy'], name='Nutrient'))
    else:
        want = pd.DataFrame([[198., 246.], [0., 0.]],
                            index=pd.MultiIndex.from_tuples([('h1', '2020'), ('h2', '2020')], names=['i', 't']),
                            columns=pd.Index(['Protein', 'Energy'], name='Nutrient'))
    pd.testing.assert_frame_equal(written[f'../var/{requested}.parquet'], want)


def test_income_materializes_siblings_before_reading_raw(monkeypatch):
    import lsms_library as ll
    import lsms_library.local_tools as lt

    idx = pd.MultiIndex.from_tuples([('2020', 'h1')], names=['t', 'i'])
    raw = {'earnings': pd.DataFrame({'Earnings': [2.]}, index=idx),
           'enterprise_income': pd.DataFrame({'profits': [3.]}, index=idx)}
    events = []

    class Country:
        def __init__(self, name):
            assert name == 'Uganda'
        def __getattr__(self, table):
            assert table in raw
            def build():
                events.append(('build', table))
                # A finalized API answer must not become income's operand.
                return pd.DataFrame({'wrong_finalized_value': [999.]})
            return build

    def read(fn):
        table = Path(fn).stem
        events.append(('read', table))
        return raw[table].copy()

    monkeypatch.setattr(ll, 'Country', Country)
    monkeypatch.setattr(lt, 'get_dataframe', read)
    written = {}
    monkeypatch.setattr(lt, 'to_parquet', lambda df, fn: written.__setitem__(fn, df.copy()))
    _exec_script('income.py')
    assert events == [('build', 'earnings'), ('read', 'earnings'),
                      ('build', 'enterprise_income'), ('read', 'enterprise_income')]
    assert list(written) == ['../var/income.parquet']
    assert written['../var/income.parquet']['income'].tolist() == [5.]


@pytest.mark.parametrize('table', ['income', 'fct', 'nutrition'])
def test_make_target_does_not_run_raw_sibling_compilers(table, tmp_path):
    if not shutil.which('make'):
        pytest.skip('make is unavailable')
    country = tmp_path / 'Uganda'
    scripts = country / '_'
    scripts.mkdir(parents=True)
    wave = country / '2020-21' / '_'
    wave.mkdir(parents=True)
    shutil.copyfile(UGANDA / 'Makefile', scripts / 'Makefile')
    for name in ['income.py', 'nutrition.py', 'earnings.py', 'enterprise_income.py',
                 'food_acquired.py', 'uganda.py', 'categorical_mapping.org',
                 'nutrition.org', 'panel_ids.json', 'updated_ids.json']:
        (scripts / name).touch()
    for name in ['earnings.py', 'enterprise_income.py', 'food_acquired.py']:
        (wave / name).touch()
    var = tmp_path / 'data' / 'Uganda' / 'var'
    proc = subprocess.run(['make', '-n', f'VAR_DIR={var}', str(var / f'{table}.parquet')],
                          cwd=scripts, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    want = 'python income.py' if table == 'income' else f'python nutrition.py {table}'
    assert proc.stdout.strip().splitlines() == [want]


@pytest.mark.parametrize('table', ['earnings', 'enterprise_income'])
def test_make_backend_sibling_target_pins_framework_backend(table, tmp_path):
    """A make-backend call must not recursively invoke its own country rule."""
    if not shutil.which('make'):
        pytest.skip('make is unavailable')
    import os

    scripts = tmp_path / 'Uganda' / '_'
    scripts.mkdir(parents=True)
    (scripts.parent / '2020-21' / '_').mkdir(parents=True)
    shutil.copyfile(UGANDA / 'Makefile', scripts / 'Makefile')
    for name in ['panel_ids.json', 'updated_ids.json']:
        (scripts / name).touch()
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    python = bin_dir / 'python'
    python.write_text('#!/bin/sh\nprintf "%s\\n" "$LSMS_BUILD_BACKEND" "$@" > "$CHECK_CALL"\n')
    python.chmod(0o755)
    record = tmp_path / 'call.txt'
    env = {**os.environ, 'LSMS_BUILD_BACKEND': 'make',
           'PATH': str(bin_dir) + os.pathsep + os.environ.get('PATH', ''),
           'CHECK_CALL': str(record)}
    var = tmp_path / 'data' / 'Uganda' / 'var'
    proc = subprocess.run(['make', '-s', f'VAR_DIR={var}', str(var / f'{table}.parquet')],
                          cwd=scripts, env=env, capture_output=True, text=True, timeout=20)
    assert proc.returncode == 0, proc.stderr
    backend, flag, call = record.read_text().splitlines()
    assert backend == 'dvc'
    assert flag == '-c'
    assert f".Country('Uganda').{table}()" in call


def test_make_all_keeps_framework_sibling_targets_reachable(tmp_path):
    if not shutil.which('make'):
        pytest.skip('make is unavailable')
    import re

    scripts = tmp_path / 'Uganda' / '_'
    scripts.mkdir(parents=True)
    wave = scripts.parent / '2020-21' / '_'
    wave.mkdir(parents=True)
    makefile = (UGANDA / 'Makefile').read_text()
    (scripts / 'Makefile').write_text(makefile)
    names = set(re.findall(r'\$\(VAR_DIR\)/(\w+)\.parquet', makefile))
    for name in names:
        (scripts / f'{name}.py').touch()
    for name in ['uganda.py', 'categorical_mapping.org', 'nutrition.org',
                 'panel_ids.json', 'updated_ids.json']:
        (scripts / name).touch()
    for name in ['earnings.py', 'enterprise_income.py']:
        (wave / name).touch()
    var = tmp_path / 'data' / 'Uganda' / 'var'
    proc = subprocess.run(['make', '-n', f'VAR_DIR={var}', 'all'], cwd=scripts,
                          capture_output=True, text=True, timeout=20)
    assert proc.returncode == 0, proc.stderr
    lines = proc.stdout.splitlines()
    assert any(".Country('Uganda').earnings()" in line for line in lines)
    assert any(".Country('Uganda').enterprise_income()" in line for line in lines)
    assert 'python earnings.py' not in lines
    assert 'python enterprise_income.py' not in lines


@pytest.mark.parametrize('original', ['make', '', None])
@pytest.mark.parametrize('fail_on', [None, 'earnings', 'enterprise_income'])
def test_income_pins_sibling_backend_and_restores_environment(original, fail_on, monkeypatch):
    import os
    import lsms_library as ll
    import lsms_library.local_tools as lt

    if original is None:
        monkeypatch.delenv('LSMS_BUILD_BACKEND', raising=False)
    else:
        monkeypatch.setenv('LSMS_BUILD_BACKEND', original)
    monkeypatch.setenv('LSMS_NO_CACHE', 'preserve-this-value')
    idx = pd.MultiIndex.from_tuples([('2020', 'h1')], names=['t', 'i'])
    seen = []

    class Country:
        def __init__(self, name):
            assert name == 'Uganda'
        def __getattr__(self, table):
            def materialize():
                seen.append(table)
                assert os.environ['LSMS_BUILD_BACKEND'] == 'dvc'
                assert os.environ['LSMS_NO_CACHE'] == 'preserve-this-value'
                if table == fail_on:
                    raise RuntimeError('source unavailable')
            return materialize

    def read(fn):
        # The original backend must already be restored before the raw read.
        assert os.environ.get('LSMS_BUILD_BACKEND') == original
        col = 'Earnings' if Path(fn).stem == 'earnings' else 'profits'
        return pd.DataFrame({col: [2.]}, index=idx)

    monkeypatch.setattr(ll, 'Country', Country)
    monkeypatch.setattr(lt, 'get_dataframe', read)
    monkeypatch.setattr(lt, 'to_parquet', lambda *a, **k: None)
    if fail_on:
        with pytest.raises(RuntimeError, match='source unavailable'):
            _exec_script('income.py')
    else:
        _exec_script('income.py')
        assert seen == ['earnings', 'enterprise_income']
    assert os.environ.get('LSMS_BUILD_BACKEND') == original
    assert ('LSMS_BUILD_BACKEND' in os.environ) == (original is not None)
    assert os.environ['LSMS_NO_CACHE'] == 'preserve-this-value'


def test_income_make_backend_expands_valid_subset_sibling_caches(tmp_path):
    """Exercise actual public cache coverage and Make routing, without data access.

    Both sibling parquets already exist with valid hashes/one-wave manifests.
    Their thin Make wrappers therefore cannot repair coverage by themselves.
    """
    if not shutil.which('make'):
        pytest.skip('make is unavailable')
    import json
    import os

    config = tmp_path / 'countries'
    scripts = config / 'Uganda' / '_'
    scripts.mkdir(parents=True)
    shutil.copyfile(UGANDA / 'income.py', scripts / 'income.py')
    shutil.copyfile(UGANDA / 'Makefile', scripts / 'Makefile')
    for name in ['panel_ids.json', 'updated_ids.json']:
        (scripts / name).write_text('{}')
    (scripts / 'data_scheme.yml').write_text("""Waves: ['2020-21', '2021-22']
Data Scheme:
  earnings:
    materialize: make
    index: (t, i)
    Earnings: float
  enterprise_income:
    materialize: make
    index: (t, i)
    profits: float
  income:
    materialize: make
    index: (t, i)
    income: float
""")
    for wave, earnings, profits in [('2020-21', 10, 1), ('2021-22', 20, 2)]:
        wave_dir = scripts.parent / wave
        (wave_dir / '_').mkdir(parents=True)
        (wave_dir / 'Data').mkdir()
        (wave_dir / 'Data' / 'sources.csv').write_text(f'hid,earnings,profits\nh1,{earnings},{profits}\n')
        (wave_dir / '_' / 'data_info.yml').write_text("""earnings:
  file: ../Data/sources.csv
  idxvars: {i: hid}
  myvars: {Earnings: earnings}
enterprise_income:
  file: ../Data/sources.csv
  idxvars: {i: hid}
  myvars: {profits: profits}
""")
    env = {**os.environ, 'LSMS_COUNTRIES_ROOT': str(config),
           'PYTHONPATH': str(Path(__file__).resolve().parents[1]),
           'LSMS_BUILD_WORKERS': '1', 'LSMS_DATA_DIR': str(tmp_path / 'data'),
           'LSMS_ALLOW_INTREE_SOURCES': '1'}
    env.pop('LSMS_NO_CACHE', None)
    code = """
import json, os
import lsms_library as ll
from lsms_library.paths import data_root
from lsms_library.local_tools import read_parquet_cache_hash, read_parquet_cache_waves
c = ll.Country('Uganda', preload_panel_ids=False)
os.environ['LSMS_BUILD_BACKEND'] = 'dvc'
for table in ['earnings', 'enterprise_income']:
    getattr(c, table)(waves=['2020-21'])
    path = data_root('Uganda') / 'var' / (table + '.parquet')
    assert read_parquet_cache_waves(path) == ['2020-21']
    assert read_parquet_cache_hash(path) == c._table_cache_hash(table, c.waves)
os.environ['LSMS_BUILD_BACKEND'] = 'make'
answer = c.income()
assert set(answer.index.get_level_values('t')) == set(c.waves), 'all-wave income reused valid partial siblings'
assert answer['income'].tolist() == [11., 22.]
for table in ['earnings', 'enterprise_income']:
    path = data_root('Uganda') / 'var' / (table + '.parquet')
    assert set(read_parquet_cache_waves(path)) == set(c.waves)
    assert read_parquet_cache_hash(path) == c._table_cache_hash(table, c.waves)
print(json.dumps({'income': answer['income'].tolist(), 'waves': c.waves}))
"""
    proc = subprocess.run([sys.executable, '-c', code], cwd=tmp_path, env=env,
                          capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr[-5000:]
    assert json.loads(proc.stdout.splitlines()[-1])['income'] == [11., 22.]
