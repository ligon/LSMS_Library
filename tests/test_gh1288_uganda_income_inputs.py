"""The private Uganda income builder must certify both existing raw siblings."""
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
INPUTS = ('earnings', 'enterprise_income')


def test_real_uganda_collects_both_managed_country_inputs():
    ug = Country('Uganda', preload_panel_ids=False)
    expected = {Path(f'Uganda/var/{table}.parquet'): table for table in INPUTS}
    assert ug._make_other_references('income') == expected
    receipt, sources = ug._make_declared_inputs('income', _with_sources=True)
    assert receipt['tables'] == {t: ug._table_cache_hash(t, ug.waves) for t in INPUTS}
    raw = {p for p in sources if p.suffix.lower() == '.dta'}
    expected_raw = set()
    for name in INPUTS:
        _, refs = ug._make_declared_inputs(name, _with_sources=True)
        native = {p for p in refs if p.suffix.lower() == '.dta'}
        assert native
        expected_raw.update(native)
    assert raw == expected_raw
    assert all(p.with_suffix('.dta.dvc').is_file() for p in raw)


@pytest.mark.parametrize('live', ['absent', 'partial'])
def test_income_cold_source_closure_and_warm_receipt_after_sibling_drift(tmp_path, live):
    if not shutil.which('make'):
        pytest.skip('make unavailable')

    config = tmp_path / 'countries'
    data = tmp_path / 'cache'
    scripts = config / 'Uganda/_'
    scripts.mkdir(parents=True)
    shutil.copyfile(UGANDA / '_/income.py', scripts / 'income.py')
    shutil.copyfile(UGANDA / '_/Makefile', scripts / 'Makefile')
    for name in ('panel_ids.json', 'updated_ids.json'):
        (scripts / name).write_text('{}')
    (scripts / 'data_scheme.yml').write_text("""Waves: ['2020-21', '2021-22']
Data Scheme:
  earnings: {materialize: make, index: '(t, i)', Earnings: float}
  enterprise_income: {materialize: make, index: '(t, i)', profits: float}
  income: {materialize: make, index: '(t, i)', income: float}
""")

    def put(wave, filename, column, value):
        path = config / 'Uganda' / wave / 'Data' / filename
        payload = f'hid,{column}\nh1,{value}\n'.encode()
        digest = hashlib.md5(payload, usedforsecurity=False).hexdigest()
        blob = data / 'dvc-cache' / digest[:2] / digest[2:]
        blob.parent.mkdir(parents=True, exist_ok=True)
        blob.write_bytes(payload)
        sidecar = path.with_suffix('.csv.dvc')
        contents = f'outs:\n- md5: {digest}\n  size: {len(payload)}\n  path: {filename}\n'
        sidecar.write_text(contents)
        assert not path.exists()
        return sidecar, contents

    for wave, earnings, profits in [('2020-21', 10, 1), ('2021-22', 20, 2)]:
        wave_dir = config / 'Uganda' / wave
        (wave_dir / '_').mkdir(parents=True)
        (wave_dir / 'Data').mkdir()
        (wave_dir / '_/data_info.yml').write_text("""earnings:
  file: wages.csv
  idxvars: {i: hid}
  myvars: {Earnings: amount}
enterprise_income:
  file: firms.csv
  idxvars: {i: hid}
  myvars: {profits: profit}
""")
        put(wave, 'wages.csv', 'amount', earnings)
        put(wave, 'firms.csv', 'profit', profits)
    _, replacement = put('2020-21', 'wages-alternative.csv', 'amount', 30)
    assert 'wages-alternative.csv' in replacement
    replacement = replacement.replace('wages-alternative.csv', 'wages.csv')

    script = """
import os, sys
from pathlib import Path
from lsms_library import Country
from lsms_library.paths import data_root
from lsms_library.local_tools import read_parquet_make_inputs

live, late_contents = sys.argv[1:]
u = Country('Uganda', preload_panel_ids=False)
root = data_root('Uganda') / 'var'
siblings = {table: root / f'{table}.parquet' for table in ('earnings', 'enterprise_income')}
def state(path):
    return (path.read_bytes(), path.stat().st_mtime_ns) if path.exists() else None

if live == 'partial':
    os.environ['LSMS_BUILD_BACKEND'] = 'dvc'
    for table in siblings:
        getattr(u, table)(waves=['2021-22'])
before = {table:state(path) for table,path in siblings.items()}
assert all((v is not None) == (live == 'partial') for v in before.values())

os.environ['LSMS_BUILD_BACKEND'] = 'make'
def load():
    return u.income().sort_index(level='t')['income'].tolist()

assert load() == [11., 22.]
initial = state(root / 'income.parquet')
make_inputs = read_parquet_make_inputs(root / 'income.parquet')
assert dict(make_inputs['tables']) == {t:u._table_cache_hash(t,u.waves) for t in siblings}
assert load() == [11., 22.] and state(root / 'income.parquet') == initial
selected = u._table_cache_hash('income', u.waves)

(u.file_path / '2020-21/Data/wages.csv.dvc').write_text(late_contents)
assert u._table_cache_hash('income', u.waves) == selected
assert u._table_cache_hash('earnings', u.waves) != make_inputs['tables']['earnings']
assert u._table_cache_hash('enterprise_income', u.waves) == make_inputs['tables']['enterprise_income']
assert load() == [31., 22.]
assert state(root / 'income.parquet') != initial
assert read_parquet_make_inputs(root / 'income.parquet')['tables'] == {
    t:u._table_cache_hash(t,u.waves) for t in siblings}
assert {table:state(path) for table,path in siblings.items()} == before
"""
    env = {**os.environ, 'LSMS_COUNTRIES_ROOT': str(config), 'LSMS_DATA_DIR': str(data),
           'PYTHONPATH': str(ROOT), 'LSMS_SKIP_AUTH': '1', 'LSMS_BUILD_WORKERS': '1',
           'LSMS_MAKE_JOBS': '1'}
    env.pop('LSMS_NO_CACHE', None)
    env.pop('LSMS_ALLOW_INTREE_SOURCES', None)
    proc = subprocess.run([sys.executable, '-c', script, live, replacement],
                          cwd=tmp_path, env=env, text=True, capture_output=True, timeout=150)
    assert proc.returncode == 0, proc.stderr[-6000:]
