"""A no-cache build writes the frame an ordinary build writes (GH #1455).

Ruling, @ligon 2026-11-04: remove the memory-route reducer from
``Wave.food_acquired``, and only ``1``, ``true`` or ``yes`` turn ``LSMS_NO_CACHE``
on.  Both read gates do; the L2-wave write gate waits for GH #1501.  The reducer
ran wherever the L2-wave write did not happen -- the flag set in
any spelling, or a failed write -- and the country tier then stamped what it
produced with the current hash, so it graded ``fresh`` and every later read
served it: ``Derivation`` gone, ``Expenditure`` and ``Quantity`` NaN turned to 0,
rows with a NaN key deleted.

Synthetic and data-free, except ``TestDelivered``.
"""
from __future__ import annotations

import ast
import os
import pickle
import subprocess
import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest

import lsms_library
import lsms_library.country as C
from lsms_library.local_tools import cache_freshness
from lsms_library.paths import countries_root, data_root

COUNTRY = 'Fakeland'
WAVE = '2001'
TABLE = 'food_acquired'
ON = ['1', 'true', 'TRUE', 'True', 'yes', 'YES']
SKIPS_THE_WAVE_WRITE = ['0', 'false', 'no', 'off']
OFF = ['0', '', 'false', 'FALSE', 'no', 'off', '2', ' 1 ']

# A row with no Expenditure and one with no Quantity (the reducer's sum made both
# 0), a NaN unit (its groupby deleted the row, as Senegal's 21) and a column it
# did not name.
SOURCE = (
    'id,item,unit,source,quantity,expenditure,derivation\n'
    'h1,A,Kg,purchased,2,10,key-1\n'
    'h2,B,Kg,produced,,7,\n'
    'h3,C,Kg,inkind,4,,\n'
    'h4,D,,purchased,1,2,key-1\n'
)
# A repeated key, which is `_normalize_dataframe_index`'s to reduce and audit.
REPEATS = SOURCE + 'h1,A,Kg,purchased,3,5,key-1\n'


@pytest.fixture
def fakeland(tmp_path, monkeypatch):
    croot = tmp_path / 'countries'
    droot = tmp_path / 'data'
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(croot))
    monkeypatch.setenv('LSMS_DATA_DIR', str(droot))
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    country_dir = croot / COUNTRY / '_'
    wave_dir = croot / COUNTRY / WAVE / '_'
    country_dir.mkdir(parents=True)
    wave_dir.mkdir(parents=True)
    source = tmp_path / 'observations.csv'
    source.write_text(SOURCE)
    (country_dir / 'data_scheme.yml').write_text(
        f'Country: {COUNTRY}\nWaves: ["{WAVE}"]\nData Scheme:\n'
        f'  {TABLE}:\n    index: (t, i, j, u, s)\n'
        '    Quantity: float\n    Expenditure: float\n')
    (wave_dir / 'data_info.yml').write_text(
        f'{TABLE}:\n  file: {source}\n'
        '  idxvars: {i: id, j: item, u: unit, s: source}\n'
        '  myvars:\n    Quantity: quantity\n    Expenditure: expenditure\n'
        '    Derivation: derivation\n')

    class Paths:
        var = droot / COUNTRY / 'var' / f'{TABLE}.parquet'
        wave = droot / COUNTRY / WAVE / '_' / f'{TABLE}.parquet'
        wave_dir = droot / COUNTRY / WAVE / '_'

        @staticmethod
        def country():
            return C.Country(COUNTRY, preload_panel_ids=False)

        def read(self, **env):
            """A public read with *env* set for that read alone."""
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                with monkeypatch.context() as scoped:
                    for key, value in env.items():
                        scoped.setenv(key, value)
                    return self.country().food_acquired()

        def clear(self):
            for path in (self.var, self.wave):
                path.unlink(missing_ok=True)

        @staticmethod
        def use(text):
            source.write_text(text)

    try:
        yield Paths()
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


def _same(actual, expected):
    """Equal values, NA pattern, dtypes and index, whatever order the rows are in."""
    pd.testing.assert_frame_equal(
        actual.sort_index(kind='mergesort'), expected.sort_index(kind='mergesort'),
        check_exact=True)


class TestThePredicate:
    @pytest.mark.parametrize('value', ON)
    def test_on(self, monkeypatch, value):
        monkeypatch.setenv('LSMS_NO_CACHE', value)
        assert C._no_cache()

    @pytest.mark.parametrize('value', OFF)
    def test_off(self, monkeypatch, value):
        monkeypatch.setenv('LSMS_NO_CACHE', value)
        assert not C._no_cache()

    def test_unset_is_off(self, monkeypatch):
        monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
        assert not C._no_cache()

    def test_it_reads_as_grain_strict_reads(self, monkeypatch):
        """One spelling of a lever across the two predicates."""
        for value in ON + OFF:
            monkeypatch.setenv('LSMS_NO_CACHE', value)
            monkeypatch.setenv('LSMS_GRAIN_STRICT', value)
            assert C._no_cache() == C._grain_strict()

    def test_no_new_raw_read_in_a_top_level_function(self):
        """The unrouted reads were how `"0"` meant on at one gate and off at
        another.  Scope: `environ.get` inside functions of `lsms_library/*.py`.
        It does not see a country module (Niger's `wave_producers` saves and
        restores the flag), `environ[...]` or a module-level read.  `grab_data`
        is the L2-wave write gate, held for GH #1501."""
        readers = []
        for path in Path(lsms_library.__file__).parent.glob('*.py'):
            tree = ast.parse(path.read_text(encoding='utf-8'))
            for fn in ast.walk(tree):
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for node in ast.walk(fn):
                    if (isinstance(node, ast.Call)
                            and isinstance(node.func, ast.Attribute)
                            and node.func.attr == 'get'
                            and 'environ' in ast.dump(node.func.value)
                            and node.args
                            and isinstance(node.args[0], ast.Constant)
                            and node.args[0].value == 'LSMS_NO_CACHE'):
                        readers.append(f'{path.name}:{fn.name}')
        assert sorted(set(readers)) == ['country.py:_no_cache',
                                        'country.py:grab_data']


class TestTheRoute:
    """`Wave.food_acquired` hands back what `grab_data` built, on every route."""

    def test_the_memory_route_keeps_every_row_and_column(self, fakeland, monkeypatch):
        monkeypatch.setenv('LSMS_NO_CACHE', '1')
        wave = fakeland.country()[WAVE]
        raw, origin = wave.grab_data(TABLE, _with_origin=True)
        assert origin == 'memory'
        frame, origin = wave.food_acquired(_with_origin=True)
        assert origin == 'memory'
        pd.testing.assert_frame_equal(frame, raw)
        assert 'Derivation' in frame.columns and len(frame) == 4
        assert frame['Expenditure'].isna().sum() == 1
        assert frame['Quantity'].isna().sum() == 1

    def test_it_does_not_change_the_public_interface(self, fakeland):
        wave = fakeland.country()[WAVE]
        assert isinstance(wave.food_acquired(), pd.DataFrame)
        frame, origin = wave.food_acquired(_with_origin=True)
        assert isinstance(frame, pd.DataFrame) and origin == 'parquet'


class TestNoCacheEqualsOrdinary:
    @pytest.mark.parametrize('value', ['1', 'true', 'YES'])
    def test_no_cache_over_a_warm_root(self, fakeland, value):
        clean_api = fakeland.read()
        clean_parquet = pd.read_parquet(fakeland.var)
        no_cache_api = fakeland.read(LSMS_NO_CACHE=value)
        _same(no_cache_api, clean_api)
        _same(pd.read_parquet(fakeland.var), clean_parquet)
        _same(fakeland.read(), clean_api)

    def test_no_cache_from_cold(self, fakeland):
        clean_api = fakeland.read()
        clean_parquet = pd.read_parquet(fakeland.var)
        fakeland.clear()
        _same(fakeland.read(LSMS_NO_CACHE='1'), clean_api)
        _same(pd.read_parquet(fakeland.var), clean_parquet)
        _same(fakeland.read(), clean_api)

    def test_derivation_and_na_survive_to_the_public_frame(self, fakeland):
        served = fakeland.read(LSMS_NO_CACHE='1')
        assert served['Derivation'].notna().sum() > 0
        assert served['Expenditure'].isna().sum() == 1
        assert served['Quantity'].isna().sum() == 1
        assert not (served['Expenditure'] == 0).any()
        assert not (served['Quantity'] == 0).any()

    def test_a_nan_key_is_served_as_an_ordinary_build_serves_it(self, fakeland):
        """Senegal's 21 rows: the reducer's `groupby` deleted them, and a frame
        with a unique index runs no collapse, so an ordinary build keeps them."""
        clean = fakeland.read()
        assert 'h4' in clean.index.get_level_values('i')
        served = fakeland.read(LSMS_NO_CACHE='1')
        _same(served, clean)
        assert pd.isna(served.xs('h4', level='i').index.get_level_values('u')).all()

    def test_a_repeated_key_goes_to_the_same_collapse(self, fakeland):
        """The reducer also summed repeats, and `_normalize_dataframe_index`
        does that for both routes, with its audit."""
        fakeland.use(REPEATS)
        clean = fakeland.read()
        assert clean.xs('h1', level='i')['Quantity'].tolist() == [5.0]
        _same(fakeland.read(LSMS_NO_CACHE='1'), clean)


class TestOffSpellingsBypassNoRead:
    @pytest.mark.parametrize('value', ['0', 'false', 'no', ''])
    def test_a_warm_country_parquet_is_served_not_rebuilt(self, fakeland, value):
        clean = fakeland.read()
        stamp = fakeland.var.stat().st_mtime_ns
        _same(fakeland.read(LSMS_NO_CACHE=value), clean)
        assert fakeland.var.stat().st_mtime_ns == stamp

    def test_an_off_spelling_from_cold_stores_the_clean_frame(self, fakeland):
        clean_api = fakeland.read()
        clean_parquet = pd.read_parquet(fakeland.var)
        fakeland.clear()
        _same(fakeland.read(LSMS_NO_CACHE='0'), clean_api)
        _same(pd.read_parquet(fakeland.var), clean_parquet)
        _same(fakeland.read(), clean_api)

    @pytest.mark.parametrize('value', SKIPS_THE_WAVE_WRITE)
    def test_a_non_empty_off_spelling_still_skips_the_wave_write(self, fakeland, value):
        """As documented, until GH #1501 routes the write gate: this pin flips
        then.  It costs a wave parquet and no content."""
        fakeland.read(LSMS_NO_CACHE=value)
        assert fakeland.var.exists() and not fakeland.wave.exists()

    def test_an_empty_string_writes_the_wave_parquet(self, fakeland):
        fakeland.read(LSMS_NO_CACHE='')
        assert fakeland.wave.exists()


@pytest.mark.skipif(
    not hasattr(os, 'geteuid') or os.geteuid() == 0,
    reason='a read-only directory stops only a non-root writer')
class TestAFailedWaveWrite:
    """A real `OSError`, from a read-only wave directory, with no code patched:
    a patch inside the fingerprint closure would itself move the hash."""

    def test_it_warns_and_the_country_tier_stores_the_clean_frame(self, fakeland):
        clean_api = fakeland.read()
        clean_parquet = pd.read_parquet(fakeland.var)
        fakeland.clear()
        fakeland.wave_dir.chmod(0o555)
        try:
            with pytest.warns(UserWarning, match='L2-wave cache write failed'):
                served = fakeland.country().food_acquired()
        finally:
            fakeland.wave_dir.chmod(0o755)
        _same(served, clean_api)
        assert not fakeland.wave.exists()
        _same(pd.read_parquet(fakeland.var), clean_parquet)
        country = fakeland.country()
        expected = country._table_cache_hash(TABLE, country.waves)
        assert cache_freshness(fakeland.var, expected) == 'fresh'
        _same(fakeland.read(), clean_api)


# ---------------------------------------------------------------------------
# Real data, in a private root
# ---------------------------------------------------------------------------

_READ = '''
import pickle, sys, warnings
warnings.simplefilter("ignore")
import lsms_library as ll
frame = getattr(ll.Country(sys.argv[1]), sys.argv[2])()
with open(sys.argv[3], "wb") as out:
    pickle.dump(frame, out)
'''


@pytest.mark.requires_s3
class TestDelivered:
    """Each read in its own process, so nothing in-process can stand in for the
    parquet.  Never against a shared root: this writes `var/` on purpose."""

    @pytest.fixture
    def togo(self, tmp_path):
        root = tmp_path / 'data'
        root.mkdir()
        blobs = data_root() / 'dvc-cache'
        if blobs.exists():
            (root / 'dvc-cache').symlink_to(blobs)
        env = {key: val for key, val in os.environ.items() if key != 'LSMS_NO_CACHE'}
        env['LSMS_DATA_DIR'] = str(root)
        repo = str(Path(lsms_library.__file__).parent.parent)
        env['PYTHONPATH'] = repo

        def read(table=TABLE, **extra):
            out = tmp_path / 'frame.pkl'
            subprocess.run(
                [sys.executable, '-c', _READ, 'Togo', table, str(out)],
                env={**env, **extra}, check=True, cwd=repo, capture_output=True)
            with open(out, 'rb') as handle:
                return pickle.load(handle)

        read.var = root / 'Togo' / 'var' / f'{TABLE}.parquet'
        return read

    @pytest.mark.parametrize('value', ['1', '0'])
    def test_togo_food_acquired_is_the_ordinary_frame(self, togo, value):
        clean = togo()
        parquet = pd.read_parquet(togo.var)
        assert clean['Derivation'].notna().sum() > 0
        assert clean['Expenditure'].isna().sum() > 0
        _same(togo(LSMS_NO_CACHE=value), clean)
        _same(pd.read_parquet(togo.var), parquet)
        _same(togo(), clean)

    @pytest.mark.parametrize('table', ['food_expenditures', 'food_prices',
                                       'food_quantities'])
    def test_togo_derived_tables_do_not_move(self, togo, table):
        clean = togo(table)
        _same(togo(table, LSMS_NO_CACHE='1'), clean)
