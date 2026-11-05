"""A failure must not read as an empty or incomplete table (GH #1546, #1516, #1524, #1532).

Data-free: real registries, invented frames, private data roots.  Each test states a rule
about which failure may be read as what:

* a derived table falls through to the legacy path only if that path can build it, and only
  for a bare builtin: a subclass is somebody's statement (#1516, #1532);
* a by-path class or a dead worker reaches the parent as ``WaveWorkerError``, which nothing
  reads as absence (#1524);
* the v-join and the market lookup degrade only for a source that is not there (#1546).
"""
import importlib.util
import pickle
import warnings

import pandas as pd
import pytest

from lsms_library import _parallel_waves as pw
from lsms_library import country as C
from lsms_library.build_transforms import GrainConflict
from lsms_library.country import (Country, GrainCollapseError, LabelPassThroughError,
                                  LegacyFallbackWarning, _rebuild_failure_error)
from lsms_library.datetime_parse import DatetimeParseError
from lsms_library.errors import UnitsUnavailableError, WaveWorkerError
from lsms_library.local_tools import IdWalkError, InTreeSourceError, to_parquet
from lsms_library.null_index_audit import NullIndexKeyError
from lsms_library.null_read_audit import NullReadError
from lsms_library.paths import data_root
from lsms_library.quantity_audit import QuantityImplausibleError
from lsms_library.sampling import ClusterIdentityError

FOOD = ['food_expenditures', 'food_prices', 'food_quantities']
DERIVED = FOOD + ['household_characteristics']
GENERIC = [FileNotFoundError, KeyError, ValueError, RuntimeError]
NAMED = [GrainCollapseError, NullReadError, NullIndexKeyError, DatetimeParseError,
         LabelPassThroughError, QuantityImplausibleError, InTreeSourceError, GrainConflict,
         NotImplementedError]
NOT_CAUGHT_BY_CLASS = [ClusterIdentityError, IdWalkError, WaveWorkerError]


def by_path_guard(tmp_path, base='Exception'):
    """A guard class as a by-path ``mapping.py`` has one: importable by nobody."""
    path = tmp_path / 'mapping.py'
    path.write_text(f'class Guard({base}):\n    """A guard."""\n')
    spec = importlib.util.spec_from_file_location('mapping', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Guard



class PicklesButDoesNotLoad(Exception):
    def __init__(self, first, second):
        super().__init__(f'{first} {second}')



class TestWorkerRelay:
    def test_a_by_path_guard_arrives_as_wave_worker_error(self, tmp_path):
        Guard = by_path_guard(tmp_path)
        try:
            raise Guard('the two files disagree')
        except Guard as exc:
            packed = pw._pack_exception(exc)
        assert packed[0] is None and packed[1] == 'Guard'
        payload = {'wave': 'w', 'warnings': [], 'grain': [], 'null': [], 'error': packed}
        with pytest.raises(WaveWorkerError, match='Guard: the two files disagree') as caught:
            pw.relay(payload, country='X', table='t')
        assert caught.value.original_type == 'Guard'
        assert 'X/w/t' in caught.value.__notes__[0]
        assert 'Traceback' in caught.value.__notes__[0]

    @pytest.mark.parametrize('base', ['Exception', 'ValueError', 'RuntimeError'])
    def test_its_base_class_does_not_survive_to_be_caught(self, tmp_path, base):
        """A by-path ``ValueError`` is no more a build failure than an ``Exception``."""
        Guard = by_path_guard(tmp_path, base)
        packed = pw._pack_exception(Guard('refused'))
        payload = {'wave': 'w', 'warnings': [], 'grain': [], 'null': [], 'error': packed}
        with pytest.raises(WaveWorkerError) as caught:
            pw.relay(payload, country='X', table='t')
        assert not isinstance(caught.value, (ValueError, KeyError, RuntimeError, OSError))

    def test_a_class_that_pickles_but_will_not_load_does_not_break_the_pool(self):
        """``dumps`` passes, the parent's load fails, and the pool calls the worker dead."""
        exc = PicklesButDoesNotLoad('two', 'arguments')
        pickle.dumps(exc)
        assert pw._pack_exception(exc)[0] is None

    def test_an_importable_class_still_travels_whole(self):
        exc = GrainCollapseError('strict')
        assert pw._pack_exception(exc)[0] is exc

    def test_a_dead_worker_is_not_a_build_failure(self):
        payload = {'wave': 'w', 'warnings': [], 'grain': [], 'null': [],
                   'error': (None, 'BrokenProcessPool', 'worker process died', '')}
        with pytest.raises(WaveWorkerError, match='worker process died'):
            pw.relay(payload, country='X', table='t')

    def test_the_error_itself_pickles(self):
        back = pickle.loads(pickle.dumps(WaveWorkerError('Guard: x', original_type='Guard')))
        assert str(back) == 'Guard: x' and back.original_type == 'Guard'

    def test_it_is_not_a_class_a_derived_handler_catches(self):
        assert not issubclass(WaveWorkerError, tuple(GENERIC + [AttributeError, OSError]))



def _food():
    index = pd.MultiIndex.from_tuples(
        [('2018-19', 'c', 'h', 'food', 'kg', 'purchased')],
        names=['t', 'v', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Quantity': [2.0], 'Expenditure': [8.0]}, index=index)



def install_fault(monkeypatch, method, error, site='source', builder=False):
    """``method``'s source raises ``error``; returns the tables that were then asked for."""
    source = 'household_roster' if method == 'household_characteristics' else 'food_acquired'
    calls = []
    legacy = pd.DataFrame({'legacy': [1.0]}, index=pd.Index(['h'], name='i'))

    def aggregate(self, waves, table, *args, **kwargs):
        calls.append(table)
        if table == source:
            if site == 'source':
                raise error
            return _food()
        return legacy.copy()

    def finalize(*args, **kwargs):
        raise error

    monkeypatch.setattr(Country, '_aggregate_wave_data', aggregate)
    monkeypatch.setattr(Country, '_unit_kg_factors', lambda self: {})
    monkeypatch.setattr(Country, '_no_kg_factor_cells', lambda self: {})
    monkeypatch.setattr(Country, '_has_country_builder', lambda self, table: builder)
    if site == 'finalize':
        monkeypatch.setattr(Country, '_finalize_result', finalize)
    return calls, legacy



def _forms(country):
    waves = list(country.waves)
    return [None, waves[:1], waves[:2], waves]



@pytest.mark.parametrize('error_type', NAMED + NOT_CAUGHT_BY_CLASS)
@pytest.mark.parametrize('method', DERIVED)
@pytest.mark.parametrize('builder', [False, True])
@pytest.mark.parametrize('form', range(4))
def test_a_named_failure_reaches_the_caller_as_itself(monkeypatch, error_type, method, builder, form):
    """Whether or not the legacy path could build the table: a strict refusal is not a
    reason to try something else, and is not a failed build."""
    error = error_type('the refusal')
    calls, _ = install_fault(monkeypatch, method, error, builder=builder)
    country = Country('Ethiopia')
    with pytest.raises(error_type) as caught:
        getattr(country, method)(waves=_forms(country)[form])
    assert caught.value is error
    assert method not in calls



@pytest.mark.parametrize('error_type', NAMED)
@pytest.mark.parametrize('method', FOOD)
def test_a_named_failure_of_the_derivation_itself(monkeypatch, error_type, method):
    error = error_type('the refusal')
    calls, _ = install_fault(monkeypatch, method, error, site='finalize', builder=True)
    with pytest.raises(error_type) as caught:
        getattr(Country('Ethiopia'), method)()
    assert caught.value is error
    assert method not in calls



@pytest.mark.parametrize('method', DERIVED)
def test_a_by_path_guard_reaches_the_caller_as_itself(monkeypatch, tmp_path, method):
    Guard = by_path_guard(tmp_path, 'ValueError')
    error = Guard('the refusal')
    calls, _ = install_fault(monkeypatch, method, error, builder=True)
    with pytest.raises(Guard):
        getattr(Country('Ethiopia'), method)()
    assert method not in calls



@pytest.mark.parametrize('error_type', GENERIC)
@pytest.mark.parametrize('method', DERIVED)
@pytest.mark.parametrize('form', range(4))
def test_with_nothing_to_fall_through_to_the_failure_is_raised(monkeypatch, error_type, method, form):
    """#1516.  No requested wave declares the table and no country builder exists, so the
    legacy path could only say 0 rows on a subset or "Could not materialize" otherwise."""
    error = error_type('the cause')
    calls, _ = install_fault(monkeypatch, method, error, builder=False)
    country = Country('Ethiopia')
    with pytest.raises(error_type) as caught:
        getattr(country, method)(waves=_forms(country)[form])
    assert caught.value is error
    assert method not in calls



@pytest.mark.parametrize('error_type', GENERIC)
@pytest.mark.parametrize('method', ['food_expenditures', 'household_characteristics'])
def test_a_real_legacy_route_still_answers_and_says_so(monkeypatch, error_type, method):
    calls, legacy = install_fault(monkeypatch, method, error_type('the cause'), builder=True)
    with pytest.warns(LegacyFallbackWarning, match='the cause'):
        out = getattr(Country('Ethiopia'), method)()
    pd.testing.assert_frame_equal(out, legacy)
    assert calls[-1] == method



def test_a_units_refusal_with_nothing_to_fall_through_to_names_the_country(monkeypatch):
    error = UnitsUnavailableError(units='kgvalue', missing=['Expenditure'])
    calls, _ = install_fault(monkeypatch, 'food_prices', error, builder=False)
    with pytest.raises(UnitsUnavailableError) as caught:
        Country('Ethiopia').food_prices()
    assert caught.value.country == 'Ethiopia'
    assert 'food_prices' not in calls



class TestLegacyCanBuild:
    def _country(self, name):
        return Country(name, preload_panel_ids=False)

    def test_a_declaring_wave_is_a_route(self):
        assert self._country('CotedIvoire')._legacy_can_build('food_expenditures', ['1985-86'])

    def test_a_country_script_is_a_route(self):
        assert self._country('Tanzania')._legacy_can_build('food_expenditures', None)

    def test_nothing_is_no_route(self):
        assert not self._country('Mali')._legacy_can_build('household_characteristics', None)
        assert not self._country('Albania')._legacy_can_build('household_characteristics', None)

    def test_a_makefile_that_cannot_be_inspected_keeps_the_route(self, monkeypatch):
        monkeypatch.setattr(C.shutil, 'which', lambda *a, **k: None)
        assert self._country('Mali')._legacy_can_build('household_characteristics', None)



def _roster(y):
    rows = [(y, f'{y}h{k}', '1') for k in range(3)]
    index = pd.MultiIndex.from_tuples(rows, names=['t', 'i', 'pid'])
    return pd.DataFrame({'Sex': ['M'] * 3, 'Age': [40] * 3, 'Relationship': ['Head'] * 3}, index=index)



def _sample(y):
    rows = [(f'{y}h{k}', y) for k in range(3)]
    index = pd.MultiIndex.from_tuples(rows, names=['i', 't'])
    return pd.DataFrame({'v': ['c1'] * 3, 'weight': [1.0] * 3, 'panel_weight': [1.0] * 3,
                         'strata': ['s'] * 3, 'Rural': ['Rural'] * 3}, index=index)



def _purchases(y):
    rows = [(y, f'{y}h{k}', 'Rice', 'Kg', 'purchased') for k in range(3)]
    index = pd.MultiIndex.from_tuples(rows, names=['t', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Quantity': [1.0] * 3, 'Expenditure': [100.0] * 3}, index=index)



BUILDERS = {'household_roster': _roster, 'sample': _sample, 'food_acquired': _purchases}



@pytest.fixture
def registry(tmp_path, monkeypatch):
    """Malawi's real registry over invented frames, in a private root.

    ``install(table, wave, error)`` makes that wave of that table raise ``error``.
    """
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'dvc')
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    for lever in ('LSMS_NO_CACHE', 'LSMS_GRAIN_STRICT', 'LSMS_READ_STRICT', 'LSMS_QUANTITY_STRICT'):
        monkeypatch.delenv(lever, raising=False)
    data_root.cache_clear()
    faults = {}

    def grab(wave, request, *, _with_origin=False):
        fault = faults.get((request, wave.year))
        if fault is not None:
            raise fault
        out = BUILDERS[request](wave.year)
        path = data_root('Malawi') / wave.year / '_' / f'{request}.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        to_parquet(out, path, absolute_path=True)
        return (out, 'parquet') if _with_origin else out

    monkeypatch.setattr(C.Wave, 'grab_data', grab)
    c = Country('Malawi', preload_panel_ids=False)
    c._panel_ids_attempted = True
    c._updated_ids_cache = {}
    waves = [w for w in c.waves if 'household_roster' in c[w].data_scheme]
    try:
        yield c, waves, lambda table, wave, error: faults.__setitem__((table, wave), error)
    finally:
        data_root.cache_clear()



@pytest.fixture
def quiet():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        yield



@pytest.mark.usefixtures('quiet')
class TestABrokenWaveInTheRoster:
    """#1516, through the real ``_aggregate_wave_data``, as the issue measured it."""

    def _broken(self, registry):
        c, waves, install = registry
        install('household_roster', waves[0], FileNotFoundError(f'source of {waves[0]}'))
        return c, waves[1], waves[0]

    def test_alone_it_raises(self, registry):
        c, _, bad = self._broken(registry)
        with pytest.raises(FileNotFoundError, match='source of'):
            c.household_characteristics(waves=[bad])

    def test_with_a_good_wave_it_raises(self, registry):
        c, good, bad = self._broken(registry)
        with pytest.raises(FileNotFoundError, match='source of'):
            c.household_characteristics(waves=[good, bad])

    def test_all_waves_name_the_cause(self, registry):
        c, _, _ = self._broken(registry)
        with pytest.raises(FileNotFoundError, match='source of'):
            c.household_characteristics()

    def test_a_good_wave_still_serves(self, registry):
        c, good, _ = self._broken(registry)
        assert len(c.household_characteristics(waves=[good])) == 3

    def test_a_real_absence_still_answers_empty(self, registry):
        c, waves, _ = registry
        absent = [w for w in c.waves if w not in waves]
        assert absent, 'Malawi 2024-25 declares no roster'
        assert len(c.household_characteristics(waves=absent)) == 0



def test_the_rebuild_failure_is_still_a_bare_runtime_error():
    """The one generic failure the fall-through exists for."""
    assert type(_rebuild_failure_error('X', 't')) in C._GENERIC_BUILD_FAILURES
