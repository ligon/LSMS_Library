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

import lsms_library as ll
from lsms_library import _build_registry as R
from lsms_library import _parallel_waves as pw
from lsms_library import country as C
from lsms_library.build_transforms import GrainConflict
from lsms_library.country import (Country, GrainCollapseError, LabelPassThroughError,
                                  LegacyFallbackWarning, SampleUnavailableWarning,
                                  _rebuild_failure_error)
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


# ---------------------------------------------------------------------------
# #1524: what crosses back from a worker
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# #1532 and #1516: the dispatch, on a controlled source
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# The same rules through the real build, on invented frames
# ---------------------------------------------------------------------------

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


@pytest.mark.usefixtures('quiet')
class TestARefusalInSample:
    """#1546.  Through the real v-join, from the table the user asked for."""

    @pytest.mark.parametrize('error_type,raised', [
        (ValueError, ValueError), (RuntimeError, RuntimeError), (GrainConflict, GrainConflict),
        (KeyError, RuntimeError),   # build_wave names the wave (GH #1281)
    ])
    @pytest.mark.parametrize('table', ['household_roster', 'household_characteristics',
                                       'food_acquired', 'food_expenditures'])
    def test_it_reaches_the_caller(self, registry, error_type, raised, table):
        c, waves, install = registry
        install('sample', waves[0], error_type('the refusal'))
        with pytest.raises(raised, match='the refusal'):
            getattr(c, table)(waves=[waves[1]])

    def test_a_by_path_guard_reaches_the_caller(self, registry, tmp_path):
        c, waves, install = registry
        Guard = by_path_guard(tmp_path)
        install('sample', waves[0], Guard('the refusal'))
        with pytest.raises(Guard):
            c.household_roster()

    def test_a_missing_source_serves_the_rows_and_says_so(self, registry):
        c, waves, install = registry
        install('sample', waves[0], FileNotFoundError('no such blob'))
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter('always')
            out = c.household_roster(waves=[waves[1]])
        assert 'v' not in out.index.names and len(out) == 3
        said = [str(w.message) for w in seen if issubclass(w.category, SampleUnavailableWarning)]
        assert said and 'no such blob' in said[0]

    def test_a_clean_sample_joins_v(self, registry):
        c, waves, _ = registry
        out = c.household_roster(waves=[waves[1]])
        assert 'v' in out.index.names and out.index.get_level_values('v').notna().all()


@pytest.mark.usefixtures('quiet')
class TestUnderParallelWorkers:
    """#1524.  A by-path guard in a worker, on two targets, as the issue measured it."""

    @pytest.fixture(autouse=True)
    def two_workers(self, registry, monkeypatch):
        monkeypatch.setenv('LSMS_BUILD_WORKERS', '2')
        monkeypatch.setattr(pw, 'visible_cpus', lambda env=None: 4)
        monkeypatch.setattr(pw, 'memory_worker_cap', lambda env=None: None)

    @pytest.mark.parametrize('table', ['household_roster', 'household_characteristics',
                                       'food_acquired', 'food_expenditures', 'food_quantities'])
    def test_it_is_not_served_as_empty(self, registry, tmp_path, table):
        c, waves, install = registry
        install('sample', waves[0], by_path_guard(tmp_path)('the refusal'))
        with pytest.raises(WaveWorkerError, match='Guard: the refusal') as caught:
            getattr(c, table)(waves=[waves[1]])
        assert caught.value.original_type == 'Guard'

    def test_in_the_table_itself_it_is_not_served_as_empty(self, registry, tmp_path):
        c, waves, install = registry
        install('household_roster', waves[0], by_path_guard(tmp_path)('the refusal'))
        with pytest.raises(WaveWorkerError, match='Guard: the refusal'):
            c.household_characteristics()

    def test_a_clean_build_is_unchanged(self, registry):
        c, waves, _ = registry
        assert len(c.household_characteristics(waves=waves[:2])) == 6


# ---------------------------------------------------------------------------
# #1546: the two reads of sample, directly
# ---------------------------------------------------------------------------

def _frame():
    return pd.DataFrame({'x': [1]}, index=pd.MultiIndex.from_tuples([('a', '2014')], names=['i', 't']))


class TestVJoin:
    def _country(self, monkeypatch, sample):
        country = Country('Malawi', preload_panel_ids=False)
        calls = []

        def fake(*args, **kwargs):
            calls.append(1)
            if isinstance(sample, Exception):
                raise sample
            return sample

        monkeypatch.setattr(country, 'sample', fake, raising=False)
        return country, calls

    @pytest.mark.parametrize('error', [ValueError('x'), KeyError('x'), AttributeError('x'),
                                       GrainConflict('x'), RuntimeError('x')])
    def test_a_refusal_is_raised_and_not_remembered(self, monkeypatch, error):
        country, calls = self._country(monkeypatch, error)
        for _ in range(2):
            with pytest.raises(type(error)):
                country._join_v_from_sample(_frame())
        assert len(calls) == 2

    def test_a_missing_source_warns_on_every_table_and_reads_once(self, monkeypatch):
        country, calls = self._country(monkeypatch, FileNotFoundError('gone'))
        for _ in range(2):
            with pytest.warns(SampleUnavailableWarning, match='gone'):
                out = country._join_v_from_sample(_frame())
            pd.testing.assert_frame_equal(out, _frame())
        assert len(calls) == 1

    def test_a_sample_with_no_v_warns(self, monkeypatch):
        sample = pd.DataFrame({'weight': [1.0]}, index=_frame().index)
        country, _ = self._country(monkeypatch, sample)
        with pytest.warns(SampleUnavailableWarning, match='no `v`'):
            out = country._join_v_from_sample(_frame())
        assert 'v' not in out.index.names

    def test_a_country_with_no_sample_is_not_asked(self):
        """Armenia declares no ``sample``, and the guard is at the call in ``_finalize_result``."""
        country = Country('Armenia', preload_panel_ids=False)
        assert 'sample' not in country.data_scheme
        src = C.Country._finalize_result.__code__
        assert '_join_v_from_sample' in src.co_names


class _MarketStub:
    name = 'Fixtureland'
    data_scheme = {'sample': {}, 'cluster_features': {}}
    _add_market_index = Country._add_market_index
    _report_unlabelled_markets = Country._report_unlabelled_markets

    def __init__(self, sample):
        self._sample = sample

    def sample(self):
        raise self._sample

    def _market_lookup(self, column='Region'):
        return pd.DataFrame({'t': ['2000'], 'v': ['c1'], 'm': ['North']})


def _table():
    index = pd.MultiIndex.from_tuples([('2000', 'c1', 'h1')], names=['t', 'v', 'i'])
    return pd.DataFrame({'x': [1.0]}, index=index)


class TestMarketLookup:
    @pytest.mark.parametrize('error', [ValueError('x'), KeyError('x'), AttributeError('x'),
                                       GrainConflict('x')])
    def test_a_refusal_is_raised(self, error):
        with pytest.raises(type(error)):
            _MarketStub(error)._add_market_index(_table())

    @pytest.mark.parametrize('error', [FileNotFoundError('gone'), OSError('gone')])
    def test_an_unreachable_source_labels_from_the_cluster_and_says_so(self, error):
        with pytest.warns(SampleUnavailableWarning, match='gone'):
            out = _MarketStub(error)._add_market_index(_table())
        assert out.index.get_level_values('m').tolist() == ['North']


# ---------------------------------------------------------------------------
# What must keep working
# ---------------------------------------------------------------------------

def test_the_feature_loop_still_drops_a_country_with_a_warning(monkeypatch, tmp_path):
    Guard = by_path_guard(tmp_path)
    good = _roster('2018-19').assign(Generation=0, Distance=0, Affinity='consanguineal')
    good = good.drop(columns='Relationship')

    def aggregate(self, waves, table, *args, **kwargs):
        if self.name == 'Ethiopia':
            raise Guard('the refusal')
        return good.copy()

    monkeypatch.setattr(Country, '_aggregate_wave_data', aggregate)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        out = ll.Feature('household_characteristics')(['Ethiopia', 'Malawi'])
    assert set(out.index.get_level_values('country')) == {'Malawi'}
    assert any('Ethiopia' in str(w.message) and 'Guard: the refusal' in str(w.message) for w in seen)


def test_the_rebuild_failure_is_still_a_bare_runtime_error():
    """The one generic failure the fall-through exists for."""
    assert type(_rebuild_failure_error('X', 't')) in C._GENERIC_BUILD_FAILURES


def test_no_handler_edit_can_move_a_build_fingerprint():
    """Every callable these rules live in stays out of every table's closure."""
    seen, parts = set(), []
    for _qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    for name in ('Country._join_v_from_sample', 'Country._add_market_index',
                 'Country._legacy_can_build', 'Country._warn_legacy_fallback',
                 'Country._proven_absent', 'Country.__getattr__',
                 '_parallel_waves._pack_exception', '_parallel_waves.relay',
                 'WaveWorkerError', 'LegacyFallbackWarning', 'SampleUnavailableWarning'):
        assert not [q for q in seen if q.endswith(name)], name
