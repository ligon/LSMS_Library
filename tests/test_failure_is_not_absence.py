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

import pandas as pd
import pytest

from lsms_library import _parallel_waves as pw
from lsms_library import country as C
from lsms_library.build_transforms import GrainConflict
from lsms_library.country import (Country, GrainCollapseError, LabelPassThroughError,
                                  _rebuild_failure_error)
from lsms_library.datetime_parse import DatetimeParseError
from lsms_library.errors import WaveWorkerError
from lsms_library.local_tools import IdWalkError, InTreeSourceError
from lsms_library.null_index_audit import NullIndexKeyError
from lsms_library.null_read_audit import NullReadError
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



def test_the_rebuild_failure_is_still_a_bare_runtime_error():
    """The one generic failure the fall-through exists for."""
    assert type(_rebuild_failure_error('X', 't')) in C._GENERIC_BUILD_FAILURES
