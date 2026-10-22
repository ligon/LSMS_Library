"""GH #1081: typed categorical errors survive generated derived dispatch."""
import pandas as pd
import pytest
from lsms_library.country import Country, _rebuild_failure_error
from lsms_library.local_tools import (
    CategoricalKeyDeclarationError, CategoricalMappingConflictError,
    EmptyCategoricalTableError, get_categorical_mapping, load_categorical_level,
)

ERRORS = ['empty', 'conflict', 'key']
FOOD = ['food_expenditures', 'food_prices', 'food_quantities']
METHODS = FOOD + ['household_characteristics']


def parser_error(tmp_path, kind):
    path = tmp_path / 'categorical_mapping.org'
    text = '#+name: probe\n| Code | Preferred Label |\n|---+---|\n| 1 | A |\n'
    if kind == 'empty':
        path.write_text('#+name: probe\n| Code | Label |\n|---+---|\n')
        operation = lambda: get_categorical_mapping(
            tablename='probe', dirs=[str(tmp_path)], Label='Label')
        error_type = EmptyCategoricalTableError
    elif kind == 'conflict':
        path.write_text(text)
        other = tmp_path / 'categorical_mapping'
        other.mkdir()
        (other / 'probe.org').write_text(text)
        operation = lambda: load_categorical_level(tmp_path)
        error_type = CategoricalMappingConflictError
    else:
        path.write_text('#+key: Missing\n' + text)
        operation = lambda: load_categorical_level(tmp_path)
        error_type = CategoricalKeyDeclarationError
    with pytest.raises(error_type) as info:
        operation()
    error = info.value
    error.add_note('preserve the actionable parser context')
    error.probe_context = {'table': 'probe', 'origin': path}
    return error


def food():
    index = pd.MultiIndex.from_tuples(
        [('2018-19', 'synthetic-cluster', 'synthetic-household',
          'synthetic-food', 'kg', 'purchased')],
        names=['t', 'v', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Quantity': [2.0], 'Expenditure': [8.0]}, index=index)


def install_fault(monkeypatch, method, error, site):
    source = ('household_roster' if method == 'household_characteristics'
              else 'food_acquired')
    calls = []

    def aggregate(self, waves, table, *args, **kwargs):
        calls.append(table)
        if table == source:
            if site == 'source':
                raise error
            return food()
        raise _rebuild_failure_error(self.name, table)

    monkeypatch.setattr(Country, '_aggregate_wave_data', aggregate)
    monkeypatch.setattr(Country, '_unit_kg_factors', lambda self: {})
    monkeypatch.setattr(Country, '_no_kg_factor_cells', lambda self: {})
    if site == 'finalize':
        def finalize(*args, **kwargs):
            raise error
        monkeypatch.setattr(Country, '_finalize_result', finalize)
    return calls


def assert_original(call, error):
    context = (error.args, error.__cause__, tuple(error.__notes__),
               error.probe_context)
    with pytest.raises(type(error)) as info:
        call()
    assert info.value is error
    assert context == (info.value.args, info.value.__cause__,
                       tuple(info.value.__notes__), info.value.probe_context)


@pytest.mark.parametrize('kind', ERRORS)
@pytest.mark.parametrize('method', METHODS)
@pytest.mark.parametrize('labels', ['Preferred', 'Aggregate'])
def test_source_error_keeps_identity_and_context(monkeypatch, tmp_path,
                                                kind, method, labels):
    error = parser_error(tmp_path, kind)
    calls = install_fault(monkeypatch, method, error, 'source')
    country = Country('Ethiopia')
    assert_original(lambda: getattr(country, method)(labels=labels), error)
    assert calls == [('household_roster' if method == 'household_characteristics'
                      else 'food_acquired')]


@pytest.mark.parametrize('kind', ERRORS)
@pytest.mark.parametrize('method', FOOD)
@pytest.mark.parametrize('labels', ['Preferred', 'Aggregate'])
def test_derived_finalize_error_keeps_identity(monkeypatch, tmp_path,
                                              kind, method, labels):
    error = parser_error(tmp_path, kind)
    calls = install_fault(monkeypatch, method, error, 'finalize')
    country = Country('Ethiopia')
    assert_original(lambda: getattr(country, method)(labels=labels), error)
    assert calls == ['food_acquired']


@pytest.mark.parametrize('kind', ERRORS)
@pytest.mark.parametrize('method,kwargs', [
    *[(method, {'labels': {'Sex': 'Alternate'}}) for method in FOOD],
    ('food_prices', {'units': 'unitvalue'}),
    ('food_quantities', {'units': 'units'}),
])
def test_existing_explicit_food_error_path_stays_exact(monkeypatch, tmp_path,
                                                       kind, method, kwargs):
    error = parser_error(tmp_path, kind)
    calls = install_fault(monkeypatch, method, error, 'source')
    country = Country('Ethiopia')
    assert_original(lambda: getattr(country, method)(**kwargs), error)
    assert calls == ['food_acquired']


@pytest.mark.parametrize('error_type', [FileNotFoundError, KeyError,
                                       ValueError, RuntimeError])
@pytest.mark.parametrize('method', ['food_expenditures',
                                    'household_characteristics'])
def test_unrelated_generic_legacy_fallback_stays(monkeypatch, error_type, method):
    calls = []
    source = ('food_acquired' if method == 'food_expenditures'
              else 'household_roster')
    expected = pd.DataFrame({'legacy': [1.0]},
                            index=pd.Index(['synthetic'], name='i'))

    def aggregate(self, waves, table, *args, **kwargs):
        calls.append(table)
        if table == source:
            raise error_type('generic failure retains its established fallback')
        return expected.copy()

    monkeypatch.setattr(Country, '_aggregate_wave_data', aggregate)
    country = Country('Ethiopia')
    pd.testing.assert_frame_equal(getattr(country, method)(), expected)
    assert calls == [source, method]


@pytest.mark.parametrize('error_type', [FileNotFoundError, KeyError,
                                       ValueError, RuntimeError])
@pytest.mark.parametrize('method', ['food_expenditures',
                                    'household_characteristics'])
def test_unrelated_generic_legacy_failure_stays(monkeypatch, error_type, method):
    calls = []
    source = ('food_acquired' if method == 'food_expenditures'
              else 'household_roster')
    original = error_type('generic failure retains its established fallback')

    def aggregate(self, waves, table, *args, **kwargs):
        calls.append(table)
        if table == source:
            raise original
        raise _rebuild_failure_error(self.name, table)

    monkeypatch.setattr(Country, '_aggregate_wave_data', aggregate)
    country = Country('Ethiopia')
    with pytest.raises(RuntimeError, match='Could not materialize Ethiopia/') as info:
        getattr(country, method)()
    assert info.value is not original
    assert info.value.__cause__ is None
    assert info.value.__context__ is None
    assert calls == [source, method]
