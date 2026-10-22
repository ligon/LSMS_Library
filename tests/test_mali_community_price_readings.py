"""Mali EAC-I records three price readings; GH #822 / ruling GH #840."""
from __future__ import annotations

import importlib.util
import runpy
import sys

import pandas as pd
import pytest

from lsms_library import local_tools
from lsms_library.paths import countries_root


@pytest.fixture
def mali(monkeypatch):
    path = countries_root() / 'Mali' / '_' / 'mali.py'
    spec = importlib.util.spec_from_file_location('mali', path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'mali', module)
    spec.loader.exec_module(module)
    monkeypatch.chdir(countries_root() / 'Mali' / '2014-15' / '_')
    return module


def _source(visit, first_unit=1.0):
    codes = pd.DataFrame({
        'grappe': [1, 2], 's04q01': [501, 501], 's04q02': [5011, 5012],
        's04q03': [first_unit, 1.7], 's04q04': [1.0, 1.0],
        's04q05': [100.0, 200.0],
        's04q06': [8.0, 100.0], 's04q07': [999.0, 1.0],
        's04q08': [110.0, 210.0],
        's04q09': [5.0, 1.0], 's04q10': [0.0, 9999.0],
        's04q11': [120.0, 0.0 if visit == 1 else 9999.0],
    })
    labels = codes.copy()
    labels['s04q01'] = 'Riz'
    labels['s04q02'] = [f'rice {chr(34) if visit == 1 else chr(39)}Aicha', 'rice 2']
    labels['s04q09'] = ['Unit\u00e9', 'Kilogramme']
    if visit == 1:
        labels['s04q03'] = ['Kilogramme', 'Manquant']
        labels['s04q06'] = ['Bo\u00eete', '0.0']
    # p2 q03/q06 intentionally have numeric codes and no value labels.
    return labels, codes


def _run_script(monkeypatch, first_unit=1.0):
    captured = []
    def read(fn, convert_categoricals=True):
        visit = 1 if str(fn).endswith('p1.dta') else 2
        labels, codes = _source(visit, first_unit=first_unit)
        return (labels if convert_categoricals else codes).copy()
    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    monkeypatch.setattr(local_tools, 'to_parquet', lambda df, fn: captured.append(df))
    runpy.run_path(str(countries_root() / 'Mali' / '2014-15' / '_' / 'community_prices.py'))
    assert len(captured) == 1
    return captured[0].droplevel('i')


def test_actual_script_restores_all_slots_and_missing_p2_unit_labels(mali, monkeypatch):
    out = _run_script(monkeypatch)
    assert list(out.index.names) == ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
    assert len(out) == 7 and out.index.is_unique
    assert out.groupby(level=['visit', 'obs']).size().to_dict() == {
        (1, 1): 1, (1, 2): 1, (1, 3): 1,
        (2, 1): 1, (2, 2): 1, (2, 3): 2,
    }
    first = out.xs('1', level='v').reset_index().set_index(['visit', 'obs'])
    assert first['u'].to_dict() == {
        (1, 1): 'Kg', (1, 2): 'Boite', (1, 3): 'Unit\u00e9',
        (2, 1): 'Kg', (2, 2): 'Boite', (2, 3): 'Unit\u00e9',
    }
    assert first['Price'].to_dict() == {
        (1, 1): 100.0, (1, 2): 110.0, (1, 3): 120.0,
        (2, 1): 100.0, (2, 2): 110.0, (2, 3): 120.0,
    }
    assert first.loc[(1, 2), 'Quantity'] is pd.NA or pd.isna(first.loc[(1, 2), 'Quantity'])
    assert first.loc[(2, 2), 'Quantity'] is pd.NA or pd.isna(first.loc[(2, 2), 'Quantity'])
    assert first.loc[(1, 3), 'Quantity'] == 0
    assert first.loc[(1, 1), 'Description'] != first.loc[(2, 1), 'Description']
    assert set(first['variety']) == {'5011'}  # CODE survives label quote drift.
    last = out.xs('2', level='v')
    assert len(last) == 1 and last.index.get_level_values('obs').tolist() == [3]
    # Existing eligibility is Price>0, with no new price-sentinel policy.
    assert last['Price'].iloc[0] == 9999.0
    assert last['Quantity'].isna().all()


@pytest.mark.parametrize('unit', [1, 1.0, '1', '1.0', '1.000'])
def test_known_p2_unit_code_uses_exact_numeric_value(mali, monkeypatch, unit):
    out = _run_script(monkeypatch, first_unit=unit).reset_index()
    row = out[(out.v == '1') & (out.visit == 2) & (out.obs == 1)]
    assert len(row) == 1 and row.u.iloc[0] == 'Kg'


@pytest.mark.parametrize('unit', [0, 99, 15, 50, 100, 0.75, 1.228, 1.7])
def test_unlisted_p2_units_are_not_rounded_or_assigned_a_known_unit(mali, monkeypatch, unit):
    out = _run_script(monkeypatch, first_unit=unit).reset_index()
    assert not ((out.v == '1') & (out.visit == 2) & (out.obs == 1)).any()
    assert len(out) == 6


def test_duplicate_full_native_observation_key_is_an_error(mali):
    row = {'t': '2014-15', 'v': '1', 'j': 'Riz', 'u': 'Kilogramme',
           'variety': '5011', 'visit': 1, 'obs': 1, 'Description': 'rice',
           'Price': 100.0, 'Quantity': 1.0}
    with pytest.raises(AssertionError, match='rows share a'):
        mali.community_prices_finalize(pd.DataFrame([row, row]))


def test_equal_prices_on_distinct_reading_slots_are_preserved(mali):
    row = {'t': '2014-15', 'v': '1', 'j': 'Riz', 'u': 'Kilogramme',
           'variety': '5011', 'visit': 1, 'obs': 1, 'Description': 'rice',
           'Price': 100.0, 'Quantity': 1.0}
    out = mali.community_prices_finalize(pd.DataFrame([row, {**row, 'obs': 2}]))
    assert len(out) == 2 and out.index.is_unique
    assert out.index.get_level_values('obs').tolist() == [1, 2]


@pytest.mark.parametrize('obs', [0, 4, 1.7, None])
def test_obs_is_a_native_form_slot(mali, obs):
    row = {'t': '2014-15', 'v': '1', 'j': 'Riz', 'u': 'Kilogramme',
           'variety': '5011', 'visit': 1, 'obs': obs, 'Description': 'rice',
           'Price': 100.0, 'Quantity': 1.0}
    with pytest.raises(AssertionError, match='obs must be a reading slot'):
        mali.community_prices_finalize(pd.DataFrame([row]))


@pytest.mark.requires_s3
def test_public_country_and_feature_carry_native_readings():
    import lsms_library as ll
    out = ll.Country('Mali', preload_panel_ids=False).community_prices()
    assert list(out.index.names) == ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
    assert out.index.is_unique and len(out) == 155931
    assert out.groupby(level='obs').size().to_dict() == {1: 66699, 2: 49014, 3: 40218}
    assert pd.api.types.is_integer_dtype(out.index.get_level_values('obs'))
    feature = ll.Feature('community_prices')(['Mali'], currency=None)
    pd.testing.assert_frame_equal(feature.droplevel('country').sort_index(), out.sort_index())
    assert 'canonical_alignment' not in feature.attrs
