"""Tanzania #905: exercise actual helper duplicate paths on synthetic sources.

Ledger 905-tanzania-public-extraction §§2-6: optimize livestock only; preserve
held legacy reducers, source-use, all-NA values, exact keys/order/dtypes,
unit guards and the signed fertilizer sum.
No Country, provider, source-file or cache build is needed.
"""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'lsms_library/countries/Tanzania/_/tanzania.py'


def load_helper():
    spec = importlib.util.spec_from_file_location('_tanzania_905', SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def stub_maps(module, monkeypatch):
    monkeypatch.setattr(module, '_crop_labels', lambda: {1: 'Pulses', 2: 'Pulses'})
    monkeypatch.setattr(module, '_plot_harmonized_codes', lambda *a, **kw: {1: 'Urea'})
    monkeypatch.setattr(module, '_unit_preferred_labels', lambda: {'kg': 'Kg'})
    # livestock resolves its source-wave code via this imported leaf.
    import lsms_library.local_tools as local_tools
    monkeypatch.setattr(local_tools, 'get_categorical_mapping',
                        lambda *a, **kw: {1: 'Cattle', 2: 'Cattle'})


def source_case(kind, *, wave='2020-21'):
    """Explicit inert defaults; duplicate, all-NA and untouched singleton keys."""
    if kind == 'plot_inputs':
        names = ('hhid', 'plot', 'ag3a_41', 'ag3a_42', 'ag3a_43', 'ag3a_44',
                 'ag3a_47', 'ag3a_48', 'ag3a_49', 'ag3a_51', 'ag3a_54',
                 'ag3a_55', 'ag3a_56', 'ag3a_58', 'ag3a_60', 'ag3a_62_1',
                 'ag3a_62_2', 'ag3a_63', 'ag3a_65a', 'ag3a_65b_1',
                 'ag3a_65b_2', 'ag3a_65c')
        sec3 = pd.DataFrame([{name: row.get(name, float('nan')) for name in names}
                            for row in [dict(hhid=2, plot=1, ag3a_47=1,
                                             ag3a_48=1, ag3a_49=50,
                                             ag3a_54=1, ag3a_55=1, ag3a_56=100)]])
        base = dict(hhid=1, plot=1, crop=1, ag4a_10_1=0.1, ag4a_10_2=1,
                    ag4a_10c_1=pd.NA, ag4a_10c_2=pd.NA, ag4a_12=pd.NA, ag4a_08=2)
        sec4 = pd.DataFrame([
            dict(base), dict(base, crop=2, ag4a_10_1=0.2, ag4a_08=1,
                             ag4a_10c_1=0.1, ag4a_10c_2=1),
            dict(base, plot=2, ag4a_10_1=pd.NA),
            dict(base, plot=2, crop=2, ag4a_10_1=pd.NA),
            dict(base, plot=3, ag4a_10_1=7),
        ])
        return (wave, sec3, sec4, dict(hhid='hhid', plot='plot', crop='crop'))
    if kind == 'livestock':
        raw = pd.DataFrame({
            'hhid': [1, 1, 2, 2, 3, 4], 'species': [1, 2, 1, 2, 1, 1],
            'own': [1, 1, 1, 1, 1, 2],
            'indigenous': [1, 3, pd.NA, pd.NA, 7, 99],
            'improved': [pd.NA] * 6, 'bought': [0.1, 0.2, pd.NA, pd.NA, 1, 99],
            'sold': [pd.NA, pd.NA, pd.NA, pd.NA, 2, 99],
        })
        return (wave, raw, dict(hhid='hhid', animal='species', own='own',
                                owned=['indigenous', 'improved'], bought='bought',
                                sold='sold', species_col=wave))
    names = [f'ag3a_74_{task}{gender}' for task in (1, 2, 3)
             for gender in ('a', 'b', 'c', 'd')]
    raw = pd.DataFrame([{name: row.get(name, float('nan')) for name in
                        ['hhid', 'plot', 'ag3a_73'] + names} for row in [
        dict(hhid=1, plot=1, ag3a_73=1, ag3a_74_1a=0.1, ag3a_74_1d=10),
        dict(hhid=1, plot=1, ag3a_73=1, ag3a_74_1a=0.2, ag3a_74_1d=20),
        dict(hhid=2, plot=1, ag3a_73=1), dict(hhid=2, plot=1, ag3a_73=1),
        dict(hhid=3, plot=1, ag3a_73=2),
    ]])
    if wave == '2020-21':
        raw['ag3a_72c_1'] = [2, 3, pd.NA, pd.NA, 7]
    else:
        raw['ag3a_72b_1'] = [100, 200, 300, 400, 500]  # roster IDs, never days
    return (wave, raw, dict(hhid='hhid', plot='plot'))


def build(module, kind, wave='2020-21'):
    return getattr(module, f'{kind}_for_wave')(*source_case(kind, wave=wave))


@pytest.fixture
def helper(monkeypatch):
    module = load_helper()
    stub_maps(module, monkeypatch)
    return module


@pytest.mark.parametrize('kind', ['plot_inputs', 'livestock', 'plot_labor'])
def test_duplicate_helpers_match_declared_numeric_sum_scope(helper, monkeypatch, kind):
    original = pd.Series.sum
    calls = []

    def spy(series, *args, **kwargs):
        if kwargs.get('min_count') == 1:
            calls.append(series.name)
        return original(series, *args, **kwargs)

    monkeypatch.setattr(pd.Series, 'sum', spy)
    out = build(helper, kind)
    assert len(out) > 1
    if kind == 'livestock':
        assert calls == [], f'livestock per-group Python sum callbacks remain: {calls}'
    else:
        # Root-approved partial scope: signed cancellation exceeded 4ULP here.
        # Keep the exact public legacy callbacks; do not change the bound.
        assert len(calls) > 0, f'held {kind} legacy sum callbacks were replaced'


def test_plot_input_rows_flags_units_and_na_survive(helper):
    out = build(helper, 'plot_inputs')
    assert list(out.columns) == helper.PLOT_INPUTS_COLUMNS
    assert list(out.index.names) == helper.PLOT_INPUTS_INDEX
    assert all(str(out[c].dtype) == 'Float64' for c in ('Quantity', 'Quantity_purchased'))
    assert str(out['Unit_purchased'].dtype) == 'string'
    seed = out.xs('Seed', level='input')
    first = seed.xs('1', level='plot_id').iloc[0]
    np.testing.assert_array_max_ulp([first['Quantity']], [0.3], maxulp=4)
    assert first['Quantity_purchased'] == 0.1
    assert bool(first['Purchased']) and bool(first['Improved'])
    assert first['Unit_purchased'] == 'Kg'
    assert seed.xs('2', level='plot_id')['Quantity'].isna().all()
    assert seed.xs('2', level='plot_id')['Quantity_purchased'].isna().all()
    assert seed.xs('3', level='plot_id')['Quantity'].tolist() == [7]
    # Signed #637 fertilizer-slot sum; crop NA keys stay present.
    fert = out.xs('Urea', level='input')
    assert fert['Quantity'].tolist() == [150]
    assert fert['Quantity_purchased'].isna().all()
    assert fert.index.get_level_values('crop').isna().all()


def test_livestock_values_all_na_and_owned_filter_survive(helper):
    out = build(helper, 'livestock')
    assert list(out.columns) == helper.LIVESTOCK_COLUMNS
    assert list(out.index.names) == ['t', 'i', 'animal']
    assert out.dtypes.astype(str).tolist() == ['Float64'] * 3
    assert out.xs('1', level='i')['HeadCount'].tolist() == [4]
    np.testing.assert_array_max_ulp(
        [out.xs('1', level='i')['HeadAcquired'].iloc[0]], [0.3], maxulp=4)
    assert out.xs('1', level='i')['HeadSold'].isna().all()
    assert out.xs('2', level='i').isna().all().all()
    assert set(out.index.get_level_values('i')) == {'1', '2', '3'}


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
def test_labor_values_and_wave_specific_family_source_survive(helper, wave):
    out = build(helper, 'plot_labor', wave)
    assert list(out.columns) == helper.PLOT_LABOR_COLUMNS
    assert list(out.index.names) == ['t', 'i', 'plot_id', 'source']
    assert out.dtypes.astype(str).tolist() == ['Float64'] * 2
    hired = out.xs('hired', level='source')
    np.testing.assert_array_max_ulp(
        [hired.xs('1', level='i')['PersonDays'].iloc[0]], [0.3], maxulp=4)
    assert hired.xs('1', level='i')['Wage'].tolist() == [30]
    assert hired.xs('2', level='i').isna().all().all()
    assert hired.xs('3', level='i').iloc[0].tolist() == [0, 0]
    if wave == '2019-20':
        assert set(out.index.get_level_values('source')) == {'hired'}
    else:
        family = out.xs('family', level='source')
        assert family.xs('1', level='i')['PersonDays'].tolist() == [5]
        assert family['Wage'].isna().all()
