"""Tanzania plot-input units retain source information and use U_UNKNOWN.

Synthetic source records only: no microdata, cache or network dependency.
"""
import importlib.util

import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.transformations import U_UNKNOWN


@pytest.fixture
def tanzania(monkeypatch):
    path = countries_root() / 'Tanzania' / '_' / 'tanzania.py'
    spec = importlib.util.spec_from_file_location('_tanzania_1118', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, '_crop_labels', lambda: {1: 'Maize', 2: 'Maize'})
    monkeypatch.setattr(mod, '_plot_harmonized_codes', lambda *a: {1: 'Urea'})
    return mod


def sources(seed_rows=()):
    """Give every supported source field an explicit, inert default."""
    sec3 = pd.DataFrame({name: pd.Series(dtype='float64') for name in (
        'hhid', 'plot', 'ag3a_41', 'ag3a_42', 'ag3a_43', 'ag3a_44',
        'ag3a_47', 'ag3a_48', 'ag3a_49', 'ag3a_51', 'ag3a_54',
        'ag3a_55', 'ag3a_56', 'ag3a_58', 'ag3a_60', 'ag3a_62_1',
        'ag3a_62_2', 'ag3a_63', 'ag3a_65a', 'ag3a_65b_1',
        'ag3a_65b_2', 'ag3a_65c')})
    defaults = dict(hhid=1, plot=1, crop=1, ag4a_10_1=2,
                    ag4a_10_2=1, ag4a_10c_1=pd.NA,
                    ag4a_12=pd.NA, ag4a_08=2)
    sec4 = pd.DataFrame([dict(defaults, **row) for row in seed_rows],
                        columns=list(defaults))
    return sec3, sec4


def build(mod, sec3, sec4):
    return mod.plot_inputs_for_wave(
        'synthetic', sec3, sec4, {'hhid': 'hhid', 'plot': 'plot', 'crop': 'crop'})


@pytest.mark.parametrize('quantity', [pd.NA, 0, 3])
@pytest.mark.parametrize('unit', [pd.NA, 5])
def test_missing_or_other_unit_is_unknown_with_or_without_amount(
        tanzania, quantity, unit):
    sec3, sec4 = sources([dict(ag4a_10_1=quantity, ag4a_10_2=unit)])
    out = build(tanzania, sec3, sec4)
    assert len(out) == 1
    assert out.iloc[0]['u'] == U_UNKNOWN
    assert (pd.isna(out.iloc[0]['Quantity']) if pd.isna(quantity)
            else out.iloc[0]['Quantity'] == quantity)
    assert pd.isna(out.iloc[0]['Quantity_purchased'])


@pytest.mark.parametrize('label', ['', ' \t ', ' kg '])
@pytest.mark.parametrize('quantity', [pd.NA, 7])
def test_blank_labels_normalize_but_nonblank_labels_remain_verbatim(
        tanzania, monkeypatch, label, quantity):
    monkeypatch.setattr(tanzania, '_SEED_UNIT_LABELS', {1: label})
    sec3, sec4 = sources([dict(ag4a_10_1=quantity)])
    out = build(tanzania, sec3, sec4)
    assert len(out) == 1
    assert out.iloc[0]['u'] == (label if label.strip() else U_UNKNOWN)
    assert (pd.isna(out.iloc[0]['Quantity']) if pd.isna(quantity)
            else out.iloc[0]['Quantity'] == quantity)


def test_duplicate_missing_first_unit_does_not_displace_later_known_unit(tanzania):
    sec3, sec4 = sources([
        dict(crop=1, ag4a_10_1=2, ag4a_10_2=pd.NA),
        dict(crop=2, ag4a_10_1=pd.NA, ag4a_10_2=1),
    ])
    out = build(tanzania, sec3, sec4)
    assert len(out) == 1
    assert out.iloc[0]['u'] == 'kg'
    assert out.iloc[0]['Quantity'] == 2
    assert pd.isna(out.iloc[0]['Quantity_purchased'])
    assert list(out.index.names) == ['t', 'i', 'plot_id', 'input', 'crop']
    assert list(out.columns) == tanzania.PLOT_INPUTS_COLUMNS


def test_all_missing_duplicate_units_normalize_after_unchanged_reduction(tanzania):
    sec3, sec4 = sources([
        dict(crop=1, ag4a_10_1=pd.NA, ag4a_10_2=pd.NA, ag4a_08=1),
        dict(crop=2, ag4a_10_1=pd.NA, ag4a_10_2=5, ag4a_12=12),
    ])
    out = build(tanzania, sec3, sec4)
    assert len(out) == 1
    assert out.iloc[0]['u'] == U_UNKNOWN
    assert pd.isna(out.iloc[0]['Quantity'])
    assert pd.isna(out.iloc[0]['Quantity_purchased'])
    assert out.iloc[0]['Purchased']
    assert out.iloc[0]['Improved']


def test_no_amount_source_use_keeps_every_input_and_its_recorded_unit(tanzania):
    sec3, sec4 = sources([dict(ag4a_10_1=pd.NA)])
    sec3.loc[0] = pd.NA
    sec3.loc[0, ['hhid', 'plot']] = [1, 1]
    sec3.loc[0, ['ag3a_41', 'ag3a_47', 'ag3a_54', 'ag3a_60', 'ag3a_65a']] = 1
    sec3.loc[0, ['ag3a_48', 'ag3a_55']] = 1
    sec3.loc[0, ['ag3a_62_2', 'ag3a_65b_2']] = [2, 3]
    out = build(tanzania, sec3, sec4).reset_index().set_index('input')
    assert len(out) == 5  # unchanged collapse of the two Urea source slots
    assert out['u'].to_dict() == {
        'Seed': 'kg', 'Organic Fertilizer': 'kg', 'Urea': 'kg',
        'Herbicide': 'litre', 'Pesticide': 'millilitre'}
    assert out['Quantity'].isna().all()
    assert out['Quantity_purchased'].isna().all()


def test_conflicting_recorded_duplicate_units_keep_existing_first_policy(tanzania):
    sec3, sec4 = sources([
        dict(crop=1, ag4a_10_1=2, ag4a_10_2=2),
        dict(crop=2, ag4a_10_1=3, ag4a_10_2=1),
    ])
    out = build(tanzania, sec3, sec4)
    assert len(out) == 1
    assert out.iloc[0]['u'] == '20 liter bucket'
    assert out.iloc[0]['Quantity'] == 5
