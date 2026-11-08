"""Invented own-source native carriers; no reader/category-card/API acceptance."""
import importlib.util
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from lsms_library.paths import countries_root

RAINY = '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta'
DRY = '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3c_p1.dta'
MAPS = dict(input_map={'semences': 'Seed', 'urée': 'Urea'},
            unit_map={'kg': 'Kg', 'manquant': 'Unknown'}, crop_map={'mil': 'Mil'})


@pytest.fixture
def driver(monkeypatch):
    root = countries_root() / 'Niger'
    monkeypatch.syspath_prepend(str(root / '_'))
    monkeypatch.delitem(sys.modules, 'niger', raising=False)
    path = root / '2011-12/_/plot_inputs.py'
    spec = importlib.util.spec_from_file_location('_invented_niger1529', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # import must not execute source IO/main
    return module


def frames(prefix):
    numeric = pd.DataFrame({
        'hid': [703]*7,
        prefix+'02': [11, 12, 13, 1, 2, 3, 4],
        prefix+'03': pd.array([1, 1, 1, 1, 0, 2, pd.NA], dtype='Int64'),
        prefix+'04': [1, 1, np.nan, 999, 1, 1, 1],
        prefix+'05a': [0, 0, np.nan, 99, 123, 456, 789],
        prefix+'05b': [1, 1, np.nan, 99, 1, 1, 1],
        prefix+'07': [2, 2, 9, 1, 1, 1, 1],
        prefix+'08a': [np.nan, np.nan, np.nan, 99, 777, 888, 999],
        prefix+'08b': [50000, 50000, 50000, 50000, 1, 1, 1],
    }, index=[90, 50, 30, 10, 20, 40, 60])
    labeled = numeric.copy(deep=True)
    labeled[prefix+'02'] = ['semences', 'semences', 'semences', 'urée',
                             'unselected', 'unselected', 'unselected']
    labeled[prefix+'04'] = ['mil', 'mil', pd.NA, 'unmapped own crop', 'mil', 'mil', 'mil']
    labeled[prefix+'05b'] = ['kg', 'kg', pd.NA, '99', 'kg', 'kg', 'kg']
    return labeled, numeric


def build(driver, source, labeled, numeric):
    return driver._build_input_source(labeled, numeric, source_path=source, **MAPS)


def original_2c(driver, src, srcn):
    """Original bb8282C arithmetic, copied independently before new builder.

    No dry branch or candidate source dispatch. Reuses unchanged label/slot/
    finalizer helpers exactly as the original driver did, not source readers.
    """
    applied = srcn['as02cq03'] == 1
    src = src[applied.values]
    srcn = srcn[applied.values]
    hh = srcn['hid'].apply(lambda x: driver.niger_i(x) if pd.notna(x) else pd.NA)
    purchased = srcn['as02cq07'].map({1: True, 2: False})
    obs = driver._ecvma_input_observations(hh, srcn['as02cq02'], RAINY,
                                         script_path=driver.__file__)
    df = pd.DataFrame({
        'input_obs': obs.values, 'i': hh.values,
        'input': driver._input_labels(src['as02cq02'], MAPS['input_map']).values,
        'crop': driver._crop_labels(srcn['as02cq04'], src['as02cq04'], MAPS['crop_map']).values,
        'u': driver._unit_labels(src['as02cq05b'], MAPS['unit_map']).values,
        'Quantity': pd.to_numeric(srcn['as02cq05a'], errors='coerce').values,
        'Purchased': purchased.values,
        'Quantity_purchased': pd.to_numeric(srcn['as02cq08a'], errors='coerce').values,
    })
    return driver._finish_plot_inputs(df, '2011-12')


@pytest.mark.parametrize('empty', [False, True])
def test_original_2c_exact_order_masks_dtypes_and_immutability(driver, empty):
    labeled, numeric = frames('as02cq')
    if empty:
        numeric['as02cq03'] = 2
    before_l, before_n = labeled.copy(deep=True), numeric.copy(deep=True)
    expected = original_2c(driver, labeled, numeric)
    actual = build(driver, RAINY, labeled, numeric)
    assert_frame_equal(actual, expected, check_exact=True)
    assert actual.attrs == expected.attrs
    assert_frame_equal(labeled, before_l, check_exact=True)
    assert_frame_equal(numeric, before_n, check_exact=True)


def test_dry_own_gate_unit_money_zero_missing_and_literal99(driver):
    labeled, numeric = frames('as03cq')
    before_l, before_n = labeled.copy(deep=True), numeric.copy(deep=True)
    actual = build(driver, DRY, labeled, numeric)
    assert len(actual) == 4 and actual.index.is_unique
    assert actual.Quantity.tolist()[:2] == [0, 0]
    assert pd.isna(actual.Quantity.iloc[2]) and actual.Quantity.iloc[3] == 99
    assert actual.Quantity_purchased.iloc[3] == 99
    assert actual.Purchased.tolist()[:2] == [False, False]
    assert pd.isna(actual.Purchased.iloc[2]) and actual.Purchased.iloc[3]
    assert actual.index.get_level_values('u').tolist() == ['Kg', 'Kg', 'Unknown', '99']
    assert actual.index.get_level_values('crop').tolist() == ['Mil', 'Mil', '(not crop-specific)', 'unmapped own crop']
    assert actual.index.get_level_values('input').tolist() == ['Seed']*3+['Urea']
    assert actual.Quantity.dtype == pd.Float64Dtype()
    assert actual.Purchased.dtype == pd.BooleanDtype()
    assert 'Expenditure' not in actual.columns and 'Quantity_kg' not in actual.columns
    assert not any('50000' in str(x) for x in actual.index.get_level_values('u'))
    assert_frame_equal(labeled, before_l, check_exact=True)
    assert_frame_equal(numeric, before_n, check_exact=True)


def test_same_household_slot_and_equal_seeds_survive_across_sources(driver):
    a, an = frames('as02cq'); b, bn = frames('as03cq')
    rainy, dry = build(driver, RAINY, a, an), build(driver, DRY, b, bn)
    both = pd.concat([rainy, dry])
    assert len(both) == 8 and both.index.is_unique
    tokens = both.index.get_level_values('input_obs')
    assert all('ecvmaas2c_p1.dta' in t and ':as02cq02=' in t for t in tokens[:4])
    assert all('ecvmaas3c_p1.dta' in t and ':as03cq02=' in t for t in tokens[4:])
    assert tokens[0] != tokens[1] and tokens[0] != tokens[4]
    assert both.Quantity.iloc[0] == both.Quantity.iloc[1] == both.Quantity.iloc[4] == 0


@pytest.mark.parametrize('gate', [0, 2, pd.NA])
def test_positive_amounts_cannot_create_application(driver, gate):
    labeled, numeric = frames('as03cq')
    numeric['as03cq03'] = pd.array([gate]*7, dtype='Int64')
    numeric['as03cq05a'] = 100
    numeric['as03cq08a'] = 100
    assert len(build(driver, DRY, labeled, numeric)) == 0


@pytest.mark.parametrize('flag, expected', [(1, True), (2, False), (9, None), (pd.NA, None)])
def test_purchase_flag_is_not_inferred_from_amount(driver, flag, expected):
    labeled, numeric = frames('as03cq')
    numeric['as03cq07'] = pd.array([flag]*7, dtype='Int64')
    numeric['as03cq08a'] = 100
    actual = build(driver, DRY, labeled, numeric)
    if expected is None:
        assert actual.Purchased.isna().all()
    else:
        assert (actual.Purchased == expected).all()
    assert (actual.Quantity_purchased == 100).all()


@pytest.mark.parametrize('mutation', ['null-slot', 'fraction-slot', 'duplicate-slot', 'missing-household'])
def test_dry_invalid_native_observations_refuse(driver, mutation):
    labeled, numeric = frames('as03cq')
    if mutation == 'null-slot': numeric.loc[90, 'as03cq02'] = np.nan
    if mutation == 'fraction-slot': numeric.loc[90, 'as03cq02'] = 11.5
    if mutation == 'duplicate-slot': numeric.loc[50, 'as03cq02'] = 11
    if mutation == 'missing-household': numeric.loc[90, 'hid'] = np.nan
    with pytest.raises(ValueError, match='Niger input observations'):
        build(driver, DRY, labeled, numeric)


def test_source_prefix_is_own_and_rows_align(driver):
    labeled, numeric = frames('as03cq')
    with pytest.raises(KeyError): build(driver, RAINY, labeled, numeric)
    with pytest.raises(KeyError): build(driver, 'invented-other-source.dta', labeled, numeric)
    with pytest.raises(ValueError, match='align'):
        build(driver, DRY, labeled.iloc[::-1], numeric)


def test_main_uses_both_own_sources_once_and_preserves_2c_prefix(driver, monkeypatch):
    rainy, rn = frames('as02cq'); dry, dn = frames('as03cq')
    expected_rainy = original_2c(driver, rainy, rn)
    expected_dry = build(driver, DRY, dry, dn)
    reads, writes = [], []
    def invented_reader(path, convert_categoricals=True):
        reads.append((path, convert_categoricals))
        assert path in (RAINY, DRY)
        source = (rainy, rn) if path == RAINY else (dry, dn)
        return source[0 if convert_categoricals else 1].copy(deep=True)
    monkeypatch.setattr(driver, 'get_dataframe', invented_reader)
    monkeypatch.setattr(driver, 'to_parquet', lambda frame, path: writes.append((frame, path)))
    monkeypatch.setattr(driver, '_input_maps', lambda: (MAPS['input_map'], MAPS['unit_map'], {}))
    monkeypatch.setattr(driver, '_crop_maps', lambda: (MAPS['crop_map'], {}))
    driver.main()
    assert reads == [(RAINY, True), (RAINY, False), (DRY, True), (DRY, False)]
    assert len(writes) == 1 and writes[0][1] == 'plot_inputs.parquet'
    actual = writes[0][0]
    assert_frame_equal(actual.iloc[:len(expected_rainy)], expected_rainy, check_exact=True)
    assert_frame_equal(actual.iloc[len(expected_rainy):], expected_dry, check_exact=True)
    assert actual.index.is_unique
