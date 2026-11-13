"""Invented PP report controls; no native/reader/API execution is involved.

These exercise the six own layouts and the pending pure projection, not five
served cells. Explicit toy maps below assert identity mechanics only; they do
not curate any real item/unit or authorize the pending Price representation.
"""
from copy import deepcopy
import hashlib
from pathlib import Path
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.countries.Nigeria._ import nigeria
from lsms_library.countries.Nigeria._ import pp_community_prices as pp
from tests.test_gh1196_parquet_index_policy import _exact

SOURCES = ('W1', 'W2', 'W3-first', 'W3-second', 'W4', 'W5')
TIMES = ('2010Q3', '2012Q3', '2015Q3', '2015Q3', '2018Q3', '2023Q3')
MD5S = (
    '623e90a35db141717c0ef50c7dbd3243', 'fb48f2180b1355c44edd1771e0ce1b41',
    '40a7128bf55d65a7eb29d0e2e4ccabd1', '75e1ac76e51e548e1eb414729c22df6c',
    'cdf4cf71f59d7e49a1df072d49609755', '26bb1bff326650f78bd77136b50edb90',
)


def _raw(source_key):
    spec = pp.pp_source(source_key)
    values = {}
    for column in spec['variable_order']:
        storage = spec['storage_types'][column]
        values[column] = (pd.Series(['', 'native text'], dtype='string')
                          if storage.isdigit() else
                          pd.Series([1., 1.], dtype='float64'))
    raw = pd.DataFrame(values)
    raw['state'] = pd.Series([1, 1], dtype='int8')
    raw['lga'] = pd.Series([101, 101], dtype='int16')
    raw['ea'] = pd.Series([2, 1], dtype='int16')
    raw['item_cd'] = pd.Series([10, 11], dtype='int16')
    operands = spec['operand_columns']
    raw[operands['availability']] = pd.Series([1., np.nan])
    raw[operands['price']] = pd.Series([75., -3.])
    raw[operands['unit']] = pd.Series([1., 1.])
    if 'quantity' in operands:
        raw[operands['quantity']] = pd.Series([2.5, np.nan])
    if 'factor' in operands:
        raw[operands['factor']] = pd.Series([.35, np.nan], dtype='float32')
    if 'size' in operands:
        raw[operands['size']] = pd.Series([0., 99.])
    if 'other' in operands:
        raw[operands['other']] = pd.Series(['300gram', ''], dtype='string')
    raw.index = pd.Index([17, 4], name='native_record')
    raw['UninterpretedSourceField'] = pd.array(['keep', None], dtype='string')
    raw.attrs = {'native': {'version': ('invented', 1), 'flag': True}}
    return raw


def _toy_maps(raw, source_key):
    unit = pp.pp_source(source_key)['operand_columns']['unit']
    return ({int(code): f'Toy item {int(code)}' for code in raw.item_cd},
            {int(code): f'Toy unit {int(code)}' for code in raw[unit].dropna()})


@pytest.mark.parametrize('source_key,t,md5', zip(SOURCES, TIMES, MD5S))
def test_six_native_layouts_are_reversible_without_filter_or_normalization(source_key, t, md5):
    raw = _raw(source_key)
    before = deepcopy(raw)
    report = pp.pp_community_price_report(raw, source_key)
    _exact(raw, before)
    _exact(report.native_frame(), before)
    assert report.frame.columns[:len(raw.columns)].tolist() == raw.columns.tolist()
    assert report.frame.index.tolist() == [17, 4]
    assert report.frame['t'].tolist() == [t, t]
    assert report.frame['v'].tolist() == ['1/101/2', '1/101/1']
    assert report.frame['obs'].tolist() == ([2, 2] if source_key == 'W3-second' else [1, 1])
    assert report.frame['ReportedPrice'].tolist() == [75., -3.]
    assert pd.isna(report.frame['NativeAvailabilityCode'].iloc[1])
    assert 'Price' not in report.frame  # raw Naira is not standardized Price
    assert report.source['source_md5'] == md5
    assert report.source['source_bytes'] > 0
    # Metadata-only binding, never open a native file.
    root = Path(pp.__file__).parents[1]
    sidecar = root / (report.source['source_path'] + '.dvc')
    assert hashlib.sha256(sidecar.read_bytes()).hexdigest() == report.source['source_sidecar_sha256']
    # Caller mutations cannot retrospectively alter the copied report/attrs.
    raw.attrs['native']['flag'] = False
    _exact(report.native_frame(), before)


@pytest.mark.parametrize('source_key', SOURCES)
def test_explicit_pending_projection_keeps_native_reports_and_price_missing(source_key):
    raw = _raw(source_key)
    report = pp.pp_community_price_report(raw, source_key)
    items, units = _toy_maps(raw, source_key)
    out = pp.pp_community_price_projection(report, item_map=items, unit_map=units)
    assert tuple(out.index.names) == ('t', 'v', 'j', 'u', 'variety', 'obs')
    assert len(out) == len(raw) and out.index.is_unique
    assert out.index.get_level_values('v').tolist() == ['1/101/2', '1/101/1']
    assert out['ReportedPrice'].tolist() == [75., -3.]
    assert out['Price'].isna().all()
    assert str(out.Price.dtype) == 'Float64'
    assert out.attrs == raw.attrs
    assert out.NativeItemCode.tolist() == raw.item_cd.tolist()
    if source_key in ('W1', 'W2'):
        assert out.index.get_level_values('variety').tolist() == ['10', '11']
    _exact(report.native_frame(), raw)


@pytest.mark.parametrize('source_key,quantity_alias', [('W1', 'ReportedNumberOfUnits'), ('W5', 'ReportedQuantity')])
def test_quantity_factor_and_missing_denominators_remain_distinct(source_key, quantity_alias):
    raw = _raw(source_key)
    report = pp.pp_community_price_report(raw, source_key)
    assert report.frame[quantity_alias].iloc[0] == 2.5
    assert pd.isna(report.frame[quantity_alias].iloc[1])
    other = 'ReportedQuantity' if source_key == 'W1' else 'ReportedNumberOfUnits'
    assert report.frame[other].isna().all()
    if source_key == 'W5':
        assert report.frame['ReportedConversionFactor'].iloc[0] == raw.c2q2_cvn.iloc[0]
        assert pd.isna(report.frame['ReportedConversionFactor'].iloc[1])
    else:
        assert report.frame['ReportedConversionFactor'].isna().all()
    assert report.frame.ReportedPrice.tolist() == [75., -3.]  # no division
    _exact(report.native_frame(), raw)


def test_w3_print_card_is_not_used_to_fill_native_unlabelled_units_or_other_unit_size():
    raw = _raw('W3-first')
    raw['c2q2'] = [31., 22.]  # own native31 unlabelled, native22 labelled
    report = pp.pp_community_price_report(raw, 'W3-first')
    assert report.frame.NativeUnitCode.tolist() == [31., 22.]
    assert pd.isna(report.frame.NativeUnitLabel.iloc[0])
    assert report.frame.NativeUnitLabel.iloc[1] == '22. MUDU LARGE'
    assert report.frame.ReportedOtherUnit.tolist() == ['300gram', '']
    assert report.frame.NativeSizeCode.isna().all()
    assert report.frame.NativeSizeLabel.isna().all()
    assert len(report.frame) == 2  # not the PH metric-size deletion route
    _exact(report.native_frame(), raw)


def test_w3_equal_prices_in_two_native_slots_are_two_observations():
    parts = []
    for source in ['W3-first', 'W3-second']:
        raw = _raw(source).iloc[:1].copy()
        raw['c2q3'] = 50.
        report = pp.pp_community_price_report(raw, source)
        items, units = _toy_maps(raw, source)
        parts.append(pp.pp_community_price_projection(report, item_map=items, unit_map=units))
    out = pd.concat(parts)
    assert out.index.is_unique and len(out) == 2
    assert out.index.get_level_values('obs').tolist() == [1, 2]
    assert out.ReportedPrice.tolist() == [50., 50.]
    assert out.Price.isna().all()


@pytest.mark.parametrize('key', ['state', 'lga', 'ea', 'item_cd'])
@pytest.mark.parametrize('bad', [np.nan, .25, np.inf, 'not a native code', True])
def test_missing_nonintegral_or_unusable_native_keys_refuse_without_mutation(key, bad):
    raw = _raw('W3-first')
    raw[key] = raw[key].astype('object')
    raw.loc[17, key] = bad
    before = deepcopy(raw)
    with pytest.raises(ValueError, match='native keys'):
        pp.pp_community_price_report(raw, 'W3-first')
    _exact(raw, before)


def test_unresolved_moved_ea_does_not_create_a_fake_sampling_cluster():
    raw = _raw('W3-first')
    raw['ea'] = 0
    with pytest.raises(ValueError, match='sampling-cluster'):
        pp.pp_community_price_report(raw, 'W3-first')


def test_repeated_native_key_is_fatal_even_for_equal_prices():
    raw = _raw('W3-first')
    for key in ['state', 'lga', 'ea', 'item_cd']:
        raw[key] = [raw[key].iloc[0]] * 2
    raw.c2q3 = 50.
    before = deepcopy(raw)
    with pytest.raises(ValueError, match='repeats a complete native key'):
        pp.pp_community_price_report(raw, 'W3-first')
    _exact(raw, before)


def test_mapping_collision_is_not_an_aggregation_or_first_kept_choice():
    raw = _raw('W3-first')
    raw['ea'] = 1
    report = pp.pp_community_price_report(raw, 'W3-first')
    with pytest.raises(ValueError, match='collides'):
        pp.pp_community_price_projection(report, item_map={10: 'Same', 11: 'Same'}, unit_map={1: 'Same unit'})
    _exact(report.native_frame(), raw)


def test_w1_native_variety_prevents_a_false_mapping_collapse():
    raw = _raw('W1')
    raw['ea'] = 1
    report = pp.pp_community_price_report(raw, 'W1')
    out = pp.pp_community_price_projection(report, item_map={10: 'Same', 11: 'Same'}, unit_map={1: 'Same unit'})
    assert len(out) == 2 and out.index.is_unique
    assert out.index.get_level_values('variety').tolist() == ['10', '11']


@pytest.mark.parametrize('maps', [({}, {1: 'Known'}), ({10: 'Known', 11: 'Known'}, {})])
def test_missing_canonical_label_authority_cannot_silently_drop_raw_reports(maps):
    raw = _raw('W3-first')
    report = pp.pp_community_price_report(raw, 'W3-first')
    with pytest.raises(ValueError, match='unresolved'):
        pp.pp_community_price_projection(report, item_map=maps[0], unit_map=maps[1])
    _exact(report.native_frame(), raw)


def test_missing_unit_is_preserved_raw_but_public_identity_needs_explicit_disposition():
    raw = _raw('W5')
    raw.loc[17, 'c2q2b'] = np.nan
    report = pp.pp_community_price_report(raw, 'W5')
    assert pd.isna(report.frame.NativeUnitCode.iloc[0])
    with pytest.raises(ValueError, match='unresolved'):
        pp.pp_community_price_projection(report, item_map={10: 'Toy', 11: 'Other toy'}, unit_map={1: 'Toy unit'})
    _exact(report.native_frame(), raw)


def test_alias_collision_is_not_allowed_to_overwrite_a_native_field():
    raw = _raw('W1')
    raw['ReportedPrice'] = 999.
    before = deepcopy(raw)
    with pytest.raises(ValueError, match='overwrite native'):
        pp.pp_community_price_report(raw, 'W1')
    _exact(raw, before)


def test_own_source_cards_keep_w1_w2_and_w3_semantics_separate():
    def labels(source, operand):
        spec = pp.pp_source(source)
        col = spec['operand_columns'][operand]
        return {r['code']: r['label'] for r in spec['report_value_label_sets'][spec['variable_value_label_associations'][col]]}
    assert labels('W1', 'unit')[101] == 'kilograms'
    assert labels('W2', 'unit')[5] == 'mudu'
    assert labels('W3-first', 'unit')[22] == '22. MUDU LARGE'
    assert 31 not in labels('W3-first', 'unit')
    assert 42 not in labels('W3-second', 'unit')
    assert 'size' not in pp.pp_source('W3-first')['operand_columns']
    assert pp.pp_source('W5')['operand_columns']['quantity'] == 'c2q2a'
    assert pp.pp_source('W5')['operand_columns']['factor'] == 'c2q2_cvn'


def test_existing_ph_builder_retains_its_separate_metric_size_policy():
    # GH #587 (ruling 2026-11-12, "Measure PH, then align"; measured 2026-11-13):
    # PH W3's blank-size Kg / l price per unit, so they are now served as PP's
    # are.  The PH policy that remains separate is g / cl with no size: dropped.
    raw = pd.DataFrame({'state': [1, 1], 'lga': [101, 101], 'ea': [1, 2],
                        'item_cd': [10, 10], 'c8q2': [1, 2], 'c8q2b': ['', ''],
                        'c8q3': [50., 60.]})
    decoded = raw.copy()
    decoded['c8q2'] = ['1. KILOGRAMS (KG)', '2. GRAMS (G)']
    before = deepcopy(raw)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        out = nigeria.community_prices_for_wave('2016Q1', [dict(
            df=raw, dec=decoded, ea='ea', item='item_cd', unit='c8q2',
            size_text='c8q2b', price='c8q3', obs=1)], 'codes', {10: 'Toy item'})
    assert out.index.get_level_values('u').tolist() == ['Kg']   # g, no size: dropped
    assert out['Price'].tolist() == [50.]
    _exact(raw, before)


def test_no_unknown_and_missing_availability_or_price_never_select_away_raw_reports():
    one = _raw('W4').iloc[:1].copy()
    raw = pd.concat([one.copy(deep=True) for _ in range(4)])
    raw['ea'] = pd.array([1, 2, 3, 4], dtype='int16')
    raw['c2q1'] = [1., 2., 9., np.nan]
    raw['c2q3'] = [50., np.nan, -2., 0.]
    before = deepcopy(raw)
    report = pp.pp_community_price_report(raw, 'W4')
    _exact(report.native_frame(), before)
    assert len(report.frame) == 4
    assert report.frame.NativeAvailabilityCode.iloc[:3].tolist() == [1., 2., 9.]
    assert pd.isna(report.frame.NativeAvailabilityCode.iloc[3])
    assert pd.isna(report.frame.NativeAvailabilityLabel.iloc[2])
    assert pd.isna(report.frame.NativeAvailabilityLabel.iloc[3])
    assert report.frame.ReportedPrice.iloc[[0, 2, 3]].tolist() == [50., -2., 0.]
    assert pd.isna(report.frame.ReportedPrice.iloc[1])
    _exact(raw, before)
