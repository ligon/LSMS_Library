"""Source-free mechanism controls for ledger1537; no production recuration.

Use an invented metadata provider with the unbound relabel method. The two
proposed label-only rows exist ONLY in memory. These controls establish no
survey population, source truth, real Country/Feature API or cache acceptance.
"""
import ast
import hashlib
import os
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from lsms_library.country import (
    Country, LabelPassThroughError, LabelPassThroughWarning,
    merge_label_pass_through_reports,
)
from lsms_library.errors import LabelUnavailableError
from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.countries.Nigeria._ import nigeria
from lsms_library.transformations import food_prices_from_acquired


@pytest.fixture
def curation():
    # ROOT 2026-11-08: historical diagnosis assertions bind the immutable
    # pre-curation artifact; candidate tests read the live Org separately.
    path = Path(__file__).resolve().parent / 'fixtures/gh1537/baseline-categorical_mapping.org'
    assert hashlib.sha256(path.read_bytes()).hexdigest() == (
        '1ec375a0939d1bebc0398935580eba931bb798189a9940d31be1a29f34c41f29')
    return df_from_orgfile(path, name='harmonize_food')


def provider(table):
    return SimpleNamespace(name='InventedNigeriaRoute', categorical_mapping=table)


def relabel(frame, table, labels='Aggregate'):
    # Reuse the finished-frame route, ledger1537 §2; never construct Country.
    return Country._relabel_j(provider(table), frame, labels, reaggregate=False)


def prices(items):
    index = pd.MultiIndex.from_arrays([
        ['T'] * len(items), ['H'] * len(items), items,
        ['kg'] * len(items), ['purchased'] * len(items),
    ], names=['t', 'i', 'j', 'u', 's'])
    out = pd.DataFrame({'Price': np.arange(len(items), dtype=float) + 2.5}, index=index)
    out.attrs = {'id_converted': True, 'opaque': {'keep': [1, 2]}}
    return out


def proposal(table):
    # An unapproved option (§5), not a production requirement or new decoder.
    additions = pd.DataFrame([
        {'Preferred Label': 'Groundnuts', 'Aggregate Label': 'Groundnuts'},
        {'Preferred Label': 'Maize (shelled)', 'Aggregate Label': 'Maize'},
    ]).reindex(columns=table.columns, fill_value='')
    return pd.concat([table, additions], ignore_index=True)


@pytest.mark.parametrize('labels', [None, 'Preferred', 'Preferred Label'])
def test_default_is_identity_even_with_proposed_rows(curation, labels):
    frame = prices(['Groundnuts', 'Maize (shelled)', 'Pepper', None])
    assert relabel(frame, {'harmonize_food': curation}, labels) is frame
    assert relabel(frame, {'harmonize_food': proposal(curation)}, labels) is frame


def test_committed_quarter_decoding_has_no_new_specificity(curation):
    expected = {
        '2010Q3': {43: 'Groundnuts', 76: 'Pepper'},
        '2011Q1': {43: 'Groundnuts', 76: 'Pepper'},
        '2012Q3': {21: 'Maize (shelled)', 43: 'Groundnuts', 76: 'Pepper'},
        '2013Q1': {21: 'Maize (shelled)', 43: 'Groundnuts (unshelled)',
                   76: 'Fresh Pepper'},
    }
    for quarter, facts in expected.items():
        decoded = nigeria.harmonized_food_labels(quarter)
        for code, native in facts.items():
            assert decoded[code] == native
    assert 'Original Label' not in curation.columns
    assert not curation['Preferred Label'].isin(
        ['Groundnuts', 'Maize (shelled)', 'Pepper']).any()
    assert 'Pepper' not in set(curation['Aggregate Label'])


def test_current_passthrough_is_disclosed_without_summing(curation):
    frame = prices(['Groundnuts', 'Maize (shelled)', 'Pepper', 'Rice--local'])
    before, table_before = deepcopy(frame), deepcopy(curation)
    with pytest.warns(LabelPassThroughWarning, match='UNRENAMED'):
        result = relabel(frame, {'harmonize_food': curation})
    assert result.index.get_level_values('j').tolist() == [
        'Groundnuts', 'Maize (shelled)', 'Pepper', 'Rice']
    assert result['Price'].tolist() == frame['Price'].tolist()
    assert result.attrs['labels_unmatched'] == {
        'labels': 'Aggregate', 'column': 'Aggregate Label',
        'n_unmatched': 3, 'n_values': 4, 'rows': 3, 'row_share': .75,
        'examples': ['Groundnuts', 'Maize (shelled)', 'Pepper'],
    }
    assert_frame_equal(frame, before)
    assert frame.attrs == before.attrs
    assert_frame_equal(curation, table_before)


def test_proposed_partial_rows_preserve_pepper_and_unknown_warning(curation):
    frame = prices(['Groundnuts', 'Maize (shelled)', 'Pepper', 'Uncurated item'])
    with pytest.warns(LabelPassThroughWarning):
        result = relabel(frame, {'harmonize_food': proposal(curation)})
    assert result.index.get_level_values('j').tolist() == [
        'Groundnuts', 'Maize', 'Pepper', 'Uncurated item']
    assert result['Price'].tolist() == frame['Price'].tolist()
    assert result.attrs['labels_unmatched']['examples'] == ['Pepper', 'Uncurated item']
    assert result.attrs['labels_unmatched']['rows'] == 2


def test_proposed_rows_leave_complete_quarter_decoders_unchanged(curation, monkeypatch):
    before = {t: nigeria.harmonized_food_labels(t) for t in
              ['2010Q3', '2011Q1', '2012Q3', '2013Q1',
               '2015Q3', '2016Q1', '2018Q3', '2019Q1']}
    from lsms_library import local_tools
    proposed = proposal(curation)
    monkeypatch.setattr(local_tools, 'df_from_orgfile', lambda *a, **k: proposed.copy())
    for t, expected in before.items():
        assert nigeria.harmonized_food_labels(t) == expected


@pytest.mark.parametrize('items', [
    ['Maize (shelled)', 'Maize (shelled, on the cob)', 'Maize'],
    ['Groundnuts', 'Groundnuts (shelled)', 'Groundnuts (unshelled)'],
])
def test_partial_option_keeps_colliding_full_price_keys_and_values(curation, items):
    frame = prices(items)
    result = relabel(frame, {'harmonize_food': proposal(curation)})
    assert result.index.nunique() == 1
    assert len(result) == 3
    assert result['Price'].tolist() == frame['Price'].tolist()
    # No first/median/sum is introduced at the Aggregate rename step.
    assert result['Price'].tolist() == [2.5, 3.5, 4.5]


@pytest.mark.parametrize('kind', ['object', 'string', 'category'])
def test_typed_missing_and_numeric_j_pass_through_unchanged(kind):
    frame = prices(pd.Series(['Fine', 'unknown', None, 17], dtype=kind))
    table = pd.DataFrame({'Preferred Label': ['Fine'], 'Aggregate Label': ['Coarse']})
    before = deepcopy(frame)
    with pytest.warns(LabelPassThroughWarning):
        result = relabel(frame, {'harmonize_food': table})
    j = result.index.get_level_values('j')
    assert j[0] == 'Coarse'
    assert j[1] == before.index.get_level_values('j')[1]
    assert pd.isna(j[2])
    assert j[3] == before.index.get_level_values('j')[3]
    assert result.attrs['labels_unmatched']['rows'] == 2
    assert result.attrs['labels_unmatched']['n_values'] == 3
    pd.testing.assert_series_equal(result.reset_index(drop=True)['Price'],
                                   frame.reset_index(drop=True)['Price'])
    assert_frame_equal(frame, before)


@pytest.mark.parametrize('maps', [
    {}, {'harmonize_food': pd.DataFrame({'Preferred Label': ['Fine']})},
])
def test_missing_curation_raises_contract_b_error(maps):
    with pytest.raises(LabelUnavailableError):
        relabel(prices(['Fine']), maps)


@pytest.mark.parametrize('table,frame', [
    (pd.DataFrame({'Aggregate Label': ['Coarse']}), prices(['Fine'])),
    (pd.DataFrame({'Preferred Label': ['Fine'], 'Aggregate Label': ['Coarse']}),
     pd.DataFrame({'Price': [1.]}, index=pd.Index(['H'], name='i'))),
])
def test_malformed_table_and_no_j_are_plain_keyerror(table, frame):
    with pytest.raises(KeyError) as caught:
        relabel(frame, {'harmonize_food': table})
    assert type(caught.value) is KeyError


@pytest.mark.parametrize('target', [
    pd.Series([1], dtype='Int64'), pd.Series([True], dtype='boolean'),
])
def test_numeric_target_is_unavailable(target):
    table = pd.DataFrame({'Preferred Label': ['Fine'], 'Aggregate Label': target})
    with pytest.raises(LabelUnavailableError, match='NUMERIC'):
        relabel(prices(['Fine']), {'harmonize_food': table})


def test_blank_curation_does_not_make_an_unnamed_bucket():
    table = pd.DataFrame({'Preferred Label': ['Fine', 'Blank', '', None],
                          'Aggregate Label': ['Coarse', ' ', 'Wrong', 'Wrong']})
    with pytest.warns(LabelPassThroughWarning):
        result = relabel(prices(['Fine', 'Blank', '', None]), {'harmonize_food': table})
    j = result.index.get_level_values('j')
    assert j[:3].tolist() == ['Coarse', 'Blank', '']
    assert pd.isna(j[3])


def test_strict_passthrough_refuses_before_mutation(curation, monkeypatch):
    monkeypatch.setenv('LSMS_LABELS_STRICT', '1')
    frame = prices(['Pepper'])
    before = deepcopy(frame)
    with pytest.raises(LabelPassThroughError):
        relabel(frame, {'harmonize_food': proposal(curation)})
    assert_frame_equal(frame, before)


def test_price_transform_precedes_relabel_and_preserves_native_units(curation):
    frame = prices(['Maize (shelled)', 'Maize (shelled, on the cob)', 'Pepper'])
    frame = frame.rename(index={'kg': 'invented-unit'}, level='u')
    raw = frame.assign(Quantity=[2., 4., 8.], Expenditure=[18., 28., 40.])
    before = deepcopy(raw)
    native = food_prices_from_acquired(raw, units='unitvalue')
    with pytest.warns(LabelPassThroughWarning):
        result = relabel(native, {'harmonize_food': proposal(curation)})
    assert sorted(result['Price']) == [5., 7., 9.]
    assert len(result) == 3
    assert result.index.get_level_values('u').tolist() == ['invented-unit'] * 3
    assert_frame_equal(raw, before)


def test_relabel_keeps_every_other_full_grain_coordinate(curation):
    frame = prices(['Maize (shelled)'] * 4).reset_index()
    frame['i'] = ['H', 'H', 'H', 'H2']
    frame['u'] = ['kg', 'invented-unit', 'kg', 'kg']
    frame['s'] = ['purchased', 'purchased', 'produced', 'purchased']
    frame = frame.set_index(['t', 'i', 'j', 'u', 's'])
    before = deepcopy(frame)
    result = relabel(frame, {'harmonize_food': proposal(curation)})
    assert result.index.is_unique
    for level in ['t', 'i', 'u', 's']:
        assert result.index.get_level_values(level).equals(frame.index.get_level_values(level))
    pd.testing.assert_series_equal(result.reset_index(drop=True)['Price'],
                                   frame.reset_index(drop=True)['Price'])
    assert_frame_equal(frame, before)


def test_feature_contract_b_exact_nested_marker_with_invented_frame():
    # Execute only the exact metadata marker helper, not Feature.__call__.
    # The caller's exception discrimination is inspected separately below.
    import warnings
    import lsms_library.feature as feature_module
    tree = ast.parse(Path(feature_module.__file__).read_text())
    marker = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == '_mark_labels_unavailable')
    ns = {'pd': pd, 'warnings': warnings,
          'labels_unavailable': ['Uncurated'], 'kwargs': {'labels': 'Aggregate'},
          'self': SimpleNamespace(table_name='food_prices')}
    exec(compile(ast.Module(body=[marker], type_ignores=[]), '<exact-Feature-marker>', 'exec'), ns)
    frame = prices(['Maize'])
    with pytest.warns(UserWarning, match='dropped from the assembly'):
        result = ns['_mark_labels_unavailable'](frame, 1)
    assert result is frame
    assert result.attrs['labels_unavailable'] == ['Uncurated']
    handlers = [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]
    unavailable = [h for h in handlers if isinstance(h.type, ast.Name)
                   and h.type.id == 'LabelUnavailableError']
    assert len(unavailable) == 1
    assert not any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                   and n.func.id == 'Country' for n in ast.walk(marker))
    assert not any(isinstance(h.type, ast.Name) and h.type.id == 'KeyError'
                   for h in handlers)


def test_feature_report_carrier_only_uses_supplied_country_reports():
    report = {'labels': 'Aggregate', 'rows': 2, 'examples': ['Pepper']}
    sources = [{'labels_unmatched': {'Invented': report}}, {}]
    before = deepcopy(sources)
    assert merge_label_pass_through_reports(sources) == {'Invented': report}
    assert sources == before
