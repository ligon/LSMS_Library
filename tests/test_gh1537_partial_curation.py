"""Bounded metadata/invented-input controls; no source or Country/Feature API.

Ledger 1537-partial-curation §§2–5. The default/crop boundary test injects the
metadata result to preserve the original guard's df_data_grabber ban; it does
not claim an end-to-end execution of that banned metadata cascade.
"""
from copy import deepcopy
import warnings

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from lsms_library import local_tools
from lsms_library.countries.Nigeria._ import nigeria
from lsms_library.paths import countries_root
from tests.test_gh1537_aggregate_diagnosis import (
    curation, prices, relabel, GH587_SEMOVITA_ROW, gh587_semovita, gh587_semovita_cell)
from lsms_library.country import LabelPassThroughError, LabelPassThroughWarning


ORG = countries_root() / 'Nigeria/_/categorical_mapping.org'
QUARTERS = ('2010Q3', '2011Q1', '2012Q3', '2013Q1',
            '2015Q3', '2016Q1', '2018Q3', '2019Q1')


def test_actual_parsers_only_add_two_label_rows(curation):
    # Both production Org readers, never an in-memory-only proposal.
    one = local_tools.df_from_orgfile(ORG, name='harmonize_food')
    all_tables = local_tools.all_dfs_from_orgfile(ORG)
    many = all_tables['harmonize_food']
    assert_frame_equal(one, many)
    # The two #1537 label rows, then GH #587's Semovita row (ruling
    # 2026-11-12, "Add a Semovita row"), whose change also names Semovita in
    # code 15's 2012Q3 cell -- the only other cell that differs from baseline.
    assert len(one) == len(curation) + 3
    assert list(one.columns) == list(curation.columns)
    # Empty numeric cells promote Code to float; every old numeric code is
    # exactly representable, and format_id restores its original spelling.
    pd.testing.assert_frame_equal(one.iloc[:-3].drop(columns='Code'),
                                  gh587_semovita_cell(curation).drop(columns='Code'))
    np.testing.assert_array_equal(one['Code'].iloc[:-3], curation['Code'])
    assert one['Code'].iloc[:-3].map(local_tools.format_id).tolist() == (
        curation['Code'].map(local_tools.format_id).tolist())
    semovita = one.iloc[-1:]
    assert semovita['Preferred Label'].tolist() == [GH587_SEMOVITA_ROW['Preferred Label']]
    assert semovita['Aggregate Label'].tolist() == [GH587_SEMOVITA_ROW['Aggregate Label']]
    assert semovita['Code'].isna().all()
    assert (semovita[list(QUARTERS)] == '').all().all()
    assert GH587_SEMOVITA_ROW['Aggregate Label'] in set(curation['Aggregate Label'])
    additions = one.iloc[-3:-1]
    assert additions['Preferred Label'].tolist() == ['Groundnuts', 'Maize (shelled)']
    assert additions['Aggregate Label'].tolist() == ['Groundnuts', 'Maize']
    assert additions['Code'].isna().all()
    assert additions['Code'].map(local_tools.format_id).isna().all()
    assert (additions[list(QUARTERS)] == '').all().all()
    for target in additions['Aggregate Label']:
        assert target in set(curation['Aggregate Label'])
    assert 'Pepper' not in set(one['Preferred Label'])
    assert 'Pepper' not in set(one['Aggregate Label'])


def test_actual_quarter_maps_warnings_and_missing_codes_equal_baseline(curation, monkeypatch):
    candidate = local_tools.df_from_orgfile(ORG, name='harmonize_food')
    observed = candidate['Code'].iloc[:-3].tolist() + [None, np.nan, pd.NA, '', 99999]
    # The baseline as GH #587 changed it (its one cell and its label row):
    # the #1537 rows must leave every quarter map exactly as that table gives it.
    baseline_587 = gh587_semovita(curation)
    for quarter in QUARTERS:
        with warnings.catch_warnings(record=True) as current_warnings:
            warnings.simplefilter('always')
            actual = nigeria.harmonized_food_labels(quarter, observed_codes=observed)
        with monkeypatch.context() as scoped:
            scoped.setattr(local_tools, 'df_from_orgfile', lambda *a, **k: baseline_587.copy())
            with warnings.catch_warnings(record=True) as prior_warnings:
                warnings.simplefilter('always')
                expected = nigeria.harmonized_food_labels(quarter, observed_codes=observed)
        with monkeypatch.context() as scoped:
            scoped.setattr(local_tools, 'df_from_orgfile', lambda *a, **k: curation.copy())
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                pure = nigeria.harmonized_food_labels(quarter, observed_codes=observed)
        # GH #587 moves exactly one code of one quarter: 2012Q3 code 15.
        moved = {k: v for k, v in actual.items() if pure.get(k) != v}
        assert set(pure) - set(actual) == set()
        assert moved == ({15: 'Semovita', '15': 'Semovita', '15.0': 'Semovita'}
                         if quarter == '2012Q3' else {})
        assert actual == expected
        assert [(w.category, str(w.message)) for w in current_warnings] == [
            (w.category, str(w.message)) for w in prior_warnings]
        raw = pd.Series([21, 43, 76, 99999, None, np.nan, pd.NA, '', '43.0'], dtype=object)
        pd.testing.assert_series_equal(raw.replace(actual), raw.replace(expected))
    for quarter in ('2023Q3', '2024Q1', 'not-a-quarter'):
        with pytest.raises(KeyError, match='no quarter codebook'):
            nigeria.harmonized_food_labels(quarter)


def test_default_crop_decoder_boundary_skips_actual_parser_blank_keys(curation, monkeypatch):
    # Real parser + actual format_id + unchanged noarg/crop decoder. Only the
    # forbidden metadata IO boundary is injected, not its numeric skip logic.
    candidate = local_tools.df_from_orgfile(ORG, name='harmonize_food')
    results = []
    for table in (curation, candidate):
        raw = pd.Series(table['Preferred Label'].to_numpy(),
                        index=table['Code'].map(local_tools.format_id)).to_dict()
        if table is candidate:
            assert any(pd.isna(key) for key in raw)  # parser null key is retained at this boundary
        with monkeypatch.context() as scoped:
            scoped.setattr(local_tools, 'get_categorical_mapping', lambda **k: raw)
            default = nigeria.harmonized_food_labels()
            assert nigeria._crop_labels() == default
            assert nigeria._crop_labels_from_anywhere() == default
        assert all(type(code) is int for code in default)
        assert None not in default
        results.append(default)
    assert results[0] == results[1]
    # Each possible parser/null-key scalar is skipped by the exact decoder,
    # preserving the source guard instead of invoking df_data_grabber.
    for blank in (None, np.nan, pd.NA, ''):
        with monkeypatch.context() as scoped:
            scoped.setattr(local_tools, 'get_categorical_mapping',
                           lambda **k: {**results[0], blank: 'Never a source code'})
            assert nigeria.harmonized_food_labels() == results[0]
    # Crop / community code-mode consumers map exactly these integer codes.
    codes = pd.Series([1080, 1060, 2140, 12, 21, 43, 76, 99999, None], dtype='Int64')
    pd.testing.assert_series_equal(codes.map(results[0]), codes.map(results[1]))


def test_actual_candidate_labels_preserve_price_grain_payload_and_pepper(curation, monkeypatch):
    candidate = local_tools.all_dfs_from_orgfile(ORG)['harmonize_food']
    raw = prices(['Groundnuts', 'Groundnuts (shelled)', 'Maize (shelled)',
                  'Maize (shelled, on the cob)', 'Pepper', None, 17, 'Uncurated'])
    raw['Quantity'] = [1., 2., 3., np.nan, 0., 5., 6., 7.]
    raw['Expenditure'] = [2., 4., 8., 16., np.nan, 0., 32., 64.]
    raw['Quantity_kg'] = [np.nan, 1., 2., 3., 4., 5., 6., 7.]
    before, table_before = deepcopy(raw), deepcopy(candidate)
    for labels in (None, 'Preferred', 'Preferred Label'):
        assert relabel(raw, {'harmonize_food': candidate}, labels) is raw
    with pytest.warns(LabelPassThroughWarning):
        result = relabel(raw, {'harmonize_food': candidate})
    assert result.index.get_level_values('j')[:5].tolist() == [
        'Groundnuts', 'Groundnuts', 'Maize', 'Maize', 'Pepper']
    assert len(result) == len(raw)
    assert result.index.duplicated(keep=False).sum() == 4
    assert_frame_equal(result.reset_index(drop=True), raw.reset_index(drop=True))
    for level in ('t', 'i', 'u', 's'):
        assert result.index.get_level_values(level).equals(raw.index.get_level_values(level))
    assert result.attrs['opaque'] == raw.attrs['opaque']
    assert result.attrs['id_converted'] is True
    assert result.attrs['labels_unmatched']['examples'] == ['17', 'Pepper', 'Uncurated']
    with monkeypatch.context() as scoped:
        scoped.setenv('LSMS_LABELS_STRICT', '1')
        with pytest.raises(LabelPassThroughError, match='Pepper'):
            relabel(prices(['Pepper']), {'harmonize_food': candidate})
        # The two covered names no longer produce a strict coverage refusal.
        relabel(prices(['Groundnuts', 'Maize (shelled)']), {'harmonize_food': candidate})
    assert_frame_equal(raw, before)
    assert raw.attrs == before.attrs
    assert_frame_equal(candidate, table_before)


def test_other_label_routes_do_not_gain_blank_target_mappings(curation):
    candidate = local_tools.df_from_orgfile(ORG, name='harmonize_food')
    frame = prices(['Groundnuts', 'Maize (shelled)', 'Pepper', 'Rice--local', None])
    for labels in (*QUARTERS, 'Original', 'Code'):
        outcomes = []
        for table in (curation, candidate):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                try:
                    result = relabel(frame, {'harmonize_food': table}, labels)
                except KeyError as error:
                    outcomes.append(type(error))
                else:
                    outcomes.append(result)
            if table is curation:
                prior_warnings = [(w.category, str(w.message)) for w in caught]
            else:
                assert [(w.category, str(w.message)) for w in caught] == prior_warnings
        if isinstance(outcomes[0], type):
            assert outcomes[0] is outcomes[1]
        else:
            assert_frame_equal(*outcomes)
            assert outcomes[0].attrs == outcomes[1].attrs
