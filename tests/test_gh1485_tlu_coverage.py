"""GH1485 Option A: disclose missing factors; retain the existing TLU estimate.

All operands are invented. Ledger1485 §§2–5; no new species equivalence.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import transformations as T


def _herd(animals, heads, axis='index', dtype='Float64', head_col='HeadCount'):
    frame = pd.DataFrame({'t': ['invented-wave'] * len(animals),
                          'i': ['invented-household'] * len(animals),
                          'animal': animals, head_col: pd.Series(heads, dtype=dtype)})
    return frame.set_index(['t', 'i', 'animal'] if axis == 'index' else ['t', 'i'])


def _existing_reference(frame, factors=None, head_col='HeadCount'):
    # Independent named-key arithmetic, preserving actual legacy output groups.
    mapping = T._TLU_FACTORS if factors is None else factors
    flat = frame.reset_index()
    flat['_weight'] = flat['animal'].astype(str).str.lower().map(mapping).astype(float)
    flat['_amount'] = pd.to_numeric(flat[head_col], errors='coerce') * flat['_weight']
    return flat.loc[flat['_amount'].notna()].groupby(['t', 'i'])['_amount'].sum().to_frame('TLU')


def _run(frame, **kwargs):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = T.tlu(frame, **kwargs)
    return out, caught


@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('dtype', ['Float64', 'Int64', 'object'])
def test_unsupported_sign_and_missing_partitions_are_counted_before_reduction(axis, dtype):
    x = _herd(['Chicken', 'Cow', 'Cow', 'Cow', 'Cow', 'Local Hen'],
              [10, 3, 0, pd.NA, -2, 4], axis, dtype)
    x.attrs['provenance'] = {'native': 'invented'}
    before = x.copy(deep=True)
    out, caught = _run(x)
    pd.testing.assert_frame_equal(out, _existing_reference(x))
    tally = out.attrs['tlu_species_coverage']
    assert tally == {
        'head_col': 'HeadCount', 'input_rows': 6, 'supported_rows': 1,
        'unsupported_rows': 5,
        'unsupported_labels': {
            'Cow': {'rows': 4, 'positive_rows': 1, 'zero_rows': 1,
                    'missing_rows': 1, 'negative_rows': 1},
            'Local Hen': {'rows': 1, 'positive_rows': 1, 'zero_rows': 0,
                          'missing_rows': 0, 'negative_rows': 0}}}
    coverage = [w for w in caught if issubclass(w.category, T.TLUCoverageWarning)]
    assert len(coverage) == 1
    text = str(coverage[0].message)
    assert 'omitted contributions' in text and '2' in text
    assert 'Cow' in text and 'Local Hen' in text and "attrs['tlu_species_coverage']" in text
    pd.testing.assert_frame_equal(x, before)
    assert x.attrs == before.attrs


@pytest.mark.parametrize('heads', [[0], [pd.NA], [-3], [0, pd.NA, -3]])
def test_unsupported_nonpositive_or_missing_rows_have_coverage_without_warning(heads):
    x = _herd(['Cow'] * len(heads), heads)
    out, caught = _run(x)
    assert out.empty
    pd.testing.assert_frame_equal(out, _existing_reference(x))
    assert not caught
    counts = out.attrs['tlu_species_coverage']['unsupported_labels']['Cow']
    assert counts['rows'] == len(heads)
    assert counts['positive_rows'] == 0
    assert sum(counts[k] for k in ['positive_rows', 'zero_rows', 'missing_rows', 'negative_rows']) == len(heads)


def test_unsupported_positive_only_household_remains_absent_and_loud():
    out, caught = _run(_herd(['Cow'], [5]))
    assert out.empty
    assert len(caught) == 1 and issubclass(caught[0].category, T.TLUCoverageWarning)
    assert out.attrs['tlu_species_coverage']['unsupported_rows'] == 1


@pytest.mark.parametrize('axis', ['index', 'column'])
@pytest.mark.parametrize('head_col', ['HeadCount', 'HeadSold'])
def test_custom_factors_and_head_column_reuse_existing_lookup(axis, head_col):
    x = _herd(['Cow', 'Chicken', 'Local Hen'], [2, 10, 4], axis, head_col=head_col)
    factors = {'cow': .7, 'chicken': .01, 'local hen': .005}
    before = factors.copy()
    out, caught = _run(x, tlu_factors=factors, head_col=head_col)
    pd.testing.assert_frame_equal(out, _existing_reference(x, factors, head_col))
    assert not caught and factors == before
    assert out.attrs['tlu_species_coverage']['unsupported_labels'] == {}
    assert out.attrs['tlu_species_coverage']['head_col'] == head_col


@pytest.mark.parametrize('factor,unsupported,warns', [(0., False, False), (None, True, True),
                                                    (np.nan, True, True)])
def test_custom_zero_is_supported_while_missing_factor_is_unsupported(factor, unsupported, warns):
    out, caught = _run(_herd(['Cow'], [3]), tlu_factors={'cow': factor})
    assert bool(out.empty) == unsupported
    if not unsupported:
        assert out.TLU.tolist() == [0.]
    assert out.attrs['tlu_species_coverage']['unsupported_rows'] == int(unsupported)
    assert bool(caught) == warns


def test_raw_label_counts_do_not_invent_species_normalization():
    x = _herd(['Cow', 'cow', ' cow ', 'CHICKEN'], [1, 2, 3, 10])
    out, caught = _run(x)
    assert out.TLU.tolist() == [.1]
    assert set(out.attrs['tlu_species_coverage']['unsupported_labels']) == {'Cow', 'cow', ' cow '}
    assert len(caught) == 1


def test_numeric_coercion_preserves_nonnumeric_missing_count():
    x = _herd(['Cow', 'Cow', 'Chicken'], ['not a number', None, '10'], dtype='object')
    out, caught = _run(x)
    assert not caught and out.TLU.tolist() == [.1]
    assert out.attrs['tlu_species_coverage']['unsupported_labels']['Cow']['missing_rows'] == 2


def test_existing_index_animal_precedence_is_preserved():
    x = _herd(['Chicken'], [10]); x['animal'] = 'Cow'
    out, caught = _run(x)
    assert not caught and out.TLU.tolist() == [.1]
    assert out.attrs['tlu_species_coverage']['supported_rows'] == 1


def test_unsupported_labels_with_duplicate_index_count_each_row():
    x = _herd(['Cow', 'Cow', 'Chicken', 'Chicken'], [2, 3, 10, 20])
    out, caught = _run(x)
    pd.testing.assert_frame_equal(out, _existing_reference(x))
    assert out.attrs['tlu_species_coverage']['unsupported_labels']['Cow']['positive_rows'] == 2
    assert len(caught) == 1


def test_empty_input_has_explicit_zero_coverage_and_existing_empty_shape():
    x = _herd([], [])
    out, caught = _run(x)
    pd.testing.assert_frame_equal(out, _existing_reference(x))
    assert not caught and out.empty
    assert out.attrs['tlu_species_coverage'] == {'head_col': 'HeadCount', 'input_rows': 0,
                                             'supported_rows': 0, 'unsupported_rows': 0,
                                             'unsupported_labels': {}}


def test_supported_negative_zero_and_missing_heads_keep_existing_values():
    x = _herd(['Chicken'] * 4, [pd.NA, -2, 0, 5])
    out, caught = _run(x)
    pd.testing.assert_frame_equal(out, _existing_reference(x))
    assert not caught and out.attrs['tlu_species_coverage']['supported_rows'] == 4


def test_country_repair_preserves_existing_factors_warning_and_coverage():
    # GH1616 supersedes the explicitly temporary country-pooling pin; the
    # numeric factors, warning behavior and all-input coverage remain unchanged.
    x = _herd(['Chicken', 'Chicken'], [10, 20], axis='column')
    x['country'] = ['invented-A', 'invented-B']
    x = x.set_index(['country', 'animal'], append=True)
    out, caught = _run(x)
    assert out.index.names == ['country', 't', 'i'] and len(out) == 2
    assert out.index.get_level_values('country').tolist() == ['invented-A', 'invented-B']
    assert out.TLU.tolist() == [10 * .01, 20 * .01]
    assert not caught and out.attrs['tlu_species_coverage']['input_rows'] == 2


def test_current_factors_are_not_expanded_to_match_malawi_labels():
    assert 'cow' not in T._TLU_FACTORS and 'goat' not in T._TLU_FACTORS
    assert 'local hen' not in T._TLU_FACTORS
    assert T._TLU_FACTORS['cattle'] == .7 and T._TLU_FACTORS['chicken'] == .01


def test_custom_pdna_factor_keeps_existing_numeric_conversion_error():
    with pytest.raises(TypeError):
        T.tlu(_herd(['Cow'], [3]), tlu_factors={'cow': pd.NA})


@pytest.mark.parametrize('axis', ['index', 'column'])
def test_missing_animal_labels_are_visible_without_factor_inference(axis):
    x = _herd([None, pd.NA, 'Chicken'], [2, 3, 10], axis)
    out, caught = _run(x)
    assert out.TLU.tolist() == [.1] and len(caught) == 1
    tally = out.attrs['tlu_species_coverage']
    assert tally['unsupported_rows'] == 2
    assert sum(v['positive_rows'] for v in tally['unsupported_labels'].values()) == 2
    assert all(isinstance(k, str) for k in tally['unsupported_labels'])


def test_infinite_and_negative_supported_contributions_are_not_newly_screened():
    x = _herd(['Chicken', 'Chicken'], [np.inf, -2.])
    out, caught = _run(x)
    pd.testing.assert_frame_equal(out, _existing_reference(x))
    assert not caught and np.isinf(out.TLU.iloc[0])


def test_large_unsupported_vocabulary_is_fully_counted_even_when_warning_is_bounded():
    labels = [f'Invented unsupported species {i}' for i in range(12)]
    out, caught = _run(_herd(labels, [1] * len(labels)))
    assert out.empty and len(caught) == 1
    assert len(out.attrs['tlu_species_coverage']['unsupported_labels']) == 12
    assert out.attrs['tlu_species_coverage']['unsupported_rows'] == 12


def test_tally_is_json_serializable():
    import json
    out, _ = _run(_herd(['Chicken', 'Cow'], [10, 3]))
    tally = out.attrs['tlu_species_coverage']
    assert json.loads(json.dumps(tally)) == tally
