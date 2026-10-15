"""Data-free tests of GhanaLSS draft membership and support denominators."""

import copy

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from SkunkWorks.aggregation.ghana_inventory import (
    _delivered_goods, draft_partition, label_provenance, wave_inventory,
)


def proposal(members=('a', 'b'), label='ab'):
    return {'groups': [dict(id='ab', label=label, members=list(members),
                            reason='Explicit test group.', evidence=['fixture'])]}


@pytest.fixture
def frames():
    sample = pd.DataFrame({'weight': [1., 2., 3., 0.]}, index=pd.MultiIndex.from_product(
        [['h0', 'h1', 'h2', 'h3'], ['wave']], names=['i', 't']))
    rows = [('h0', 'wave', None, 'a', 'Value', 'purchased', 2),
            ('h0', 'wave', None, 'a', 'Value', 'purchased', 3),
            ('h1', 'wave', 'v1', 'b', 'Kg', 'produced', 2),
            ('h2', 'wave', 'v1', 'c', 'Kg', 'produced', 2),
            ('h4', 'wave', 'v2', 'c', 'Value', 'purchased', 2),
            ('h1', 'wave', 'v1', 'Tobacco', 'Value', 'purchased', 2)]
    index = pd.MultiIndex.from_tuples(rows, names=['i', 't', 'v', 'j', 'u', 's', 'visit'])
    food = pd.DataFrame({'Expenditure': [2., 3., 10., np.nan, 7., 1.],
                         'Quantity': [2., 3., 5., np.nan, 7., 1.],
                         'Price': [np.nan, np.nan, 2., 99., np.nan, np.nan]}, index=index)
    membership, _, _ = draft_partition(['a', 'b', 'c', 'Tobacco'], proposal())
    return food, sample, membership


def test_inventory_uses_full_sample_and_retains_outside_sample_accounting(frames):
    food, sample, membership = frames
    before = food.copy()
    profiles, items, visits, groups = wave_inventory(food, sample, 'wave', membership)
    p = profiles.set_index('basis')
    assert (p.n_sample == 4).all()  # Includes h2 with a price only and h3 with no rows.
    assert (p.n_food_labels == 3).all()  # Tobacco is inventoried but excluded from this count.
    assert p.loc['purchased', 'n_positive_food_households'] == 1
    assert p.loc['total', 'n_positive_food_households'] == 2
    assert p.loc['purchased', 'weighted_food_household_coverage'] == pytest.approx(1/6)
    assert p.loc['total', 'weighted_food_household_coverage'] == .5
    assert (p.n_invalid_weight == 1).all()
    assert (p.n_positive_households_outside_sample == 1).all()
    assert (p.food_expenditure_outside_sample == 7.).all()
    c = items.loc[(items.preferred == 'c') & (items.basis == 'total')].iloc[0]
    assert c.n_positive_sample == 0 and c.n_positive_all == 1
    assert_frame_equal(food, before)


def test_visit_support_counts_positive_money_not_repeated_prices(frames):
    food, sample, membership = frames
    profiles, items, visits, groups = wave_inventory(food, sample, 'wave', membership)
    by_visit = visits.set_index(['basis', 'visit'])
    assert by_visit.loc[('purchased', 2), 'n_positive_food_households'] == 1
    assert by_visit.loc[('total', 2), 'n_positive_food_households'] == 2
    assert by_visit.loc[('total', 3), 'n_positive_household_item_cells'] == 1
    a = items.loc[(items.preferred == 'a') & (items.basis == 'total')].iloc[0]
    assert a.n_positive_sample == 1 and a.expenditure_sample == 5.  # Sum visits, not households.
    total = groups.loc[groups.basis == 'total'].iloc[0]
    assert total.n_positive_sample == 2 and total.additional_hh_vs_best_member == 1


def test_zero_amounts_and_unavailable_amounts_have_identical_support(frames):
    food, sample, membership = frames
    first = wave_inventory(food, sample, 'wave', membership)
    food = food.fillna({'Expenditure': 0.})
    second = wave_inventory(food, sample, 'wave', membership)
    for before, after in zip(first, second):
        assert_frame_equal(before, after)


def test_draft_defaults_to_singletons_and_records_absent_members():
    membership, review, missing = draft_partition(['a', 'b', 'c', 'Tobacco'], proposal(('a', 'b', 'absent')))
    assert membership.to_dict() == {'Tobacco': 'Tobacco', 'a': 'ab', 'b': 'ab', 'c': 'c'}
    assert review.loc['c', 'status'] == 'singleton'
    assert review.loc['Tobacco', 'status'] == 'nonfood_singleton'
    assert missing == [('ab', 'absent')]


def test_singleton_cautions_are_carried_into_review_rows():
    spec = proposal()
    spec['singleton_cautions'] = {'c': 'Broad source bundle; do not merge.'}
    membership, review, _ = draft_partition(['a', 'b', 'c'], spec)
    assert membership['c'] == 'c'
    assert review.loc['c', 'status'] == 'singleton'
    assert review.loc['c', 'reason'] == spec['singleton_cautions']['c']


@pytest.mark.parametrize('members,label', [(('a', 'a'), 'ab'), (('a', 'b'), 'c'),
                                          (('a', 'Tobacco'), 'ab')])
def test_draft_rejects_duplicate_members_name_collisions_and_nonfood_merges(members, label):
    with pytest.raises(ValueError):
        draft_partition(['a', 'b', 'c', 'Tobacco'], proposal(members, label))


def test_draft_rejects_overlapping_groups():
    spec = proposal()
    spec['groups'].append(dict(id='bc', label='bc', members=['b', 'c'], reason='test', evidence=['test']))
    with pytest.raises(ValueError, match='disjoint'):
        draft_partition(['a', 'b', 'c'], spec)


def test_draft_rejects_duplicate_group_identifiers():
    spec = proposal()
    spec['groups'].append(dict(id='ab', label='cd', members=['c', 'd'],
                               reason='test', evidence=['test']))
    with pytest.raises(ValueError, match='identifiers'):
        draft_partition(['a', 'b', 'c', 'd'], spec)


def test_delivered_labels_retain_exact_whitespace_identity(frames):
    food, _, _ = frames
    food = food.rename(index={'a': ' a '}, level='j')
    goods = _delivered_goods(food)
    assert ' a ' in goods and 'a' not in goods
    membership, _, missing = draft_partition(goods, proposal())
    assert membership[' a '] == ' a '
    assert missing == [('ab', 'a')]


@pytest.mark.parametrize('level', ['i', 't', 'j'])
def test_missing_delivered_keys_cannot_silently_drop_records(frames, level):
    food, sample, membership = frames
    labels = food.index.to_frame(index=False)
    labels.loc[0, level] = None
    food.index = pd.MultiIndex.from_frame(labels)
    with pytest.raises(ValueError, match='cannot be missing'):
        wave_inventory(food, sample, 'wave', membership)


def test_country_crosswalk_is_provenance_and_does_not_rename_delivered_labels():
    country = pd.DataFrame({'Preferred Label': ['Sugar'], 'wave': ['Sugar (cubed)']})
    wave = pd.DataFrame({'Preferred Label': ['Sugar (cubed)'], 'Code_9b': [1],
                         'Label_9b': ['cube sugar'], 'Aggregate Label': ['Historical sweets']})
    result = label_provenance(['Sugar (cubed)'], country, {'wave': wave})
    assert result.index.tolist() == ['Sugar (cubed)']
    assert not result.iloc[0].in_country_preferred
    assert result.iloc[0].country_crosswalk_targets == 'Sugar'
    assert 'Historical sweets' not in result.to_string()


@pytest.mark.parametrize('bad', [-1., np.inf])
def test_invalid_source_expenditures_are_rejected(frames, bad):
    food, sample, membership = copy.deepcopy(frames)
    food.iloc[0, food.columns.get_loc('Expenditure')] = bad
    with pytest.raises(ValueError, match='nonnegative'):
        wave_inventory(food, sample, 'wave', membership)
