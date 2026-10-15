"""Data-free checks of the survey-mask adapter and simulation contracts."""

import copy

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from SkunkWorks.aggregation.evaluation import aggregate_expenditures, evaluate_partitions
from SkunkWorks.aggregation.survey_simulation import (
    curated_membership, survey_inputs, template_from_frames,
)


@pytest.fixture
def frames():
    index = pd.MultiIndex.from_product([range(160), ['wave']], names=['i', 't'])
    sample = pd.DataFrame({'Region': np.where(np.arange(160) % 2, 'East', 'West'),
                           'weight': 1.+np.arange(160) % 5}, index=index)
    goods = ['Salt', 'a1', 'a2', 'b1', 'b2', 'c', 'Cigarettes', 'never']
    table = pd.DataFrame({'Preferred Label': goods,
                          'Aggregate Label': ['Salt', 'a', 'a', 'b', 'b', 'c', 'Tobacco', 'never']})
    mask = np.random.default_rng(2).random((160, len(goods))) < .6
    mask[:, 0] = True
    mask[:, -1] = False
    mask[::17] = False  # Sample households absent from the food table.
    x = pd.DataFrame(np.where(mask, 2., np.nan), index=index, columns=pd.Index(goods, name='j'))
    food = x.stack('j').rename('Expenditure').to_frame()
    return sample, food, goods, table


def template(frames):
    return template_from_frames(*frames, wave='wave', basis='total')


def test_whole_masks_include_zero_food_households_and_all_delivered_goods(frames):
    sample, food, universe, _ = frames
    result = template(frames)
    expected = food.Expenditure.unstack('j').reindex(
        index=sample.index, columns=sorted(set(universe)-{'Cigarettes'})).fillna(0).gt(0)
    expected.index = result.mask.index
    assert_frame_equal(result.mask, expected)
    observed = result.mask.astype(int)
    assert_frame_equal(observed.T @ observed, expected.astype(int).T @ expected.astype(int))
    assert len(result.mask) == len(sample)
    assert (~result.mask.any(axis=1)).sum() == 10
    assert not result.mask['never'].any()
    assert result.profile['n_food_goods'] == 7
    assert result.profile['n_food_households'] == 150
    assert result.weights.tolist() == sample.weight.tolist()


def test_unknown_markets_bad_weights_and_outside_households_are_reported(frames):
    sample, food, universe, table = copy.deepcopy(frames)
    sample.iloc[1, 0] = None
    sample.iloc[2, 1] = 0.
    outside = pd.DataFrame({'Expenditure': [2.]}, index=pd.MultiIndex.from_tuples(
        [(999, 'wave', 'Salt')], names=['i', 't', 'j']))
    result = template_from_frames(sample, pd.concat([food, outside]), universe, table,
                                  wave='wave', basis='total')
    assert len(result.mask) == 158
    assert result.profile['n_sample'] == 160
    assert result.profile['n_missing_region'] == 1
    assert result.profile['n_invalid_weight'] == 1
    assert result.profile['n_food_households_outside_sample'] == 1


def test_conflicting_curated_labels_stay_singleton_without_name_collision():
    table = pd.DataFrame({'Preferred Label': ['a', 'a', 'b', 'c'],
                          'Aggregate Label': ['other', 'a', 'a', 'a']})
    mapping, conflicts = curated_membership(['a', 'b', 'c'], table)
    assert conflicts == ['a']
    assert mapping['a'] != mapping['b'] == mapping['c']
    with pytest.raises(ValueError, match='No curated'):
        curated_membership(['absent'], table)


@pytest.mark.parametrize('bad', [-1., np.inf])
def test_bad_delivered_amounts_are_rejected(frames, bad):
    sample, food, universe, table = copy.deepcopy(frames)
    food.iloc[0, 0] = bad
    with pytest.raises(ValueError, match='nonnegative'):
        template_from_frames(sample, food, universe, table, wave='wave', basis='total')


def test_basis_cannot_silently_introduce_goods_outside_declared_universe(frames):
    sample, food, universe, table = frames
    with pytest.raises(ValueError, match='universe omits'):
        template_from_frames(sample, food, ['Salt'], table, wave='wave', basis='purchased')


@pytest.mark.parametrize('regime', ['masked', 'substitutes', 'unequal'])
def test_each_regime_preserves_joint_mask_weights_markets_and_zero_rows(frames, regime):
    source = template(frames)
    before = copy.deepcopy(source)
    args = survey_inputs(source, seed=9, regime=regime, min_obs=5)
    assert_frame_equal(args['x'].gt(0), source.mask)
    assert_series_equal(args['weights'], source.weights)
    assert_frame_equal(source.mask, before.mask)
    train, evaluation = set(args['train_households']), set(args['evaluation_households'])
    assert not train.intersection(evaluation)
    assert train.union(evaluation) == set(source.mask.index.get_level_values('i'))
    assert source.mask.loc[args['reference_cells'], 'Salt'].all()
    assert source.mask.loc[args['comparison_cells'], 'Salt'].all()
    assert len(args['x']) == 160  # No household x market Cartesian expansion.
    reference = args['reference_cells']
    assert abs(np.average(args['truth'].loc[reference], weights=source.weights.loc[reference])) < 1e-12


def test_substitution_holds_equal_beta_no_error_group_amount_fixed(frames):
    source = template(frames)
    raw = survey_inputs(source, seed=9, regime='masked', sigma_eps=0.)
    subs = survey_inputs(source, seed=9, regime='substitutes', sigma_eps=0.)
    count = source.mask.T.groupby(source.membership, sort=False).sum().T
    group_raw = aggregate_expenditures(raw['x'], source.membership)
    group_subs = aggregate_expenditures(subs['x'], source.membership)
    expected = group_raw.div(count.clip(lower=1))
    expected.columns.name = 'j'
    assert_frame_equal(group_subs, expected)
    assert_frame_equal(raw['d'], subs['d'])
    assert_series_equal(raw['truth'], subs['truth'])


def test_empirical_template_fits_through_existing_evaluator(frames):
    args = survey_inputs(template(frames), seed=11, min_obs=5)
    result = evaluate_partitions(**args)
    assert (result.summary.status == 'ok').all()
    assert result.summary.core_complete.all()
    assert np.isfinite(result.summary.core_mse).all()
    assert (result.summary.score_coverage < 1).all()  # Zero-food rows remain.


def test_empirical_dgp_is_reproducible_without_global_rng_side_effects(frames):
    source = template(frames)
    state = np.random.get_state()
    first = survey_inputs(source, seed=9)
    after = np.random.get_state()
    assert state[0] == after[0] and state[2:] == after[2:]
    assert np.array_equal(state[1], after[1])
    second = survey_inputs(source, seed=9)
    assert_frame_equal(first['x'], second['x'])
    assert_series_equal(first['truth'], second['truth'])
