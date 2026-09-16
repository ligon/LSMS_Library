"""Data-free tests; run with --confcutdir=SkunkWorks/aggregation.

This avoids LSMS's country-cache hooks for an isolated simulation analysis.
"""

import copy

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from SkunkWorks.aggregation.evaluation import aggregate_expenditures, evaluate_partitions
from SkunkWorks.aggregation.simulation import simulation_inputs


@pytest.fixture(scope='module')
def inputs():
    args = simulation_inputs(seed=11, households=120, regime='substitutes')
    args['fit_options']['min_obs'] = 10
    return args


@pytest.fixture(scope='module')
def result(inputs):
    return evaluate_partitions(**inputs)


def _fine_only(inputs):
    args = copy.deepcopy(inputs)
    args['partitions'] = {'fine': args['partitions']['fine']}
    return args


def test_aggregation_sums_levels_and_uses_union_of_support():
    index = pd.MultiIndex.from_tuples([(0, 0, 0), (1, 0, 0), (2, 0, 0)], names=['i', 't', 'm'])
    x = pd.DataFrame([[1., 3., 0.], [0., np.nan, 2.], [np.nan, 0., 0.]],
                     index=index, columns=pd.Index(['a', 'b', 'c'], name='j'))
    before = x.copy()
    grouped = aggregate_expenditures(x, {'a': 'ab', 'b': 'ab', 'c': 'c'})
    assert grouped.ab.tolist() == [4., 0., 0.]
    assert grouped.c.tolist() == [0., 2., 0.]
    assert np.log(grouped.ab.where(grouped.ab > 0)).iloc[0] == np.log(4.)
    assert_frame_equal(grouped, aggregate_expenditures(x.fillna(0), {'a': 'ab', 'b': 'ab', 'c': 'c'}))
    assert_frame_equal(x, before)


@pytest.mark.parametrize('bad', [-1., np.inf, -np.inf])
def test_invalid_expenditure_is_rejected(inputs, bad):
    x = inputs['x'].copy()
    x.iloc[0, 0] = bad
    with pytest.raises(ValueError, match='nonnegative'):
        aggregate_expenditures(x, inputs['partitions']['fine'])


def test_partition_must_be_exhaustive_and_keep_anchor(inputs):
    args = _fine_only(inputs)
    args['partitions']['fine'].pop('g9')
    with pytest.raises(ValueError, match='exactly the fine goods'):
        evaluate_partitions(**args)
    args = _fine_only(inputs)
    args['partitions']['fine']['g1'] = 'g0'
    with pytest.raises(ValueError, match='singleton'):
        evaluate_partitions(**args)


def test_no_household_can_cross_training_evaluation_split(inputs):
    args = _fine_only(inputs)
    args['evaluation_households'] = np.append(args['evaluation_households'], args['train_households'][0])
    with pytest.raises(ValueError, match='disjoint'):
        evaluate_partitions(**args)


def test_comparison_and_reference_populations_have_correct_roles(inputs):
    args = _fine_only(inputs)
    args['comparison_cells'] = args['reference_cells']
    with pytest.raises(ValueError, match='comparison_cells'):
        evaluate_partitions(**args)
    args = _fine_only(inputs)
    args['reference_cells'] = args['comparison_cells']
    with pytest.raises(ValueError, match='reference_cells'):
        evaluate_partitions(**args)


def test_sparse_pool_improves_scoring_coverage(inputs, result):
    assert (result.summary.status == 'ok').all()
    assert (result.summary.core_complete).all()
    assert result.summary.loc['rare_pool', 'score_coverage'] == 1.
    assert result.summary.loc['fine', 'score_coverage'] < 1.
    for candidate in result.candidates.values():
        assert candidate.paired_support.min().min() >= inputs['fit_options']['min_obs']
        assert candidate.model.attrs['compute_se'] is False
        assert candidate.model.beta_se is None


def test_common_error_and_weighted_coverage_are_literal_population_means(inputs):
    args = _fine_only(inputs)
    args['weights'] = pd.Series(1+np.arange(len(args['x'])) % 7, index=args['x'].index, dtype=float)
    reference = args['reference_cells']
    args['truth'] -= np.average(args['truth'].loc[reference], weights=args['weights'].loc[reference])
    evaluated = evaluate_partitions(**args)
    candidate = evaluated.candidates['fine']
    cells = args['comparison_cells']
    error = candidate.scores.loc[cells]-args['truth'].loc[cells]
    expected = np.average(error**2, weights=args['weights'].loc[cells])
    assert np.isclose(evaluated.summary.loc['fine', 'core_mse'], expected)
    weights = args['weights'].reindex(candidate.scores.index)
    expected_coverage = weights[candidate.scores.notna()].sum()/weights.sum()
    assert np.isclose(evaluated.summary.loc['fine', 'score_coverage'], expected_coverage)
    market = evaluated.coverage_by_market.loc['fine']
    assert np.isclose(market.scored_weight.sum()/market.weight.sum(), expected_coverage)


def test_reference_center_and_anchor_units_are_fixed_from_training(inputs, result):
    for name, candidate in result.candidates.items():
        amounts = aggregate_expenditures(inputs['x'], candidate.membership)
        logs = np.log(amounts.where(amounts > 0))
        cells = inputs['reference_cells']
        raw = candidate.model.score_w(logs.loc[cells], inputs['d'].loc[cells])
        row = result.summary.loc[name]
        centered = row.scale*(raw-row.origin)
        assert abs(np.average(centered, weights=inputs['weights'].loc[cells])) < 1e-12
        anchor_group = candidate.membership.loc[inputs['anchor']]
        assert np.isclose(candidate.model.beta.loc[anchor_group]/row.scale, inputs['anchor_loading'])


def test_truth_never_changes_fitting_scores_or_normalization(inputs, result):
    args = _fine_only(inputs)
    args['truth'] += 1000.
    changed = evaluate_partitions(**args)
    assert_series_equal(changed.candidates['fine'].scores, result.candidates['fine'].scores)
    assert_series_equal(changed.candidates['fine'].model.beta, result.candidates['fine'].model.beta)
    assert changed.summary.loc['fine', 'origin'] == result.summary.loc['fine', 'origin']
    assert np.isclose(changed.summary.loc['fine', 'core_mean_error'],
                      result.summary.loc['fine', 'core_mean_error']-1000.)
    args['truth'] = None
    empirical = evaluate_partitions(**args)
    assert_series_equal(empirical.candidates['fine'].scores, result.candidates['fine'].scores)
    assert pd.isna(empirical.summary.loc['fine', 'core_mse'])


def test_held_out_expenditures_cannot_change_fitted_parameters(inputs, result):
    args = _fine_only(inputs)
    evaluation = result.specification['evaluation_cells']
    args['x'].loc[evaluation] *= 2.
    changed = evaluate_partitions(**args)
    before, after = result.candidates['fine'].model, changed.candidates['fine'].model
    for attr in ['beta', 'w', 'gamma_d']:
        assert_series_equal(getattr(before, attr), getattr(after, attr))
    assert_frame_equal(before.gamma, after.gamma)
    assert changed.summary.loc['fine', 'origin'] == result.summary.loc['fine', 'origin']
    assert changed.summary.loc['fine', 'scale'] == result.summary.loc['fine', 'scale']


def test_missing_core_score_suppresses_mse_instead_of_shrinking_population(inputs):
    args = _fine_only(inputs)
    missing = args['comparison_cells'][0]
    args['x'].loc[missing] = 0.
    evaluated = evaluate_partitions(**args)
    row = evaluated.summary.loc['fine']
    assert row.status == 'ok' and not row.core_complete
    assert pd.isna(row.core_mse)
    assert pd.isna(evaluated.candidates['fine'].scores.loc[missing])
    assert evaluated.specification['comparison_cells'].equals(args['comparison_cells'])


def test_zero_weight_core_cell_does_not_change_error_population(inputs):
    args = _fine_only(inputs)
    missing = args['comparison_cells'][0]
    args['x'].loc[missing] = 0.
    args['weights'].loc[missing] = 0.
    args['truth'].loc[missing] = np.nan
    evaluated = evaluate_partitions(**args)
    assert evaluated.summary.loc['fine', 'core_complete']
    assert np.isfinite(evaluated.summary.loc['fine', 'core_mse'])


def test_missing_reference_score_is_explicit_failure(inputs):
    args = _fine_only(inputs)
    args['x'].loc[args['reference_cells'][0]] = 0.
    evaluated = evaluate_partitions(**args)
    assert evaluated.summary.loc['fine', 'status'] == 'reference_unscored'
    assert pd.isna(evaluated.summary.loc['fine', 'core_mse'])


def test_missing_anchor_is_explicit_failure(inputs):
    args = _fine_only(inputs)
    args['x']['g0'] = 0.
    evaluated = evaluate_partitions(**args)
    assert evaluated.summary.loc['fine', 'status'] == 'anchor_unavailable'


def test_single_retained_good_is_reported_before_estimation(inputs):
    args = _fine_only(inputs)
    args['x'].loc[:, args['x'].columns != 'g0'] = 0.
    evaluated = evaluate_partitions(**args)
    assert evaluated.summary.loc['fine', 'status'] == 'constant_loadings'
    assert evaluated.candidates['fine'].scores.isna().all()


def _unidentified_market_inputs():
    cells = pd.MultiIndex.from_product([range(80), [0], [0, 1]], names=['i', 't', 'm'])
    goods = pd.Index(['a', 'b', 'c', 'd'], name='j')
    rng = np.random.default_rng(71)
    w = rng.normal(size=len(cells))
    x = pd.DataFrame(np.exp(2+np.outer(w, [.4, .8, 1.2, 2.])+rng.normal(scale=.1, size=(len(cells), 4))),
                     index=cells, columns=goods)
    bad = cells.get_level_values('m') == 1
    one_good = cells.get_level_values('i')[bad].to_numpy() % 4
    x.loc[bad] *= one_good[:, None] == np.arange(4)[None, :]
    train, evaluation = np.arange(60), np.arange(60, 80)
    reference = cells[(cells.get_level_values('i') < 60) & ~bad]
    comparison = cells[cells.get_level_values('i') >= 60]
    return dict(x=x, d=pd.DataFrame(index=cells), partitions={'fine': {g: g for g in goods}},
                train_households=train, evaluation_households=evaluation,
                reference_cells=reference, comparison_cells=comparison,
                weights=pd.Series(1., index=cells), anchor='a', anchor_loading=.4,
                truth=pd.Series(w-w[cells.isin(reference)].mean(), index=cells),
                fit_options={'min_obs': 10, 'min_prop_items': .1, 'alltm': True})


def test_unidentified_markets_cannot_supply_coverage_or_comparable_mse():
    args = _unidentified_market_inputs()
    evaluated = evaluate_partitions(**args)
    candidate = evaluated.candidates['fine']
    assert candidate.market_identified.loc[(0, 0)]
    assert not candidate.market_identified.loc[(0, 1)]
    assert evaluated.summary.loc['fine', 'n_unidentified_markets'] == 1
    assert evaluated.summary.loc['fine', 'score_coverage'] == .5
    assert not evaluated.summary.loc['fine', 'core_complete']
    assert pd.isna(evaluated.summary.loc['fine', 'core_mse'])
    assert candidate.scores.xs(1, level='m').isna().all()
    assert (candidate.scoring.xs(1, level='m').status == 'unidentified_market').all()


def test_normalization_cannot_use_an_unidentified_market():
    args = _unidentified_market_inputs()
    args['reference_cells'] = args['x'].index[args['x'].index.get_level_values('i') < 60]
    evaluated = evaluate_partitions(**args)
    assert evaluated.summary.loc['fine', 'status'] == 'reference_unscored'
    assert evaluated.candidates['fine'].scores.isna().all()


def test_anchor_unit_change_rescales_both_scores_and_error(inputs, result):
    args = _fine_only(inputs)
    args['anchor_loading'] *= 2
    args['truth'] /= 2
    changed = evaluate_partitions(**args)
    assert_series_equal(changed.candidates['fine'].scores, result.candidates['fine'].scores/2)
    assert np.isclose(changed.summary.loc['fine', 'core_mse'], result.summary.loc['fine', 'core_mse']/4)


def test_input_order_and_caller_objects_do_not_change_results(inputs, result):
    args = _fine_only(inputs)
    args['x'] = args['x'].iloc[::-1, ::-1]
    args['d'] = args['d'].iloc[::-1]
    before = copy.deepcopy(args)
    changed = evaluate_partitions(**args)
    assert_series_equal(changed.candidates['fine'].scores, result.candidates['fine'].scores)
    for name in ['x', 'd']:
        assert_frame_equal(args[name], before[name])
    for name in ['weights', 'truth']:
        assert_series_equal(args[name], before[name])
    assert args['partitions'] == before['partitions']


def test_simulation_reuses_dgp_without_changing_global_random_state():
    state = np.random.get_state()
    first = simulation_inputs(seed=53, households=80)
    after = np.random.get_state()
    assert state[0] == after[0] and np.array_equal(state[1], after[1]) and state[2:] == after[2:]
    second = simulation_inputs(seed=53, households=80)
    assert_frame_equal(first['x'], second['x'])
    rare = first['x'].loc[:, [f'g{j}' for j in range(4, 10)]]
    assert ((rare > 0).sum(axis=1) == 1).all()
    assert set(first['train_households']).isdisjoint(first['evaluation_households'])
