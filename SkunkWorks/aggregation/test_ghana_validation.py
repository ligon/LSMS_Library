"""Population and leakage contracts for the Ghana empirical comparison."""

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal
from types import SimpleNamespace

from SkunkWorks.aggregation.ghana_validation import (
    GhanaData, align_inputs, candidate_maps, evaluation_arguments, household_split,
    prediction_diagnostics, resample_training_rows,
    paired_prediction_difference,
    simulation_template,
)


def test_alignment_preserves_missing_market_controls_and_zero_food_households():
    sample = pd.DataFrame({'weight': [1., 2., 3., 4.], 'Rural': ['Rural']*4,
                            'v': ['v0']*4}, index=pd.MultiIndex.from_product(
                                [['h0', 'h1', 'h2', 'h3'], ['wave']], names=['i', 't']))
    index = pd.MultiIndex.from_tuples([('h0', 'wave', 'r'), ('h1', 'wave', 'r'),
                                      ('h2', 'wave', 'r')], names=['i', 't', 'm'])
    marketed = pd.DataFrame(index=index)
    d = pd.DataFrame({'size': [2.]}, index=index[:1])
    food = pd.DataFrame({'Expenditure': [2., 4., 9., 3.]}, index=pd.MultiIndex.from_tuples(
        [('h0', 'wave', 'a', 'purchased'), ('h2', 'wave', 'a', 'purchased'),
         ('h3', 'wave', 'a', 'purchased'), ('outside', 'wave', 'b', 'purchased')],
        names=['i', 't', 'j', 's']))
    food['Expenditure'] = food.Expenditure.astype('Float64')
    data = align_inputs(sample, marketed, d, food, wave='wave', basis='purchased')
    assert len(data.x) == 4
    assert data.x.columns.name == 'j'
    assert data.x.dtypes.eq(np.dtype('float64')).all()
    assert data.profile['n_missing_market'] == 1
    assert data.profile['n_missing_characteristics'] == 3
    assert data.profile['n_positive_households_outside_sample'] == 1
    assert data.x.loc[('h1', 'wave', 'r')].sum() == 0
    assert data.x.loc[('h3', 'wave', '__unavailable_market__'), 'a'] == 9.
    assert data.weights.sum() == 10.
    assert data.d.loc[('h3', 'wave', '__unavailable_market__')].isna().all()


@pytest.fixture
def data():
    index = pd.MultiIndex.from_product([range(40), ['wave'], ['market']], names=['i', 't', 'm'])
    x = pd.DataFrame({'Salt': 1., 'a': 2., 'b': 3., 'Onion': 4., 'Egg': 5.}, index=index)
    x.columns.name = 'j'
    d = pd.DataFrame({'size': 2.}, index=index)
    weights = pd.Series(1., index=index)
    strata = pd.Series(['rural']*20+['urban']*20, index=index)
    psu = pd.Series(np.arange(40)//5, index=index)
    return GhanaData(x, d, weights, strata, psu, {})


def test_split_is_household_disjoint_stratified_and_order_invariant(data):
    first = household_split(data.strata)
    second = household_split(data.strata.sample(frac=1., random_state=3)).reindex(first.index)
    assert_series_equal(first, second)
    assert first.value_counts().to_dict() == {'train': 24, 'validation': 8, 'test': 8}
    assert not first.isna().any()
    for _, values in first.groupby(data.strata):
        assert values.value_counts().to_dict() == {'train': 12, 'validation': 4, 'test': 4}


def test_reserved_targets_do_not_enter_fit_score_or_reference_design(data):
    split = household_split(data.strata)
    first = evaluation_arguments(data, split)
    data.x.loc[:, ['Onion', 'Egg']] *= 1000
    second = evaluation_arguments(data, split)
    assert_frame_equal(first['x'], second['x'])
    assert 'Onion' not in first['x'] and 'Egg' not in first['x']
    assert first['reference_cells'].equals(second['reference_cells'])
    assert set(first['train_households']).isdisjoint(first['evaluation_households'])
    assert not set(first['evaluation_households']).intersection(
        split.index.get_level_values('i')[split == 'test'])


def test_missing_controls_remain_in_denominator_but_not_reference(data):
    split = household_split(data.strata)
    cell = split.loc[split == 'train'].index[0]
    data.d.loc[cell, 'size'] = np.nan
    args = evaluation_arguments(data, split)
    assert cell not in args['reference_cells']
    assert cell in args['weights'].index
    assert cell[0] in args['train_households']


def test_maps_exclude_reserved_groups_and_preserve_other_identities(data):
    spec = {'groups': [dict(id='ab', label='ab', members=['a', 'b'],
                            reason='test', evidence=['fixture'])],
            'deferred_alternatives': [dict(id='onion', label='onions', members=['Onion', 'a'],
                                           reason='test', evidence=['fixture'])]}
    maps, skipped = candidate_maps(data.x.columns.drop(['Onion', 'Egg']), spec, ['Onion', 'Egg'])
    assert set(maps) == {'fine', 'ab', 'draft'}
    assert 'onion' in skipped
    assert maps['ab'].loc['Salt'] == 'Salt'
    assert maps['ab'].loc['a'] == maps['ab'].loc['b'] == 'ab'


def test_combined_map_accepts_structured_source_evidence(data):
    spec = {'groups': [dict(id='ab', label='ab', members=['a', 'b'],
                            reason='test', evidence=[{'path': 'source', 'code': 1}])]}
    maps, _ = candidate_maps(data.x.columns, spec)
    assert_series_equal(maps['ab'], maps['draft'])


def test_ghana_simulation_preserves_whole_masks_weights_and_zero_rows(data):
    data.x.iloc[0] = 0.
    data.d.iloc[1] = np.nan
    data.profile.update(wave='wave', basis='purchased', n_missing_market=0)
    spec = {'groups': [dict(id='ab', label='ab', members=['a', 'b'],
                            reason='test', evidence=['fixture'])]}
    template = simulation_template(data, spec)
    np.testing.assert_array_equal(template.mask, data.x.gt(0))
    np.testing.assert_array_equal(template.weights, data.weights)
    assert not template.mask.iloc[0].any()
    assert len(template.mask) == len(data.x)
    assert template.mask.index.get_level_values('i').tolist() == list(range(len(data.x)))


def test_psu_resampling_keeps_blocks_intact_and_does_not_use_heldout_rows(data):
    split = household_split(data.strata)
    train = split.index[split == 'train']
    rows = resample_training_rows(data, train, np.random.default_rng(42))
    multiplicity = pd.Series(np.bincount(rows, minlength=len(train)), index=train)
    for _, block in multiplicity.groupby([data.strata.loc[train], data.psu.loc[train]]):
        assert block.nunique() == 1
    assert train.take(rows).isin(train).all()


def test_prediction_calibration_uses_only_training_targets_and_fixed_population(data):
    split = household_split(data.strata)
    w = pd.Series(np.sin(np.arange(len(data.x))), index=data.x.index)
    data.x['Onion'] = np.exp(1.+.5*w)
    data.x['Egg'] = np.exp(2.+.8*w)
    args = evaluation_arguments(data, split)
    evaluation = split.index[split == 'validation']
    origin = w.loc[args['reference_cells']].mean()

    class FrozenModel:
        beta = pd.Series({'Salt': 1.})

        def score_w(self, logs, controls):
            assert not set(logs.columns).intersection({'Onion', 'Egg'})
            return w.reindex(logs.index)

    candidate = SimpleNamespace(model=FrozenModel(),
        membership=pd.Series(args['x'].columns, index=args['x'].columns),
        market_identified=pd.Series(True, index=pd.MultiIndex.from_tuples(
            [('wave', 'market')], names=['t', 'm'])),
        scores=w.loc[evaluation]-origin)
    first = prediction_diagnostics(data, args, candidate, ['Onion', 'Egg'])
    assert first.complete.all() and (first.mse < 1e-20).all()
    data.x.loc[evaluation, ['Onion', 'Egg']] *= np.e
    changed = prediction_diagnostics(data, args, candidate, ['Onion', 'Egg'])
    assert np.allclose(changed.mse, 1.)
    assert np.allclose(changed.mean_error, -1.)
    candidate.scores.iloc[0] = np.nan
    missing = prediction_diagnostics(data, args, candidate, ['Onion', 'Egg'])
    assert not missing.complete.any() and missing.mse.isna().all()
    assert (missing.n_evaluation == first.n_evaluation).all()
    # Empty calibration must not become an arbitrary zero prediction.
    candidate.scores = w.loc[evaluation]-origin
    data.x.loc[split == 'train', 'Egg'] = 0.
    empty = prediction_diagnostics(data, args, candidate, ['Egg']).iloc[0]
    assert empty.n_calibration == empty['rank'] == empty.n_estimable == 0
    assert not empty.complete and np.isnan(empty.mse)
    # Size was a constant in training, collinear with the market indicator.
    # The unchanged validation design above was estimable; changing its size
    # breaks that relation and the target prediction is then unidentified.
    data.d.loc[evaluation, 'size'] = 3.
    unsupported = prediction_diagnostics(data, args, candidate, ['Onion']).iloc[0]
    assert not unsupported.complete and unsupported.n_estimable == 0


def test_paired_loss_resampling_keeps_the_declared_population(data):
    error = pd.Series(2., index=data.x.index[:20])
    baseline = pd.Series(1., index=error.index)
    result = paired_prediction_difference(error, baseline, data,
                                          evaluation_cells=data.x.index, draws=20)
    assert result['delta_mse'] == result['delta_q025'] == result['delta_q975'] == 3.
    with pytest.raises(ValueError, match='same predeclared'):
        paired_prediction_difference(error.iloc[1:], baseline, data, evaluation_cells=data.x.index)
    sparse = paired_prediction_difference(error.iloc[:1], baseline.iloc[:1], data,
                                          evaluation_cells=data.x.index, draws=100)
    assert sparse['n_target_psus'] == 1 and sparse['n_validation_psus'] == 8
    assert sparse['n_zero_weight_draws'] > 0
