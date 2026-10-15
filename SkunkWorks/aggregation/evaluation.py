"""Evaluate supplied food partitions using the existing CFE estimator.

This is a SkunkWorks analysis interface, not a Country API extension. Its
contracts are recorded in .coder/ledger/cfe-aggregation-evaluation.md.
"""

from dataclasses import dataclass, field
from collections.abc import Mapping
import warnings

import numpy as np
import pandas as pd


@dataclass
class Candidate:
    membership: pd.Series
    model: object = None
    scores: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    scoring: pd.DataFrame = field(default_factory=pd.DataFrame)
    paired_support: pd.DataFrame = field(default_factory=pd.DataFrame)
    residual_variance: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    raw_fit_conditional_variance: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    market_identified: pd.Series = field(default_factory=lambda: pd.Series(dtype=bool))
    warnings: list = field(default_factory=list)


@dataclass
class Evaluation:
    summary: pd.DataFrame
    candidates: dict
    coverage_by_market: pd.DataFrame
    specification: dict


def _cells(index, name):
    if not isinstance(index, pd.MultiIndex) or index.names != ['i', 't', 'm']:
        raise ValueError(f'{name} must have index (i,t,m).')
    if not index.is_unique or index.to_frame(index=False).isna().any().any():
        raise ValueError(f'{name} must have unique, nonmissing cell identifiers.')
    return index


def _expenditures(x):
    if not isinstance(x, pd.DataFrame) or x.columns.name != 'j':
        raise ValueError('x must be a wide DataFrame with columns named j.')
    _cells(x.index, 'x')
    if not x.columns.is_unique or x.columns.hasnans or not len(x.columns):
        raise ValueError('Fine goods must be unique, nonmissing, and nonempty.')
    x = x.astype(float)
    values = x.to_numpy()
    if np.isinf(values).any() or (values < 0).any():
        raise ValueError('Expenditures must be nonnegative and finite or unavailable.')
    # Canonical ordering makes the reused greedy selector reproducible (ledger 4).
    order = sorted(x.columns, key=lambda item: (type(item).__name__, repr(item)))
    return x.loc[:, order].sort_index()


def _membership(partition, goods):
    if not isinstance(partition, (Mapping, pd.Series)):
        raise TypeError('A partition must map every fine good to one group.')
    membership = pd.Series(partition, dtype=object)
    if not membership.index.is_unique or set(membership.index) != set(goods):
        raise ValueError('Partition membership must cover exactly the fine goods.')
    membership = membership.reindex(goods)
    if any(not pd.api.types.is_scalar(v) or pd.isna(v) for v in membership):
        raise ValueError('Group labels must be nonmissing scalar identifiers.')
    membership.index.name = 'j'
    return membership.rename('group')


def aggregate_expenditures(x, partition):
    """Sum nonnegative monetary amounts before logs using an exhaustive map.

    Within the declared item universe, NaN and zero both contribute zero.
    The caller owns expenditure basis, recall comparability, and semantics.
    This adapter accepts trial maps; Country.labels selects stored columns
    and cannot accept these maps (ledger 2 and 5).
    """
    x = _expenditures(x)
    membership = _membership(partition, x.columns)
    result = x.fillna(0).T.groupby(membership, sort=False).sum().T
    result.columns.name = 'j'
    if not np.isfinite(result.to_numpy()).all():
        raise ValueError('Aggregated expenditures overflowed.')
    return result


def _weighted_mean(values, weights):
    positive = weights > 0
    return float(np.average(values.loc[positive], weights=weights.loc[positive]))


def _subset(index, universe, name):
    _cells(index, name)
    if not len(index) or not index.isin(universe).all():
        raise ValueError(f'{name} must be a nonempty subset of its declared population.')
    return index


def evaluate_partitions(x, d, partitions, *, train_households,
                        evaluation_households, reference_cells, comparison_cells,
                        weights, anchor, anchor_loading=1., truth=None,
                        fit_options=None, min_goods=1):
    """Fit supplied partitions on training households and score held-out cells.

    All population and normalization inputs are declared before fitting.
    ``reference_cells`` fixes the weighted score origin using training data;
    ``comparison_cells`` fixes the held-out population for comparable MSE.
    ``weights`` gives nonnegative population weights on every input cell.
    ``truth``, if supplied, is already in the declared anchor units and origin.
    It only enters error reporting, never fitting or normalization.

    A missing comparison score suppresses comparable MSE; the population is
    never silently reduced. Coverage still uses every evaluation cell. Error
    outside the comparison set is descriptive of each candidate's own scored
    subset and must not be compared as if its population were fixed.

    Parameters are estimated by CFEDemands. Point estimates and the model's
    raw conditional fitting variances are retained; no interval for normalized
    held-out scores or adaptively selected partitions is asserted (ledger 4).
    """
    from cfe.regression import Regression

    if not hasattr(Regression, 'score_w'):
        raise RuntimeError('Regression.score_w requires CFEDemands>=0.10.0.')
    x = _expenditures(x)
    if not isinstance(d, pd.DataFrame):
        raise ValueError('d must be a DataFrame of numeric characteristics.')
    _cells(d.index, 'd')
    if not d.columns.is_unique or any(not pd.api.types.is_numeric_dtype(v) for v in d.dtypes):
        raise ValueError('Only unique numeric characteristic columns are supported.')
    d = d.reindex(x.index).astype(float)
    if not partitions:
        raise ValueError('Supply at least one partition.')
    maps = {name: _membership(p, x.columns) for name, p in partitions.items()}
    if anchor not in x.columns or not np.isfinite(anchor_loading) or anchor_loading <= 0:
        raise ValueError('Declare an existing anchor good and a positive finite anchor loading.')
    for membership in maps.values():
        if (membership == membership.loc[anchor]).sum() != 1:
            raise ValueError('The anchor good must remain a singleton in every partition.')

    households = x.index.get_level_values('i')
    train_households, evaluation_households = pd.Index(train_households), pd.Index(evaluation_households)
    for selected in [train_households, evaluation_households]:
        if not selected.is_unique or not selected.isin(households).all() or not len(selected):
            raise ValueError('Declare nonempty sets of existing, unique household identifiers.')
    if len(train_households.intersection(evaluation_households)):
        raise ValueError('Training and evaluation households must be disjoint, including panel rows.')
    train = x.index[households.isin(train_households)]
    evaluation = x.index[households.isin(evaluation_households)]
    reference = _subset(reference_cells, train, 'reference_cells')
    comparison = _subset(comparison_cells, evaluation, 'comparison_cells')
    if not isinstance(weights, pd.Series):
        raise ValueError('weights must be a Series on the input cells.')
    _cells(weights.index, 'weights')
    weights = weights.reindex(x.index).astype(float)
    if not np.isfinite(weights).all() or (weights < 0).any():
        raise ValueError('Every input cell needs a finite nonnegative population weight.')
    for cells in [train, evaluation, reference, comparison]:
        total = weights.loc[cells].sum()
        if not np.isfinite(total) or total <= 0:
            raise ValueError('Each declared population needs positive finite total weight.')
    reference = reference[weights.loc[reference] > 0]
    comparison = comparison[weights.loc[comparison] > 0]
    if truth is not None:
        if not isinstance(truth, pd.Series):
            raise ValueError('truth must be a Series on the evaluation cells.')
        _cells(truth.index, 'truth')
        truth = truth.reindex(evaluation).astype(float)
        if not np.isfinite(truth.loc[weights.loc[evaluation] > 0]).all():
            raise ValueError('Truth must be finite on every positive-weight evaluation cell.')
    fit_options = dict(fit_options or {})
    if set(fit_options) - {'min_obs', 'min_prop_items', 'alltm', 'verbose'}:
        raise ValueError('fit_options may only specify preparation thresholds, alltm, and verbose.')
    if not isinstance(min_goods, (int, np.integer)) or min_goods < 1:
        raise ValueError('min_goods must be a positive integer.')

    candidates, rows, market_rows = {}, {}, []
    for name, membership in maps.items():
        candidate = Candidate(membership=membership.copy())
        candidates[name] = candidate
        row = dict(status='fit_failed', error='', n_groups=membership.nunique(),
                   n_fit_goods=0, n_fit_cells=0, fit_coverage=0., score_coverage=0.,
                   core_complete=False, core_mse=np.nan, core_mean_error=np.nan,
                   outside_core_coverage=0., outside_core_mse=np.nan,
                   scale=np.nan, origin=np.nan, loading_spread=np.nan,
                   nonpositive_loadings=0, n_unidentified_markets=0)
        rows[name] = row
        candidate.scores = pd.Series(np.nan, index=evaluation, name='w')
        try:
            amounts = aggregate_expenditures(x, membership)
            logs = np.log(amounts.where(amounts > 0))
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                model = Regression(y=logs.loc[train], d=d.loc[train], **fit_options)
                candidate.model = model
                if model.y.index.get_level_values('j').nunique() < 2:
                    row.update(status='constant_loadings', n_fit_goods=1,
                               error='Fewer than two goods survived preparation.')
                    continue
                model.get_beta(compute_se=False)
                model.get_w()
                covariance = model.w_cov(min_goods=1)
                candidate.raw_fit_conditional_variance = covariance.var()
                # Reuse the fitted intercept rank check, not global loading
                # spread: single-good-only markets remain unidentified (ledger 4).
                candidate.market_identified = np.isfinite(covariance.V22)
            candidate.warnings = [str(w.message) for w in caught]
            row.update(n_fit_goods=len(model.beta), n_fit_cells=len(model.d),
                       fit_coverage=weights.reindex(model.d.index).sum()/weights.loc[train].sum(),
                       nonpositive_loadings=int((model.beta <= 0).sum()),
                       n_unidentified_markets=int((~candidate.market_identified).sum()))
            observed = model.y.unstack('j').notna().astype('int64')
            candidate.paired_support = observed.T @ observed
            candidate.residual_variance = model.e3.groupby('j', observed=True).var(ddof=0)
            group = membership.loc[anchor]
            if group not in model.beta.index or not np.isfinite(model.beta.loc[group]) or model.beta.loc[group] <= 0:
                row.update(status='anchor_unavailable', error='Anchor loading was dropped, nonfinite, or nonpositive.')
                continue
            if not candidate.market_identified.any():
                row.update(status='unidentified_markets', error='No fitted market separates welfare from its intercept.')
                continue
            unidentified = candidate.market_identified.index[~candidate.market_identified]
            reference_w = model.score_w(logs.loc[reference], d.loc[reference], min_goods=min_goods)
            reference_w = reference_w.where(~reference_w.index.droplevel('i').isin(unidentified))
            if not np.isfinite(reference_w).all():
                row.update(status='reference_unscored', error='The fixed training reference cannot be fully scored.')
                continue
            scale = float(model.beta.loc[group]/anchor_loading)
            origin = _weighted_mean(reference_w, weights.loc[reference])
            raw_w, candidate.scoring = model.score_w(logs.loc[evaluation], d.loc[evaluation],
                                                    min_goods=min_goods, return_diagnostics=True)
            bad_market = raw_w.index.droplevel('i').isin(unidentified)
            raw_w = raw_w.where(~bad_market)
            candidate.scoring.loc[bad_market, 'status'] = 'unidentified_market'
            candidate.scores = (scale*(raw_w-origin)).rename('w')
            available = np.isfinite(candidate.scores)
            candidate.scores = candidate.scores.where(available)
            core_complete = bool(available.loc[comparison].all())
            outside = evaluation[~evaluation.isin(comparison)]
            outside_scored = outside[available.loc[outside] & (weights.loc[outside] > 0)]
            row.update(status='ok', scale=scale, origin=origin,
                       loading_spread=float((model.beta/scale).std(ddof=0)),
                       score_coverage=weights.loc[evaluation][available].sum()/weights.loc[evaluation].sum(),
                       core_complete=core_complete,
                       outside_core_coverage=(weights.loc[outside_scored].sum()/weights.loc[outside].sum()
                                              if weights.loc[outside].sum() else np.nan))
            if truth is not None:
                error = candidate.scores - truth
                if core_complete:
                    row['core_mse'] = _weighted_mean(error.loc[comparison]**2, weights.loc[comparison])
                    row['core_mean_error'] = _weighted_mean(error.loc[comparison], weights.loc[comparison])
                if len(outside_scored):
                    row['outside_core_mse'] = _weighted_mean(error.loc[outside_scored]**2, weights.loc[outside_scored])
        except (ValueError, AssertionError, np.linalg.LinAlgError) as exc:
            row.update(status='fit_failed', error=f'{type(exc).__name__}: {exc}')
        finally:
            if candidate.scoring.empty:
                candidate.scoring = pd.DataFrame({'status': row['status']}, index=evaluation)
            supported = np.isfinite(candidate.scores)
            market = pd.DataFrame({'weight': weights.loc[evaluation],
                                   'scored_weight': weights.loc[evaluation]*supported})
            market = market.groupby(['t', 'm'], observed=True).sum()
            for (t, m), values in market.iterrows():
                market_rows.append((name, t, m, values.weight, values.scored_weight,
                                    values.scored_weight/values.weight if values.weight else np.nan))

    summary = pd.DataFrame.from_dict(rows, orient='index').rename_axis('partition')
    summary['core_rmse'] = np.sqrt(summary.core_mse)
    market = pd.DataFrame(market_rows, columns=['partition', 't', 'm', 'weight', 'scored_weight', 'coverage'])
    market = market.set_index(['partition', 't', 'm'])
    specification = dict(train_cells=train.copy(), evaluation_cells=evaluation.copy(),
                         reference_cells=reference.copy(), comparison_cells=comparison.copy(),
                         weights=weights.copy(), anchor=anchor, anchor_loading=anchor_loading,
                         fit_options=fit_options, min_goods=min_goods)
    return Evaluation(summary, candidates, market, specification)
