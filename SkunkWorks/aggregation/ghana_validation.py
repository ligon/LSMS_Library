"""Empirical checks of supplied GhanaLSS food partitions.

Reuses CFE fitting and frozen scoring; reports reserved-food prediction,
support, and loadings, not error against unobserved true welfare. See
.coder/ledger/ghanalss-aggregate-validation.md. No household outputs are saved.
"""

import argparse
from collections import Counter
from dataclasses import dataclass
import json
import hashlib
from pathlib import Path
import sys
import time
import warnings

import numpy as np
import pandas as pd
import yaml

from .evaluation import aggregate_expenditures, evaluate_partitions
from .ghana_inventory import NONFOOD, _amounts, _delivered_goods, draft_partition


@dataclass
class GhanaData:
    x: pd.DataFrame
    d: pd.DataFrame
    weights: pd.Series
    strata: pd.Series
    psu: pd.Series
    profile: dict


def align_inputs(sample, marketed_sample, characteristics, food, *, wave, basis):
    """Keep every sample household, including missing controls and markets."""
    if sample.index.names != ['i', 't'] or not sample.index.is_unique:
        raise ValueError('Sample needs unique (i,t) rows.')
    if set(sample.index.get_level_values('t')) != {wave}:
        raise ValueError('Use exactly one declared wave.')
    if not marketed_sample.index.droplevel('m').is_unique:
        raise ValueError('Market lookup must be unique by household and wave.')
    markets = pd.Series(marketed_sample.index.get_level_values('m'),
                        index=marketed_sample.index.droplevel('m')).reindex(sample.index)
    unknown = markets.isna()
    if '__unavailable_market__' in markets.values:
        raise ValueError('Reserved missing-market label already occurs in sample.')
    cells = sample.index.to_frame(index=False)
    cells['m'] = markets.fillna('__unavailable_market__').to_numpy()
    index = pd.MultiIndex.from_frame(cells)
    goods = _delivered_goods(food).difference(pd.Index(list(NONFOOD)), sort=False).rename('j')
    amounts = _amounts(food, basis).reindex(columns=goods).fillna(0.)
    outside = amounts.loc[~amounts.index.isin(sample.index)]
    x = amounts.reindex(sample.index, fill_value=0.).set_axis(index).astype(float)
    d = characteristics.reorder_levels(['i', 't', 'm']).reindex(index).astype(float)
    # An unknown market is retained only in the denominator, never fitted as
    # a made-up price regime. It cannot pass complete-controls preparation.
    d.loc[unknown.to_numpy(), :] = np.nan
    weights = sample.weight.astype(float).set_axis(index)
    if not np.isfinite(weights).all() or (weights <= 0).any():
        raise ValueError('This evaluation requires positive finite sample weights.')
    strata = (markets.fillna('__unavailable_market__').astype(str) + '/' +
              sample.Rural.fillna('__unavailable_rural__').astype(str)).set_axis(index)
    psu = sample.v.set_axis(index)
    profile = dict(wave=wave, basis=basis, n_sample=len(sample), n_foods=len(goods),
                   n_missing_market=int(unknown.sum()),
                   n_missing_characteristics=int((~np.isfinite(d).all(axis=1)).sum()),
                   n_positive_households_outside_sample=int(outside.gt(0).any(axis=1).sum()),
                   n_positive_households=int(x.gt(0).any(axis=1).sum()),
                   median_positive_foods=float(x.gt(0).sum(axis=1).median()))
    return GhanaData(x, d, weights, strata, psu, profile)


def load_ghana(wave, basis):
    import lsms_library as ll

    country = ll.Country('GhanaLSS', verbose=False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        sample = country.sample(waves=[wave])
        marketed = country.sample(waves=[wave], market='Region')
        d = country.household_characteristics(waves=[wave], market='Region')
        food = country.food_acquired(waves=[wave], labels='Preferred')
    data = align_inputs(sample, marketed, d, food, wave=wave, basis=basis)
    data.profile['api_warning_counts'] = dict(Counter(type(w.message).__name__ for w in caught
        if not isinstance(w.message, (ResourceWarning, DeprecationWarning))))
    return data


def household_split(strata, seed=20260916):
    """Fixed 60/20/20 household split, within Region x Rural strata."""
    if strata.index.get_level_values('i').duplicated().any():
        raise ValueError('The one-wave split requires one row per household.')
    result = pd.Series(index=strata.index, dtype=object, name='split')
    rng = np.random.default_rng(seed)
    for _, values in strata.sort_index().groupby(strata, sort=True):
        order = rng.permutation(len(values))
        n_train, n_validation = int(.6*len(values)), int(.2*len(values))
        if min(n_train, n_validation, len(values)-n_train-n_validation) < 1:
            raise ValueError('Each stratum needs enough households for all three splits.')
        result.loc[values.index[order[:n_train]]] = 'train'
        result.loc[values.index[order[n_train:n_train+n_validation]]] = 'validation'
        result.loc[values.index[order[n_train+n_validation:]]] = 'test'
    return result


def candidate_maps(goods, specification, reserved=()):
    """Offer separate merges and the original combined draft, without search."""
    identity = pd.Series(goods, index=goods, name='group')
    maps = {'fine': identity}
    skipped = {}
    def text_evidence(group):
        group = dict(group)
        group['evidence'] = [e if isinstance(e, str) else json.dumps(e, sort_keys=True)
                             for e in group['evidence']]
        return group

    for group in specification['groups'] + specification.get('deferred_alternatives', []):
        group = text_evidence(group)
        if set(group['members']).intersection(reserved):
            skipped[group['id']] = 'Touches a reserved prediction target.'
            continue
        available = set(group['members']).intersection(goods)
        if len(available) < 2:
            skipped[group['id']] = 'Fewer than two delivered members in this wave.'
            continue
        maps[group['id']], _, _ = draft_partition(goods, {'groups': [group]})
    active = [text_evidence(g) for g in specification['groups']
              if not set(g['members']).intersection(reserved)]
    maps['draft'], _, _ = draft_partition(goods, {'groups': active})
    return maps, skipped


def evaluation_arguments(data, split, *, evaluation='validation', reserved=('Onion', 'Egg'),
                         anchor='Salt', min_obs=30, min_prop_items=.1, min_goods=1):
    if evaluation not in ('validation', 'test'):
        raise ValueError('Use a declared validation or test split.')
    if anchor in reserved or anchor not in data.x:
        raise ValueError('The anchor must remain available for score construction.')
    if set(reserved).difference(data.x.columns):
        raise ValueError('Every reserved prediction target must be delivered.')
    complete = np.isfinite(data.d).all(axis=1)
    reference = data.x.index[(split == 'train') & complete & data.x[anchor].gt(0)]
    comparison = data.x.index[(split == evaluation) & complete & data.x[anchor].gt(0)]
    return dict(x=data.x.drop(columns=list(reserved)), d=data.d,
                train_households=data.x.index.get_level_values('i')[split == 'train'],
                evaluation_households=data.x.index.get_level_values('i')[split == evaluation],
                reference_cells=reference, comparison_cells=comparison,
                weights=data.weights, anchor=anchor, min_goods=min_goods,
                fit_options=dict(min_obs=min_obs, min_prop_items=min_prop_items, alltm=False))


def _design(data, cells, scores):
    # Standard WLS prediction diagnostic. Demographic definitions and market
    # effects are the same for every partition; all coefficients use training.
    markets = pd.get_dummies(data.x.index.get_level_values('m'), dtype=float)
    markets.index = data.x.index
    design = data.d.join(markets.add_prefix('market:'))
    design['w'] = scores.reindex(design.index)
    return design.reindex(cells)


def prediction_diagnostics(data, args, candidate, reserved, *, return_errors=False):
    """Predict positive reserved-food logs without including them in scores."""
    from .evaluation import _weighted_mean

    if candidate.model is None or not np.isfinite(candidate.scores).any():
        return (pd.DataFrame(), {}) if return_errors else pd.DataFrame()
    amounts = aggregate_expenditures(args['x'], candidate.membership)
    logs = np.log(amounts.where(amounts > 0))
    train = data.x.index[data.x.index.get_level_values('i').isin(args['train_households'])]
    # Score with the declared cutoff, never score_w's default: since
    # CFEDemands 0.10 that default is the fit's own floor(min_prop_items*J)+1,
    # which on these fits (J in the eighties) is 9 rather than 1.  Centering
    # on a different subsample from the one evaluate_partitions scored would
    # silently move the origin.
    train_scores = candidate.model.score_w(logs.loc[train], data.d.loc[train],
                                           min_goods=args['min_goods'])
    bad_markets = candidate.market_identified.index[~candidate.market_identified]
    train_scores = train_scores.where(~train_scores.index.droplevel('i').isin(bad_markets))
    # Put training scores in the same anchor units and origin as evaluation
    # scores. Both calibration and normalization use training data only.
    origin = _weighted_mean(train_scores.loc[args['reference_cells']],
                            data.weights.loc[args['reference_cells']])
    scale = float(candidate.model.beta.loc[candidate.membership.loc[args['anchor']]])
    scores = pd.concat([scale*(train_scores-origin), candidate.scores])
    design = _design(data, scores.index, scores)
    rows, errors = [], {}
    for target in reserved:
        observed = data.x[target].gt(0)
        calibration = train[observed.loc[train] & np.isfinite(design.reindex(train)).all(axis=1)]
        population = args['comparison_cells'][observed.loc[args['comparison_cells']]]
        y = np.log(data.x.loc[calibration, target])
        weights = data.weights.loc[calibration].to_numpy()
        X = design.loc[calibration].to_numpy()
        evaluation_design = design.reindex(population).to_numpy()
        prediction = pd.Series(np.nan, index=population)
        rank = 0
        if len(calibration):
            weighted_design = X*np.sqrt(weights[:, None])
            coeff, _, rank, _ = np.linalg.lstsq(weighted_design,
                                               y.to_numpy()*np.sqrt(weights), rcond=None)
            # Collinearity need not prevent prediction, but extrapolation
            # outside the training row space is not identified by that fit.
            _, _, vt = np.linalg.svd(weighted_design, full_matrices=False)
            projection = vt[:rank].T @ vt[:rank]
            distance = np.linalg.norm(evaluation_design-evaluation_design@projection, axis=1)
            estimable = (distance <= 1e-10*(1.+np.linalg.norm(evaluation_design, axis=1)))
            prediction.loc[estimable] = evaluation_design[estimable] @ coeff
        complete = bool(len(population) and np.isfinite(prediction).all())
        row = dict(target=target, n_calibration=len(calibration), rank=int(rank),
                   n_evaluation=len(population), complete=complete, mse=np.nan,
                   n_estimable=int(prediction.notna().sum()), mean_error=np.nan)
        if complete:
            error = prediction - np.log(data.x.loc[population, target])
            errors[target] = error
            row.update(mse=_weighted_mean(error**2, data.weights.loc[population]),
                       mean_error=_weighted_mean(error, data.weights.loc[population]))
        rows.append(row)
    frame = pd.DataFrame(rows)
    return (frame, errors) if return_errors else frame


def paired_prediction_difference(error, baseline, data, *, evaluation_cells,
                                  draws=1000, seed=20260918):
    """Validation-PSU resampling sensitivity, conditional on the training fits."""
    if (not error.index.equals(baseline.index) or not np.isfinite(error).all()
            or not np.isfinite(baseline).all() or not error.index.isin(evaluation_cells).all()):
        raise ValueError('Compare finite errors on the same predeclared population.')
    weight = data.weights.reindex(error.index)
    delta = error**2-baseline**2
    # Zero-contribution PSUs still belong to the evaluation survey sample.
    # Dropping them would condition resampling on positive target reporting.
    frame = pd.DataFrame({'weight': weight.reindex(evaluation_cells, fill_value=0.),
                          'loss': (weight*delta).reindex(evaluation_cells, fill_value=0.),
                          'stratum': data.strata.reindex(evaluation_cells),
                          'psu': data.psu.reindex(evaluation_cells)})
    if frame.psu.isna().any():
        raise ValueError('Prediction resampling requires known PSUs.')
    blocks = frame.groupby(['stratum', 'psu'])[['weight', 'loss']].sum()
    rng = np.random.default_rng(seed)
    numerator, denominator = np.zeros(draws), np.zeros(draws)
    for _, values in blocks.groupby('stratum'):
        selected = rng.integers(0, len(values), size=(draws, len(values)))
        numerator += values.loss.to_numpy()[selected].sum(axis=1)
        denominator += values.weight.to_numpy()[selected].sum(axis=1)
    usable = denominator > 0
    distribution = numerator[usable]/denominator[usable]
    return dict(delta_mse=float(np.average(delta, weights=weight)),
                delta_q025=float(np.quantile(distribution, .025)) if len(distribution) else np.nan,
                delta_q975=float(np.quantile(distribution, .975)) if len(distribution) else np.nan,
                n_validation_psus=len(blocks), n_target_psus=int((blocks.weight > 0).sum()),
                n_zero_weight_draws=int((~usable).sum()))


def run_comparison(data, specification, *, names=None, seed=20260916,
                   evaluation='validation', reserved=('Onion', 'Egg'),
                   output=None, min_obs=30, min_prop_items=.1):
    split = household_split(data.strata, seed)
    args = evaluation_arguments(data, split, evaluation=evaluation, reserved=reserved,
                                min_obs=min_obs, min_prop_items=min_prop_items)
    maps, skipped = candidate_maps(args['x'].columns, specification, reserved)
    if names is not None:
        unknown = set(names).difference(maps)
        if unknown:
            raise ValueError(f'Unavailable candidate names: {sorted(unknown)}; skipped: {skipped}')
        ordered = (['fine'] if 'fine' in names else []) + [name for name in names if name != 'fine']
        maps = {name: maps[name] for name in ordered}
    results, predictions, loadings, support = [], [], [], []
    baseline_errors = {}
    for name, membership in maps.items():
        start = time.monotonic()
        result = evaluate_partitions(**args, partitions={name: membership})
        summary = result.summary.copy()
        candidate = result.candidates[name]
        summary['seconds'] = time.monotonic()-start
        summary['n_scored'] = candidate.scores.notna().sum()
        summary['n_evaluation'] = len(candidate.scores)
        summary['n_warnings'] = len(candidate.warnings)
        if summary.loc[name, 'status'] == 'ok':
            ref = candidate.model.score_w(
                np.log(aggregate_expenditures(args['x'].loc[args['reference_cells']],
                                              membership).replace(0, np.nan)),
                data.d.loc[args['reference_cells']], min_goods=args['min_goods'])
            ref = summary.loc[name, 'scale']*(ref-summary.loc[name, 'origin'])
            summary['reference_q05'] = ref.quantile(.05)
            summary['reference_q95'] = ref.quantile(.95)
            predicted, errors = prediction_diagnostics(data, args, candidate, reserved, return_errors=True)
            if name == 'fine':
                baseline_errors = errors
            for target in errors.keys() & baseline_errors.keys():
                difference = paired_prediction_difference(errors[target], baseline_errors[target], data,
                                                           evaluation_cells=candidate.scores.index)
                for field, value in difference.items():
                    predicted.loc[predicted.target == target, field] = value
            predicted.insert(0, 'partition', name)
            predictions.append(predicted)
        if candidate.model is not None and candidate.model.beta is not None:
            b = candidate.model.beta.to_frame('beta_raw')
            b['beta_anchor_units'] = b.beta_raw / b.loc[args['anchor'], 'beta_raw'] if args['anchor'] in b.index else np.nan
            b['residual_variance'] = candidate.residual_variance
            b['n_fit_observations'] = candidate.model.y.groupby('j').size()
            b['partition'] = name
            loadings.append(b.reset_index())
        if candidate.model is not None and candidate.model.preparation is not None:
            counts = candidate.model.preparation['goods'].copy()
            counts['partition'] = name
            support.append(counts.reset_index())
        results.append(summary.reset_index())
        print(f'{data.profile["wave"]} {data.profile["basis"]} {name}: '
              f'{summary.loc[name, "status"]}, {summary.loc[name, "n_fit_goods"]} fit goods, '
              f'{summary.loc[name, "score_coverage"]:.4f} score coverage, '
              f'{summary.loc[name, "seconds"]:.1f}s', file=sys.stderr, flush=True)
        if output is not None:
            output.mkdir(parents=True, exist_ok=True)
            for filename, frames in [('summary', results), ('prediction', predictions),
                                      ('loadings', loadings), ('preparation', support)]:
                if frames:
                    pd.concat(frames, ignore_index=True).to_csv(output / f'{filename}.csv', index=False)
    metadata = dict(**data.profile, seed=seed, evaluation=evaluation, reserved=list(reserved),
                    anchor=args['anchor'], min_obs=min_obs, min_prop_items=min_prop_items,
                    split_counts=split.value_counts().to_dict(), skipped_candidates=skipped,
                    partitions=list(maps), lsms_revision='a02edf4c4', cfe_revision='3cdfae9')
    metadata['candidate_membership_sha256'] = hashlib.sha256(
        yaml.safe_dump({name: membership.to_dict() for name, membership in maps.items()},
                       sort_keys=True).encode()).hexdigest()
    if output is not None:
        (output / 'specification.yml').write_text(yaml.safe_dump(metadata, sort_keys=False))
    return pd.concat(results, ignore_index=True), metadata


def resample_training_rows(data, train, rng):
    """Sample PSUs within Region x Rural, keeping each training block intact."""
    frame = pd.DataFrame({'stratum': data.strata.loc[train], 'psu': data.psu.loc[train],
                          'position': np.arange(len(train))})
    if frame.psu.isna().any():
        raise ValueError('Cluster bootstrap requires every training household to have a PSU.')
    positions = []
    for _, stratum in frame.groupby('stratum', sort=True):
        blocks = [g.position.to_numpy() for _, g in stratum.groupby('psu', sort=True)]
        for selected in rng.integers(0, len(blocks), size=len(blocks)):
            positions.extend(blocks[selected])
    return np.asarray(positions, dtype=int)


def bootstrap_loadings(data, *, draws=60, seed=20260916, reserved=('Onion', 'Egg'), output=None):
    """Refit the fine model in joint cluster draws; do not nest beta bootstrap.

    The saved coefficient draws contain no household identifiers. Selection
    is repeated, so an absent loading stays absent rather than being filled.
    """
    from cfe.regression import Regression

    split = household_split(data.strata, seed)
    args = evaluation_arguments(data, split, reserved=reserved)
    train = data.x.index[split == 'train']
    logs = np.log(args['x'].where(args['x'] > 0)).loc[train]
    d = data.d.loc[train]
    rng = np.random.default_rng(seed+1)
    coefficients, rows = [], []
    if output is not None:
        output.mkdir(parents=True, exist_ok=True)
    for draw in range(draws):
        start = time.monotonic()
        positions = resample_training_rows(data, train, rng)
        cells = train.take(positions).to_frame(index=False)
        cells['i'] = np.arange(len(cells))
        index = pd.MultiIndex.from_frame(cells)
        row = dict(draw=draw, status='failed', n_cells=len(cells), n_goods=0, seconds=0., error='')
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                model = Regression(y=logs.iloc[positions].set_axis(index),
                                   d=d.iloc[positions].set_axis(index), **args['fit_options'])
                b = model.get_beta(compute_se=False)
            if args['anchor'] not in b or b[args['anchor']] <= 0:
                raise ValueError('Positive anchor loading unavailable.')
            coefficients.append((b / b[args['anchor']]).rename(draw))
            row.update(status='ok', n_goods=len(b), n_warnings=len(caught))
        except (ValueError, AssertionError, np.linalg.LinAlgError) as exc:
            row['error'] = str(exc)
        row['seconds'] = time.monotonic()-start
        rows.append(row)
        if output is not None:
            pd.DataFrame(coefficients).rename_axis('draw').to_csv(output/'beta_resamples.csv')
            pd.DataFrame(rows).to_csv(output/'bootstrap_status.csv', index=False)
        if (draw+1) % 5 == 0 or draw+1 == draws:
            print(f'Bootstrap {draw+1}/{draws}; {len(coefficients)} successful draws; '
                  f'last {row["seconds"]:.1f}s', file=sys.stderr, flush=True)
    if output is not None:
        metadata = dict(**data.profile, draws=draws, seed=seed, resample_seed=seed+1,
                        resampling='PSUs within Region x Rural, training households only',
                        reserved=list(reserved), anchor=args['anchor'], fit_options=args['fit_options'])
        (output/'bootstrap_specification.yml').write_text(yaml.safe_dump(metadata, sort_keys=False))
    return pd.DataFrame(coefficients), pd.DataFrame(rows)


def probe_loadings(data, members, *, seed=20260916, reserved=('Onion', 'Egg'),
                   draws=0, output=None):
    """Fit each candidate member with the same dense reference foods.

    This diagnoses loadings excluded by the large all-pairs covariance
    requirement. Every fit still uses unmodified CFE preparation/estimation.
    Distinct members need not have 30 observations in common with each other;
    any comparison relies on the common-factor interpretation of the shared
    reference foods. Joint PSU draws include that calibration uncertainty.
    """
    from cfe.regression import Regression

    split = household_split(data.strata, seed)
    args = evaluation_arguments(data, split, reserved=reserved)
    members = sorted(set(members).intersection(args['x'].columns)-{args['anchor']})
    train = data.x.index[split == 'train']
    counts = args['x'].loc[train].gt(0).sum().drop(members).drop(args['anchor'])
    reference = sorted(counts.nlargest(8).index.tolist()+[args['anchor']])
    logs = np.log(args['x'].where(args['x'] > 0)).loc[train]
    d = data.d.loc[train]
    rng = np.random.default_rng(seed+1)
    rows = []
    if output is not None:
        output.mkdir(parents=True, exist_ok=True)
    for draw in range(draws+1):
        start = time.monotonic()
        positions = (np.arange(len(train)) if draw == 0 else resample_training_rows(data, train, rng))
        cells = train.take(positions).to_frame(index=False)
        cells['i'] = np.arange(len(cells))
        index = pd.MultiIndex.from_frame(cells)
        D = d.iloc[positions].set_axis(index)
        for member in members:
            row = dict(draw=draw, j=member, status='failed', beta_anchor_units=np.nan,
                       n_observations=0, n_fit_reference=0, error='')
            try:
                with warnings.catch_warnings(record=True):
                    warnings.simplefilter('always')
                    model = Regression(y=logs[reference+[member]].iloc[positions].set_axis(index),
                                       d=D, **args['fit_options'])
                    beta = model.get_beta(compute_se=False)
                row['n_fit_reference'] = len(set(reference).intersection(beta.index))
                if member not in beta:
                    row['status'] = 'member_unavailable'
                elif args['anchor'] not in beta or beta[args['anchor']] <= 0:
                    row['status'] = 'anchor_unavailable'
                elif row['n_fit_reference'] != len(reference):
                    row['status'] = 'reference_incomplete'
                else:
                    row.update(status='ok', beta_anchor_units=float(beta[member]/beta[args['anchor']]),
                               n_observations=int(model.y.index.get_level_values('j').isin([member]).sum()))
            except (ValueError, AssertionError, np.linalg.LinAlgError) as exc:
                row['error'] = str(exc)
            rows.append(row)
        if output is not None:
            pd.DataFrame(rows).to_csv(output/'probe_loadings.csv', index=False)
            metadata = dict(**data.profile, seed=seed, resample_seed=seed+1, draws=draws,
                            reserved=list(reserved), reference=reference, members=members,
                            anchor=args['anchor'], fit_options=args['fit_options'],
                            draw_zero='Original training data; positive draws resample PSUs within strata.')
            (output/'probe_specification.yml').write_text(yaml.safe_dump(metadata, sort_keys=False))
        print(f'Probe draw {draw}/{draws}: {len(members)} members in '
              f'{time.monotonic()-start:.1f}s', file=sys.stderr, flush=True)
    return pd.DataFrame(rows)


def simulation_template(data, specification):
    """Preserve every Ghana mask; the reused DGP supplies synthetic controls.

    This experiment conditions on observed positivity, not recorded amounts,
    and cannot validate selection depending on unobserved true welfare.
    """
    from .survey_simulation import SurveyTemplate

    if data.profile.get('n_missing_market', 0):
        raise ValueError('The Ghana mask experiment needs known markets for every household.')
    membership = candidate_maps(data.x.columns, specification)[0]['draft']
    cells = data.x.index.to_frame(index=False)
    cells['i'] = np.arange(len(cells))
    index = pd.MultiIndex.from_frame(cells)
    return SurveyTemplate(data.x.gt(0).set_axis(index), data.weights.set_axis(index),
                          membership, dict(data.profile))


def simulate_selected(data, specification, output, seeds=(11, 12, 13)):
    from .survey_simulation import REGIMES, run_template

    template = simulation_template(data, specification)
    output.mkdir(parents=True, exist_ok=True)
    rows = []
    for seed in seeds:
        rows.append(run_template(template, seeds=[seed], regimes=REGIMES))
        pd.concat(rows, ignore_index=True).to_csv(output/'simulation.csv', index=False)
    metadata = dict(**data.profile, seeds=list(seeds), regimes=list(REGIMES),
                    source='survey_simulation.survey_inputs; cfe.dgp.expenditures',
                    masks='All sampled households; synthetic controls and welfare independent of mask.',
                    anchor='Salt', anchor_loading=.6, sigma_eps=.2,
                    fit_options={'min_obs': 30, 'min_prop_items': .1, 'alltm': True})
    (output/'simulation_specification.yml').write_text(yaml.safe_dump(metadata, sort_keys=False))
    return pd.concat(rows, ignore_index=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wave', default='2016-17')
    parser.add_argument('--basis', choices=['purchased', 'total'], default='purchased')
    parser.add_argument('--seed', type=int, default=20260916)
    parser.add_argument('--evaluation', choices=['validation', 'test'], default='validation')
    parser.add_argument('--names', nargs='+')
    parser.add_argument('--reserved', nargs='*', default=['Onion', 'Egg'])
    parser.add_argument('--bootstrap-only', type=int, metavar='DRAWS')
    parser.add_argument('--probe-members', nargs='+')
    parser.add_argument('--probe-draws', type=int, default=0)
    parser.add_argument('--simulate', action='store_true')
    parser.add_argument('--specification', type=Path,
                        default=Path(__file__).parent/'ghanalss'/'proposed_groups.yml')
    parser.add_argument('--additional', type=Path, action='append', default=[])
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    data = load_ghana(args.wave, args.basis)
    if args.bootstrap_only is not None:
        _, status = bootstrap_loadings(data, draws=args.bootstrap_only, seed=args.seed,
                                       reserved=args.reserved, output=args.output)
        return 0 if status.status.eq('ok').all() else 1
    if args.probe_members is not None:
        probe_loadings(data, args.probe_members, seed=args.seed, reserved=args.reserved,
                       draws=args.probe_draws, output=args.output)
        return 0
    specification = yaml.safe_load(args.specification.read_text())
    for path in args.additional:
        extra = yaml.safe_load(path.read_text())
        specification.setdefault('deferred_alternatives', []).extend(extra['groups'])
    if args.simulate:
        result = simulate_selected(data, specification, args.output)
        return 0 if result.status.eq('ok').all() else 1
    result, _ = run_comparison(data, specification, names=args.names, seed=args.seed,
                               evaluation=args.evaluation, reserved=args.reserved, output=args.output)
    return 0 if result.status.eq('ok').all() else 1


if __name__ == '__main__':
    raise SystemExit(main())
