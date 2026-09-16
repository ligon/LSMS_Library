"""Known-welfare simulations with complete Uganda household observation masks.

Run from LSMS with the CFE feature checkout on PYTHONPATH. Only aggregate
summaries are written; survey household masks and identifiers stay local.
See .coder/ledger/cfe-aggregation-evaluation.md, sections 2--5.
"""

import argparse
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
import warnings

import numpy as np
import pandas as pd

from .evaluation import aggregate_expenditures, evaluate_partitions


REGIMES = ('masked', 'substitutes', 'unequal')
NONFOOD = ('Cigarettes', 'Other Tobacco')


@dataclass
class SurveyTemplate:
    mask: pd.DataFrame
    weights: pd.Series
    membership: pd.Series
    profile: dict


def curated_membership(goods, table):
    """Use unambiguous stored groups; report and isolate conflicting labels.

    A trial partition, not a claim of economic admissibility. Prefixes prevent
    an isolated ambiguous label from accidentally joining a named group.
    """
    mapping, conflicts = {}, []
    for good in goods:
        targets = table.loc[table['Preferred Label'] == good, 'Aggregate Label']
        targets = targets.dropna().unique()
        if not len(targets):
            raise ValueError(f'No curated aggregate label for {good!r}.')
        if len(targets) > 1:
            conflicts.append(good)
            mapping[good] = f'fine:{good}'
        else:
            mapping[good] = f'aggregate:{targets[0]}'
    return pd.Series(mapping, name='group').rename_axis('j'), conflicts


def template_from_frames(sample, food, universe, table, *, wave, basis):
    """Preserve the sample denominator and whole masks (ledger 3--4).

    ``universe`` is the delivered Preferred universe from total recorded food
    acquisition in this wave, before selecting a purchased/total basis. Foods
    absent from the delivered table altogether are outside this universe.
    """
    if sample.index.names != ['i', 't'] or not sample.index.is_unique:
        raise ValueError('sample must have unique (i,t) rows.')
    if set(sample.index.get_level_values('t')) != {wave}:
        raise ValueError('Use one declared survey wave per template.')
    goods = pd.Index(sorted(set(universe) - set(NONFOOD)), name='j')
    if not len(goods):
        raise ValueError('The delivered food universe is empty.')
    amounts = food['Expenditure'].groupby(level=['i', 't', 'j'], observed=True,
                                          dropna=False).sum(min_count=1).unstack('j')
    if amounts.columns.hasnans:
        raise ValueError('Food labels cannot be missing.')
    if set(amounts.columns) - set(universe):
        raise ValueError('The declared universe omits delivered labels.')
    outside = int((~amounts.index.isin(sample.index)).sum())
    amounts = amounts.reindex(index=sample.index, columns=goods).astype(float)
    if np.isinf(amounts.to_numpy()).any() or (amounts < 0).any().any():
        raise ValueError('Delivered food expenditures must be nonnegative and finite or unavailable.')
    mask = amounts.fillna(0).gt(0)
    counts = mask.sum(axis=1)
    paired = mask.astype('int64').T @ mask.astype('int64')
    off_diagonal = paired.to_numpy()[np.triu_indices(len(goods), k=1)]
    weights = sample['weight'].astype(float)
    valid_weight = np.isfinite(weights) & (weights > 0)
    known_market = sample['Region'].notna()
    usable = valid_weight & known_market
    if not usable.any():
        raise ValueError('No sample households have a known market and positive weight.')
    membership, conflicts = curated_membership(goods, table)
    profile = dict(wave=wave, basis=basis, n_sample=len(sample),
                   n_delivered_labels=len(set(universe)), n_food_goods=len(goods),
                   n_positive_goods=int(mask.any().sum()),
                   n_food_households=int((counts > 0).sum()),
                   median_positive_goods=float(counts.median()),
                   p10_positive_goods=float(counts.quantile(.1)),
                   p90_positive_goods=float(counts.quantile(.9)),
                   entry_density=float(mask.to_numpy().mean()),
                   full_sample_median_pair_count=(float(np.median(off_diagonal))
                                                  if len(off_diagonal) else np.nan),
                   full_sample_fraction_pairs_ge30=(float((off_diagonal >= 30).mean())
                                                    if len(off_diagonal) else np.nan),
                   n_missing_region=int((~known_market).sum()),
                   n_invalid_weight=int((~valid_weight).sum()),
                   n_food_households_outside_sample=outside,
                   n_simulation_households=int(usable.sum()),
                   n_groups=int(membership.nunique()),
                   ambiguous_labels='; '.join(conflicts))
    # Survey identifiers are unnecessary for this one-wave experiment. Retain
    # each whole row, its market, and its weight, replacing only its identifier.
    cells = pd.MultiIndex.from_arrays(
        [np.arange(usable.sum()), sample.index.get_level_values('t')[usable],
         sample.loc[usable, 'Region'].astype(str)], names=['i', 't', 'm'])
    mask = mask.loc[usable].set_axis(cells)
    weights = weights.loc[usable].set_axis(cells).rename('weight')
    return SurveyTemplate(mask, weights, membership, profile)


def load_uganda(wave='2019-20', basis='total'):
    """Load through Country, including its existing extraction limitations."""
    import lsms_library as ll

    if basis not in ('purchased', 'total'):
        raise ValueError('Use purchased or total recorded acquisition.')
    country = ll.Country('Uganda', verbose=False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        sample = country.sample(waves=[wave])
        total = country.food_expenditures(waves=[wave], basis='total', labels='Preferred')
        food = (total if basis == 'total' else
                country.food_expenditures(waves=[wave], basis=basis, labels='Preferred'))
    template = template_from_frames(
        sample, food, total.index.get_level_values('j').unique(),
        country.categorical_mapping['harmonize_food'], wave=wave, basis=basis)
    # Do not include individual identifiers from library audit messages in
    # public outputs. Retain warning classes and counts, not silent suppression.
    audits = Counter(type(w.message).__name__ for w in caught
                     if not isinstance(w.message, (ResourceWarning, DeprecationWarning)))
    template.profile['api_warnings'] = '; '.join(f'{k}:{v}' for k, v in sorted(audits.items()))
    return template


def survey_inputs(template, seed=11, regime='masked', sigma_eps=.2, anchor='Salt',
                  min_obs=30, min_prop_items=.1):
    """Reuse the CFE DGP; impose exactly the observed joint mask (ledger 5).

    Welfare and one complete numeric control are synthetic and independent of
    the mask. The primary ``masked`` regime retains homoskedastic fine-good log
    errors. ``substitutes`` divides amounts by each row's positive member count
    within a curated group, holding its no-error baseline total fixed when
    within-group betas agree. ``unequal`` uses that same substitution mechanism
    with within-group betas ranging from 0.5 to 1.5 times the group loading.
    """
    from cfe.dgp import expenditures

    if regime not in REGIMES:
        raise ValueError(f'Unknown regime: {regime}.')
    mask, membership = template.mask, template.membership
    if anchor not in membership or (membership == membership.loc[anchor]).sum() != 1:
        raise ValueError('The declared anchor must be a singleton in the curated map.')
    n, j = mask.shape
    groups = sorted(membership.unique())
    group_beta = pd.Series(np.random.default_rng(2718).permutation(
        np.linspace(.4, 2., len(groups))), index=groups)
    beta = membership.map(group_beta).astype(float)
    if regime == 'unequal':
        for group in groups:
            goods = membership.index[membership == group]
            if len(goods) > 1:
                beta.loc[goods] *= np.linspace(.5, 1.5, len(goods))
    beta.loc[anchor] = .6
    prices = pd.Series(1., index=pd.MultiIndex.from_product(
        [[0], [0], range(j)], names=['t', 'm', 'i']))
    state = np.random.get_state()
    try:
        np.random.seed(seed)
        generated, latent = expenditures(n, 1, 1, j, 1, beta.to_numpy(),
                                          sigma_eps=sigma_eps, p=prices)
    finally:
        np.random.set_state(state)
    # Generate one synthetic cell per actual row, not a household x market
    # Cartesian panel. Reassign market labels after generation; prices are one.
    x = pd.DataFrame(generated.to_numpy().reshape(n, j), index=mask.index, columns=mask.columns)
    x = x.where(mask, 0.)
    if regime in ('substitutes', 'unequal'):
        count = mask.T.groupby(membership).sum().T
        divisors = count.reindex(columns=membership.to_numpy()).to_numpy()
        x /= np.maximum(divisors, 1)
    d = np.log(latent['characteristics']).set_axis(mask.index)
    w = pd.Series(-np.log(latent['lambdas'].to_numpy()), index=mask.index, name='w')
    split = np.random.default_rng(seed+1000).permutation(n)
    train, evaluation = split[:int(.6*n)], split[int(.6*n):]
    household = mask.index.get_level_values('i')
    reference = mask.index[household.isin(train) & mask[anchor]]
    comparison = mask.index[household.isin(evaluation) & mask[anchor]]
    if not len(reference) or not len(comparison):
        raise ValueError('The split needs anchor observations in training and evaluation.')
    # Use a common anchor-observed population, never a post-fit intersection.
    truth = w - np.average(w.loc[reference], weights=template.weights.loc[reference])
    return dict(x=x, d=d, partitions={'fine': {g: g for g in mask.columns},
                                      'curated': membership},
                train_households=train, evaluation_households=evaluation,
                reference_cells=reference, comparison_cells=comparison,
                weights=template.weights, anchor=anchor, anchor_loading=.6, truth=truth,
                fit_options={'min_obs': min_obs, 'min_prop_items': min_prop_items, 'alltm': True})


def load_legacy_preparation(revision='e4fc38f'):
    """Read the original selector from the CFE checkout for a historical check.

    No copied or alternate selection algorithm: execute only the named function
    from the trusted, explicit local git revision, with its existing helpers.
    This is optional analysis machinery; production imports remain unchanged.
    """
    import cfe.regression as regression

    root = Path(regression.__file__).resolve().parents[1]
    source = subprocess.check_output(
        ['git', '-C', str(root), 'show', f'{revision}:Empirics/regression.org'], text=True)
    start = source.index('\ndef prepare_data(')+1
    end = source.index('\ndef find_optimal_K', start)
    namespace = vars(regression).copy()
    exec(compile(source[start:end], f'{revision}:prepare_data', 'exec'), namespace)
    return namespace['prepare_data']


def run_template(template, seeds=(11, 12, 13), regimes=REGIMES, legacy_prepare=None):
    """Refit each replicate and retain failures; never select a winning map."""
    rows = []
    for seed in seeds:
        for regime in regimes:
            args = survey_inputs(template, seed=seed, regime=regime)
            result = evaluate_partitions(**args)
            frame = result.summary.copy()
            train = result.specification['train_cells']
            evaluation = result.specification['evaluation_cells']
            weight = args['weights']
            for name, candidate in result.candidates.items():
                amounts = aggregate_expenditures(args['x'], candidate.membership)
                logs = np.log(amounts.where(amounts > 0))
                frame.loc[name, 'n_train'] = len(train)
                frame.loc[name, 'n_evaluation'] = len(evaluation)
                frame.loc[name, 'n_comparison'] = len(args['comparison_cells'])
                frame.loc[name, 'comparison_weight_share'] = (
                    weight.loc[args['comparison_cells']].sum()/weight.loc[evaluation].sum())
                frame.loc[name, 'n_scored'] = candidate.scores.notna().sum()
                frame.loc[name, 'n_warnings'] = len(candidate.warnings)
                if frame.loc[name, 'status'] == 'ok':
                    scores = candidate.model.score_w(logs.loc[train], args['d'].loc[train])
                    bad = candidate.market_identified.index[~candidate.market_identified]
                    available = np.isfinite(scores) & ~scores.index.droplevel('i').isin(bad)
                    frame.loc[name, 'train_score_coverage'] = weight.loc[train][available].sum()/weight.loc[train].sum()
                    frame.loc[name, 'n_train_scored'] = available.sum()
                if legacy_prepare is not None:
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter('ignore')
                            old_y, _ = legacy_prepare(logs.loc[train].stack('j').dropna(),
                                                      args['d'].loc[train], **args['fit_options'])
                        old_cells = old_y.index.droplevel('j').unique()
                        frame.loc[name, 'legacy_fit_coverage'] = weight.loc[old_cells].sum()/weight.loc[train].sum()
                        frame.loc[name, 'legacy_n_fit_cells'] = len(old_cells)
                        frame.loc[name, 'legacy_n_fit_goods'] = old_y.index.get_level_values('j').nunique()
                        frame.loc[name, 'legacy_error'] = ''
                    except (ValueError, AssertionError, RecursionError) as exc:
                        frame.loc[name, 'legacy_error'] = str(exc)
            frame = frame.reset_index()
            for name, value in [('regime', regime), ('seed', seed),
                                ('basis', template.profile['basis']), ('wave', template.profile['wave'])]:
                frame.insert(0, name, value)
            rows.append(frame)
            print(f"{template.profile['wave']} {template.profile['basis']} seed={seed} "
                  f"{regime}: {result.summary.status.to_dict()}", file=sys.stderr, flush=True)
    return pd.concat(rows, ignore_index=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--waves', nargs='+', default=['2019-20'])
    parser.add_argument('--bases', nargs='+', choices=['purchased', 'total'], default=['purchased', 'total'])
    parser.add_argument('--seeds', nargs='+', type=int, default=[11, 12, 13])
    parser.add_argument('--regimes', nargs='+', choices=REGIMES, default=list(REGIMES))
    parser.add_argument('--profile-only', action='store_true')
    parser.add_argument('--legacy-revision', help='Optional trusted local CFE revision, e.g. e4fc38f.')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--profile-output', type=Path, required=True)
    args = parser.parse_args()
    legacy = load_legacy_preparation(args.legacy_revision) if args.legacy_revision else None
    profiles, results = [], []
    for wave in args.waves:
        for basis in args.bases:
            template = load_uganda(wave, basis)
            profiles.append(template.profile)
            if not args.profile_only:
                results.append(run_template(template, args.seeds, args.regimes, legacy))
                pd.concat(results, ignore_index=True).to_csv(args.output, index=False)
            pd.DataFrame(profiles).to_csv(args.profile_output, index=False)
    return 0 if not results or all((frame.status == 'ok').all() for frame in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
