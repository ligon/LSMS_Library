"""Small known-welfare experiments for the supplied-partition evaluator.

Run from the LSMS checkout with the CFEDemands feature branch on PYTHONPATH:
    python -m SkunkWorks.aggregation.simulation --seeds 11 12 13
"""

import argparse
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from .evaluation import evaluate_partitions


REGIMES = ('complete', 'substitutes', 'unequal', 'unreported')


def simulation_inputs(seed=11, households=240, regime='substitutes'):
    """Adapt the tested CFE DGP, adding disclosed zero-pattern regimes.

    The DGP uses the old household j / good i names. Only those names change
    here (ledger 2). All periods of a household stay on one side of the split.
    Global NumPy state is restored because the existing DGP uses legacy RNGs;
    this helper is intended for sequential or process-isolated execution.
    """
    from cfe.dgp import expenditures

    if regime not in REGIMES:
        raise ValueError(f'Unknown regime: {regime}.')
    if not isinstance(households, int) or households < 80:
        raise ValueError('Use at least 80 households for this support experiment.')
    beta = np.array([.6, .4, 1.1, 2.] + [1.2]*6)
    if regime == 'unequal':
        beta[4:] = np.linspace(.2, 3., 6)
    price_index = pd.MultiIndex.from_product([range(2), [0], range(10)], names=['t', 'm', 'i'])
    prices = pd.Series(1., index=price_index)
    state = np.random.get_state()
    try:
        np.random.seed(seed)
        x, latent = expenditures(households, 2, 1, 10, 1, beta,
                                 sigma_eps=.2, p=prices)
    finally:
        np.random.set_state(state)
    x.index = x.index.set_names(['i', 't', 'm', 'j'])
    x = x.unstack('j').rename(columns=lambda j: f'g{j}')
    d = np.log(latent['characteristics'])
    d.index = d.index.set_names(['i', 't', 'm'])
    w = -np.log(latent['lambdas'])
    w.index = w.index.set_names(['i', 't', 'm'])
    sparse_only = x.index.get_level_values('i') % 4 == 0
    rng = np.random.default_rng(seed+100)
    if regime != 'complete':
        x.loc[sparse_only, ['g0', 'g1', 'g2', 'g3']] = 0.
        if regime in ('substitutes', 'unequal'):
            # One available input per cell, chosen independently of errors.
            selected = rng.integers(0, 6, size=len(x))
            mask = selected[:, None] == np.arange(6)[None, :]
        else:
            # Positive amounts go unreported. Observation may depend on w,
            # while remaining independent of expenditure errors given w.
            probability = 1/(1+np.exp(1.5-.5*w.to_numpy()))
            mask = rng.random((len(x), 6)) < probability[:, None]
        x.loc[:, [f'g{j}' for j in range(4, 10)]] *= mask

    split = np.random.default_rng(seed+1000).permutation(households)
    train_households = split[:int(.6*households)]
    evaluation_households = split[int(.6*households):]
    train_mask = x.index.get_level_values('i').isin(train_households)
    evaluation_mask = x.index.get_level_values('i').isin(evaluation_households)
    reference = x.index[train_mask & ~sparse_only]
    comparison = x.index[evaluation_mask & ~sparse_only]
    weights = pd.Series(1., index=x.index, name='weight')
    # Truth is centered on the same declared reference, using its own values.
    # It never enters estimation or the estimated normalization.
    truth = (w-w.loc[reference].mean()).rename('w')
    fine = {g: g for g in x.columns}
    rare = {g: ('rare_pool' if int(g[1:]) >= 4 else g) for g in x.columns}
    broad = {g: ('broad_pool' if int(g[1:]) >= 2 else g) for g in x.columns}
    return dict(x=x, d=d, partitions={'fine': fine, 'rare_pool': rare, 'broad_pool': broad},
                train_households=train_households, evaluation_households=evaluation_households,
                reference_cells=reference, comparison_cells=comparison, weights=weights,
                anchor='g0', anchor_loading=beta[0], truth=truth,
                fit_options={'min_obs': 30, 'min_prop_items': .1, 'alltm': True})


def run_experiment(seeds=(11, 12, 13), households=240, regimes=REGIMES):
    """Return each replicate, including failures; do not select a winner."""
    rows = []
    for seed in seeds:
        for regime in regimes:
            result = evaluate_partitions(**simulation_inputs(seed, households, regime))
            frame = result.summary.reset_index()
            frame.insert(0, 'regime', regime)
            frame.insert(0, 'seed', seed)
            rows.append(frame)
    return pd.concat(rows, ignore_index=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seeds', nargs='+', type=int, default=[11, 12, 13])
    parser.add_argument('--households', type=int, default=240)
    parser.add_argument('--regimes', nargs='+', choices=REGIMES, default=list(REGIMES))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = run_experiment(args.seeds, args.households, args.regimes)
    result.to_csv(args.output if args.output else sys.stdout, index=False)
    return 0 if (result.status == 'ok').all() else 1


if __name__ == '__main__':
    raise SystemExit(main())
