#!/usr/bin/env python3
"""
Estimate CFE demand systems from LSMS Library data.

Usage::

    import lsms_library as ll
    from cfe.regression import Regression
    from lsms_library.demands import prepare_demand_inputs

    tz = ll.Country('Tanzania')

    x = tz.food_expenditures(market='Region', basis='purchased')
    d = tz.household_characteristics(market='Region')
    y, d = prepare_demand_inputs(x, d)

    r = Regression(y=y, d=d, alltm=False)
    r.get_beta()

The ``market`` argument names a column from ``cluster_features``
(e.g. ``'Region'``, ``'District'``) to use as the market index *m*
required by ``cfe.Regression``.
"""

import numpy as np
import pandas as pd
from cfe.regression import Regression
import lsms_library as ll


def prepare_demand_inputs(expenditures, characteristics):
    """Adapt single-country monetary levels to CFE's ``y, d`` inputs.

    ``expenditures`` must carry ``Expenditure`` on ``(i,t,m,j)`` with an
    optional acquisition-source ``s`` level; ``characteristics`` has columns
    on ``(i,t,m)``. Level order may differ. Native keys must be unique and
    nonmissing, and a household-period must identify one consistent market.
    Extra axes, including ``country``, are refused: explicitly select the
    same single country from BOTH Feature inputs before calling this helper.

    Sum every supplied source in monetary LEVELS, then take logs. The Country
    API's ``basis=`` determines which sources were supplied: this adapter
    never selects a source or values unreported consumption. ``min_count=1``
    keeps all-missing groups missing; partially missing groups give the sum
    of recorded amounts, not an imputed complete total. Zero totals have no
    log and are omitted, as in the original recipe. Negative or nonfinite
    totals raise. Inputs, source values and their attrs are not modified.

    This performs no fitting or support selection. CFE still selects its
    fitting sample (including missing characteristics and unmatched rows).
    See ledger ``1439-demand-source-index.md`` §§2–4.
    """
    y_levels = ['i', 't', 'm', 'j']
    d_levels = ['i', 't', 'm']
    for name, frame, required, optional in (
        ('expenditures', expenditures, y_levels, {'s'}),
        ('characteristics', characteristics, d_levels, set()),
    ):
        if not isinstance(frame, pd.DataFrame):
            raise TypeError(f'{name} must be a DataFrame')
        if 'country' in frame.columns:
            raise ValueError(f'{name} carries country identity; select one country explicitly from both inputs')
        names = list(frame.index.names)
        if (len(set(names)) != len(names)
                or not set(required).issubset(names)
                or set(names).difference(required).difference(optional)):
            raise ValueError(
                f'{name} must have index {tuple(required)}'
                + (' with optional s' if optional else '')
                + f'; got {tuple(names)}. Select one country explicitly '
                  'from both inputs; do not discard extra identity levels.'
            )
        if not frame.index.is_unique or not frame.columns.is_unique:
            raise ValueError(f'{name} must have unique native keys and columns')
        if frame.index.to_frame(index=False).isna().any(axis=None):
            raise ValueError(f'{name} has missing index keys; resolve identities/markets first')

    countries = [frame.attrs.get('country') for frame in
                 (expenditures, characteristics)]
    if any(value is not None for value in countries):
        if (not all(isinstance(value, str) for value in countries)
                or countries[0] != countries[1]):
            raise ValueError('inputs must identify the same single country in their attrs')

    # A CFE household-period cannot be assigned two markets or be rekeyed by
    # one operand. Do not let CFE's inner join silently erase a disagreement.
    markets = []
    for name, frame in (('expenditures', expenditures),
                        ('characteristics', characteristics)):
        keys = frame.index.to_frame(index=False)[d_levels].drop_duplicates()
        if keys.duplicated(['i', 't']).any():
            raise ValueError(f'{name} assigns multiple markets to a household-period')
        markets.append(keys.set_index(['i', 't'])['m'])
    shared = markets[0].index.intersection(markets[1].index)
    if (markets[0].reindex(shared) != markets[1].reindex(shared)).any():
        raise ValueError('inputs disagree on market for a shared household-period')

    amounts = expenditures['Expenditure']
    if not pd.api.types.is_numeric_dtype(amounts.dtype) or pd.api.types.is_complex_dtype(amounts.dtype):
        raise TypeError('Expenditure must be real numeric monetary levels')
    if np.isinf(amounts).any():
        raise ValueError('Expenditure contains nonfinite monetary values')
    # Explicit consumer aggregation authorized by food_expenditures' Notes;
    # no other identity axis may disappear (ledger §§3–4).
    totals = amounts.groupby(level=y_levels, observed=True, sort=False,
                             dropna=False).sum(min_count=1)
    if np.isinf(totals).any() or (totals < 0).any():
        raise ValueError('CFE requires nonnegative finite expenditure totals')
    y = np.log(totals.replace(0, np.nan)).dropna()
    y.attrs = dict(expenditures.attrs)
    d = characteristics.reorder_levels(d_levels).copy()
    return y, d


def main(country, market='Region', alltm=False, basis='purchased'):
    """Estimate a CFE demand system for *country*.

    Parameters
    ----------
    country : str
        Country name (e.g. ``'Tanzania'``, ``'Uganda'``).
    market : str
        Column from ``cluster_features`` to use as market index.
    alltm : bool
        If True, require all goods to be observed in every (t, m) cell.
    basis : {'purchased', 'total'}, default 'purchased'
        Cash outlay (the existing default) or all recorded acquisition value.
        All supplied sources are summed explicitly before logs; no valuation
        of unreported own-production or in-kind amounts is introduced.

    Returns
    -------
    cfe.regression.Regression
    """
    c = ll.Country(country)

    x = c.food_expenditures(market=market, basis=basis)
    d = c.household_characteristics(market=market)
    y, d = prepare_demand_inputs(x, d)

    r = Regression(y=y, d=d, alltm=alltm)
    r.get_beta()

    return r


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser('Estimate CFE demand system for country.')
    parser.add_argument('country')
    parser.add_argument('--market', default='Region',
                        help='cluster_features column for market index (default: Region)')
    parser.add_argument('--alltm', action='store_true',
                        help='Require all goods observed in all periods and markets.')
    parser.add_argument('--basis', choices=['purchased', 'total'], default='purchased',
                        help='Cash outlay or all recorded acquisition value (default: purchased)')

    args = parser.parse_args()

    r = main(args.country, market=args.market, alltm=args.alltm, basis=args.basis)
