#!/usr/bin/env python3
"""
Estimate CFE demand systems from LSMS Library data.

Usage::

    import numpy as np
    import lsms_library as ll
    from cfe.regression import Regression

    tz = ll.Country('Tanzania')

    x = tz.food_expenditures(market='Region')
    d = tz.household_characteristics(market='Region')
    y = np.log(x['Expenditure'].replace(0, np.nan)).dropna()

    r = Regression(y=y, d=d, alltm=False)
    r.get_beta()

The ``market`` argument names a column from ``cluster_features``
(e.g. ``'Region'``, ``'District'``) to use as the market index *m*
required by ``cfe.Regression``.
"""

import numpy as np
from cfe.regression import Regression
import lsms_library as ll


def main(country, market='Region', alltm=False):
    """Estimate a CFE demand system for *country*.

    Parameters
    ----------
    country : str
        Country name (e.g. ``'Tanzania'``, ``'Uganda'``).
    market : str
        Column from ``cluster_features`` to use as market index.
    alltm : bool
        If True, require all goods to be observed in every (t, m) cell.

    Returns
    -------
    cfe.regression.Regression
    """
    c = ll.Country(country)

    x = c.food_expenditures(market=market)
    d = c.household_characteristics(market=market)
    y = np.log(x['Expenditure'].replace(0, np.nan)).dropna()

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

    args = parser.parse_args()

    r = main(args.country, market=args.market, alltm=args.alltm)
