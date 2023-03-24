#!/usr/bin/env python
"""
We're interested here in checking for "harmony" across different datasets;
in particular checking whether indices and column labels are consistent.
"""

import pandas as pd
import argparse
from local_tools import add_markets_from_other_features


def main(country):
    x = pd.read_parquet(f"../{country}/var/food_expenditures.parquet")

    assert x.index.names == ['j','t','m','i'], "Indices incorrectly named or ordered."
    x = add_markets_from_other_features(country,x)
    x.index.names = ['i','t','m','j']

    z = pd.read_parquet(f"../{country}/var/household_characteristics.parquet")
    assert z.index.names == ['j','t','m'], "Indices incorrectly named or ordered in household_characteristics."
    z.columns.name = 'k'
    z = add_markets_from_other_features(country,z)
    assert z.columns.name == 'k', "Columns incorrectly named or ordered in household_characteristics."
    z.index.names = ['i','t','m']

    p = pd.read_parquet(f"../{country}/var/food_prices.parquet")

    try:
        p = p.stack().groupby(['t','m','i','u']).median()
    except KeyError:
        warnings.warn('food_prices indices are incorrect (or incorrectly labelled)')
    p.index.names = ['t','m','j','u']

    # Food labels consistent?
    plabels = set(p.index.get_level_values('j'))
    xlabels = set(x.index.get_level_values('j'))

    assert len(xlabels.intersection(plabels)) == len(plabels)

    return x,z,p

if __name__=='__main__':
    parser = argparse.ArgumentParser('Check for consistency of datasets.')
    parser.add_argument("country")

    args = parser.parse_args()

    main(args.country)
