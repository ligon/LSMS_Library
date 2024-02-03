#!/usr/bin/env python
"""
We're interested here in checking for "harmony" across different datasets;
in particular checking whether indices and column labels are consistent.
"""

import pandas as pd
import argparse
from local_tools import add_markets_from_other_features
import warnings


def main(country):
    xfn = f"../{country}/var/food_expenditures.parquet"
    x = pd.read_parquet(xfn)

    idx_value_counts = {idxname:len(set(x.index.get_level_values(idxname))) for idxname in x.index.names}

    if idx_value_counts['j'] < idx_value_counts['i']:
        warnings.warn(f'In {xfn} the index j should index households; index i should index goods.\n'
        + 'It looks like these are backwards? (There should probably be more households than goods!)\n'
        + 'Switching these around.')
        x = x.rename(index={'i':'j','j':'i'})

    if 't' not in idx_value_counts.keys():
        warnings.warn(f'In {xfn} we should have a t index for period!\nAdding a dummy value.')
        x['t'] = 0
        x = x.set_index('t',append=True)

    if 'm' not in idx_value_counts.keys():
        warnings.warn(f'In {xfn} we should have a m index for markets!\nAdding a dummy value.')
        x['m'] = 0
        x = x.set_index('m',append=True)

    x = x.reorder_levels(['j','t','m','i'])

    assert x.index.names == ['j','t','m','i'], "Indices incorrectly named or ordered."
    x = add_markets_from_other_features(country,x)
    x.index.names = ['i','t','m','j']

    z = pd.read_parquet(f"../{country}/var/household_characteristics.parquet")
    assert z.index.names == ['j','t','m'], "Indices incorrectly named or ordered in household_characteristics."
    z.columns.name = 'k'
    z = add_markets_from_other_features(country,z,additional_other_features=True)
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
