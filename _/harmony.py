#!/usr/bin/env python
"""
We're interested here in checking for "harmony" across different datasets;
in particular checking whether indices and column labels are consistent.
"""

import pandas as pd
import argparse

def add_markets_from_other_features(country,df):
    of = pd.read_parquet(f"../{country}/var/other_features.parquet")

    df_idx = df.index.names

    try:
        df = df.droplevel('m')
    except KeyError:
        pass

    df = df.join(of.reset_index('m')['m'],on=['j','t'])
    df = df.reset_index().set_index(df_idx)

    return df


def main(country):
    x = pd.read_parquet(f"../{country}/var/food_expenditures.parquet")

    assert x.index.names == ['j','t','m','i'], "Indices incorrectly named or ordered."

    z = pd.read_parquet(f"../{country}/var/household_characteristics.parquet")
    assert z.index.names == ['j','t','m'], "Indices incorrectly named or ordered."
    assert z.columns.name == 'k'

    x = add_markets_from_other_features(country,x)
    z = add_markets_from_other_features(country,z)

    p = pd.read_parquet(f"../{country}/var/food_prices.parquet")

    p = p.stack().groupby(['t','m','i','units']).median()

    # Food labels consistent?
    plabels = set(p.index.get_level_values('i'))
    xlabels = set(x.index.get_level_values('i'))

    assert len(xlabels.intersection(plabels)) == len(plabels)

if __name__=='__main__':
    parser = argparse.ArgumentParser('Find optimal solution to wordle.')
    parser.add_argument("country")

    args = parser.parse_args()

    main(args.country)
