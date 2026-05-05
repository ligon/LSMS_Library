#!/usr/bin/env python
"""Malawi-specific helpers for wave-level food_acquired.py scripts.

The live surface is three functions used by the four IHS3+ wave scripts
(2010-11, 2013-14, 2016-17, 2019-20) to apply Malawi's region-keyed
unit-conversion CSV and to handle "300 grams"-style free-text units.
Other helpers (roster decomposition, get_other_features, etc.) were
removed in 2026-05-05 alongside the shadowed
food_prices_quantities_and_expenditures.py — see GH #218.
"""

import pandas as pd
import numpy as np
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import conversion_table_matching_global


def _extract_kg_conversion(series):
    """Extract kilogram conversion factors from a unit-detail string series.

    Parses patterns like '300 grams', '1kg', '2 kilo' and returns
    a Series of conversion factors in kilograms.
    """
    grams = r'(\d+)\s*g(?:\s+|r)'
    kgs = r'(\d+)\s*k(?:g|ilo)'

    lower = series.str.lower()
    conv = pd.concat([lower.str.extract(grams).astype(float) * 0.01,
                      lower.str.extract(kgs).astype(float)], axis=0).dropna()
    return conv


def handling_unusual_units(df, suffixes=None):
    """Convert unusual unit descriptions to kg-based quantities.

    Parameters
    ----------
    df : DataFrame
    suffixes : list[str], optional
        Column suffixes to process (e.g. ``['consumed', 'bought']``).
        For each suffix, expects columns ``unitsdetail_{suffix}``,
        ``cfactor_{suffix}``, ``quantity_{suffix}``, and ``units_{suffix}``.
        Defaults to ``['consumed', 'bought']`` for backward compatibility.
    """
    if suffixes is None:
        suffixes = ['consumed', 'bought']

    for suffix in suffixes:
        detail_col = f'unitsdetail_{suffix}'
        cfactor_col = f'cfactor_{suffix}'
        quantity_col = f'quantity_{suffix}'
        units_col = f'units_{suffix}'
        u_col = f'u_{suffix}'

        if detail_col not in df.columns:
            continue

        conv_kg = _extract_kg_conversion(df[detail_col])

        df[cfactor_col] = df.apply(lambda x, c=cfactor_col: x[c] or conv_kg, axis=1)
        df[quantity_col] = df[quantity_col].mul(df[cfactor_col].fillna(1))
        df[u_col] = np.where(~df[cfactor_col].isna(), 'kg', df[detail_col])
        df[u_col] = df[u_col].replace('nan', pd.NA).fillna(df[units_col])

    return df


def conversion_table_matching(df, conversions, conversion_label_name, num_matches=3, cutoff=0.6):
    return conversion_table_matching_global(df, conversions, conversion_label_name,
                                            num_matches=num_matches, cutoff=cutoff)
