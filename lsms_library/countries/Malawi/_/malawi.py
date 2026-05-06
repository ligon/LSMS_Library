#!/usr/bin/env python

import pandas as pd
import numpy as np
import json
from ligonlibrary.dataframes import from_dta
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import conversion_table_matching_global

def _household_roster_from_df(df, sex, age, HHID, sex_converter=None, age_converter=None,
                               months_spent='months_spent', Age_ints=None):
    """Inline replacement for lsms.tools.get_household_roster(fn_type=None)."""
    cols = [c for c in [HHID, sex, age, months_spent] if c in df.columns]
    df = df.loc[:, cols].rename(columns={HHID: 'HHID', sex: 'sex', age: 'age',
                                          months_spent: 'months_spent'})
    if sex_converter is not None:
        df['sex'] = df['sex'].apply(sex_converter)
    df = df.dropna(how='any')
    df['sex'] = df['sex'].apply(lambda s: str(s[0]).lower())
    if age_converter is not None:
        df['age'] = df['age'].apply(age_converter)
    df['boys']  = (df['sex'] == 'm') & (df['age'] < 18)
    df['girls'] = (df['sex'] == 'f') & (df['age'] < 18)
    df['men']   = (df['sex'] == 'm') & (df['age'] >= 18)
    df['women'] = (df['sex'] == 'f') & (df['age'] >= 18)
    if Age_ints is None:
        Age_ints = ((0,1),(1,5),(5,10),(10,15),(15,20),(20,30),(30,50),(50,60),(60,100))
    valvars = list({'HHID','girls','boys','men','women'}.intersection(df.columns))
    for lo, hi in Age_ints:
        s, e = lo, hi - 1
        df['Males %02d-%02d' % (s, e)]   = (df['sex'] == 'm') & (df['age'] >= lo) & (df['age'] < hi)
        df['Females %02d-%02d' % (s, e)] = (df['sex'] == 'f') & (df['age'] >= lo) & (df['age'] < hi)
        valvars += ['Males %02d-%02d' % (s, e), 'Females %02d-%02d' % (s, e)]
    try:
        if df['HHID'].iloc[0].split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError):
        pass
    if 'months_spent' in df.columns and df['months_spent'].count() > 0:
        g = df.loc[df['months_spent'] > 0, valvars].groupby('HHID')
    else:
        g = df[valvars].groupby('HHID')
    return g.sum()


def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    df = _household_roster_from_df(df, sex=sex, age=age, HHID=hhid,
                                   sex_converter=sex_converter,
                                   age_converter=age_converter,
                                   Age_ints=Age_ints)
    df['log HSize'] = np.log(df[['girls', 'boys', 'men', 'women']].sum(axis=1))
    df.index.name = 'j'
    return df

def sex_conv(x):
    if str.lower(x) == 'female':
        return 'f'
    elif str.lower(x) == 'male':
        return 'm'

#household characteristic code for 2010-11, 2016-17, 2019-20
def get_household_characteristics(df, year, hhid = 'case_id'):
    df = age_sex_composition(df, sex='hh_b03', sex_converter=sex_conv,
                                age='hh_b05a', age_converter=None, hhid=hhid)

    df = df.reset_index()
    df['t'] = year
    df = df.set_index(['j','t'])
    df.columns.name = 'k'
    return df

#other features code for 2010-11, 2016-17, 2019-20
def get_other_features(df, year, reside):
    df = df.loc[:,[ "case_id", "region", reside]]
    df['region'] = df['region'].map({1: 'North', 2: 'Central', 3: 'Southern'})
    df =  df.rename({'case_id': 'j', 'region' : 'm', reside: 'Rural'}, axis = 1)
    df['t'] = year
    df = df.set_index(['j','t'])
    df.columns.name = 'k'
    return df

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

def conversion_table_matching(df, conversions, conversion_label_name, num_matches=3, cutoff = 0.6):
    return conversion_table_matching_global(df, conversions, conversion_label_name, num_matches=num_matches, cutoff = cutoff)

def Sex(value):
    if isinstance(value, str) and value.strip():
        return value.strip().upper()[0]
    else:
        return np.nan


def harmonize_food_labels(df, level='i'):
    """Apply the cross-wave union of Malawi's harmonize_food map to ``df``.

    The wave-level food_acquired.py scripts apply
    ``df['i'].astype(str).str.capitalize()`` before renaming, which produces
    sentence-cased labels (e.g. ``'Sugar cane'``).  The per-wave columns of
    ``harmonize_food`` in ``categorical_mapping.org`` mix Title-case and
    sentence-case entries, so the per-wave rename via
    ``get_categorical_mapping(idxvars={'j': wave})`` silently misses any
    label whose harmonize_food entry is in a different case than the
    post-``.capitalize()`` data — see GH #216.

    This helper sidesteps the drift by building a single label map from
    *all* wave columns of ``harmonize_food`` (including each value's
    ``.capitalize()`` variant) and applying it once.  A label that's
    documented in *any* wave column gets resolved to its Preferred Label
    regardless of which wave's data we're processing.

    The Preferred Label column is honoured as-is; any truncation (e.g.
    ``'Maize Ufa Mgaiwa (Normal F'``) carries through to the output.
    Truncation cleanup is a separate concern (GH #169 / #216 follow-up).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame whose index includes the food-item level.
    level : str, default 'i'
        Index level name carrying the food labels.  In Malawi's wave-level
        builds the item lives on ``'i'`` (the framework's ``map_index``
        swaps it to canonical ``'j'`` downstream).

    Returns
    -------
    pd.DataFrame
        ``df`` with food labels remapped to Preferred Labels where the
        union map covers them.  Labels not in the map pass through
        unchanged.
    """
    import os
    from lsms_library.local_tools import all_dfs_from_orgfile

    org_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'categorical_mapping.org')
    hf = all_dfs_from_orgfile(org_path)['harmonize_food']

    unify = {}
    skip_cols = {'Preferred Label', 'GD Category'}
    for col in hf.columns:
        if col in skip_cols:
            continue
        for _, row in hf.iterrows():
            v = row.get(col)
            p = row.get('Preferred Label')
            if pd.isna(v) or pd.isna(p):
                continue
            v_str = str(v).strip()
            if v_str in ('', '---'):
                continue
            # Map both the literal harmonize_food entry and its
            # .capitalize() form (since wave scripts apply .capitalize()
            # to the data before this rename runs).
            unify.setdefault(v_str, p)
            unify.setdefault(v_str.capitalize(), p)

    return df.rename(index=unify, level=level)
