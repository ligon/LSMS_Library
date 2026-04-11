#!/usr/bin/env python3

"""
A collection of mappings to transform dataframes.
"""
import pandas as pd
import numpy as np
from pandas import concat, get_dummies, MultiIndex
from cfe.df_utils import use_indices
from .local_tools import format_id

def age_intervals(age,age_cuts=(0,4,9,14,19,31,51)):
    """
    Take as input a Series (e.g., a row from a dataframe), and use variables =Age= and =Sex=
    to create a set of coarser categories.
    """
    age_cuts = [-np.inf]+list(age_cuts)+[np.inf]
    return pd.cut(age,age_cuts,duplicates='drop', right = False)

def dummies(df,cols,suffix=False):
    """From a dataframe df, construct an array of indicator (dummy) variables,
    with a column for every unique row df[cols]. Note that the list cols can
    include names of levels of multiindices.

    The optional argument =suffix=, if provided as a string, will append suffix
    to column names of dummy variables. If suffix=True, then the string '_d'
    will be appended.
    """
    idxcols = list(set(df.index.names).intersection(cols))
    colcols = list(set(cols).difference(idxcols))

    v = concat([use_indices(df,idxcols),df[colcols]],axis=1)

    usecols = []
    for s in idxcols+colcols:
        usecols.append(v[s].squeeze())

    tuples = pd.Series(list(zip(*usecols)),index=v.index)

    v = get_dummies(tuples).astype(int)

    if suffix==True:
        suffix = '_d'

    if suffix!=False and len(suffix)>0:
        columns = [tuple([str(c)+suffix for c in t]) for t in v.columns]
    else:
        columns = v.columns

    v.columns = MultiIndex.from_tuples(columns,names=idxcols+colcols)

    return v

def format_interval(interval):
    if interval.right == np.inf:
        return f"{int(interval.left)}+"
    elif interval.left == -np.inf:
        return f'00-03'
    else:
        return f"{int(interval.left):02d}-{int(interval.right-1):02d}"

def roster_to_characteristics(df, age_cuts=(0,4,9,14,19,31,51), drop = 'pid', final_index = ['t','v','i']):
    """Collapse a household roster into household-level sex × age counts.

    Drives the derived ``household_characteristics`` table: takes a
    person-level roster indexed by (at least) ``('t', 'v', 'i', 'pid')``,
    buckets each person into a ``sex_age`` category, and returns a
    household-level DataFrame with one integer column per bucket plus a
    ``log HSize`` column (log of household size). Called automatically
    via :data:`lsms_library.country._ROSTER_DERIVED` when a user asks
    ``Country(name).household_characteristics()``.

    Parameters
    ----------
    df : pandas.DataFrame
        Household roster with ``Sex`` and ``Age`` columns (case-insensitive).
    age_cuts : tuple[int, ...]
        Upper bounds for age buckets; ``(0, 4, 9, 14, 19, 31, 51)`` by
        default, producing the buckets ``00-03``, ``04-08``, …, ``51+``.
    drop : str
        Index level to drop before aggregation (typically ``'pid'``).
    final_index : list[str]
        Final groupby level; defaults to the household key ``('t', 'v',
        'i')`` but ``Country._finalize_result`` can pass a different
        tuple when the roster's actual index differs.

    Returns
    -------
    pandas.DataFrame
        Household-level counts with one column per sex × age bucket and
        a ``log HSize`` column.
    """
    roster_df = df.copy()
    roster_df.columns = roster_df.columns.str.lower()
    # Clean stringified NA sentinels that leak through from to_parquet/astype(str)
    _na_strings = {'<NA>', 'None', 'nan', ''}
    for col in ('sex', 'age'):
        if col in roster_df.columns:
            roster_df[col] = roster_df[col].replace({s: pd.NA for s in _na_strings})
    roster_df = roster_df.dropna(subset=['sex', 'age'])
    roster_df['age_interval'] = age_intervals(roster_df['age'], age_cuts)
    roster_df['sex_age'] = roster_df.apply(
        lambda x: f"{x['sex']} {format_interval(x['age_interval'])}" if not pd.isna(x['age_interval']) else f"{x['sex']} NA",
        axis=1
    )
    roster_df = dummies(roster_df,['sex_age'])
    roster_df.index = roster_df.index.droplevel(drop)
    result = roster_df.groupby(level=final_index).sum()
    result['log HSize'] = np.log(result.sum(axis=1))
    result.columns = result.columns.get_level_values(0)
    return result

def conversion_to_kgs(df, price = ['Expenditure'], quantity = 'Quantity', index=['t','m','i'], unit_col = 'u'):
    """Infer local-unit → kg conversion factors from price ratios.

    For each unit that does not appear in :data:`KNOWN_METRIC`, this
    function computes a factor by assuming the *price per kilogram*
    should be roughly constant across units for the same item/market.
    That is: if a "bunch" of item j trades at roughly 2× the unit value
    of a kg of item j, the inferred factor is 2 kg per bunch.

    The mechanics: expenditure is divided by quantity to get a per-unit
    price, grouped to the ``index`` level (default ``('t','m','i')``),
    then the median across rows is compared to the unit-wise median to
    back out kg per unit. Used by :func:`_get_kg_factors` as a fallback
    when a survey doesn't ship its own conversion table.

    Parameters
    ----------
    df : pandas.DataFrame
        Food-acquired frame with ``Expenditure`` and ``Quantity``
        columns and ``u`` (or ``unit_col``) in the index.
    price : list[str]
        Column(s) interpreted as expenditure for the ratio calculation.
    quantity : str
        Column interpreted as quantity.
    index : list[str]
        Groupby levels for the per-item/period median step.
    unit_col : str
        Name of the unit index level; renamed to ``u`` if different.

    Returns
    -------
    dict[str, float]
        Mapping of (lowercased) unit label → inferred kg factor.
        Units already in :data:`KNOWN_METRIC` or that cannot be inferred
        are absent from the output.
    """
    v = df.copy()
    v = v.replace(0, np.nan)
    unit_conversion = {
        'kg': 1,
        'kilogram': 1,
        'gram': 1 / 1000,
        'g': 1 / 1000,
        'pound': 0.453592,
        'lbs': 0.453592,
        'kilogramme': 1,
        'gramm': 1 / 1000
    }
    #convert the value type in index level 'u' to be string
    v = v.reset_index(unit_col)
    if unit_col != 'u':
        v = v.rename(columns={unit_col: 'u'})
    v['u'] = v['u'].astype(str)
    v['Kgs'] = v.apply(lambda row: row[quantity] * unit_conversion.get(row['u'].lower(), np.nan), axis=1)
    v = v.set_index('u', append=True)
    pkg = v[price].divide(v['Kgs'], axis=0)
    pkg = pkg.groupby(index).median().median(axis=1)
    po = v[price].groupby(index + ['u']).median().median(axis=1)
    kgper = (po / pkg).dropna()
    kgper = kgper.groupby('u').median()
    #convert to dict
    kgper = kgper.to_dict()
    return kgper


# ---------------------------------------------------------------------------
# Derived food tables from food_acquired
# ---------------------------------------------------------------------------

# Column aliases: map legacy (Tanzania, etc.) names to the canonical names
# used by the transformation functions below.
_COLUMN_ALIASES = {
    'value_purchase': 'Expenditure',
    'expenditure': 'Expenditure',
    'quant_ttl_consume': 'Quantity',
    'quantity_consumed': 'Quantity',
    'quant_purchase': 'Quantity',  # fallback if quant_ttl_consume absent
}

# Column names that should be promoted to the 'u' index level
_UNIT_COLUMN_ALIASES = ['u', 'units', 'u_consumed', 'unit']


def _normalize_columns(df):
    """Rename legacy food_acquired columns to canonical names if needed.

    Also promotes a unit column to the 'u' index level when 'u' is not
    already in the index.
    """
    renames = {}
    for old, new in _COLUMN_ALIASES.items():
        if new not in df.columns and old in df.columns and new not in renames.values():
            renames[old] = new
    if renames:
        df = df.rename(columns=renames)

    # Promote unit column to 'u' index level if not already present
    if 'u' not in df.index.names:
        for col in _UNIT_COLUMN_ALIASES:
            if col in df.columns:
                df = df.rename(columns={col: 'u'}).set_index('u', append=True)
                break
            elif col in df.index.names and col != 'u':
                df.index = df.index.rename({col: 'u'})
                break

    return df

KNOWN_METRIC = {
    'kg': 1, 'kilogram': 1, 'kilogramme': 1,
    'g': 1/1000, 'gram': 1/1000, 'gramm': 1/1000,
    'l': 1, 'litre': 1, 'liter': 1,
    'ml': 1/1000, 'cl': 1/100,
    'pound': 0.453592, 'lbs': 0.453592,
}


def _get_kg_factors(df):
    """Build a combined kg-per-unit mapping from known metric units
    and price-ratio inference on the data."""
    factors = dict(KNOWN_METRIC)

    # Infer additional factors from price ratios where possible
    if 'Expenditure' in df.columns and 'Quantity' in df.columns:
        # Determine which index levels are available for grouping
        idx_names = list(df.index.names)
        group_levels = [n for n in ['t', 'm', 'i'] if n in idx_names]
        if group_levels:
            try:
                inferred = conversion_to_kgs(df, index=group_levels)
                # Inferred factors fill in where known metric doesn't cover
                for unit, factor in inferred.items():
                    if unit.lower() not in factors and np.isfinite(factor) and factor > 0:
                        factors[unit.lower()] = factor
            except Exception:
                pass  # If inference fails, proceed with known metric only

    return factors


def _apply_kg_conversion(df, factors):
    """Convert Quantity to kg using the factors dict.
    Returns a copy with a 'Quantity_kg' column added."""
    v = df.copy()
    if 'u' in v.index.names:
        units = v.index.get_level_values('u').astype(str).str.lower()
    else:
        return v

    v['Quantity_kg'] = v['Quantity'] * units.map(factors)
    return v


def food_expenditures_from_acquired(df):
    """Derive food expenditures from food_acquired.

    Returns a DataFrame of total expenditure per household × item × period,
    summed over units.
    """
    df = _normalize_columns(df)
    if 'Expenditure' not in df.columns:
        raise ValueError("food_acquired must have an 'Expenditure' column")

    idx_names = list(df.index.names)
    group_by = [n for n in ['t', 'v', 'i', 'j'] if n in idx_names]

    x = df[['Expenditure']].replace(0, np.nan).dropna()
    x = x.groupby(group_by).sum()
    return x


def food_quantities_from_acquired(df):
    """Derive food quantities (in kg) from food_acquired.

    Uses known metric conversions and price-ratio inference to convert
    local units to kg, then sums per household × item × period.
    """
    df = _normalize_columns(df)
    if 'Quantity' not in df.columns:
        raise ValueError("food_acquired must have a 'Quantity' column")

    factors = _get_kg_factors(df)
    v = _apply_kg_conversion(df, factors)

    idx_names = list(v.index.names)
    group_by = [n for n in ['t', 'v', 'i', 'j'] if n in idx_names]

    q = v[['Quantity_kg']].rename(columns={'Quantity_kg': 'Quantity'})
    q = q.replace(0, np.nan).dropna()
    q = q.groupby(group_by).sum()
    return q


def food_prices_from_acquired(df):
    """Derive food prices (per kg) from food_acquired.

    Unit values are computed as Expenditure / Quantity_kg, then
    the median is taken across households within each item × period.
    """
    df = _normalize_columns(df)
    if 'Expenditure' not in df.columns or 'Quantity' not in df.columns:
        raise ValueError("food_acquired must have 'Expenditure' and 'Quantity' columns")

    factors = _get_kg_factors(df)
    v = _apply_kg_conversion(df, factors)

    v['price_per_kg'] = v['Expenditure'] / v['Quantity_kg']
    v = v[['price_per_kg']].replace([0, np.inf, -np.inf], np.nan).dropna()

    idx_names = list(v.index.names)
    group_by = [n for n in ['t', 'v', 'm', 'i'] if n in idx_names]

    p = v.groupby(group_by).median()
    p = p.rename(columns={'price_per_kg': 'Price'})
    return p
