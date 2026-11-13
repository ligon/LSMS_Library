"""Wave-level formatting helpers for Uganda 2015-16.

Reconstructs ``Age`` from h2q9a/b/c via the shared ``age_handler``
glue in ``../../_/_age_helpers.py``.  Month sentinel in this wave
is the lowercase string ``'dk'``; the helper accepts that case-
insensitively.  GH #177.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import warnings

import pandas as pd

from lsms_library.local_tools import format_id

_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_age_helpers.py"
_spec = importlib.util.spec_from_file_location("_uganda_age_helpers", _HELPERS)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

INTERVIEW_YEAR = 2015


def Age(value):
    return _mod.age_components(value)


def household_roster(df):
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)


def housing_ids(df):
    """Validate gsec1's hh -> HHID relation before the housing join.

    Ledger #1290 §§2/4: reuse format_id and the declared YAML left merge.
    Missing source keys never match another missing key. Agreeing pairs may
    repeat, but two recorded targets for one formatted hh are ambiguous.
    """
    pairs = df.reset_index()
    pairs['i'] = pairs['i'].map(format_id)
    pairs['_housing_HHID'] = pairs['_housing_HHID'].map(format_id)
    pairs = pairs.dropna(subset=['i', '_housing_HHID']).drop_duplicates()
    if pairs.empty:
        raise ValueError('Uganda/2015-16 housing: no usable hh/HHID bridge pairs')
    ambiguous = pairs.duplicated(['t', 'i'], keep=False)
    if ambiguous.any():
        raise ValueError(
            'Uganda/2015-16 housing: ambiguous hh/HHID bridge '
            f'({int(ambiguous.sum())} pairs)')
    return pairs.set_index(['t', 'i'])


def housing_ag_ids(df):
    """Validate the recorded AG identity with the same primary-pair rules.

    Ledger GH1290 §§2–4: this is an identity projection only. D1 independently
    supplies sample membership; no agricultural location or weight is read here.
    """
    pairs = housing_ids(df.rename(columns={'_housing_AG_HHID': '_housing_HHID'}))
    return pairs.rename(columns={'_housing_HHID': '_housing_AG_HHID'})


def housing(df):
    """Re-key housing without changing payloads or guessing unmatched IDs.

    Both identity helper columns are mandatory: a dropped declared source
    must never silently serve an incomplete bridge. No sample/cluster inference
    here; conflicting recorded targets refuse before any fallback.
    """
    if not {'_housing_HHID', '_housing_AG_HHID'} <= set(df.columns):
        raise ValueError('Uganda/2015-16 housing: required hh/HHID bridge unavailable')
    flat = df.reset_index()
    target = flat.pop('_housing_HHID')
    alternate = flat.pop('_housing_AG_HHID')
    # Ledger GH1290 §4: never override a recorded primary identity. Two
    # conflicting recorded answers are not authority for choosing either one.
    conflict = (target.notna() & alternate.notna()
                & target.ne(alternate).fillna(False))
    if conflict.any():
        raise ValueError(
            'Uganda/2015-16 housing: conflicting household/agriculture '
            f'hh/HHID bridge ({int(conflict.sum())} rows)')
    target = target.where(target.notna(), alternate)
    unresolved = target.isna()
    flat['i'] = flat['i'].where(unresolved, target)
    duplicate = flat.duplicated(['t', 'i'], keep=False)
    if duplicate.any():
        raise ValueError(
            'Uganda/2015-16 housing: re-keying creates duplicate household keys '
            f'({int(duplicate.sum())} rows)')
    if unresolved.any():
        missing = int(flat.loc[unresolved, 'i'].isna().sum())
        warnings.warn(
            'Uganda/2015-16 housing: unresolved hh/HHID bridge for '
            f'{int(unresolved.sum())} of {len(flat)} housing rows '
            f'({missing} missing source IDs); retaining recorded keys',
            UserWarning, stacklevel=2)
    return flat.set_index(['t', 'i'])
