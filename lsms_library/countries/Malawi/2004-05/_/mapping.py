#!/usr/bin/env python
"""Wave-level formatting functions for Malawi 2004-05 (IHS2).

The shocks module (sec_ab.dta) is read with ``converted_categoricals: False``
because its ab02 value labels are not one-to-one with its codes: 117 and 118
BOTH label as "Other" (the questionnaire's two separate "Other (specify)"
roster slots).  Letting Stata's labels collapse them made (t, i, Shock)
non-unique for 11,077 rows, which the framework then dropped silently via
groupby().first() -- GH #323.  We therefore decode the codes ourselves and
keep the two "Other" slots distinct.

The maps below are transcribed from the value labels carried by sec_ab.dta
itself; tests/test_malawi_gh323.py re-reads those labels from the source and
asserts these dicts still agree with them, so a source change cannot silently
drift away from this transcription.
"""
import pandas as pd

# ab02 -- shock type.  Codes 117 and 118 both carry the Stata label "Other";
# they are the two distinct "Other (specify)" roster slots and are kept apart
# here so the (t, i, Shock) index stays unique.
_SHOCK = {
    101: 'Lower crop yields due to drought or floods',
    102: 'Crop disease of crop pests',
    103: 'Livestock died or were stolen',
    104: 'Household business failure, non-agr',
    105: 'Loss of salaried employment or nonpayment of salaries',
    106: 'End of regular assistance/aid/remittances from outside HH',
    107: 'Large fall in sales prices for crops',
    108: 'Large rise in price of food',
    109: 'Illness or accident of household member',
    110: 'Birth in the household',
    111: 'Death of household head',
    112: 'Death of working member of household',
    113: 'Death of other family member',
    114: 'Breakup of the household',
    115: 'Theft',
    116: 'Dwelling damaged, destroyed',
    117: 'Other (specify) 1',   # Stata label: "Other"
    118: 'Other (specify) 2',   # Stata label: "Other"  <- the collision
    119: 'Rise in Farm Inputs Prices',
}

# ab07a/b/c -- coping strategy.
_COPING = {
    1: 'Spent cash savings',
    2: 'Sent children to live with relatives',
    3: 'Sold assets',
    4: 'Sold farmland',
    5: 'Rented out animals',
    6: 'Sold animals',
    7: 'Sold more crops',
    8: 'Worked longer hours, worked more',
    9: 'Other HH members went to work',
    10: 'Started a new business',
    11: 'Removed children from school',
    12: 'Went elsewhere to find work for more than 1 month',
    13: 'Borrowed money from relatives',
    14: 'Borrowed money from money lender',
    15: 'Borrowed money from institution',
    16: 'Received help from religious institution',
    17: 'Received help from local NGO',
    18: 'Received help from international NGO',
    19: 'Received help from government',
    20: 'Reduced food consumption',
    21: 'Consumed lower cost but less preferred foods',
    22: 'Reduced nonfood expenditures',
    23: 'Spiritual effort, prayer, consulted diviner',
    24: 'Did not do anything',
    25: 'Other, specify',
    26: 'Applying Manure/Chemicals',
    99: 'Dont know',
}


def _decode(value, table):
    """Decode one raw Stata code to its label.

    ``value`` arrives as a one-element row Series (df[[col]].apply(f, axis=1)),
    matching the convention used by cs_i in the later waves.  Unconverted Stata
    numerics come through as floats (101.0), so coerce to int before lookup.  An
    unmapped code is returned as-is rather than silently becoming NA: a code we
    do not know about must be visible, not vanish.
    """
    x = value.iloc[0] if hasattr(value, 'iloc') else value
    if pd.isna(x):
        return pd.NA
    try:
        return table[int(x)]
    except (KeyError, ValueError, TypeError):
        return x


def shock_label(value):
    """ab02 code -> shock label, keeping the two 'Other' slots (117/118) apart."""
    return _decode(value, _SHOCK)


def coping_label(value):
    """ab07a/b/c code -> coping-strategy label."""
    return _decode(value, _COPING)
