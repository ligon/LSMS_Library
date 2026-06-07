"""
Kazakhstan 1996 subjective_well_being (Cantril / self-anchoring ladder).

Source: ../Data/KZ96OCC_PUF.dta (catalog F20, the individual occupation file).
Variable ``d_095_`` carries the variable label "personal staircase
identification" -- a Cantril self-anchoring ladder.  Its Stata value labels are
{1: 'lowest', 2..8: literal, 9: 'highest'}, i.e. a 1-9 step scale where higher =
better off (consistent with the Malawi anchor and the Cantril convention).  The
raw column stores the labelled strings ('lowest', '2', ..., '8', 'highest'); we
recover the integer step from the value labels.

This is an INDIVIDUAL-level file (one row per respondent within a household,
keyed by rn = household, personnr = person).  The canonical
subjective_well_being table is household level (t, i) with i = rn (the household
key used by household_roster, idxvars i: rn).  We reduce to the household by
taking the HOUSEHOLD HEAD's ladder step:

  - The roster (KZ96REL.dta) has exactly one head per household (relhead == 1,
    1996 households).  1992 of those heads appear in the OCC respondent file and
    1983 give a non-missing ladder step.
  - The head is the natural representative for a household-level welfare
    self-placement, and using a single well-defined member avoids arbitrary
    aggregation (mean/mode) across respondents.

Households whose head did not answer the OCC ladder question are dropped.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

# 1-9 Cantril ladder.  Raw values are the Stata labels; map back to int step.
step_mapping = {
    'lowest': 1,
    '1': 1,
    '2': 2,
    '3': 3,
    '4': 4,
    '5': 5,
    '6': 6,
    '7': 7,
    '8': 8,
    '9': 9,
    'highest': 9,
}

# Heads from the roster: relhead == 1, one per household.
rel = get_dataframe('../Data/KZ96REL.dta')
heads = rel[rel['relhead'].astype(str).str.split('.').str[0] == '1'][['rn', 'personnr']].copy()
heads['i'] = heads['rn'].astype(float).astype(int).astype(str)
heads['personnr'] = heads['personnr'].astype(float).astype(int)

# Individual ladder from the occupation file.
occ = get_dataframe('../Data/KZ96OCC_PUF.dta')[['rn', 'personnr', 'd_095_']].copy()
occ['i'] = occ['rn'].astype(float).astype(int).astype(str)
occ['personnr'] = occ['personnr'].astype(float).astype(int)


def to_step(x):
    if pd.isna(x):
        return pd.NA
    return step_mapping.get(str(x).strip(), pd.NA)


occ['Own Step'] = occ['d_095_'].map(to_step)

# Restrict to the household head, reduce to one row per household.
head_step = heads.merge(occ[['i', 'personnr', 'Own Step']],
                        on=['i', 'personnr'], how='left')

out = head_step[['i', 'Own Step']].copy()
out['t'] = '1996'
out = out.dropna(subset=['Own Step'])
out['Own Step'] = out['Own Step'].astype('Int64')
out = out.drop_duplicates(subset=['t', 'i']).set_index(['t', 'i']).sort_index()

to_parquet(df=out, fn='subjective_well_being.parquet')
