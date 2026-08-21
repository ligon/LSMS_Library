# Formatting functions for GhanaSPS 2017-18 (Wave 3).
import pandas as pd


def v(value):
    '''Cluster (enumeration area) id, recovered from the household id.

    GhanaSPS household ids are `S | RR | EEE | HHH` -- split counter, region,
    EA, household.  The EA is therefore the 3 characters ENDING 3 from the
    right.

    Slice from the RIGHT, never the left.  The id is not fixed width: the
    leading split counter runs to two digits in wave 3, so 37 rows there are
    10 characters long and a left-anchored `[3:6]` shifts by one on exactly
    those rows -- yielding 336 EAs including 2 that do not exist in wave 1.
    `[-6:-3]` yields exactly 334 in every wave, all of them wave-1 EAs.
    (GH #140's `FPrimary[:6]` hypothesis is wrong by a wide margin: 583
    prefixes here and 893 in wave 3, against 334 real EAs.)
    '''
    if pd.isna(value):
        return pd.NA
    s = str(value).strip()
    if len(s) < 6:
        return pd.NA
    return s[-6:-3]
