"""Wave-level formatting functions for Ethiopia 2021-22 (W5).

The ``food_security`` function is picked up by name as the ``df_edit``
hook for the ``food_security`` request (see ``Wave.column_mapping`` /
``get_formatting_functions``).  It runs on the DataFrame after the 8
FIES item columns have been mapped to True/False/NaN, and it:

  1. coerces the 8 items to pandas nullable ``boolean`` dtype, and
  2. adds ``FIES_score`` = count of True across the 8 items (NaN only
     when all 8 items are NaN).

``interview_date`` is the same kind of by-name df_edit hook (GH #323).
"""

import pandas as pd

FIES_ITEMS = [
    "Worried", "HealthyDiet", "FewFoods", "SkippedMeal",
    "AteLess", "RanOut", "Hungry", "WholeDay",
]


def food_security(df):
    """Coerce FIES items to nullable boolean and add FIES_score."""
    df = df.copy()

    items = [c for c in FIES_ITEMS if c in df.columns]
    for c in items:
        df[c] = df[c].astype("boolean")

    # Count of affirmative responses; NaN treated as not-counted for the
    # sum, but rows where every item is NaN get a NaN score (question not
    # administered / unit non-response), not a spurious 0.
    score = df[items].sum(axis=1, skipna=True)
    all_na = df[items].isna().all(axis=1)
    score = score.where(~all_na, other=pd.NA)
    df["FIES_score"] = score.astype("Int64")

    return df


def interview_date(df):
    """Coerce the W5 §PH-cover InterviewDate to a real datetime (GH #323).

    The source column is a STRING with an explicit '##N/A##' sentinel in 112
    of its 1,596 holder rows.  Left as-is, the sentinel is a truthy string:
    it survives into the table as a bogus "date", and it makes the column's
    dtype object, so the declared ``min`` reducer in data_scheme.yml would
    compare strings rather than timestamps -- and '##N/A##' sorts BEFORE any
    ISO date, so min() would return the sentinel for every affected household.

    ``errors='coerce'`` maps the sentinel (and any other unparseable value) to
    NaT, which min() then skips.  A household keeps a date if either of its
    holders reported one, and stays NaT only if neither did -- class-2
    (honestly missing) rather than class-1 (silently wrong).
    """
    df = df.copy()
    if 'int_t' in df.columns:
        df['int_t'] = pd.to_datetime(df['int_t'], errors='coerce')
    return df
