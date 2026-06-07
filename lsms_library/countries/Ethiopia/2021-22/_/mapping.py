"""Wave-level formatting functions for Ethiopia 2021-22 (W5).

The ``food_security`` function is picked up by name as the ``df_edit``
hook for the ``food_security`` request (see ``Wave.column_mapping`` /
``get_formatting_functions``).  It runs on the DataFrame after the 8
FIES item columns have been mapped to True/False/NaN, and it:

  1. coerces the 8 items to pandas nullable ``boolean`` dtype, and
  2. adds ``FIES_score`` = count of True across the 8 items (NaN only
     when all 8 items are NaN).
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
