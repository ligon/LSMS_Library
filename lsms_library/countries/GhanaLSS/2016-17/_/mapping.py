# Formatting Functions for Ghana 2016-17
import pandas as pd
import lsms_library.local_tools as tools


def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)


def Int_t(value):
    '''
    Build interview date from (ddate, mdate, ydate).

    mdate is a month *name* (e.g. "October"); ddate and ydate are numeric.
    '''
    d, m, y = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(d) or pd.isna(m) or pd.isna(y):
        return pd.NaT
    s = f"{int(y)}-{str(m).strip()}-{int(d)}"
    return pd.to_datetime(s, errors='coerce')


def _fies_item(value):
    '''
    Binarize a single FAO FIES item: Yes->True, No->False,
    "Don't Know"/Refused/NaN -> NA.
    '''
    s = str(value).strip().lower()
    if s == 'yes':
        return True
    if s == 'no':
        return False
    return pd.NA


# The 8 FAO FIES experience items in canonical order.
Worried = _fies_item       # s9cq1: worried wouldn't have enough food
HealthyDiet = _fies_item   # s9cq2: unable to eat healthy/nutritious food
FewFoods = _fies_item      # s9cq3: ate only a few kinds of foods
SkippedMeal = _fies_item   # s9cq4: had to skip a meal
AteLess = _fies_item       # s9cq5: ate less than thought should
RanOut = _fies_item        # s9cq6: household ran out of food
Hungry = _fies_item        # s9cq7: hungry but did not eat
WholeDay = _fies_item      # s9cq8: went a whole day without eating


def FIES_score(value):
    '''
    Raw FIES score = count of affirmative (Yes) responses across the 8
    FAO FIES items.  Returns NA only when all 8 items are missing/non-Yes-No.

    `value` is the 8-column row (s9cq1..s9cq8) in FAO order.
    '''
    items = [_fies_item(value.iloc[k]) for k in range(8)]
    if all(pd.isna(x) for x in items):
        return pd.NA
    return int(sum(1 for x in items if x is True))
