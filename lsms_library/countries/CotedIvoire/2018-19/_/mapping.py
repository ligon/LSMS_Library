# Formatting Functions for CotedIvoire 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import food_acquired_to_canonical as food_acquired


def pid(value):
    '''Formatting person id from (grappe, menage, individual).'''
    return (tools.format_id(value.iloc[0]) + '0'
            + tools.format_id(value.iloc[1], zeropadding=2) + '0'
            + tools.format_id(value.iloc[2], zeropadding=2))


def Age(value):
    '''
    Pass Age columns through as a list for age_handler.

    CotedIvoire s01q03b (month) is already an integer (1-12); no
    month_map conversion needed.  The list is returned unchanged so
    household_roster() can unpack [age_raw, day, month, year].
    '''
    return list(value)


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    CotedIvoire s01q03b is an integer month (1-12) — no month_map needed.
    DOB columns use true NaN (no sentinel), so no additional sentinel handling required.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # Pass None for negative sentinel values so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        result = tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df

