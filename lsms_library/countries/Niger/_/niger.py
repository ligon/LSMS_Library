import pandas as pd
import numpy as np
import json
import dvc.api
from ligonlibrary.dataframes import from_dta
from lsms.tools import get_household_roster
import pyreadstat
import lsms_library.local_tools as tools


def i(value):
    '''
    Formatting household id. Handles both scalar (hid) and composite (grappe, menage) cases.
    Matches existing convention: str(grappe) + str(menage) (no padding).
    '''
    if isinstance(value, (pd.Series, np.ndarray, list, tuple)):
        return tools.format_id(value[0]) + tools.format_id(value[1])
    return tools.format_id(value)

Waves = {'2011-12': (),
         '2014-15': (),
         '2018-19': (),
         '2021-22': ()}

def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = get_household_roster(df, sex=sex, sex_converter=sex_converter,
                                  age=age, age_converter=age_converter, HHID=hhid,
                                  convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf

def age_handler(df, interview_date = None, format_interv = None, age = None, dob = None, format_dob  = None, m = None, d = None, y = None, interview_year = None):
    '''
    a function to fill ages with the best available information for age, prioritizes more precise estimates

    Args:
        interview_date : column name containing interview date
        format_interv: argument to be passed into pd.to_datetime(, format=) for interview_date
        age : column name containing age in years
        dob: column name containing date of birth
        format_dob: to be passed into pd.to_datetime(, format=) for date of birth
        m, d, y: pass column names for month, day, and year respectively
        interview_year: column name containing year of interview; please enter an estimation in case an interview date is not found

    Returns:
    dataframe: mutates the dataframe to add an 'age' column and returns the dataframe
    '''

    if interview_date:
        df[interview_date] = pd.to_datetime(df[interview_date], format = format_interv)
    if dob:
        df[dob] = pd.to_datetime(df[dob], format = format_dob)

    def _safe_int(val):
        """Convert to int, returning None for Stata missing codes ('.')."""
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    def fill_func(x):
        if age and pd.notna(x[age]):
            v = _safe_int(x[age])
            return v if v is not None else pd.NA

        #conversion to pd.datetime obj of the date of birth if we are given mdy
        date_of_birth = None
        year_born = None
        if (m and d and y) and (x[[m, d, y]].notna().all()):
            mi, di, yi = _safe_int(x[m]), _safe_int(x[d]), _safe_int(x[y])
            if mi is not None and di is not None and yi is not None:
                date_conv = f'{mi}/{di}/{yi}'
                date_of_birth = pd.to_datetime(date_conv, format='%m/%d/%Y',
                                               errors='coerce')

        if dob and pd.notna(x[dob]):
            date_of_birth = x[dob]

        if pd.notna(date_of_birth):
            year_born = date_of_birth.year
            if interview_date and pd.notna(x[interview_date]):
                return (x[interview_date] - date_of_birth).days / 365.25

        elif (y and pd.notna(x[y])) or pd.notna(year_born):
            used_year = year_born or _safe_int(x[y])
            if used_year is None:
                return pd.NA
            if interview_date and pd.notna(x[interview_date]):
                return x[interview_date].year - used_year
            elif interview_year and pd.notna(x[interview_year]):
                iy = _safe_int(x[interview_year])
                return (iy - used_year) if iy is not None else pd.NA

        else:
            return pd.NA

    df['age'] = df.apply(fill_func, axis = 1)

    return df
