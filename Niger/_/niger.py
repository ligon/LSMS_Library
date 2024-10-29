import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
import pyreadstat

def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = get_household_roster(df, sex=sex, sex_converter=sex_converter,
                                  age=age, age_converter=age_converter, HHID=hhid,
                                  convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf

def age_handler(df, interview_date = None, format_interv = None, age = None, dob = None, format_dob  = None, m = None, d = None, y = None, interview_year = None):
    # a function to fill ages with the best available information for age, prioritizes more precise estimates
    # interview_date : column name containing interview date
    # format_interv: argument to be passed into pd.to_datetime(, format=) for interview_date
    # age : column name containing age in years
    # dob: column name containing date of birth
    # format_dob: to be passed into pd.to_datetime(, format=) for date of birth
    # m, d, y: pass column names for month, day, and year respectively
    # interview_year: column name containing year of interview; please enter an estimation in case an interview date is not found
    # this could be improved for mildly more precise calculations given month and year, but I'm not sure if that's relevant enough
    if interview_date:
        df[interview_date] = pd.to_datetime(df[interview_date], format = format_interv)
    if dob:
        df[dob] = pd.to_datetime(df[dob], format = format_dob)

    def fill_func(x):
        if age and pd.notna(x[age]):
            return int(x[age])

        #conversion to pd.datetime obj of the date of birth if we are given mdy
        date_of_birth = None
        year_born = None
        if (m and d and y) and (x[[m, d, y]].notna().all()):
            date_conv = str(int(x[m])) + '/' + str(int(x[d])) + '/' + str(int(x[y]))
            date_of_birth = pd.to_datetime(date_conv, format = '%m/%d/%Y')

        if dob and pd.notna(x[dob]):
            date_of_birth = x[dob]

        if pd.notna(date_of_birth):
            year_born = date_of_birth.year
            if interview_date and pd.notna(x[interview_date]):
                return (x[interview_date] - date_of_birth).days / 365.25

        elif (y and pd.notna(x[y])) or pd.notna(year_born):
            used_year = year_born or x[y]
            if interview_date and pd.notna(x[interview_date]):
                return x[interview_date].year - int(used_year)
            elif interview_year and pd.notna(x[interview_year]):
                return int(x[interview_year]) - int(used_year)

        else:
            return np.nan

    df['age'] = df.apply(fill_func, axis = 1)

    return df
