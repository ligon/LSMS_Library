import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
import pyreadstat

def i(x):
    """Create hhid from grappe + menage concatenation.
    
    Detects which wave based on column case:
    - 2014-15 (ECVMA): uppercase columns (GRAPPE, MENAGE) -> no prefix
    - 2018-19 (EHCVM): lowercase columns (grappe, menage) -> 'E_' prefix
    """
    if isinstance(x, pd.Series):
        grappe = str(int(x.iloc[0])) if pd.notna(x.iloc[0]) else ''
        menage = str(int(x.iloc[1])) if pd.notna(x.iloc[1]) else ''
        
        # Check column names to detect which wave
        # 2018-19 uses lowercase 'grappe', 2014-15 uses uppercase 'GRAPPE'
        col_names = x.index.tolist()
        is_ehcvm_2018 = any(c.islower() for c in str(col_names[0]))
        
        if is_ehcvm_2018:
            # 2018-19 EHCVM: add prefix to prevent matching with ECVMA panel
            return 'E_' + grappe + menage
        else:
            # 2014-15 ECVMA: no prefix, may include EXTENSION
            if len(x) > 2:
                extension = str(int(x.iloc[2])) if pd.notna(x.iloc[2]) else '0'
                return grappe + menage + extension
            return grappe + menage
    return str(int(x))

def panel_ids(df):
    """Construct previous_i from GRAPPE + MENAGE to match 2011-12 hid format"""
    # previous_i is just GRAPPE + MENAGE (without EXTENSION) to match 2011's hid
    df['previous_i'] = (
        df['previous_grappe'].astype(str).str.replace('.0', '', regex=False).str.strip() + 
        df['previous_menage'].astype(str).str.replace('.0', '', regex=False).str.strip()
    )
    return df[['previous_i']]

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
