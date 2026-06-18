#!/usr/bin/env python

import pandas as pd
import pyreadstat
import numpy as np
import json
def _household_roster_from_df(df, sex, age, HHID, sex_converter=None, age_converter=None,
                               months_spent='months_spent', Age_ints=None):
    """Inline replacement for lsms.tools.get_household_roster(fn_type=None)."""
    cols = [c for c in [HHID, sex, age, months_spent] if c in df.columns]
    df = df.loc[:, cols].rename(columns={HHID: 'HHID', sex: 'sex', age: 'age',
                                          months_spent: 'months_spent'})
    if sex_converter is not None:
        df['sex'] = df['sex'].apply(sex_converter)
    df = df.dropna(how='any')
    df['sex'] = df['sex'].apply(lambda s: str(s[0]).lower())
    if age_converter is not None:
        df['age'] = df['age'].apply(age_converter)
    df['boys']  = (df['sex'] == 'm') & (df['age'] < 18)
    df['girls'] = (df['sex'] == 'f') & (df['age'] < 18)
    df['men']   = (df['sex'] == 'm') & (df['age'] >= 18)
    df['women'] = (df['sex'] == 'f') & (df['age'] >= 18)
    if Age_ints is None:
        Age_ints = ((0,1),(1,5),(5,10),(10,15),(15,20),(20,30),(30,50),(50,60),(60,100))
    valvars = list({'HHID','girls','boys','men','women'}.intersection(df.columns))
    for lo, hi in Age_ints:
        s, e = lo, hi - 1
        df['Males %02d-%02d' % (s, e)]   = (df['sex'] == 'm') & (df['age'] >= lo) & (df['age'] < hi)
        df['Females %02d-%02d' % (s, e)] = (df['sex'] == 'f') & (df['age'] >= lo) & (df['age'] < hi)
        valvars += ['Males %02d-%02d' % (s, e), 'Females %02d-%02d' % (s, e)]
    try:
        if df['HHID'].iloc[0].split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError):
        pass
    if 'months_spent' in df.columns and df['months_spent'].count() > 0:
        g = df.loc[df['months_spent'] > 0, valvars].groupby('HHID')
    else:
        g = df[valvars].groupby('HHID')
    return g.sum()


def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = _household_roster_from_df(df, sex=sex, age=age, HHID=hhid,
                                       sex_converter=sex_converter,
                                       age_converter=age_converter,
                                       Age_ints=Age_ints)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf


def harmonized_food_labels(fn='../../_/food_items.csv',key='Code',value='Preferred Label'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:lambda s: s.strip(),2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items.loc[:,food_items.count()>0]
    food_items = food_items.apply(lambda x: x.str.strip())

    if type(key) is not str:  # Assume a series of foods
        myfoods = set(key.values)
        for key in food_items.columns:
            if len(myfoods.difference(set(food_items[key].values)))==0: # my foods all in key
                break

    food_items = food_items[[key,value]].dropna()
    food_items = food_items.set_index(key)

    return food_items.squeeze().str.strip().to_dict()


def individual_education(df):
    """Refine Guatemala ENCOVI 2000 Educational Attainment with the grade year.

    df_edit hook for ``individual_education`` (GH #493).  By the time this
    runs, the YAML ``mappings: harmonize_education`` table has already mapped
    the bare ``p07b27a`` nivel labels onto *coarse* canonical levels:

        ninguno -> None,  preparatoria -> Pre-primary,
        primaria -> "Primary complete",  educacion media -> "Lower secondary",
        educacion superior -> Bachelor,  post-grado -> Postgraduate,
        educacion adultos -> Informal,  nivel 9 -> Unknown.

    Two levels need the grade year ``p07b27b`` (carried in ``edu_grade``) to
    place them on the ordinal scale; this hook supplies that refinement and
    drops the ``edu_grade`` helper column:

      * Primary  (6-year cycle): grado 1-5 -> "Primary incomplete",
        grado >=6 -> "Primary complete" (the coarse default, unchanged).
      * Secondary "educacion media" = ciclo basico (years 1-3, lower
        secondary) + ciclo diversificado (years 4-6, upper secondary):
            grado 1-2 -> "Lower secondary"          (some basico)
            grado 3   -> "Lower secondary complete" (basico finished)
            grado 4-5 -> "Upper secondary"          (some diversificado)
            grado >=6 -> "Upper secondary complete" (diversificado finished)

    Rows with a missing/zero grade keep the coarse entry-tier label.
    """
    grade = pd.to_numeric(df.get('edu_grade'), errors='coerce')

    # nivel code 9 is unlabelled in Stata, so convert_categoricals leaves it as
    # the numeric value 9.0 -- the extraction-time `mappings:` table (string
    # keys) can't reach it before it is stringified downstream.  Map it to
    # Unknown here, where it is still numeric.  ``replace`` matches 9 / 9.0 /
    # "9" / "9.0" so it is robust to either dtype.
    att = df['Educational Attainment']
    df['Educational Attainment'] = att = att.replace(
        {9: 'Unknown', 9.0: 'Unknown', '9': 'Unknown', '9.0': 'Unknown'})

    prim = att == 'Primary complete'
    df.loc[prim & (grade < 6), 'Educational Attainment'] = 'Primary incomplete'

    media = att == 'Lower secondary'
    df.loc[media & (grade == 3), 'Educational Attainment'] = 'Lower secondary complete'
    df.loc[media & (grade >= 4) & (grade <= 5), 'Educational Attainment'] = 'Upper secondary'
    df.loc[media & (grade >= 6), 'Educational Attainment'] = 'Upper secondary complete'

    if 'edu_grade' in df.columns:
        df = df.drop(columns='edu_grade')
    return df
