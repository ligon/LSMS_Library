#!/usr/bin/env python

import pandas as pd
import pyreadstat
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster

def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = get_household_roster(df, sex=sex, sex_converter=sex_converter,
                                  age=age, age_converter=age_converter, HHID=hhid,
                                  convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
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
    food_items.set_index(key,inplace=True)

    return food_items.squeeze().str.strip().to_dict()
