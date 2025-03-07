import pandas as pd
import numpy as np

Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
df = pd.read_parquet('household_roster.parquet')

def person_tag(x, age_ints):
    if pd.isnull(x['age']):
        return x['sex']
    for i in age_ints:
        if x['age'] in range(i[0], i[1]):
            return x['sex'] + 's ' + str(i[0]) + '-' + str(i[1])

df = df.reset_index()
# df['person_tag'] = df.apply(lambda x: person_tag(x, Age_ints), axis=1)
# pivot_table = df.pivot_table(index=['j','t'],
#                              columns='person_tag',
#                              aggfunc='size',
#                              fill_value=0)



# check whether the individual id, sex, and age seems consistent across waves
from nigeria import Waves, waves