#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe
import sys
import numpy as np
import pandas as pd
import json

C = []

lbls = json.load(open('../../_/nonfood_items.json'))

#########################
# Harvest
files = {'2016Q1':['Nigeria/2015-16/Data/sect11a_harvestw3.csv',
                   'Nigeria/2015-16/Data/sect11b_harvestw3.csv',
                   'Nigeria/2015-16/Data/sect11c_harvestw3.csv',
                   'Nigeria/2015-16/Data/sect11d_harvestw3.csv',
                   'Nigeria/2015-16/Data/sect11e_harvestw3.csv'],
         '2015Q3':['Nigeria/2015-16/Data/sect8a_plantingw3.csv',
                   'Nigeria/2015-16/Data/sect8b_plantingw3.csv',
                   'Nigeria/2015-16/Data/sect8c_plantingw3.csv']}

# GH #817: `hhid` is the HOUSEHOLD (`i`) and `item_cd` the ITEM (`j`).  These
# two were mapped the other way round, and every consumer downstream
# compensated; the canonical names are used here instead.
vars={'hhid': 'i',
      'zone': 'm',
      'item_cd' : 'j',
      's8q2': 'Expenditure',
      's8q4': 'Expenditure',
      's8q6': 'Expenditure',
      's8q8': 'Expenditure',
      's8q10': 'Expenditure',
      's11aq2': 'Expenditure',  # Has your household consumed [XX] in the past 7 days. 
      's11bq4': 'Expenditure',  # Has your household consumed [XX] in the past month.
      's11cq6': 'Expenditure',  # Has your household consumed [XX] in the past month.
      's11dq8': 'Expenditure',  # Has your household consumed [XX] in the past month.
      's11eq10': 'Expenditure',  # Has your household consumed [XX] in the past month.
      'sector': 'rural',  # 1=Urban; 2=Rural
      't':'t'
      }

for t in files.keys():
    for fn in files[t]:
        df = get_dataframe(fn)
        df = df.rename(columns=vars)

        df['t'] = t
        df = df.replace({'j':{int(k):v for k,v in lbls[t].items()}})

        df = df[list(set(vars.values()))]
        C.append(df)

###################

x = pd.concat(C,axis=0)


x['m'] = x['m'].replace({1:'North central',
                         2:'North east',
                         3:'North west',
                         4:'South east',
                         5:'South south',
                         6:'South west'})


x = x.drop_duplicates()

x['i'] = x['i'].astype(int).astype(str)

# LONG (t, i, j) x Expenditure since GH #817.  This used to end
# `x.set_index(['j','t','m','i'])['value'].unstack('i')` -- a WIDE
# item-by-household matrix with `m` in the index -- which the rest of the
# library cannot consume (`labels=`, `currency=`, `Feature()` assembly and the
# coverage grader are all written against a long `j` level) and which fills
# every unreported (household, item) pair with NaN that the country-level
# pivot then summed as 0.
#
# `m` (the zone) is DROPPED: a market/region level must not be baked into a
# cached parquet -- it is added on demand by `_add_market_index()` when the
# caller passes `market=` (CLAUDE.md, "`other_features` is obsolete").  It is
# an attribute of the household, not a key: measured across all four waves, no
# household carries more than one zone and (t, i, j) has no duplicates once it
# is removed.  The assertion below is what keeps that true.
#
# `t` stays per-ROUND (2010Q3 / 2011Q1, ...).  Nigeria is post-planting /
# post-harvest: each wave directory holds two rounds and they are never pooled
# (.claude/skills/add-feature/pp-ph/SKILL.md).
x = x.set_index(['i','t','m','j'])['Expenditure']

# Sparsity: a household that did not report an item has NO ROW (the convention
# transformations.food_expenditures_from_acquired documents).  NaN is "not
# reported"; an exact 0 is a reported zero and carries no expenditure.  Neither
# moves any total -- there is not one negative value in the corpus.
x = x.dropna()
x = x[x > 0]

x = x.reset_index().drop(columns='m').set_index(['t','i','j'])[['Expenditure']]

assert x.index.is_unique, (
    f'{int(x.index.duplicated().sum())} duplicate (t, i, j) rows -- a '
    f'household appears under two zones, so dropping `m` collapsed a real key. '
    f'Fix the key; do NOT reduce here (GH #323).')
assert len(x) > 0, 'nonfood_expenditures produced no rows'

to_parquet(x, 'nonfood_expenditures.parquet')
