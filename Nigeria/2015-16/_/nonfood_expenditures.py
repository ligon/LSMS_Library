#!/usr/bin/env python
import sys
import numpy as np
import pandas as pd
import json
import dvc.api

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

vars={'hhid': 'j',
      'zone': 'm',
      'item_cd' : 'i',
      's8q2': 'value',
      's8q4': 'value',
      's8q6': 'value',
      's8q8': 'value',
      's8q10': 'value',
      's11aq2': 'value',  # Has your household consumed [XX] in the past 7 days. 
      's11bq4': 'value',  # Has your household consumed [XX] in the past month.
      's11cq6': 'value',  # Has your household consumed [XX] in the past month.
      's11dq8': 'value',  # Has your household consumed [XX] in the past month.
      's11eq10': 'value',  # Has your household consumed [XX] in the past month.
      'sector': 'rural',  # 1=Urban; 2=Rural
      't':'t'
      }

for t in files.keys():
    for fn in files[t]:
        with dvc.api.open(fn,mode='rb') as csv:
            df = pd.read_csv(csv)

            df = df.rename(columns=vars)

            df['t'] = t
            df = df.replace({'i':{int(k):v for k,v in lbls[t].items()}})

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

x['j'] = x['j'].astype(int).astype(str)

x.set_index(['j','t','m','i'],inplace=True)

x = x['value'].unstack('i')

x.to_parquet('nonfood_expenditures.parquet',compression='gzip')
