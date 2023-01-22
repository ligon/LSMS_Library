"""
Construct mapping from consumption unit codes into labels, with conversion into metric units.
"""
import dvc.api
import pandas as pd
import json

# Preferred labels for foods
with open('../../_/food_items.json') as f:
    foodlabels=json.load(f)

# See https://microdata.worldbank.org/index.php/catalog/1952/data-dictionary/F114?file_name=food_conv_w2ph
unitcodes = {1:'Kg',
             2:'g',
             3:'l',
             4:'ml',
             5:'mudu',
             6:'olodo',
             7:'congo',
             8:'paint rubber',
             9:'large derica',
             10:'medium derica',
             11:'small derica',
             12:'milk cup',
             13:'cigarette cup',
             14:'tiya',
             15:'kobiowu',
             16:'piece'}

with dvc.api.open('../Data/food_conv_w2ph.csv') as csv:
    conv_ph = pd.read_csv(csv)

# Replace unit codes and food codes with labels
conv_ph = conv_ph.replace({'nsu_cd':unitcodes,
                     'item_cd':{int(k):v for k,v in foodlabels['2012Q3'].items()}})

with dvc.api.open('../Data/food_conv_w2pp.csv') as csv:
    conv_pp = pd.read_csv(csv)

# Replace unit codes and food codes with labels
conv_pp = conv_pp.replace({'s7bq2b':unitcodes,
                     'item_cd':{int(k):v for k,v in foodlabels['2013Q1'].items()}})

conv_ph.rename(columns={'item_cd':'i',
                        'nsu_cd':'u',
                        'conv':'Mapping'},inplace=True)

conv_pp.rename(columns={'item_cd':'i',
                        's7bq2b':'u',
                        'conv':'Mapping'},inplace=True)

conv_ph = conv_ph.set_index(['i','u'])
conv_pp = conv_pp.set_index(['i','u'])


conv = pd.DataFrame({'2012Q3':conv_ph['Mapping'],
                     '2013Q1':conv_pp['Mapping']})

conv.columns.name = 't'

conv = conv.stack('t')

conv = pd.DataFrame({'North central':conv,
                     'North east':conv,
                     'North west':conv,
                     'South east':conv,
                     'South south':conv,
                     'South west':conv})
                     
conv.columns.name = 'm'
conv = conv.stack('m')

# Add metric units
conv = conv.unstack('u')
conv['Kg'] = 1000
conv['g'] = 1
conv['ml'] = 1
conv['l'] = 1000

conv=conv.stack('u')

conv = pd.to_numeric(conv,errors='coerce')

# Drop some duplicate entries
conv = conv[~conv.index.duplicated()]

conv = conv.reorder_levels(['t','m','i','u'])

conv = conv/1000 # Kgs or L

#conv.to_json('units.json')
                        
