"""Construct mapping from consumption unit codes into labels, with conversion into metric units.

This mapping differs from the 2012-13 mapping in two ways.  First, the
mapping is the same across both post-harvest and post-planting waves.
But second, the mapping of non-metric units is allowed to vary by
zone.
"""
import dvc.api
import pandas as pd
import json

unitcodes = {1:'Kg',
             2:'g',
             3:'l',
             4:'cl',
             10:'bin/basket',
             11:'paint rubber',
             12:'milk cup',
             13:'cigarette cup',
             14:'tin',
             20:'small congo',
             21:'large congo',
             30:'small mudu',
             31:'large mudu',
             40:'small derica',
             41:'medium derica',
             42:'large derica',
             43:'very large derica',
             50:'small tiya',
             51:'medium tiya',
             52:'large tiya',
             60:'small kobiowu',
             61:'medium kobiowu',
             62:'large kobiowu',
             70:'small bowl',
             71:'medium bowl',
             72:'large bowl',
             80:'small piece',
             81:'medium piece',
             82:'large piece',
             90:'small heap',
             91:'medium heap',
             92:'large heap',
             100:'small bunch',
             101:'medium bunch',
             102:'large bunch',
             110:'small stalk',
             111:'medium stalk',
             112:'large stalk',
             120:'small packet/sachet',
             121:'medium packet/sachet',
             122:'large packet/sachet',
             900:'other specify'}

# Preferred labels for foods
with open('../../_/food_items.json') as f:
    foodlabels=json.load(f)

# See https://microdata.worldbank.org/index.php/catalog/2734/data-dictionary/F112?file_name=food_conv_w3

with dvc.api.open('../../2015-16/Data/food_conv_w3.csv') as csv:
    conv0 = pd.read_csv(csv)

# Replace unit codes and food codes with labels
conv = conv0.replace({'unit_cd':unitcodes,
                     'item_cd':{int(k):v for k,v in foodlabels['2015Q3'].items()}})

conv = conv.drop(['item_name','unit_name','conv_national','note','unit_other'],axis=1)


conv.rename(columns={'item_cd':'i',
                     'unit_cd':'u',
                     'conv_NC_1':'North central',
                     'conv_NE_2':'North east',
                     'conv_NW_3':'North west',
                     'conv_SE_4':'South east',
                     'conv_SS_5':'South south',
                     'conv_SW_6':'South west'},
            inplace=True)

conv = conv.set_index(['i','u'])
conv.columns.name = 'm'
conv = conv.stack('m')

conv = pd.DataFrame({'2015Q3':conv,
                     '2016Q1':conv})
conv.columns.name ='t'
conv = conv.stack('t')

conv = pd.to_numeric(conv,errors='coerce')

# Drop some duplicate entries
conv = conv[~conv.index.duplicated()]

conv = conv.reorder_levels(['t','m','i','u'])

#conv.to_json('units.json')
