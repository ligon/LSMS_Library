"""
Pull nutrients info for the fct_addtion using fooddatacentral; harmonize structure and labels
"""

import pandas as pd
import fooddatacentral as fdc
import warnings

#create a DataFrame of nutritional contents of food items
#Default portion: 100g
def nutrient_df(df, apikey):
    df = df.dropna().reset_index().drop(columns = 'index')
    D = {}
    count = 0
    for food in df["Preferred Label"].tolist():
        try:
            FDC = df.loc[df["Preferred Label"] ==food,:]["FDC ID"][count]
            count+=1
            D[food] = fdc.nutrients(apikey,FDC).Quantity
        except AttributeError:
            warnings.warn("Couldn't find FDC Code %s for food %s." % (food,FDC))

    D = pd.DataFrame(D,dtype=float)

    return D
    
#create a DataFrame of units of nutrients of food items
def unit_df(df, apikey):
    D = {}
    count = 0
    for food in df["Preferred Label"].tolist():
        try:
            FDC = df.loc[df["Preferred Label"] ==food,:]["FDC ID"][count]
            count+=1
            D[food] = fdc.nutrients(apikey,FDC).Units
        except AttributeError:
            warnings.warn("Couldn't find FDC Code %s for food %s." % (food,FDC))

    D = pd.DataFrame(D)

    return D

#harmonize structure and nutrient labels for the df with fdc foods 
def harmonize_nutrient(df, nutrient_labels_df):
    #fill NaNs in the "Energy" row with values in "Energy (Atwater General Factors)"
    e1 = "Energy"
    e2 = "Energy (Atwater General Factors)"
    df.loc[[e1, e2]] = df.loc[[e1, e2]].fillna(method="bfill")
    
    #rename
    n_labels_fct = dict(zip(nutrient_labels_df['FDC Label'], nutrient_labels_df['Preferred Label'], ))

    df = df.loc[n_labels_fct.keys()].rename(index = n_labels_fct).fillna(0)
    df.index.name = 'i'
        
    #convert default portion of fdc from 100g to 1kg
    df = df * 10
    
    return df.T

#match foods that are existent in the given fct (Tanzania in this case);harmonize nutrient lables, and return a fct 
def fct_filter(food_items_df, nutrient_labels_df, fct_origin_df):
    #filter original fct for only foods appeared in dataset
    food_items = dict(zip(food_items_df['Preferred Label'], food_items_df['FTC Code']))
    fct_origin = fct_origin_df.set_index('FCT Code')
    fct_origin = fct_origin.loc[fct_origin.index.intersection(food_items.values()),:]
    #harmonize food labels 
    fct = fct_origin.rename(index={v:k for k,v in food_items.items()})
    fct.index.name = 'i'
    fct = fct[~fct.index.duplicated()]
    fct = fct.drop(columns = 'FCT Label').sort_index()

    #harmonize nutrient labels
    n_labels_fct = dict(zip(nutrient_labels_df['Preferred Label'], nutrient_labels_df['FCT Label']))
    n_labels_fct = {v:k for k, v in n_labels_fct.items() if v != ''}
    fct = fct.rename(columns = n_labels_fct)
    fct.columns.name = 'n'
    # Anything that's stringlike we make a number
    fct = fct.apply(lambda x: pd.to_numeric(x,errors='coerce'))
    # Convert serving size to Kgs instead of hectograms
    fct = fct*10
    # Replace any missing values with zeros
    fct = fct.fillna(0)
    return fct

