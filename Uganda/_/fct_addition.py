"""
Pull nutrients info for the fct_addtion using fooddatacentral; harmonize structure and labels
"""

import pandas as pd
import fooddatacentral as fdc
import warnings

#create a DataFrame of nutritional contents of food items
#Default portion: 100g
def nutrient_df(df, apikey):
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

#harmonize structure and nutrient labels in accordance with the fct table nutrition.org
def harmonize_nutrient(df):
    #fill NaNs in the "Energy" row with values in "Energy (Atwater General Factors)"
    e1 = "Energy"
    e2 = "Energy (Atwater General Factors)"
    df.loc[[e1, e2]] = df.loc[[e1, e2]].fillna(method="bfill")
    
    #rename
    labels = {"Energy" : "Energy",
              "Protein" : "Protein",
              "Fiber, total dietary" : "Fiber",
              "Folate, total" : "Folate",
              "Calcium, Ca" : "Calcium",
              "Carbohydrate, by difference" : "Carbohydrate",
              "Iron, Fe" : "Iron",
              "Magnesium, Mg" : "Magnesium",
              "Niacin" : "Niacin",
              "Phosphorus, P" : "Phosphorus",
              "Potassium, K" : "Potassium",
              "Riboflavin" : "Riboflavin",
              "Thiamin" : "Thiamin",
              "Vitamin A, RAE" : "Vitamin A",
              "Vitamin B-12" : "Vitamin B-12",
              "Vitamin B-6" : "Vitamin B-6",
              "Vitamin C, total ascorbic acid" : "Vitamin C",
              "Vitamin E (alpha-tocopherol)" : "Vitamin E",
              "Vitamin K (phylloquinone)" : "Vitamin K",
              "Zinc, Zn" : "Zinc"
            }
    df = df.loc[labels.keys()].rename(index = labels).fillna(0)
    df.index.name = 'i'
        
    #convert default portion of fdc from 100g to 1kg
    df = df * 10
    
    return df.T

