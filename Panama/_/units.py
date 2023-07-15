#!/usr/bin/env python3
import json
import pandas as pd
import dvc.api

#converts the unit conversion table csv to a json
# NOTE: does NOT work as the json generated is invalid, had to be manually reformatted

with dvc.api.open('../1997/Data/unittable.csv', mode='rb') as dta:
    units = pd.read_csv(dta)

units.to_json('units.json', orient='records', lines=True)
