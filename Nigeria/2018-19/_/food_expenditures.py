#!/usr/bin/env python
import pandas as pd

harvest = pd.read_parquet('food_expenditures_harvest.parquet')
planting = pd.read_parquet('food_expenditures_planting.parquet')

x = pd.concat([harvest,planting],axis=0)

x.to_parquet('food_expenditures.parquet')


