import pandas as pd
import numpy as np

p = pd.read_parquet('food_unitvalues.parquet').squeeze()
q = pd.read_parquet('food_quantities.parquet').squeeze()

x = p*q

x = x.groupby(['j','t','i']).sum()
x = x.replace(0,np.nan).dropna()

pd.DataFrame({'x':x}).to_parquet('food_expenditures.parquet')
