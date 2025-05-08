import pandas as pd
import numpy as np
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

p = get_dataframe('food_unitvalues.parquet').squeeze()
q = get_dataframe('food_quantities.parquet').squeeze()

x = p*q

x = x.groupby(['j','t','i']).sum()
x = x.replace(0,np.nan).dropna()

to_parquet(pd.DataFrame({'x':x}), 'food_expenditures.parquet')
