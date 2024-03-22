#!/usr/bin/env python3
import pandas as pd
import numpy as np
from eep153_tools.sheets import write_sheet
from importlib.resources import files

pd.set_option('future.no_silent_downcasting', True)

class Country:
    def __init__(self,country_name):
        self.name = country_name

    @property
    def resources(self):
        var = files("lsms_library") / "countries" / self.name / "var"

        return var

    def read_parquet(self,parquet):
        try:
            return pd.read_parquet((self.resources / f'{parquet}.parquet'))
        except FileNotFoundError:
            print(f"Need to build {parquet}")

    def food_expenditures(self):
        x = self.read_parquet('food_expenditures').squeeze().dropna()
        x.index.names = ['i','t','m','j']
        return x

    def household_characteristics(self):
        x = self.read_parquet('household_characteristics')
        x.index.names = ['i','t','m']
        return x

    def fct(self):
        x = self.read_parquet('fct')
        if x is None: return
        x.index.name = 'j'
        return x

    def food_prices(self,drop_na_units=True):
        x = self.read_parquet('food_prices').squeeze()
        try:
            x = x.stack(x.columns.names,future_stack=True).dropna()
        except AttributeError: # Already a series?
            x = x.dropna()

        if len(x.index.names)==4:
            x = x.reorder_levels(['t','m','i','u'])
        elif len(x.index.names)==5: # Individual level?
            x = x.reorder_levels(['j','t','m','i','u'])
            x = x.groupby(['t','m','i','u']).median()

        x.index = x.index.rename({'i':'j'})
        if drop_na_units:
            u = x.reset_index('u')['u'].replace(['<NA>','nan'],np.nan)
            x = x.loc[~pd.isnull(u).values,:]
        x = x.reset_index().set_index(['t','m','j','u']).squeeze()
        x = x.unstack(['t','m'])

        return x

    def export_to_google_sheet(self,key=None,t=None,z=None):
        sheets = {"Food Expenditures":self.food_expenditures(),
                  'FCT':self.fct(),
                  'Food Prices':self.food_prices()}

        if z is None:
            sheets['Household Characteristics'] = self.household_characteristics()
        else:
            sheets['Household Characteristics'] = z

        if t is not None:
            sheets['Food Expenditures'] = sheets['Food Expenditures'].xs(t,level='t',drop_level=False)
            sheets['Household Characteristics'] = sheets['Household Characteristics'].xs(t,level='t',drop_level=False)
            sheets['Food Prices'] = sheets['Food Prices'].xs(t,level='t',drop_level=False,axis=1)
            modifier = f' ({t})'
        else:
            modifier = ''

        k = 'Food Expenditures'
        v = sheets.pop(k)
        if key is None:
            key = write_sheet(v.unstack('j'),
                          'ligon@berkeley.edu',user_role='writer',
                          json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
                          sheet=k+modifier)
            print(f"Key={key}")
        else:
            write_sheet(v.unstack('j'),
                        'ligon@berkeley.edu',user_role='writer',
                        json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
                        sheet=k+modifier,key=key)

        for k,v in sheets.items():
            if v is not None:
                write_sheet(v,
                            'ligon@berkeley.edu',user_role='writer',
                            json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
                            sheet=k+modifier,key=key)

        return key
