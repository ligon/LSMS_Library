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
        x.index.names = ['j','u']
        assert np.all(x.columns.names == ['t','m'])
        x = x.stack(x.columns.names,future_stack=True).dropna()
        if drop_na_units:
            u = x.reset_index('u')['u'].replace(['<NA>','nan'],np.nan)
            x = x.loc[~pd.isnull(u).values,:]
        x = x.reset_index().set_index(['j','u','t','m']).squeeze()
        x = x.unstack(['t','m'])
        return x

    def export_to_google_sheet(self,key=None,t=None,z=None):
        x = self.food_expenditures()
        if z is None:
            z = self.household_characteristics()
        fct = self.fct()
        food_prices = self.food_prices()

        if t is not None:
            x = x.xs(t,level='t',drop_level=False)
            z = z.xs(t,level='t',drop_level=False)
            modifier = f' ({t})'
            food_prices = food_prices.xs(t,level='t',drop_level=False,axis=1)
        else:
            modifier = ''

        if key is None:
            key = write_sheet(x.unstack('j'),
                          'ligon@berkeley.edu',user_role='writer',
                          json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
                          sheet='Food Expenditures'+modifier)
            print(f"Key={key}")
        else:
            write_sheet(x.unstack('j'),
                        'ligon@berkeley.edu',user_role='writer',
                        json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
                        sheet='Food Expenditures'+modifier,key=key)

        write_sheet(z,
                    'ligon@berkeley.edu',user_role='writer',
                    json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
                    sheet='Household Characteristics'+modifier,key=key)

        if fct is not None:
            write_sheet(self.fct(),
                    'ligon@berkeley.edu',user_role='writer',
                    json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
                        sheet='FCT',key=key)

        if food_prices is not None:
            write_sheet(self.food_prices(),
                    'ligon@berkeley.edu',user_role='writer',
                    json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
                        sheet='Food Prices',key=key)

        return key
