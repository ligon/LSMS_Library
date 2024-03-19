#!/usr/bin/env python3
import pandas as pd
from eep153_tools.sheets import write_sheet
from importlib.resources import files


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

    def food_prices(self):
        x = self.read_parquet('food_prices').squeeze().dropna()
        x.index.names = ['m','j','u']
        x.columns.name = 't'
        return x

    def export_to_google_sheet(self,key=None,t=None,z=None):
        x = self.food_expenditures()
        if z is None:
            z = self.household_characteristics()
        fct = self.fct()
        if t is not None:
            x = x.xs(t,level='t',drop_level=False)
            z = z.xs(t,level='t',drop_level=False)
            modifier = f' ({t})'
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

        return key
