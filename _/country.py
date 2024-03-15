#!/usr/bin/env python3
import pandas as pd
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
            x = pd.read_parquet((self.resources / f'{parquet}.parquet'))
        except FileNotFoundError:
            print("Need to build {parquet}")

        return x

    def food_expenditures(self):
        return self.read_parquet('food_expenditures')

    def household_characteristics(self):
        return self.read_parquet('household_characteristics')

    def nutrition(self):
        return self.read_parquet('nutrition')
