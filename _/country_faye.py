#!/usr/bin/env python3
import pandas as pd
import numpy as np
import yaml
from eep153_tools.sheets import write_sheet
from importlib.resources import files
import importlib
import cfe.regression as rgsn
from collections import defaultdict
from .local_tools import df_data_grabber, format_id, get_categorical_mapping
import os
import warnings
from pathlib import Path

# pd.set_option('future.no_silent_downcasting', True)


class Wave:
    def __init__(self, year, country_name, data_scheme):
        self.year = year
        self.country = country_name
        self.data_scheme = data_scheme
    
    @property
    def file_path(self):
        var = files("lsms_library") / "countries" / self.country/self.year
        return var
    
    @property
    def resources(self):
        var = self.file_path/ "_"/ "data_info.yml"
        try:
            with open(var, 'r') as file:
                data = yaml.safe_load(file)
            return data
        except FileNotFoundError:
            print(f"Need to build data_info.yml")

    @property
    def formatting_functions(self):
        """
        Properly import formmating functions from wave module
        Return a dictionary of functions
        """
        module_filename = f"{self.year}.py" 
        var = self.file_path / "_" / module_filename 

        # Load module dynamically
        spec = importlib.util.spec_from_file_location(f"formatting_{self.year}", var)
        formatting_module = importlib.util.module_from_spec(spec)

        if spec.loader is not None:
            spec.loader.exec_module(formatting_module)

        functions = {name: obj for name, obj in vars(formatting_module).items() if callable(obj)}

        return functions
    
    def column_mapping(self, request):
        """Retrieve column mappings for a given dataset request."""
        data_scheme = self.resources
        data_info = data_scheme.get(request)
        if not data_info:
            raise KeyError(f"No {request} found in {self.country}/{self.year}")
        
        relative_path = f'{self.country}/{self.year}/Data/{data_info["file"]}'

        formatting_functions = self.formatting_functions

        def get_mapping(var_name, mappings):
            """Applies formatting functions if available, otherwise uses defaults."""
            return (
                (mappings[var_name], formatting_functions[var_name]) 
                if var_name in formatting_functions else
                (mappings[var_name], lambda x: self.year if var_name == 'w' else format_id)
            )

        idxvars = {key: get_mapping(key, data_info['idxvars']) for key in data_info['idxvars']}
        myvars = {key: get_mapping(key, data_info['myvars']) for key in data_info['myvars']}

        return relative_path, idxvars, myvars
    
    def dvc_relative_path(self, data_file_path):
        '''
        Get the relative path of the data file from the current working directory to locate the dvc data file
        '''
        current_dir = Path(os.getcwd())  # Convert to Path object
        dvc_root = None
        try:
            dvc_root = Path(files('LSMS_Library'))
        except ModuleNotFoundError:
            for parent in current_dir.parents:
                if parent.name == "LSMS_Library":
                    dvc_root = parent
                    break

        if dvc_root is None:
            raise FileNotFoundError("Could not locate LSMS_Library. Make sure it's installed or exists in the current path.")
        
        rel_path = os.path.relpath(dvc_root / data_file_path, current_dir)
        
        return rel_path


    def grab_data(self, request):
        df_fn = self.file_path /f"_ /{request}.py"
        parquet_fn = self.file_path /f"_/{request}.parquet"
        if parquet_fn.exists():
            df = pd.read_parquet(parquet_fn)
            return df
        elif df_fn.exists():
            spec = importlib.util.spec_from_file_location(request, df_fn)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            df = module.df
            return df
        else:
            file, idxvars,  myvars = self.column_mapping(request)
            df = df_data_grabber(self.dvc_relative_path(file), idxvars, **myvars).drop_duplicates()
            # Oddity with large number for missing code
            na = df.select_dtypes(exclude='object').max().max()
            if na>1e99:
                warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
                df = df.replace(na,np.nan)
            return df


    def cluster_features(self):
        return self.grab_data('cluster_features')
    
    def household_roster(self):
        return self.grab_data('household_roster')
    
    def food_acquired(self):
        return self.grab_data('food_acquired')
    




class Country:
    def __init__(self,country_name):
        self.name = country_name

    @property
    def resources(self):
        var = files("lsms_library") / "countries" / self.name /"_"/ "data_info.yml"
        try:
            with open(var, 'r') as file:
                data = yaml.safe_load(file)
            return data
        except FileNotFoundError:
            print(f"Need to build data_info.yml")
    

    def waves(self):
        data = self.resources
        if 'Waves' in data:
            return data['Waves']
        else:
            print(f"No waves found for {self.name}/_/data_info.yml")
        
    def data_scheme(self):
        data = self.resources
        if'Data Scheme' in data:
            return list(data['Data Scheme'].keys())
        else:
            print(f"No data scheme found for {self.name}/_/data_info.yml")
    
    def __getitem__(self, year):
        # Ensure the year is one of the available waves
        if year in self.waves():
            return Wave(year, self.name, self.data_scheme)
        else:
            raise KeyError(f"{year} is not a valid wave for {self.name}")
        
