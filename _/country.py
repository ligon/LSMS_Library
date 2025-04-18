#!/usr/bin/env python3
import pandas as pd
import numpy as np
import yaml
from importlib.resources import files
import importlib
import cfe.regression as rgsn
from collections import defaultdict
from .local_tools import df_data_grabber, format_id, get_categorical_mapping, category_union, get_dataframe, map_index, get_formatting_functions, panel_ids, id_walk
import importlib.util
import os
import warnings
from pathlib import Path
import warnings
from .ai_agent import ai_process, gpt_agent
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UnicodeWarning)
import subprocess
import json

class Wave:
    def __init__(self,  year, country: 'Country'):
        self.year = year
        self.country = country
        self.name = f"{self.country.name}/{self.year}"
        self.formatting_functions = get_formatting_functions(mod_path=self.file_path / "_" / f"{self.year}.py",
                                                             name=f"formatting_{self.year}",
                                                             general__formatting_functions=self.country.formatting_functions)

    def __getattr__(self, method_name):
        '''
        This method is triggered when an attribute is not found in the instance, but exists in the `data_scheme`. 
        It dynamically generates a method to aggregate data for the requested attribute.

        For example, if a user calls `country_instance.food_acquired()` and `food_acquired` is part of the `data_scheme` but not an existing method, 
        the method will dynamically create a function to handle data aggregation for `food_acquired`.
        '''
        if method_name in self.country.data_scheme:
            def method():
                return self.grab_data(method_name)
            return method
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{method_name}'")
        
    @property
    def file_path(self):
        var = files("lsms_library") / "countries" / self.name
        return var
    
    @property
    def resources(self):
        """Load the data_info.yml that describes table structure, merges, etc."""
        info_path = self.file_path / "_" / "data_info.yml"
        if not info_path.exists():
            # warnings.warn(f"File not found: {info_path}")
            return {}
        with open(info_path, 'r') as file:
            return yaml.safe_load(file)
    
    @property
    def data_scheme(self):
        wave_data = [f.stem for f in (self.file_path / "_").iterdir() if f.suffix == '.py'  if f.stem not in [f'{self.year}']]
        # Customed
        replace_dic = { 'other_features': ['cluster_features']}
        # replace the key with the value in the dictionary
        for key, value in replace_dic.items():
            if key in wave_data:
                wave_data.remove(key)
                wave_data.extend(value)

        data_info = self.resources
        if data_info:
           wave_data.extend([key for key in data_info.keys() if key not in ['Wave', 'Country']])
        return list(set(wave_data).intersection(self.country.data_scheme))
    
    
    def column_mapping(self, request):
        """
        Retrieve column mappings for a given dataset request.And map into dictionary to be ready for df_data_grabber
        Input:
            request: str, the request data name in data_scheme (e.g. 'cluster_features', 'household_roster', 'food_acquired', 'interview_date')
        Output:
            final_mapping: dict, {file_name: {'idxvars': idxvar_dic, 'myvars': myvars_dic}}
        Example:
            {'data_file.dta': {'idxvars': {'cluster': ('cluster', <function format_id at 0x7f7f5b3f6c10>)},
                              'myvars': {'region': ('region', <function format_id at 0x7f7f5b3f6c10>),
                                         'urban': ('urban', <function format_id at 0x7f7f5b3f6c10>)}}}
        """
        data_info = self.resources.get(request)
        
        formatting_functions = self.formatting_functions

        def map_formatting_function(var_name, value, format_id_function = False):
            """Applies formatting functions if available, otherwise uses defaults."""
            if isinstance(value, list) and isinstance(list[1], dict):
                return tuple(value)
            if var_name in formatting_functions:
                return (value, formatting_functions[var_name])
            if format_id_function:
                return (value, format_id)
            return value
   
        files = data_info.get('file')
        idxvars = data_info.get('idxvars')
        myvars = data_info.get('myvars')
        final_mapping = dict()
        final_mapping['df_edit'] = formatting_functions.get(request)
        idxvars_updated = {key: map_formatting_function(key, value, format_id_function = True) for key, value in idxvars.items()}
        myvars_updated = {key: map_formatting_function(key, value) for key, value in myvars.items()}

        if isinstance(files, str):
            final_mapping[files] = {'idxvars': idxvars_updated, 'myvars': myvars_updated}
            return final_mapping
        
        if isinstance(files, list):
            for i in files:
                if isinstance(i, dict):
                    idxvars_override = idxvars_updated.copy()
                    myvars_override = myvars_updated.copy()
                    file_name, overrides = next(iter(i.items()))
                    for key, val in overrides.items():
                        if key in idxvars:
                            idxvars_override[key] = map_formatting_function(key, val, format_id_function = True)
                        else:
                            myvars_override[key] = map_formatting_function(key, val)
                    final_mapping[file_name] = {'idxvars': idxvars_override, 'myvars': myvars_override}
                else:
                    final_mapping[i] = {'idxvars': idxvars_updated, 'myvars': myvars_updated}
                
            return final_mapping
    
    def categorical_mapping(self, table, idxvars_code = 'Original Label', label_code = 'Preferred Label' ):
        '''
        Get the categorical mapping for the table by using get_categorical_mapping function from local_tools
        Input:
            table: str, the table name (e.g. 'unit', 'harmonize_food')
            idxvars_code: str, the column name for the code in the idxvars
            label_code: str, the column name for the label in the myvars
        Output:
            mapping: pd.DataFrame, the categorical mapping
        '''
        path = self.file_path
        try:
            mapping = get_categorical_mapping(fn='categorical_mapping.org',
                              tablename=table,
                              idxvars={'Code': idxvars_code},
                              dirs=[f'{path}/_', f'{path}/../_', f'{path}/../../_'],
                              **{'Label': label_code})
        except Exception as e:
            warnings.warn(f"Error in getting categorical mapping: {e}")
            return None
        return mapping
            

    def grab_data(self, request):
        '''
        get data from the data file
        Input:
            request: str, the request data name (e.g. 'cluster_features', 'household_roster', 'food_acquired', 'interview_date')
        Output:
            df: pd.DataFrame, the data requested
        '''
        if request not in self.data_scheme:
            warnings.warn(f"Data scheme does not contain {request} for {self.name}")
            return pd.DataFrame()
        try:
            mapping_details = self.column_mapping(request)
            convert_cat = (self.resources.get(request).get('converted_categoricals') is None)
            df_edit_function = mapping_details.pop('df_edit')
            dfs = []
            for file, mappings in mapping_details.items():
                df = df_data_grabber(f'{self.name}/Data/{file}', mappings['idxvars'], **mappings['myvars'], convert_categoricals=convert_cat)
                df = df.reset_index().drop_duplicates()
                df['t'] = self.year
                df = df.set_index(['t']+list(mappings['idxvars'].keys()))
                # Oddity with large number for missing code
                na = df.select_dtypes(exclude='object').max().max()
                if na>1e99:
                    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
                    df = df.replace(na,np.nan)
                dfs.append(df)
            df = pd.concat(dfs, axis=0, sort=False)

            if df_edit_function:
                df = df_edit_function(df)
        except KeyError as e:
            print(f"Attempting to generate using Makefile...")
            #cluster features in the old makefile is called 'other_features'
            if request =='cluster_features':
                request = 'other_features'
            parquet_fn = self.file_path/"_"/ f"{request}.parquet"

            makefile_path = self.file_path.parent /'_'/ "Makefile"
            if not makefile_path.exists():
                raise FileNotFoundError(f"Makefile not found in {makefile_path.parent}. Unable to generate required data.")

            cwd_path = self.file_path.parent / "_"
            relative_parquet_path = parquet_fn.relative_to(cwd_path.parent)  # Convert to relative path
            subprocess.run(["make", '../' + str(relative_parquet_path)], cwd=cwd_path, check=True)
            print(f"Makefile executed successfully for {self.name}. Rechecking for parquet file...")

            if not parquet_fn.exists():
                print(f"Parquet file {parquet_fn} still missing after running Makefile.")
                return pd.DataFrame()
            
            df = pd.read_parquet(parquet_fn)
        
        return map_index(df, self.name)

    # This cluster_features method is explicitly defined because additional processing is required after calling grab_data.
    def cluster_features(self):
        df = self.grab_data('cluster_features')
        # if cluster_feature data is from old other_features.parquet file, region is called 'm' so we need to rename it
        if 'm' in df.index.names:
            df = df.reset_index(level = 'm').rename(columns = {'m':'Region'})
        if 'm' in df.columns:
            df = df.rename(columns = {'m':'Region'})
        return df
    
    # Food acquired method is explicitly defined because potentially categorical mapping is required after calling grab_data.
    def food_acquired(self):
        df = self.grab_data('food_acquired')
        parquet_fn = self.file_path / "_" / "food_acquired.parquet"
        # if food_acquired data is load from parquet file, we assume its unit and food label are already mapped
        if parquet_fn.exists():
            return df
        
        #If dataframe is mapped by yml file, we need to map the unit and food label, assuming columns are only ['Expenditure', 'Quantity', 'Produced', 'Price']
        unit_mapping = self.categorical_mapping('unit')
        food_mapping = self.categorical_mapping('harmonize_food')
        #Customed
        agg_functions = {'Expenditure': 'sum', 'Quantity': 'sum', 'Produced': 'sum', 'Price': 'first'}
        index = df.index.names
        variable = df.columns
        df = df.reset_index()
        if food_mapping is not None:
            df['j'] = df['j'].map(food_mapping)
        if unit_mapping is not None:
            df['u'] = df['u'].map(unit_mapping)
        agg_func = {key: value for key, value in agg_functions.items() if key in variable}
        #replace not float value in Quantity, Expenditure, Produced with np.nan
        for col in ['Quantity', 'Expenditure', 'Produced']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.groupby(index).agg(agg_func)
        return df


    

    
    
class Country:
    #Customed: EEP 153 solving demand equation required data
    required_list = ['food_acquired', 'household_roster', 'cluster_features',
                    'interview_date', 'household_characteristics',
                    'food_expenditures', 'food_quantities', 'food_prices', 
                    'fct', 'nutrition', 'panel_ids']
    
    # from uganda:
    # required_list = ['food_expenditures.parquet', 'food_quantities.parquet', 'food_prices.parquet',
    #                 'household_characteristics.parquet', 'other_features.parquet', 'shocks.parquet',
    #                 'nonfood_expenditures.parquet', 'enterprise_income.parquet', 'assets.parquet',
    #                 'earnings.parquet', 'housing.parquet', 'income.parquet', 'fct.parquet', 'nutrition.parquet']
    

    def __init__(self,country_name):
        self.name = country_name
        self._panel_ids_cache = None
        self._updated_ids_cache = None

    @property
    def file_path(self):
        var = files("lsms_library") / "countries" / self.name
        return var
    
    @property
    def resources(self):
        # var = self.file_path / "_" / "data_info.yml"
        var = self.file_path / "_" / "data_scheme.yml"
        if not var.exists():
            return {}
        with open(var, 'r') as file:
            return yaml.safe_load(file)
    
    @property
    def formatting_functions(self):
        if self.name == 'GhanaLSS':
            general_module_filename = 'ghana.py'
        else:
            general_module_filename = f"{self.name.lower()}.py"
        general_mod_path = self.file_path/ "_"/ general_module_filename

        return get_formatting_functions(general_mod_path, f"formatting_{self.name}")
    
    @property
    def waves(self):
        # Let's first check if there is a 'waves' or 'Waves' defined in {self.name}.py in the _ folder.
        # If 'waves' exists, we will use it. If 'Waves' (usually a dictionary) exists, we will use its keys.
        general_module_filename = f"{self.name.lower()}.py"
        general_mod_path = self.file_path / "_" / general_module_filename

        if general_mod_path.exists():
            spec = importlib.util.spec_from_file_location(f"{self.name.lower()}", general_mod_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            if hasattr(module, 'waves'):
                return sorted(module.waves)
            elif hasattr(module, 'Waves'):
                return sorted(list(module.Waves.keys()))
        #Otherwise, we will check the directory for subdirectories that contain 'Documentation' and 'SOURCE'.
        waves = [
            f.name for f in self.file_path.iterdir()
            if f.is_dir() and (self.file_path / f.name / 'Documentation' / 'SOURCE').exists()
        ]
        return sorted(waves)

    @property
    def data_scheme(self): 
        data_info = self.resources
        data_list = list(data_info.get('Data Scheme', {}).keys()) if data_info else []
        
        # return list of python files in the _ folder
        py_ls = [f.stem for f in (self.file_path / "_").iterdir() if f.suffix == '.py']

        # Customed
        replace_dic = {'food_prices_quantities_and_expenditures': ['food_expenditures', 'food_quantities', 'food_prices'],
                        'unitvalues': ['food_prices'],
                        'other_features': ['cluster_features']}
        # replace the key with the value in the dictionary
        for key, value in replace_dic.items():
            if key in py_ls:
                py_ls.remove(key)
                py_ls.extend(value)
        required_list = self.required_list
        
        data_scheme = set(data_list).union(set(py_ls).intersection(required_list))

        return list(data_scheme)
    
    def __getitem__(self, year):
        # Ensure the year is one of the available waves
        if year in self.waves:
            return Wave(year, self)
        else:
            raise KeyError(f"{year} is not a valid wave for {self.name}")
    

    def _aggregate_wave_data(self,waves = None, method_name = None):
        """
        Aggregates data across multiple waves using a single dataset method.
        If the required `.parquet` file is missing, it requests `Makefile` to generate only that file.
        """
        if method_name not in self.data_scheme and method_name not in ['other_features', 'food_prices_quantities_and_expenditures', 'updated_ids']:
            warnings.warn(f"Data scheme does not contain {method_name} for {self.name}")
            return pd.DataFrame()

        if waves is None:
            waves = self.waves
        #Step 1: Check if it mapped by data_info.yml
        if method_name in self.resources.get('Data Scheme', {}):
            results = {}
            for w in waves:
                try:
                    results[w] = getattr(self[w], method_name)()
                except KeyError as e:
                    warnings.warn(str(e))
            if results:
                df= pd.concat(results.values(), axis=0, sort=False)
                return map_index(df, self.name)
        
        # Step 2: Attempt to build using makefile
        print(f"Attempting to generate using Makefile...")

        makefile_path = self.file_path /'_'/ "Makefile"
        if not makefile_path.exists():
            raise FileNotFoundError(f"Makefile not found in {self.file_path}. Unable to generate required data.")
        
        if method_name in ['cluster_features']:
        # if cluster feature is not defined in yml file, we will use makefile and the variable name is 'other_features'
            method_name = 'other_features' 

        # Step 3: Run Makefile for the specific parquet/json file
        cwd_path = self.file_path / "_"

        if method_name in ['panel_ids', 'updated_ids']:
            target_path = self.file_path / "_" / f"{method_name}.json"
            relative_path = target_path.relative_to(cwd_path)
            make_target = str(relative_path)
        else:
            target_path = self.file_path / "var" / f"{method_name}.parquet"
            relative_path = target_path.relative_to(cwd_path.parent)
            make_target = '../' + str(relative_path)

        subprocess.run(["make", make_target], cwd=cwd_path, check=True)
        print(f"Makefile executed successfully for {self.name}. Rechecking for {target_path.name}...")

        # Step 4: Recheck if the parquet file was successfully generated
        if not target_path.exists():
            print(f"Data file {target_path} still missing after running Makefile.")
            return pd.DataFrame()


        # Step 5: Read and return the parquet or JSON file
        if target_path.suffix == '.json':
            with open(target_path, 'r') as json_file:
                dic = json.load(json_file)
            return dic
        else:
            df = get_dataframe(target_path)
        if df.attrs.get('id_converted', False)==False and 'panel_ids' in self.data_scheme:
            df = id_walk(map_index(df, self.name), self.updated_ids)
        else:
            df = map_index(df, self.name)
        return df

    def _compute_panel_ids(self):
        """
        Compute and cache both panel_ids and updated_ids.
        """
        panel_ids_dic = self._aggregate_wave_data(None, 'panel_ids')
        if isinstance(panel_ids_dic, dict):
            updated_ids_dic = self._aggregate_wave_data(None, 'updated_ids')
        elif isinstance(panel_ids_dic, pd.DataFrame):
            panel_ids_dic, updated_ids_dic = panel_ids(panel_ids_dic)
            panel_ids_dic = panel_ids_dic.data
        else:
            warnings.warn(f"Invalid data for panel_ids")
            return None
        self._panel_ids_cache = panel_ids_dic
        self._updated_ids_cache = updated_ids_dic

    @property
    def panel_ids(self):
        if self._panel_ids_cache is None or self._updated_ids_cache is None:
            self._compute_panel_ids()
        return self._panel_ids_cache

    @property
    def updated_ids(self):
        if self._panel_ids_cache is None or self._updated_ids_cache is None:
            self._compute_panel_ids()
        return self._updated_ids_cache
    

    def __getattr__(self, name):
        '''
        This method is triggered when an attribute is not found in the instance, but exists in the `data_scheme`. 
        It dynamically generates a method to aggregate data for the requested attribute.

        For example, if a user calls `country_instance.food_acquired()` and `food_acquired` is part of the `data_scheme` but not an existing method, 
        the method will dynamically create a function to handle data aggregation for `food_acquired`.
        '''
        if name in self.data_scheme:
            def method(waves=None):
                return self._aggregate_wave_data(waves, name)
            return method
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


    def cluster_features(self, waves=None):
        df = self._aggregate_wave_data(waves, 'cluster_features')
        if df.empty:
            return df
        if 'm' in df.index.names:
        # if cluster_feature data is from old other_features.parquet file, region is called 'm' so we need to rename it
            df = df.reset_index(level = 'm').rename(columns = {'m':'Region'})
        return df

        








# #!/usr/bin/env python3
# import pandas as pd
# import numpy as np
# from eep153_tools.sheets import write_sheet
# from importlib.resources import files
# import cfe.regression as rgsn


# # pd.set_option('future.no_silent_downcasting', True)

# class Country:
#     def __init__(self,country_name):
#         self.name = country_name

#     @property
#     def resources(self):
#         var = files("lsms_library") / "countries" / self.name / "var"

#         return var

#     def read_parquet(self,parquet):
#         try:
#             return pd.read_parquet((self.resources / f'{parquet}.parquet'))
#         except FileNotFoundError:
#             print(f"Need to build {parquet}")

#     def food_expenditures(self):
#         x = self.read_parquet('food_expenditures').squeeze().dropna()
#         x.index.names = ['i','t','m','j']
#         return x

#     def other_features(self):
#         x = self.read_parquet('other_features').squeeze()
#         x.index.names = ['i','t','m']
#         return x


#     def household_characteristics(self,additional_other_features=False,agesex=False):
#         x = self.read_parquet('household_characteristics')
#         x.index.names = ['i','t','m']

#         if 'log HSize' not in x.columns:
#             x['log HSize'] = np.log(x.sum(axis=1).replace(0,np.nan))

#         cols = x.columns
#         if not agesex: # aggregate to girls,boys,women,men
#             agesex_cols = x.filter(axis=1,regex=r' [0-9]')
#             fcols = agesex_cols.filter(regex='^F').columns
#             x['Girls'] = x[[c for c in fcols if int(c[-2:])<=18]].sum(axis=1)
#             x['Women'] = x[[c for c in fcols if int(c[-2:])>18]].sum(axis=1)

#             mcols = x.filter(regex='^M').columns
#             x['Boys'] = x[[c for c in mcols if int(c[-2:])<=18]].sum(axis=1)
#             x['Men'] = x[[c for c in mcols if int(c[-2:])>18]].sum(axis=1)

#             x = x.drop(fcols.tolist()+mcols.tolist(),axis=1)

#         if additional_other_features:
#             of = self.other_features()
#             x = x.join(of)

#         return x

#     def fct(self):
#         x = self.read_parquet('fct')
#         if x is None: return
#         x.index.name = 'j'
#         x.columns.name = 'n'
#         return x

#     def food_prices(self,drop_na_units=True):
#         x = self.read_parquet('food_prices').squeeze()
#         try:
#             x = x.stack(x.columns.names,future_stack=True).dropna()
#         except AttributeError: # Already a series?
#             x = x.dropna()

#         if len(x.index.names)==4:
#             x = x.reorder_levels(['t','m','i','u'])
#         elif len(x.index.names)==5: # Individual level?
#             x = x.reorder_levels(['j','t','m','i','u'])
#             x = x.groupby(['t','m','i','u']).median()

#         x.index = x.index.rename({'i':'j'})
#         if drop_na_units:
#             u = x.reset_index('u')['u'].replace(['<NA>','nan'],np.nan)
#             x = x.loc[~pd.isnull(u).values,:]
#         x = x.reset_index().set_index(['t','m','j','u']).squeeze()
#         x = x.unstack(['t','m'])

#         return x

#     def export_to_google_sheet(self,key=None,t=None,z=None):
#         sheets = {"Food Expenditures":self.food_expenditures(),
#                   'FCT':self.fct(),
#                   'Food Prices':self.food_prices()}

#         if z is None:
#             sheets['Household Characteristics'] = self.household_characteristics(agesex=True,additional_other_features=True)
#         else:
#             sheets['Household Characteristics'] = z

#         if t is not None:
#             sheets['Food Expenditures'] = sheets['Food Expenditures'].xs(t,level='t',drop_level=False)
#             sheets['Household Characteristics'] = sheets['Household Characteristics'].xs(t,level='t',drop_level=False)
#             sheets['Food Prices'] = sheets['Food Prices'].xs(t,level='t',drop_level=False,axis=1)
#             modifier = f' ({t})'
#         else:
#             modifier = ''

#         k = 'Food Expenditures'
#         v = sheets.pop(k)
#         if key is None:
#             key = write_sheet(v.unstack('j'),
#                           'ligon@berkeley.edu',user_role='writer',
#                           json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
#                           sheet=k+modifier)
#             print(f"Key={key}")
#         else:
#             write_sheet(v.unstack('j'),
#                         'ligon@berkeley.edu',user_role='writer',
#                         json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
#                         sheet=k+modifier,key=key)

#         for k,v in sheets.items():
#             if v is not None:
#                 write_sheet(v,
#                             'ligon@berkeley.edu',user_role='writer',
#                             json_creds='/home/ligon/.eep153.service_accounts/instructors@eep153.iam.gserviceaccount.com',
#                             sheet=k+modifier,key=key)

#         return key

#     def cfe_regression(self,**kwargs):
#         x = self.food_expenditures()
#         z = self.household_characteristics(additional_other_features=True)
#         r = rgsn.Regression(y=np.log(x.replace(0,np.nan).dropna()),
#                             d=z,**kwargs)
#         return r


