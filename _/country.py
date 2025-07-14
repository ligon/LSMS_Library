#!/usr/bin/env python3
import pandas as pd
import numpy as np
import yaml
from importlib.resources import files
import importlib
import cfe.regression as rgsn
from collections import defaultdict
from .local_tools import df_data_grabber, format_id, get_categorical_mapping, get_dataframe, map_index, get_formatting_functions, panel_ids, id_walk, all_dfs_from_orgfile
import importlib.util
import os
import warnings
from pathlib import Path
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UnicodeWarning)
import subprocess
import json

class Wave:
    def __init__(self,  year, wave_folder, country: 'Country'):
        self.year = year
        self.country = country
        self.name = f"{self.country.name}/{self.year}"
        self.folder = f"{self.country.name}/{wave_folder}"
        self.wave_folder = wave_folder


    def __getattr__(self, method_name):
        '''
        This method is triggered when an attribute is not found in the instance, but exists in the `data_scheme`. 
        It dynamically generates a method to aggregate data for the requested attribute.

        For example, if a user calls `country_instance.food_acquired()` and `food_acquired` is part of the `data_scheme` but not an existing method, 
        the method will dynamically create a function to handle data aggregation for `food_acquired`.
        '''
        if method_name in self.data_scheme or method_name in self.country.data_scheme:
            def method():
                return self.grab_data(method_name)
            return method
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{method_name}'")
        
    @property
    def file_path(self):
        var = files("lsms_library") / "countries" / self.folder
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
        wave_data = [f.stem for f in (self.file_path / "_").iterdir() if f.suffix == '.py' and f.stem not in [f'{self.wave_folder}']]
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
        return list(set(wave_data))
    
    @property
    def formatting_functions(self):
        function_dic = self.country.formatting_functions
        for file in {f"{self.wave_folder}.py", "mapping.py"}:
            file_path = self.file_path / "_" / file
            if file_path.exists():                                            
                function_dic.update(
                    get_formatting_functions(file_path,
                                             name=f"formatting_{self.wave_folder}"))
        return function_dic
    
    def column_mapping(self, request, data_info = None):
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
        # data_info = self.resources[request]
        
        formatting_functions = self.formatting_functions

        def map_formatting_function(var_name, value, format_id_function = False):
            """Applies formatting functions if available, otherwise uses defaults."""
            if isinstance(value, list) and isinstance(value[-1], dict):
                if value[-1].get('mapping'):
                    mapping_steps = value[-1].get('mapping')
                    # given a direct mapping dictionary like {Male: M, Female: F}
                    if isinstance(mapping_steps, dict):
                        return (value[:-1], mapping_steps)
                    # given a single string which is a function nuame:
                    elif isinstance(mapping_steps, str):
                        return (value[:-1], formatting_functions[mapping_steps])
                    #given a list requiring a categorical_mapping table ['harmonize_food', 'original_key', 'mapped_key']
                    elif isinstance(mapping_steps, list) and all(not isinstance(step, list) for step in mapping_steps):
                        mapping_dic = self.categorical_mapping(mapping_steps[0]).loc[[mapping_steps[1], mapping_steps[2]]].to_dict()
                        if var_name in formatting_functions:
                            return (value[:-1], (formatting_functions[var_name], mapping_dic))
                        else:
                            return (value[:-1], mapping_dic)
                    #give a list but include another list which means requiring applying function and then categorical_mapping table
                    else:
                        mapping = ()
                        for i in mapping_steps:
                            if isinstance(i, str) and i in formatting_functions:
                                # If the first element is a function name, we apply it first
                                mapping = mapping+(formatting_functions[i])
                                break
                            elif isinstance(i, list) and len(i) == 3:
                                # If the first element is a list, we apply categorical mapping
                                mapping = mapping + (self.categorical_mapping(i[0]).loc[[i[1], i[2]]].to_dict())
                                break

                        return (value[:-1], mapping)    
                else:
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
                        if key == 't':
                            idxvars_override[key] = (idxvars_updated[key][0], lambda x, val=val: val)
                        elif key in idxvars:
                            idxvars_override[key] = map_formatting_function(key, val, format_id_function = True)
                        else:
                            myvars_override[key] = map_formatting_function(key, val)
                    final_mapping[file_name] = {'idxvars': idxvars_override, 'myvars': myvars_override}
                else:
                    final_mapping[i] = {'idxvars': idxvars_updated, 'myvars': myvars_updated}
                
            return final_mapping
    @property
    def categorical_mapping(self):
        org_fn = self.file_path / "_" / "categorical_mapping.org"
        dic = self.country.categorical_mapping
        if not org_fn.exists():
            warnings.warn(f"Categorical mapping file not found: {org_fn}")
            return {}
        else:
            return dic.update(all_dfs_from_orgfile(org_fn))

    @property
    def mapping(self):
        return self.categorical_mapping.update(self.formatting_functions)
    
    
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
            data_info = self.resources[request]
            def check_adding_t(df):
                index_list = df.index.names
                if 't' not in index_list:
                    if 't' not in df.columns:
                        df['t'] = self.year
                    final_index = ['t'] + index_list
                    df = df.reset_index().set_index(final_index)
                return df
            def get_data(data_info_dic, mapping_info):
                convert_cat = (data_info_dic.get('converted_categoricals') is None)
                df_edit_function = mapping_info.pop('df_edit')
                dfs = []
                for file, mappings in mapping_info.items():
                    df = df_data_grabber(f'{self.folder}/Data/{file}', mappings['idxvars'], **mappings['myvars'], convert_categoricals=convert_cat)
                    df = check_adding_t(df)
                    # Oddity with large number for missing code
                    na = df.select_dtypes(exclude='object').max().max()
                    if na>1e99:
                        warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
                        df = df.replace(na,np.nan)
                    dfs.append(df)
                df = pd.concat(dfs, axis=0, sort=False)

                if df_edit_function:
                    df = df_edit_function(df)
                    
                return df
            
            # Vertical Merge dfs
            if data_info.get('dfs'):
                merge_dfs = []
                merge_on =list(set('t').union(data_info.get('merge_on')))#a list
                df_edit_function = self.formatting_functions.get(request)
                idxvars_list = list(dict.fromkeys(data_info.get('final_index')))
                for i in data_info.get('dfs'):
                    sub_data_info = data_info.get(i)
                    sub_mapping_details = self.column_mapping(i, sub_data_info)
                    sub_df = get_data(sub_data_info, sub_mapping_details)
                    merge_dfs.append(sub_df.reset_index())
                df = pd.merge(merge_dfs[0], merge_dfs[1], on=merge_on, how='outer')
                if len(merge_dfs) > 2:
                    for i in range(2, len(merge_dfs)):
                        df = pd.merge(df, merge_dfs[i], on=merge_on, how='outer')
                df = df.set_index(idxvars_list)
                if df_edit_function:
                    df = df_edit_function(df)

            else:
                mapping_details = self.column_mapping(request, data_info)
                df = get_data(data_info, mapping_details)

        except KeyError as e:
            print(f'KeyError: {str(e)} in {self.name}', flush=True)
            # The reason why not just simply run the python file is some data's python files have dependencies.
            print(f"Attempting to generate using Makefile...", flush=True)
            #cluster features in the old makefile is called 'other_features'
            if request =='cluster_features':
                request = 'other_features'
            parquet_fn = self.file_path/"_"/ f"{request}.parquet"

            makefile_path = self.file_path.parent /'_'/ "Makefile"
            if not makefile_path.exists():
                warnings.warn(f"Makefile not found in {makefile_path.parent}. Unable to generate required data.")
                return pd.DataFrame()

            cwd_path = self.file_path.parent / "_"
            relative_parquet_path = parquet_fn.relative_to(cwd_path.parent)  # Convert to relative path
            subprocess.run(["make", '../' + str(relative_parquet_path)], cwd=cwd_path, check=True)
            print(f"Makefile executed successfully for {self.name}. Rechecking for parquet file...")

            if not parquet_fn.exists():
                print(f"Parquet file {parquet_fn} still missing after running Makefile.")
                return pd.DataFrame()
            
            df = pd.read_parquet(parquet_fn)
            df = map_index(df)
    
        df = check_adding_t(df)
        df = df[df.index.get_level_values('t') == self.year]
        return df

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
        if df.empty:
            return df
        parquet_fn = self.file_path / "_" / "food_acquired.parquet"
        # if food_acquired data is load from parquet file, we assume its unit and food label are already mapped
        if parquet_fn.exists():
            return df
        
        
        #If dataframe is mapped by yml file, we need to map the unit and food label, assuming columns are only ['Expenditure', 'Quantity', 'Produced', 'Price']
        # unit_mapping = self.categorical_mapping('unit').loc[['Original Label', 'Preferred Label']].to_dict()
        # food_mapping = self.categorical_mapping('harmonize_food').loc[['Original Label', 'Preferred Label']].to_dict()
        #Customed
        agg_functions = {'Expenditure': 'sum', 'Quantity': 'sum', 'Produced': 'sum', 'Price': 'first'}
        index = df.index.names
        variable = df.columns
        df = df.reset_index()
        # if food_mapping is not None:
        #     df['j'] = df['j'].map(food_mapping)
        # if unit_mapping is not None:
        #     df['u'] = df['u'].map(unit_mapping)
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
                 'fct', 'nutrition','name','_panel_ids_cache', 'panel_ids']


    # from uganda:
    # required_list = ['food_expenditures.parquet', 'food_quantities.parquet', 'food_prices.parquet',
    #                 'household_characteristics.parquet', 'other_features.parquet', 'shocks.parquet',
    #                 'nonfood_expenditures.parquet', 'enterprise_income.parquet', 'assets.parquet',
    #                 'earnings.parquet', 'housing.parquet', 'income.parquet', 'fct.parquet', 'nutrition.parquet']

    def __init__(self,country_name, preload_panel_ids = True):
        self.name = country_name
        self._panel_ids_cache = None
        self._updated_ids_cache = None
        self.wave_folder_map = {}
        if preload_panel_ids:
            print(f"Preloading panel_ids for {self.name}...")
            #ignore all the warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                _ = self._compute_panel_ids()

    @property
    def file_path(self):
        var = files("lsms_library") / "countries" / self.name
        return var

    @property
    def resources(self):
        var = self.file_path / "_" / "data_scheme.yml"
        if not var.exists():
            return {}
        with open(var, 'r') as file:
            return yaml.safe_load(file)
    
    @property
    def formatting_functions(self):
        function_dic = {}
        for file in [f"{self.name.lower()}.py", 'mapping.py']:
            general_mod_path = self.file_path/ "_"/ file
            function_dic.update(get_formatting_functions(general_mod_path, f"formatting_{self.name}"))

        return function_dic
    
    @property
    def categorical_mapping(self, dirs = ['./', '../']):
        '''
        Get the categorical mapping for the country
        '''
        for dir in dirs:
            org_fn = Path(self.file_path / dir/ "_" / "categorical_mapping.org")
            print(org_fn)
            if not org_fn.exists():
                warnings.warn(f"Categorical mapping file not found: {org_fn}")
                return {}
            else:
                return all_dfs_from_orgfile(org_fn)

    @property
    def mapping(self):
        return self.categorical_mapping.update(self.formatting_functions)
    
    @property
    def waves(self):
        """List of names of waves available for country.
        """
        # Let's first check if there is a 'waves' or 'Waves' defined in {self.name}.py in the _ folder.
        # If 'waves' exists, we will use it. If 'Waves' (usually a dictionary) exists, we will use its keys.
        general_module_filename = f"{self.name.lower()}.py"
        general_mod_path = self.file_path / "_" / general_module_filename

        if general_mod_path.exists():
            spec = importlib.util.spec_from_file_location(f"{self.name.lower()}", general_mod_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            if hasattr(module, 'wave_folder_map'):
                self.wave_folder_map = module.wave_folder_map
            if hasattr(module, 'waves'):
                return sorted(module.waves)
            elif hasattr(module, 'Waves'):
                return sorted(list(module.Waves.keys()))
        # Or if waves is defined in the data_scheme.yml file
        elif self.resources.get('Waves'):
            self.wave_folder_map = self.resources.get('wave_folder_map')
            return sorted(self.resources.get('Waves'))
        #Otherwise, we will check the directory for subdirectories that contain 'Documentation' and 'SOURCE'.
        waves = [
            f.name for f in self.file_path.iterdir()
            if f.is_dir() and ((self.file_path / f.name / 'Documentation' / 'SOURCE.org').exists() or (self.file_path / f.name / 'Documentation' / 'SOURCE').exists())
        ]
        return sorted(waves)

    @property
    def data_scheme(self):
        """List of data objects available for country.
        """
        data_info = self.resources
        data_list = list(data_info.get('Data Scheme', {}).keys()) if data_info else []
        return data_list
        # # return list of python files in the _ folder
        # py_ls = [f.stem for f in (self.file_path / "_").iterdir() if f.suffix == '.py']

        # # Customed
        # replace_dic = {'food_prices_quantities_and_expenditures': ['food_expenditures', 'food_quantities', 'food_prices'],
        #                 'unitvalues': ['food_prices'],
        #                 'other_features': ['cluster_features']}
        # # replace the key with the value in the dictionary
        # for key, value in replace_dic.items():
        #     if key in py_ls:
        #         py_ls.remove(key)
        #         py_ls.extend(value)
        # required_list = self.required_list
        
        # data_scheme = set(data_list).union(set(py_ls).intersection(required_list))
        # #data_scheme = set(data_list).union(set(py_ls).union(required_list))

        # return list(data_scheme)

    def __getitem__(self, year):
        # Ensure the year is one of the available waves
        if year in self.waves:
            wave_folder = self.wave_folder_map.get(year, year)
            return Wave(year, wave_folder, self)
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

        def load_from_waves(waves):
            results = {}
            for w in waves:
                try:
                    results[w] = getattr(self[w], method_name)()
                except KeyError as e:
                    warnings.warn(str(e))
            if results:
                return pd.concat(results.values(), axis=0, sort=False)
            raise KeyError(f"No data found for {method_name} in any wave of {self.name}.")

        makefile_path = self.file_path / "_" / "Makefile"
        if makefile_path.exists():
            print(f"Attempting to generate using Makefile for {method_name}...", flush=True)

            if method_name == 'cluster_features':
                method_name = 'other_features'

            cwd_path = self.file_path / "_"
            if method_name in ['panel_ids', 'updated_ids']:
                target_path = self.file_path / "_" / f"{method_name}.json"
                relative_path = target_path.relative_to(cwd_path)
                make_target = str(relative_path)
            else:
                target_path = self.file_path / "var" / f"{method_name}.parquet"
                relative_path = target_path.relative_to(cwd_path.parent)
                make_target = '../' + str(relative_path)

            try:
                subprocess.run(["make", make_target], cwd=cwd_path, check=True)
                print(f"Makefile executed successfully for {self.name}. Rechecking for {target_path.name}...")

                if not target_path.exists():
                    print(f"Data file {target_path} still missing after running Makefile.")
                    return pd.DataFrame()

                if target_path.suffix == '.json':
                    with open(target_path, 'r') as json_file:
                        return json.load(json_file)
                else:
                    df = get_dataframe(target_path)
                    df = map_index(df)

            except subprocess.CalledProcessError:
                print(f"Makefile failed for {method_name}. Falling back to loading from waves...")
                df = load_from_waves(waves)

        else:
            df = load_from_waves(waves)

        if 'i' in df.index.names and not df.attrs.get('id_converted') and method_name not in ['panel_ids', 'updated_ids'] and self._updated_ids_cache is not None:
            df = id_walk(df, self.updated_ids)

        return df
        # #Step 1: Check if it mapped by data_info.yml
        # if method_name in self.resources.get('Data Scheme', {}):
        #     results = {}
        #     for w in waves:
        #         try:
        #             results[w] = getattr(self[w], method_name)()
        #         except KeyError as e:
        #             warnings.warn(str(e))
        #     if results:
        #         df= pd.concat(results.values(), axis=0, sort=False)
        #         # return map_index(df, self.name)
        # else:
        #     # Step 2: Attempt to build using makefile
        #     print(f"Attempting to generate using Makefile...")

        #     makefile_path = self.file_path /'_'/ "Makefile"
        #     if not makefile_path.exists():
        #         raise FileNotFoundError(f"Makefile not found in {self.file_path}. Unable to generate required data.")
            
        #     if method_name in ['cluster_features']:
        #     # if cluster feature is not defined in yml file, we will use makefile and the variable name is 'other_features'
        #         method_name = 'other_features' 

        #     # Step 3: Run Makefile for the specific parquet/json file
        #     cwd_path = self.file_path / "_"

        #     if method_name in ['panel_ids', 'updated_ids']:
        #         target_path = self.file_path / "_" / f"{method_name}.json"
        #         relative_path = target_path.relative_to(cwd_path)
        #         make_target = str(relative_path)
        #     else:
        #         target_path = self.file_path / "var" / f"{method_name}.parquet"
        #         relative_path = target_path.relative_to(cwd_path.parent)
        #         make_target = '../' + str(relative_path)

        #     subprocess.run(["make", make_target], cwd=cwd_path, check=True)
        #     print(f"Makefile executed successfully for {self.name}. Rechecking for {target_path.name}...")

        #     # Step 4: Recheck if the parquet file was successfully generated
        #     if not target_path.exists():
        #         print(f"Data file {target_path} still missing after running Makefile.")
        #         return pd.DataFrame()


        #     # Step 5: Read and return the parquet or JSON file
        #     if target_path.suffix == '.json':
        #         with open(target_path, 'r') as json_file:
        #             dic = json.load(json_file)
        #         return dic
        #     else:
        #         df = get_dataframe(target_path)

        #     df = map_index(df)
        # if 'i' in df.index.names and not df.attrs.get('id_converted') and method_name not in ['panel_ids', 'updated_ids'] and self._updated_ids_cache is not None:
        #     df = id_walk(df, self.updated_ids)
        # return df

    def _compute_panel_ids(self):
        """
        Compute and cache both panel_ids and updated_ids.
        """
        panel_ids_dic = self._aggregate_wave_data(None, 'panel_ids')
        if isinstance(panel_ids_dic, dict):
            updated_ids_dic = self._aggregate_wave_data(None, 'updated_ids')
            self._panel_ids_cache = panel_ids_dic
            self._updated_ids_cache = updated_ids_dic
        elif isinstance(panel_ids_dic, pd.DataFrame) and not panel_ids_dic.empty:
            panel_ids_dic, updated_ids_dic = panel_ids(panel_ids_dic)
            self._panel_ids_cache = panel_ids_dic
            self._updated_ids_cache = updated_ids_dic
        else:
            print(f"Panel IDs not found in {self.name}.")
            self._panel_ids_cache = None
            self._updated_ids_cache = None

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


