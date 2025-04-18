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
    def __init__(self, year, country_name, data_scheme, formatting_functions):
        self.year = year
        self.country = country_name
        self.name = f"{self.country}/{self.year}"
        self.data_scheme = data_scheme
        self.formatting_functions = get_formatting_functions(mod_path=self.file_path / "_" / f"{self.year}.py",
                                                             name=f"formatting_{self.year}",
                                                             general__formatting_functions=formatting_functions)
    @property
    def file_path(self):
        var = files("lsms_library") / "countries" / self.name
        return var
    
    @property
    def resources(self):
        """Load the data_info.yml that describes table structure, merges, etc."""
        info_path = self.file_path / "_" / "data_info.yml"
        if not info_path.exists():
            warnings.warn(f"File not found: {info_path}")
            return None
        with open(info_path, 'r') as file:
            return yaml.safe_load(file)
    
    
    def column_mapping(self, request):
        """
        Retrieve column mappings for a given dataset request.
        Input:
            request: str, the request data name in data_scheme (e.g. 'cluster_features', 'household_roster', 'food_acquired', 'interview_date')
        Output:
            final_mapping: dict, {file_name: {'idxvars': idxvar_dic, 'myvars': myvars_dic}}
        Example:
            {'data_file.dta': {'idxvars': {'cluster': ('cluster', <function format_id at 0x7f7f5b3f6c10>)},
                              'myvars': {'region': ('region', <function format_id at 0x7f7f5b3f6c10>),
                                         'urban': ('urban', <function format_id at 0x7f7f5b3f6c10>)}}}
        """
        data_scheme = self.resources
        data_info = data_scheme.get(request)
        if data_info is None:
            raise KeyError(f"Data scheme does not contain {request} for {self.country}/{self.year}")
        
        formatting_functions = self.formatting_functions

        def map_formatting_function(var_name, value, format_id_function = False):
            """Applies formatting functions if available, otherwise uses defaults."""
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
        if self.resources is not None:
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
        else:
            print(f"Attempting to generate using Makefile...")
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

    def cluster_features(self):
        try:
            return self.grab_data('cluster_features')
        except KeyError as e:
            df = self.grab_data('other_features')
            if df.empty:
                return df
            if 'm' in df.index.names:
                df = df.reset_index(level = 'm').rename(columns = {'m':'Region'})
            return df
    
    def household_roster(self):
        return self.grab_data('household_roster')
    
    def food_acquired(self):
        df = self.grab_data('food_acquired')
        parquet_fn = self.file_path /f"_/food_acquired.parquet"
        if parquet_fn.exists():
            return df
        
        unit_mapping = self.categorical_mapping('unit')
        food_mapping = self.categorical_mapping('harmonize_food')
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
    
    def interview_date(self):
        return self.grab_data('interview_date')
    
    def panel_ids(self):
        df = self.grab_data('panel_ids')
        df = df.reset_index().loc[:,['i', 't', 'previous_i']].drop_duplicates().set_index(['i', 't'])
        return df

    

    
    
class Country:
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
        var = self.file_path / "_" / "data_scheme.yml"
        if not var.exists():
            return None
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
        waves = [
            f.name for f in self.file_path.iterdir()
            if f.is_dir() and (self.file_path / f.name / 'Documentation' / 'SOURCE').exists()
        ]
        return sorted(waves)

    @property
    def data_scheme(self):
        data_info = self.resources
        if data_info is not None:
            data_list = list(data_info['Data Scheme'].keys())
        else:
            # return list of parquet files name in the var folder
            data_list = [f.stem for f in (self.file_path / "_").iterdir() if f.suffix == '.py']
            if 'food_prices_quantities_and_expenditures' in data_list:
                data_list.extend(['food_expenditures', 'food_quantities', 'food_prices'])
            elif 'unitvalues' in data_list:
                data_list.extend(['food_prices'])
            if 'other_features' in data_list:
                data_list.extend(['cluster_features'])
            # EEP 153 solving demand equation required data
            required_list = ['food_acquired', 'household_roster', 'household_characteristics', 
             'cluster_features', 'interview_date', 'food_expenditures', 'food_quantities', 'food_prices', 'nutrition', 'panel_ids']
            # intersection of required data and available data
            data_list = list(set(data_list).intersection(required_list))

        return data_list
    
    def __getitem__(self, year):
        # Ensure the year is one of the available waves
        if year in self.waves:
            return Wave(year, self.name, self.data_scheme, self.formatting_functions)
        else:
            raise KeyError(f"{year} is not a valid wave for {self.name}")
    
    def get_categoricals(self, label_col, label_col_type = 'idxvars', data_request = 'food_acquired', ai_agent = gpt_agent()):
        """
        Use AI to generalize original labels across all waves to be consistent for mapping, such as food labels or unit labels.
        
        Parameters:
        label_col (str): The column name for which the categorical mapping is to be generated.
            - food label is 'j' as index variable ('idxvars') 
            - unit label is 'u' as index variable ('idxvars')
        label_col_type (str): Identify whether the label columns are considered as index variable ('idxvars') or required data variables ('myvars')
        ai_agent (object): The AI agent to be used for processing. Defaults to gpt_agent().

        Returns:
        pd.DataFrame: A DataFrame containing three columns named Original Label, Preferred Label, and Manual Update.

        To conver to org file or org string, please use function write_df_to_org
        """
        label_ls = []
        for i in self.waves:
            wave = self[i]
            data_info = wave.resources
            if data_info is None:
                continue
            data_info = data_info.get(data_request) 
            if data_info is None:
                warnings.warn(f"Data scheme does not contain {data_request} for {wave.country}/{wave.year}")
                continue
            file = data_info['file']
            col = data_info[label_col_type][label_col]
            # Assuming same waves survey use same code value for the same label
            if isinstance(file, list):
                file = file[0]
            df = get_dataframe(f'{wave.name}/Data/{file}')
            label_ls.append(df[col])
    
        label_ls = [{k: v.strip() if isinstance(v, str) else v for k, v in d.items()} for d in label_ls]
        union = category_union(label_ls)[0]
        union_label = pd.DataFrame(list(union.items()), columns = ['Code','Union Label'])
        union_label = union_label.loc[:,['Union Label']].sort_values(by='Union Label').reset_index(drop=True).drop_duplicates()
        if label_col == 'j':
            prompt_method = 'food_label_prompt'
        elif label_col == 'u':
            prompt_method = 'unit_prompt'

        result_df = ai_process(union_label, prompt_method, ai_agent = ai_agent)
        
        return result_df    

    def _aggregate_wave_data(self, waves, method_name):
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
        if self.resources is not None:
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
            panel_ids_dic = panel_ids.data
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
        

    def cluster_features(self, waves=None):
        try:
            return self._aggregate_wave_data(waves, 'cluster_features')
        except subprocess.CalledProcessError as e:
            print('continue with other_features instead')
            df = self._aggregate_wave_data(waves, 'other_features')
            if df.empty:
                return df
            if 'm' in df.index.names:
                df = df.reset_index(level = 'm').rename(columns = {'m':'Region'})
            return df

    def household_roster(self, waves=None):
        return self._aggregate_wave_data(waves, 'household_roster')

    def food_acquired(self, waves=None):
        return self._aggregate_wave_data(waves, 'food_acquired')

    def interview_date(self, waves=None):
        return self._aggregate_wave_data(waves, 'interview_date')
    
    def household_characteristics(self, waves=None):
        return self._aggregate_wave_data(waves, 'household_characteristics')
    
    def food_expenditures(self, waves=None):
        return self._aggregate_wave_data(waves, 'food_expenditures')

    def food_quantities(self, waves=None):
        return self._aggregate_wave_data(waves, 'food_quantities')

    def food_prices(self, waves=None):
        return self._aggregate_wave_data(waves, 'food_prices')


        








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


