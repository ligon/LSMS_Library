#!/usr/bin/env python3
import pandas as pd
import numpy as np
import yaml
from importlib.resources import files
import importlib
import cfe.regression as rgsn
from collections import defaultdict
from .local_tools import df_data_grabber, format_id, get_categorical_mapping, category_union, get_dataframe
import importlib.util
import os
import warnings
from pathlib import Path
import warnings
from .ai_agent import ai_process, gpt_agent


class Wave:
    def __init__(self, year, country_name, data_scheme):
        self.year = year
        self.country = country_name
        self.name = f"{self.country}/{self.year}"
        self.data_scheme = data_scheme
    
    @property
    def file_path(self):
        var = files("lsms_library") / "countries" / self.country/self.year
        return var
    
    @property
    def relative_path(self, data_file = None):
        '''
        Get the relative path of the data file from the current working directory
        Target: loading dvc file requires relative path
                using categorical_mapping function requires relative path
        '''
        current_dir = Path(os.getcwd())  # Convert to Path object
        data_file_path = Path(self.file_path)
        rel_path = os.path.relpath(data_file_path, current_dir)
        if data_file is not None:
            return rel_path / data_file
        else:
            return rel_path
    
    @property
    def resources(self):
        var = self.file_path / "_" / "data_info.yml"
        try:
            with open(var, 'r') as file:
                data = yaml.safe_load(file)
            return data
        except FileNotFoundError as e:
            warnings.warn(f"File not found: {var}")

    @property
    def formatting_functions(self):
        """
        Properly import formmating functions from wave module
        Return a dictionary of functions
        """
        module_filename = f"{self.year}.py" 
        var = self.file_path / "_" / module_filename 
        if not var.exists():
            module_filename = f"{self.country.lower()}.py"
            var = Country(self.country).file_path/'_'/module_filename
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
        if data_info is None:
            raise KeyError(f"Data scheme does not contain {request} for {self.country}/{self.year}")
        
        formatting_functions = self.formatting_functions

        def get_formatting_function(var_name, value, format_id_function = False):
            """Applies formatting functions if available, otherwise uses defaults."""
            return (
                (value, formatting_functions[var_name]) 
                if var_name in formatting_functions else
                (value, format_id) if format_id_function else value
            )
   
        
        files = data_info.get('file')
        idxvars = data_info.get('idxvars')
        myvars = data_info.get('myvars')
        merge = data_info.get('merge')

        final_mapping = dict()
        final_mapping['merge'] = merge

        idxvars_updated = {key: get_formatting_function(key,value, format_id_function = True) for key, value in idxvars.items()}
        myvars_updated = {key: get_formatting_function(key, value) for key, value in myvars.items()}

        # Overwrite default column mappings with provided new columns for each file if there is new mapping
        for i in files:
            if isinstance(i, dict):
                idxvars_override = idxvars_updated.copy()
                myvars_override = myvars_updated.copy()
                for key, value in i.items():
                    if key in idxvars.keys():
                        idxvars_override[key] = get_formatting_function(key, value, format_id_function = True)
                    else:
                        myvars_override[key] = get_formatting_function(key, value)
                final_mapping[i] = {'idxvars': idxvars_override, 'myvars': myvars_override}

            else:
                final_mapping[i] = {'idxvars': idxvars_updated, 'myvars': myvars_updated}
            
        return final_mapping
    
    def categorical_mapping(self, table, idxvars_code = 'Original Label', label_code = 'Preferred Label' ):
        path = self.relative_path
        mapping = get_categorical_mapping(fn = 'categorical_mapping.org',
                                          tablename=table,
                                          idxvars={'Code': idxvars_code},
                                          dirs = [f'{path}/_', f'{path}/../_', f'{path}/../../_'],
                                          **{'Label': label_code})
        
        return mapping
            

    def grab_data(self, request):
        df_fn = self.file_path /f"_ /{request}.py"
        if df_fn.exists():
            spec = importlib.util.spec_from_file_location(request, df_fn)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            df = module.df
            return df
        else:
            mapping_details = self.column_mapping(request)
            merge_way = mapping_details.get('merge')
            files = {key: value for key, value in mapping_details.items() if key != 'merge'}
            dfs = []
            for file, mappings in files.items():
                df = df_data_grabber(self.relative_path(f'Data/{file}'), mappings['idxvars'], **mappings['myvars'], convert_categoricals=True)
                df = df.reset_index().drop_duplicates()
                df['w'] = self.year
                df = df.set_index(['w']+list(mappings['idxvars'].keys()))
                # Oddity with large number for missing code
                na = df.select_dtypes(exclude='object').max().max()
                if na>1e99:
                    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
                    df = df.replace(na,np.nan)
                dfs.append(df)
            if merge_way is None:
                final_df = dfs[0]
            else:
                final_df = pd.concat(dfs, axis=merge_way)

            return final_df

    def cluster_features(self):
        return self.grab_data('cluster_features')
    
    def household_roster(self):
        return self.grab_data('household_roster')
    
    def food_acquired(self):
        unit_mapping = self.categorical_mapping('unit')
        food_mapping = self.categorical_mapping('harmonize_food')
        agg_functions = {'Expenditure': 'sum', 'Quantity': 'sum', 'Produced': 'sum', 'Price': 'first'}

        df = self.grab_data('food_acquired')
        index = df.index.names
        variable = df.columns
        df = df.reset_index()
        df['j'] = df['j'].map(food_mapping)
        df['u'] = df['u'].map(unit_mapping)
        agg_func = {key: value for key, value in agg_functions.items() if key in variable}
        df = df.groupby(index).agg(agg_func)
        return df
    
    def interview_date(self):
        return self.grab_data('interview_date')
    
    
class Country:
    def __init__(self,country_name):
        self.name = country_name

    @property
    def file_path(self):
        var = files("lsms_library") / "countries" / self.name
        return var
    
    @property
    def resources(self):
        var = self.file_path /"_"/ "data_info.yml"
        try:
            with open(var, 'r') as file:
                data = yaml.safe_load(file)
            return data
        except FileNotFoundError as e:
            warnings.warn(e)
    
    def waves(self):
        data = self.resources
        if 'Waves' in data:
            return data['Waves']
        else:
            warnings.warn(f"No waves found for {self.name}/_/data_info.yml")
        
    def data_scheme(self):
        data = self.resources
        if'Data Scheme' in data:
            return list(data['Data Scheme'].keys())
        else:
            warnings.warn(f"No data scheme found for {self.name}/_/data_info.yml")
    
    def __getitem__(self, year):
        # Ensure the year is one of the available waves
        if year in self.waves():
            return Wave(year, self.name, self.data_scheme)
        else:
            raise KeyError(f"{year} is not a valid wave for {self.name}")
    
    def ai_categorical_mapping(self, label_col, label_col_type = 'idxvars', data_request = 'food_acquired', ai_agent=gpt_agent()):
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
        for i in self.waves():
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
            df = get_dataframe(wave.relative_path(f'Data/{file}'))
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
       
    def cluster_features(self, waves=None):
        if waves is None:
            waves = self.waves()
        w = {}
        for i in waves:
            try:
                w[i] = self[i].cluster_features()
            except KeyError as e:
                 warnings.warn(e)
        return pd.concat(w.values())
    
    def household_roster(self, waves=None):
        if waves is None:
            waves = self.waves()
        w = {}
        for i in waves:
            try:
                w[i] = self[i].household_roster()
            except KeyError as e:
                warnings.warn(e)
        return pd.concat(w.values())
    
    def food_acquired(self, waves=None):
        if waves is None:
            waves = self.waves()
        w = {}
        for i in waves:
            try:
                w[i] = self[i].food_acquired()
            except KeyError as e:
                warnings.warn(e)
        return pd.concat(w.values())
    
    # def id_walk():
        







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
