#!/usr/bin/env python3
import pandas as pd
import numpy as np
import yaml
from importlib.resources import files
import importlib
import cfe.regression as rgsn
from collections import defaultdict
from .local_tools import df_data_grabber, format_id, get_categorical_mapping, get_dataframe, map_index, get_formatting_functions, panel_ids, id_walk, all_dfs_from_orgfile, to_parquet
from .paths import data_root
from .yaml_utils import load_yaml
import importlib.util
import os
import warnings
from pathlib import Path
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UnicodeWarning)
import subprocess
import json
from dataclasses import dataclass
from functools import lru_cache
from sys import stderr
import sys
from dvc.repo import Repo
from dvc.exceptions import DvcException, PathMissingError
from datetime import datetime
from typing import Any, Iterable
from contextlib import contextmanager, redirect_stdout

JSON_CACHE_METHODS = {'panel_ids', 'updated_ids'}


@contextmanager
def _working_directory(path: Path):
    """Temporarily switch the process working directory."""
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextmanager
def _redirect_stdout_to_stderr():
    """Silence DVC's stdout chatter by redirecting it to stderr."""
    with redirect_stdout(stderr):
        yield


def _slugify(value: str) -> str:
    return value.lower().replace(" ", "_").replace("-", "_")


@dataclass(frozen=True)
class StageInfo:
    stage_key: str
    stage_ref: str
    country: str
    wave: str | None
    table: str
    fmt: str
    output_path: Path


@lru_cache(maxsize=4)
def _load_materialize_stage_map(dvc_root: str) -> dict[tuple[str, str | None, str], StageInfo]:
    """
    Load mapping from (country, wave, table) to StageInfo based on available dvc.yaml materialize foreach entries.
    """
    root_path = Path(dvc_root).resolve()
    yaml_paths: set[Path] = set()

    root_yaml = root_path / "dvc.yaml"
    if root_yaml.exists():
        yaml_paths.add(root_yaml.resolve())

    for path in root_path.glob("**/dvc.yaml"):
        if ".dvc" in path.parts:
            continue
        yaml_paths.add(path.resolve())

    stage_map: dict[tuple[str, str | None, str], StageInfo] = {}

    for yaml_path in sorted(yaml_paths):
        if not yaml_path.is_file():
            continue
        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f) or {}

        stages = data.get("stages", {})
        materialize = stages.get("materialize", {})
        foreach = materialize.get("foreach", {})
        do_section = materialize.get("do", {})
        outs = do_section.get("outs", [])
        if not outs:
            continue
        out_entry = outs[0]
        if isinstance(out_entry, dict):
            out_template = out_entry.get("path")
        else:
            out_template = out_entry
        if not out_template:
            continue

        for stage_key, params in foreach.items():
            country = params.get("country")
            wave = params.get("wave")
            table = params.get("table")
            fmt = params.get("format", "parquet")

            if not country or not table:
                continue

            wave_value = wave or None

            output_rel = params.get("output")
            if not output_rel:
                output_rel = (
                    out_template
                    .replace("${item.country}", country)
                    .replace("${item.wave}", (wave or ""))
                    .replace("${item.table}", table)
                    .replace("${item.format}", fmt)
                )
            output_rel = output_rel.replace("//", "/").strip()
            output_path = (yaml_path.parent / output_rel).resolve()

            yaml_rel = yaml_path.relative_to(root_path)
            stage_ref = f"{yaml_rel.as_posix()}:materialize@{stage_key}"

            stage_map[(country, wave_value, table)] = StageInfo(
                stage_key=stage_key,
                stage_ref=stage_ref,
                country=country,
                wave=wave_value,
                table=table,
                fmt=fmt,
                output_path=output_path,
            )

    return stage_map


def _make_jobs_flag() -> str | None:
    """
    Determine an appropriate make -j flag based on environment or CPU count.
    Returns the flag string (e.g. '-j4') or None if no parallelism is desired.
    """
    make_jobs = os.getenv("LSMS_MAKE_JOBS")
    if make_jobs:
        try:
            jobs = int(make_jobs)
        except ValueError:
            jobs = None
    else:
        cpu_count = os.cpu_count() or 2
        jobs = max(1, cpu_count // 2)

    if jobs and jobs > 1:
        return f"-j{jobs}"
    return None


def _log_issue(country: str, method: str, waves, error: Exception) -> None:
    """
    Append an issue entry to ISSUES.md capturing failures during cache/materialization.
    """
    issues_path = Path(__file__).resolve().parent / "ISSUES.md"
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    wave_info = ", ".join(waves) if isinstance(waves, (list, tuple)) and waves else "All waves"
    entry = [
        f"## {timestamp} {country} – {method}",
        "",
        f"- Waves: {wave_info}",
        f"- Error: `{type(error).__name__}: {error}`",
        "",
    ]
    issues_path.parent.mkdir(parents=True, exist_ok=True)
    with open(issues_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(entry))

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
        def _property_value(instance, prop_name):
            """
            Retrieve a property directly from the class descriptor to avoid
            triggering __getattr__ on the instance.
            """
            prop = getattr(type(instance), prop_name, None)
            if isinstance(prop, property):
                return prop.__get__(instance, type(instance))
            raise AttributeError(f"'{instance.__class__.__name__}' object has no attribute '{prop_name}'")

        # Allow direct data_scheme access even if __getattr__ is re-entered for it.
        if method_name == 'data_scheme':
            return _property_value(self, 'data_scheme')

        # Prevent infinite recursion for properties that themselves rely on __getattr__
        if '_in_getattr' in self.__dict__:
            if method_name in ('resources', 'file_path', 'formatting_functions'):
                raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{method_name}'")

        # Resolve scheme membership without triggering __getattr__
        wave_scheme = _property_value(self, 'data_scheme')
        country_scheme = _property_value(self.country, 'data_scheme')

        # Set flag to track that we're inside __getattr__
        self.__dict__['_in_getattr'] = True
        try:
            if method_name in wave_scheme or method_name in country_scheme:
                def method():
                    return self.grab_data(method_name)
                return method
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{method_name}'")
        finally:
            # Always clear flag when exiting, even if exception raised
            # Use __dict__.pop() to avoid triggering __getattr__
            self.__dict__.pop('_in_getattr', None)
        
    @property
    def file_path(self):
        return files("lsms_library") / "countries" / self.folder

    @property
    def resources(self):
        """Load the data_info.yml that describes table structure, merges, etc."""
        info_path = self.file_path / "_" / "data_info.yml"
        if not info_path.exists():
            # warnings.warn(f"File not found: {info_path}")
            return {}
        with open(info_path, 'r') as file:
            return load_yaml(file)
    
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
        Retrieve column mappings for a given dataset request and map into a 
        dictionary to be ready for df_data_grabber.

        Input:
                request: str, the request data name in data_scheme 
                (e.g. 'cluster_features', 'household_roster', 'food_acquired', 
                'interview_date')

        Output:
                final_mapping: dict, {file_name: {'idxvars': idxvar_dic, 
                'myvars': myvars_dic}}

        Example:
                {'data_file.dta': {'idxvars': {'cluster': ('cluster', 
                <function format_id at 0x7f7f5b3f6c10>)}), 
                'myvars': {'region': ('region', <function format_id 
                at 0x7f7f5b3f6c10>), 
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
                        mapping_dic = self.categorical_mapping[mapping_steps[0]].loc[[mapping_steps[1], mapping_steps[2]]].to_dict()
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
                                mapping = mapping + (self.categorical_mapping[i[0]].loc[[i[1], i[2]]].to_dict())
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
        dic = dict(self.country.categorical_mapping)
        if not org_fn.exists():
            return dic if dic else {}
        else:
            dic.update(all_dfs_from_orgfile(org_fn))
            return dic

    @property
    def license(self):
        license_path = self.file_path / "Documentation" / "LICENSE.org"
        if license_path.exists():
            with open(license_path, 'r') as file:
                return file.read()
        warnings.warn(f"License file not found: {license_path}")
        return ""

    @property
    def data_source(self):
        source_path = self.file_path / "Documentation" / "SOURCE.org"
        if source_path.exists():
            with open(source_path, 'r') as file:
                return file.read()
        warnings.warn(f"Data source not found: {source_path}")
        return ""

    @property
    def mapping(self):
        return {**self.categorical_mapping, **self.formatting_functions}
    
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
        data_info = self.resources.get(request, None)

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
                data_path = self.file_path / "Data" / file
                candidates: list[str] = []

                try:
                    candidates.append(os.path.relpath(data_path, Path.cwd()))
                except ValueError:
                    pass

                try:
                    dvc_root = self.file_path.parents[1]
                    relative_path = data_path.relative_to(dvc_root)
                    candidates.append(str(relative_path))
                except ValueError:
                    pass

                candidates.append(str(data_path))

                last_error: Exception | None = None
                for candidate in dict.fromkeys(candidates):
                    try:
                        df = df_data_grabber(candidate, mappings['idxvars'], **mappings['myvars'], convert_categoricals=convert_cat)
                        break
                    except (FileNotFoundError, PathMissingError) as error:
                        last_error = error
                        continue
                else:
                    if last_error is not None:
                        raise last_error
                    raise FileNotFoundError(f"Unable to locate data file for {file} in {self.file_path}")
                df = check_adding_t(df)
                # Oddity with large number for missing code
                na = df.select_dtypes(exclude=['object', 'datetime64[ns]']).max().max()
                if na>1e99:
                    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
                    df = df.replace(na,np.nan)
                dfs.append(df)
            df = pd.concat(dfs, axis=0, sort=False)

            if df_edit_function:
                df = df_edit_function(df)

            return df

        if data_info:
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

        else:
            # The reason why not just simply run the python file is some
            # python files have dependencies.
            print(f"Attempting to generate using Makefile...", flush=True,file=stderr)
            #cluster features in the old makefile is called 'other_features'
            # if request =='cluster_features': request = 'other_features'
            # Use in-tree path for Make target, but look for output at data_root too
            intree_parquet = self.file_path / "_" / f"{request}.parquet"
            country_name = self.country.name
            external_parquet = data_root(country_name) / self.year / "_" / f"{request}.parquet"

            makefile_path = self.file_path.parent /'_'/ "Makefile"
            if not makefile_path.exists():
                warnings.warn(f"Makefile not found in {makefile_path.parent}. Unable to generate required data.")
                return pd.DataFrame()

            cwd_path = self.file_path.parent / "_"
            relative_parquet_path = intree_parquet.relative_to(cwd_path.parent)
            env = os.environ.copy()
            env["LSMS_DATA_DIR"] = str(data_root())
            subprocess.run(["make", "-s", '../' + str(relative_parquet_path)], cwd=cwd_path, check=True, env=env)
            print(f"Makefile executed successfully for {self.name}. Rechecking for parquet file...",file=stderr)

            # Check external data_root first, then in-tree fallback
            parquet_fn = None
            for candidate in [external_parquet, intree_parquet]:
                if candidate.exists():
                    parquet_fn = candidate
                    break

            if parquet_fn is None:
                print(f"Parquet file for {request} still missing after running Makefile.",file=stderr)
                return pd.DataFrame()

            df = pd.read_parquet(parquet_fn)

        if isinstance(df, pd.DataFrame):
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
        #Customed
        agg_functions = {'Expenditure': 'sum', 'Quantity': 'sum', 'Produced': 'sum', 'Price': 'first'}
        index = df.index.names
        variable = df.columns
        df = df.reset_index()
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

    def __init__(self, country_name, preload_panel_ids=False, verbose=False, trust_cache=False):
        self.name = country_name
        self._panel_ids_cache = None
        self._updated_ids_cache = None
        self.wave_folder_map = {}
        self.trust_cache = trust_cache
        scheme_map = self.resources.get("Data Scheme") if isinstance(self.resources, dict) else {}
        has_panel_ids = isinstance(scheme_map, dict) and "panel_ids" in scheme_map
        if preload_panel_ids and has_panel_ids:
            print(f"Preloading panel_ids for {self.name}...",file=stderr)
            #ignore all the warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                _ = self._compute_panel_ids()

    @property
    def file_path(self):
        return Path(__file__).resolve().parent / "countries" / self.name

    @property
    def resources(self):
        var = self.file_path / "_" / "data_scheme.yml"
        if not var.exists():
            return {}
        with open(var, 'r') as file:
            return load_yaml(file)

    def _materialization_entry(self, method_name: str) -> dict[str, Any]:
        """
        Retrieve materialization metadata for a dataset from the country-level data scheme.
        """
        resources = self.resources
        if not isinstance(resources, dict):
            return {}
        scheme_map = resources.get("Data Scheme")
        if not isinstance(scheme_map, dict):
            return {}
        entry = scheme_map.get(method_name)
        if isinstance(entry, dict):
            return entry
        return {}
    
    @property
    def formatting_functions(self):
        function_dic = {}
        for file in [f"{self.name.lower()}.py", 'mapping.py']:
            general_mod_path = self.file_path/ "_"/ file
            function_dic.update(get_formatting_functions(general_mod_path, f"formatting_{self.name}"))

        return function_dic
    
    @property
    def categorical_mapping(self):
        '''
        Get the categorical mapping for the country.
        Searches current directory, then parent directory.
        '''
        for rel in ['./', '../']:
            org_fn = Path(self.file_path / rel / "_" / "categorical_mapping.org")
            if org_fn.exists():
                return all_dfs_from_orgfile(org_fn)
        return {}

    @property
    def mapping(self):
        return {**self.categorical_mapping, **self.formatting_functions}
    
    def _resolve_materialize_stages(self, method_name: str, waves) -> list[StageInfo]:
        """
        Resolve materialize stage information for the given method and waves.
        Returns an empty list if no matching stages are registered.
        """
        try:
            stage_map = _load_materialize_stage_map(str(self.file_path.parent))
        except FileNotFoundError:
            return []

        resolved: list[StageInfo] = []

        for wave in waves:
            key = (self.name, wave, method_name)
            info = stage_map.get(key)
            if not info:
                key = (self.name, None, method_name)
                info = stage_map.get(key)
            if not info:
                return []
            resolved.append(info)

        if not resolved:
            info = stage_map.get((self.name, None, method_name))
            if info:
                resolved.append(info)

        return resolved

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

    def _location_lookup(self) -> pd.DataFrame:
        """
        Return a cached lookup table that maps (i, t) -> m so tables missing the
        region index can inherit it from other_features.
        """

        cache = getattr(self, "_location_level_cache", None)
        if isinstance(cache, pd.DataFrame):
            return cache
        if cache is False:
            raise RuntimeError(getattr(self, "_location_lookup_error", "Location metadata unavailable."))

        if getattr(self, "_location_lookup_inflight", False):
            raise RuntimeError("Location lookup already in progress.")

        location_path = data_root(self.name) / "var" / "other_features.parquet"
        if location_path.exists():
            location_df = get_dataframe(location_path)
        else:
            self._location_lookup_inflight = True
            try:
                location_df = self.other_features()
            finally:
                self._location_lookup_inflight = False

        if not isinstance(location_df, pd.DataFrame) or location_df.empty:
            self._location_level_cache = False
            self._location_lookup_error = "other_features returned no data."
            raise RuntimeError(self._location_lookup_error)

        if not isinstance(location_df.index, pd.MultiIndex) or "m" not in location_df.index.names:
            self._location_level_cache = False
            self._location_lookup_error = "other_features is missing the 'm' index."
            raise RuntimeError(self._location_lookup_error)

        index_frame = location_df.index.to_frame(index=False)
        if "i" not in index_frame.columns:
            if "j" in index_frame.columns:
                index_frame = index_frame.rename(columns={"j": "i"})
        if "i" not in index_frame.columns:
            self._location_level_cache = False
            self._location_lookup_error = "Unable to identify household id column in other_features index."
            raise RuntimeError(self._location_lookup_error)

        mapping = index_frame[["i", "t", "m"]].drop_duplicates(subset=["i", "t"])
        self._location_level_cache = mapping
        return mapping

    def _augment_index_from_related_tables(
        self,
        df: pd.DataFrame,
        scheme_entry: dict[str, Any] | None,
        wave: str | None,
    ) -> pd.DataFrame:
        """
        Attach missing index levels (currently 'm') by borrowing metadata from
        related tables such as other_features.
        """

        declared = _declared_index_levels(scheme_entry)
        if not declared or "m" not in declared:
            return df

        if not isinstance(df.index, pd.MultiIndex):
            return df

        current_names = [name for name in df.index.names if name is not None]
        if "m" in current_names:
            return df

        df_reset = df.reset_index()
        if "t" not in df_reset.columns:
            if wave is None:
                return df
            df_reset["t"] = wave

        household_col = next((candidate for candidate in ("i", "j", "household_id") if candidate in df_reset.columns), None)
        if household_col is None:
            return df

        try:
            lookup = self._location_lookup()
        except RuntimeError as exc:
            raise KeyError(f"Missing spatial index 'm' and unable to derive it: {exc}") from exc

        join_lookup = lookup
        if household_col != "i":
            join_lookup = lookup.rename(columns={"i": household_col})

        merged = df_reset.merge(
            join_lookup[[household_col, "t", "m"]],
            on=[household_col, "t"],
            how="left",
        )

        if merged["m"].isna().any():
            missing_pairs = merged.loc[merged["m"].isna(), [household_col, "t"]].drop_duplicates()
            sample = missing_pairs.head().to_dict("records")
            warnings.warn(
                f"Missing region metadata for {len(missing_pairs)} households while materializing {household_col}/m; sample={sample}"
            )

        return merged.set_index(df.index.names)
    

    def _finalize_result(self, df: Any, scheme_entry: dict[str, Any], method_name: str):
        """
        Apply final harmonization steps (index augmentation, normalization, id walk)
        before returning a dataset to callers.
        """
        if isinstance(df, dict):
            return df

        if isinstance(df, pd.DataFrame):
            df = self._augment_index_from_related_tables(df, scheme_entry, None)
            df = _normalize_dataframe_index(df, scheme_entry, None)

            if isinstance(df.index, pd.MultiIndex):
                index_names = list(df.index.names)
                preferred = ['i', 't', 'm']
                desired_order = [name for name in preferred if name in index_names]
                desired_order += [name for name in index_names if name not in desired_order]
                if desired_order != index_names:
                    try:
                        df = df.reorder_levels(desired_order)
                    except Exception:
                        pass

            if (
                'i' in df.index.names
                and not df.attrs.get('id_converted')
                and method_name not in ['panel_ids', 'updated_ids']
                and self._updated_ids_cache is not None
            ):
                df = id_walk(df, self.updated_ids)

        return df

    def _aggregate_wave_data(self, waves=None, method_name=None):
        """Aggregates data across multiple waves using a single dataset method.

        If the required `.parquet` file is missing, it requests `Makefile` to
        generate only that file.
        """
        if method_name not in self.data_scheme+['other_features', 'food_prices_quantities_and_expenditures', 'updated_ids']:
            warnings.warn(f"Data scheme does not contain {method_name} for {self.name}")
            return pd.DataFrame()

        if waves is None:
            waves = self.waves

        scheme_entry = self._materialization_entry(method_name)
        materialize_backend = None
        if isinstance(scheme_entry, dict):
            backend_value = scheme_entry.get("materialize")
            if isinstance(backend_value, str):
                materialize_backend = backend_value.lower()

        prefer_parquet_cache = bool(getattr(self, "trust_cache", False)) and method_name not in JSON_CACHE_METHODS
        if prefer_parquet_cache:
            parquet_path = data_root(self.name) / "var" / f"{method_name}.parquet"
            if parquet_path.exists():
                df_cached = get_dataframe(parquet_path)
                df_cached = map_index(df_cached)
                return self._finalize_result(df_cached, scheme_entry, method_name)

        if (
            self._updated_ids_cache is None
            and method_name not in ("panel_ids", "updated_ids")
        ):
            try:
                _ = self.updated_ids
            except Exception:
                pass

        def safe_concat_dataframe_dict(df_dict):
            # Get the target index name order from the first DataFrame
            reference_order = next(iter(df_dict.values())).index.names

            aligned_dfs = {}
            for key, df in df_dict.items():
                if list(df.index.names) != list(reference_order):
                    try:
                        df = df.reorder_levels(reference_order)
                    except Exception as e:
                        raise ValueError(f"Cannot reorder index levels for '{key}': {e}")
                aligned_dfs[key] = df

            return pd.concat(aligned_dfs.values(), axis=0, sort=False)  

        def run_make_target(method_name: str, wave: str | None = None):
            """
            Execute legacy Makefile targets either at the country level or for a specific wave.
            """
            base_path = self.file_path if wave is None else self[wave].file_path
            repo_root = self.file_path.parent
            candidate_make_dirs: list[Path] = []
            if wave is not None:
                candidate_make_dirs.append(base_path / "_")
            candidate_make_dirs.append(self.file_path / "_")

            makefile_dir = next((d for d in candidate_make_dirs if (d / "Makefile").exists()), None)
            makefile_path = makefile_dir / "Makefile" if makefile_dir else None

            script_candidates: list[Path] = []
            if wave is not None:
                script_candidates.append(base_path / "_" / f"{method_name}.py")
            script_candidates.append(self.file_path / "_" / f"{method_name}.py")
            script_path = next((p for p in script_candidates if p.exists()), None)

            output_candidates: list[Path] = []
            if method_name in JSON_CACHE_METHODS:
                if wave is not None:
                    output_candidates.append(base_path / "_" / f"{method_name}.json")
                output_candidates.append(self.file_path / "_" / f"{method_name}.json")
            else:
                # Check data_root (external) first, then in-tree as fallback
                if wave is not None:
                    output_candidates.append(data_root(self.name) / wave / "_" / f"{method_name}.parquet")
                    output_candidates.append(base_path / "var" / f"{method_name}.parquet")
                    output_candidates.append(base_path / "_" / f"{method_name}.parquet")
                output_candidates.append(data_root(self.name) / "var" / f"{method_name}.parquet")
                output_candidates.append(self.file_path / "var" / f"{method_name}.parquet")
                output_candidates.append(self.file_path / "_" / f"{method_name}.parquet")

            # deduplicate while preserving order
            unique_candidates: list[Path] = []
            seen: set[Path] = set()
            for cand in output_candidates:
                if cand in seen:
                    continue
                seen.add(cand)
                unique_candidates.append(cand)

            if makefile_path is None and script_path is None:
                warnings.warn(f"No Makefile or script found for {self.name}/{wave or '_'} {method_name}.")
                return pd.DataFrame()

            cwd_path = makefile_dir if makefile_dir is not None else script_path.parent
            if method_name in JSON_CACHE_METHODS:
                target_path = unique_candidates[0]
            else:
                target_path = unique_candidates[0]

            target_path.parent.mkdir(parents=True, exist_ok=True)

            def build_env() -> dict[str, str]:
                env = os.environ.copy()
                bin_dir = os.path.dirname(sys.executable)
                env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
                pythonpath = env.get("PYTHONPATH", "")
                if str(repo_root) not in pythonpath.split(os.pathsep):
                    pythonpath = f"{repo_root}{os.pathsep}{pythonpath}" if pythonpath else str(repo_root)
                env["PYTHONPATH"] = pythonpath
                env.setdefault("PYTHON", sys.executable)
                # Ensure subprocess scripts also redirect data paths
                env["LSMS_DATA_DIR"] = str(data_root())
                return env

            def try_make(make_dir: Path) -> Path | None:
                if make_dir is None or not (make_dir / "Makefile").exists():
                    return None
                makefile = make_dir / "Makefile"
                # Build Make targets using data_root() paths (primary) since
                # Makefiles now default VAR_DIR to data_root().  Fall back to
                # in-tree paths only if needed.  Use absolute paths directly
                # as Make handles them fine.
                make_targets = []
                if method_name in JSON_CACHE_METHODS:
                    make_targets.append(self.file_path / "_" / f"{method_name}.json")
                else:
                    # data_root() targets first (matches Makefile VAR_DIR default)
                    if wave is not None:
                        make_targets.append(data_root(self.name) / wave / "_" / f"{method_name}.parquet")
                    make_targets.append(data_root(self.name) / "var" / f"{method_name}.parquet")
                    # In-tree fallbacks
                    if wave is not None:
                        make_targets.append(base_path / "_" / f"{method_name}.parquet")
                    make_targets.append(self.file_path / "var" / f"{method_name}.parquet")
                    make_targets.append(self.file_path / "_" / f"{method_name}.parquet")

                for target in make_targets:
                    make_cmd = ["make", "-s"]
                    jobs_flag = _make_jobs_flag()
                    if jobs_flag:
                        make_cmd.append(jobs_flag)
                    make_cmd.append(str(target))
                    try:
                        subprocess.run(make_cmd, cwd=make_dir, check=True, env=build_env())
                        print(f"Makefile executed successfully for {self.name}/{wave or 'ALL'}. Rechecking for {method_name}...", file=stderr)
                    except (subprocess.CalledProcessError, FileNotFoundError) as error:
                        warnings.warn(f"Makefile execution failed for {self.name}/{wave or '_'} {method_name}: {error}")
                        continue
                    # Check all candidate locations (data_root + in-tree)
                    for candidate in unique_candidates:
                        if candidate.exists():
                            return candidate
                return None

            def try_script(script: Path) -> Path | None:
                if script is None or not script.exists():
                    return None
                python_bin = sys.executable or "python3"
                try:
                    subprocess.run([python_bin, str(script)], cwd=script.parent, check=True, env=build_env())
                    print(f"Python fallback executed for {script.parent.name}.{method_name}.", file=stderr)
                except subprocess.CalledProcessError as error:
                    warnings.warn(f"Python fallback failed for {script}: {error}")
                    return None
                for candidate in unique_candidates:
                    if candidate.exists():
                        return candidate
                return None

            output_path: Path | None = None
            if makefile_path is not None:
                output_path = try_make(makefile_path.parent)

            if output_path is None and script_path is not None:
                output_path = try_script(script_path)

            if output_path is None:
                print(f"Data file {target_path} still missing after running fallbacks.", file=stderr)
                return pd.DataFrame()

            if output_path.suffix == ".json":
                with open(output_path, "r", encoding="utf-8") as json_file:
                    return json.load(json_file)

            df_local = get_dataframe(str(output_path))
            df_local = map_index(df_local)
            return df_local

            return pd.DataFrame()

        def load_from_waves(waves):
            results = {}
            for w in waves:
                wave_obj = self[w]
                wave_has_table = method_name in wave_obj.data_scheme
                wave_result = None

                if wave_has_table:
                    try:
                        wave_result = getattr(wave_obj, method_name)()
                    except (KeyError, AttributeError) as error:
                        warnings.warn(str(error))

                use_legacy = wave_has_table and (
                    wave_result is None
                    or (isinstance(wave_result, pd.DataFrame) and wave_result.empty)
                )

                if use_legacy:
                    wave_result = run_make_target(method_name, wave=w)

                if isinstance(wave_result, pd.DataFrame):
                    if (
                        'i' in wave_result.index.names
                        and not wave_result.attrs.get('id_converted')
                        and self._updated_ids_cache is not None
                    ):
                        wave_result = id_walk(wave_result, self.updated_ids)
                    wave_result = self._augment_index_from_related_tables(
                        wave_result,
                        scheme_entry,
                        w,
                    )
                    wave_result = _normalize_dataframe_index(wave_result, scheme_entry, w)

                if wave_result is None and not wave_has_table:
                    continue
                if isinstance(wave_result, pd.DataFrame) and wave_result.empty:
                    continue
                if wave_result is None:
                    continue

                results[w] = wave_result

            if results:
                if method_name in JSON_CACHE_METHODS:
                    dict_payloads = {k: v for k, v in results.items() if isinstance(v, dict)}
                    df_payloads = {k: v for k, v in results.items() if isinstance(v, pd.DataFrame)}
                    if dict_payloads and not df_payloads:
                        combined: dict[str, Any] = {}
                        combined.update(dict_payloads)
                        return combined
                    results = {k: v for k, v in df_payloads.items() if not v.empty}
                    if not results:
                        return pd.DataFrame()

                non_empty_df = {k: df for k, df in results.items() if not df.empty}
                if not non_empty_df:
                    return pd.DataFrame()
                if len(non_empty_df) > 1:
                    return safe_concat_dataframe_dict(non_empty_df)
                return pd.concat(non_empty_df.values(), axis=0, sort=False)

            country_fallback = run_make_target(method_name, wave=None)
            if isinstance(country_fallback, dict):
                if country_fallback:
                    return country_fallback
                return {}
            if isinstance(country_fallback, pd.DataFrame):
                return country_fallback

            raise KeyError(f"No data found for {method_name} in any wave of {self.name}.")

        def load_json_cache(method_name):
            cache_path = data_root(self.name) / "_" / f"{method_name}.json"
            # Also check in-tree for legacy caches, and parquet variants
            candidates = [
                cache_path,
                self.file_path / "_" / f"{method_name}.json",
                data_root(self.name) / "_" / f"{method_name}.parquet",
                self.file_path / "_" / f"{method_name}.parquet",
            ]
            for candidate in candidates:
                if candidate.exists():
                    print(f"Reading {method_name} from cache {candidate}", file=stderr)
                    if candidate.suffix == ".json":
                        with open(candidate, 'r') as json_file:
                            return json.load(json_file)
                    else:
                        df = get_dataframe(candidate)
                        # Convert back to dict format expected by panel_ids consumers
                        if hasattr(df, 'to_dict'):
                            return df
                        return df

            try:
                make_cmd = ["make", "-s"]
                jobs_flag = _make_jobs_flag()
                if jobs_flag:
                    make_cmd.append(jobs_flag)
                make_cmd.append(f"{method_name}.json")
                subprocess.run(make_cmd, cwd=self.file_path / "_", check=True)
            except (subprocess.CalledProcessError, FileNotFoundError) as error:
                print(f"Makefile build failed for {method_name}: {error}. Falling back to wave aggregation.", file=stderr)
                result = load_from_waves(waves)
            else:
                result = run_make_target(method_name)

            if isinstance(result, dict):
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, 'w') as json_file:
                    json.dump(result, json_file)
                print(f"Writing {method_name} to cache {cache_path}", file=stderr)
            elif isinstance(result, pd.DataFrame):
                parquet_path = data_root(self.name) / "_" / f"{method_name}.parquet"
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                to_parquet(result, parquet_path)
                if cache_path.exists():
                    try:
                        cache_path.unlink()
                    except OSError:
                        pass
                print(f"Writing {method_name} to cache {parquet_path}", file=stderr)
            return result

        def load_dataframe_with_dvc(method_name):
            """
            Load data using DVC materialize stages. Falls back to load_from_waves if stages are missing.
            """
            cache_path = data_root(self.name) / "var" / f"{method_name}.parquet"
            cache_exists = cache_path.exists()

            dvc_root = self.file_path.parent

            repo: Repo | None = None
            try:
                with _working_directory(dvc_root):
                    repo = Repo(str(dvc_root))

                    stage_infos = self._resolve_materialize_stages(method_name, waves)
                    if not stage_infos:
                        df = load_from_waves(waves)
                        if isinstance(df, pd.DataFrame):
                            cache_path.parent.mkdir(parents=True, exist_ok=True)
                            to_parquet(df, cache_path)
                            print(f"Writing {method_name} to cache {cache_path}", file=stderr)
                        return df

                    deduped_infos: list[StageInfo] = []
                    seen_refs: set[str] = set()
                    for info in stage_infos:
                        if info.stage_ref in seen_refs:
                            continue
                        seen_refs.add(info.stage_ref)
                        deduped_infos.append(info)
                    stage_infos = deduped_infos

                    def collect_stage_outputs(stage_list):
                        stage_results: dict[str, pd.DataFrame] = {}
                        for stage in stage_list:
                            if stage.fmt != "parquet":
                                raise ValueError(f"Unsupported format {stage.fmt} for {method_name}")
                            output_path = stage.output_path
                            if output_path.exists():
                                df_wave = get_dataframe(output_path)
                                df_wave = map_index(df_wave)
                                df_wave = self._augment_index_from_related_tables(
                                    df_wave,
                                    scheme_entry,
                                    stage.wave,
                                )
                                df_wave = _normalize_dataframe_index(df_wave, scheme_entry, stage.wave)
                                stage_results[stage.wave or "ALL"] = df_wave
                        return stage_results

                    def consolidate_stage_outputs(stage_results: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
                        if not stage_results:
                            return None
                        non_empty_df = {k: v for k, v in stage_results.items() if not v.empty}
                        if not non_empty_df:
                            combined_df = pd.concat(stage_results.values(), axis=0, sort=False)
                        elif len(non_empty_df) > 1:
                            combined_df = safe_concat_dataframe_dict(non_empty_df)
                        else:
                            combined_df = next(iter(non_empty_df.values()))
                        cache_path.parent.mkdir(parents=True, exist_ok=True)
                        to_parquet(combined_df, cache_path)
                        print(f"Writing {method_name} to cache {cache_path}", file=stderr)
                        return combined_df

                    def _load_stage(stage_ref: str):
                        file_part, stage_name = stage_ref.split(":", 1)
                        return repo.stage.load_one(file_part, stage_name)

                    loaded_stages = []
                    dirty = True
                    try:
                        for info in stage_infos:
                            stage = _load_stage(info.stage_ref)
                            loaded_stages.append(stage)
                        with repo.lock, _redirect_stdout_to_stderr():
                            stage_status = [
                                stage.status(check_updates=False)
                                for stage in loaded_stages
                            ]
                        dirty = any(stage_status)
                    except Exception as status_error:
                        print(
                            f"DVC status failed for {method_name}: {status_error!r}. Assuming dirty outputs.",
                            file=stderr,
                        )
                        dirty = True

                    if cache_exists and not dirty:
                        try:
                            print(f"Reading {method_name} from cache {cache_path}", file=stderr)
                            cached_df = get_dataframe(cache_path)
                            cached_df = map_index(cached_df)
                            wave_hint = stage_infos[0].wave if len(stage_infos) == 1 else None
                            cached_df = self._augment_index_from_related_tables(
                                cached_df,
                                scheme_entry,
                                wave_hint,
                            )
                            cached_df = _normalize_dataframe_index(
                                cached_df,
                                scheme_entry,
                                wave_hint,
                            )
                            return cached_df
                        except (FileNotFoundError, PathMissingError):
                            dirty = True  # fall through to repro if cache missing unexpectedly

                    stage_outputs: dict[str, pd.DataFrame] | None = None
                    if not dirty:
                        stage_outputs = collect_stage_outputs(stage_infos)
                        if not stage_outputs:
                            dirty = True

                    if dirty:
                        stage_iter = loaded_stages or [_load_stage(info.stage_ref) for info in stage_infos]
                        with repo.lock, _redirect_stdout_to_stderr():
                            for stage in stage_iter:
                                try:
                                    stage.reproduce()
                                except Exception as reproduce_error:
                                    print(f"DVC reproduce failed for {stage.addressing}: {reproduce_error!r}. Falling back to legacy loaders.", file=stderr)
                                    stage_outputs = collect_stage_outputs(stage_infos)
                                    combined_outputs = consolidate_stage_outputs(stage_outputs)
                                    if combined_outputs is not None:
                                        return combined_outputs
                                    return load_from_waves(waves)

                        stage_outputs = collect_stage_outputs(stage_infos)
                        if not stage_outputs:
                            raise KeyError(f"No data produced for {method_name} via DVC.")

                    single_stage = (
                        len(stage_infos) == 1
                        and stage_infos[0].wave is None
                        and stage_infos[0].output_path == cache_path
                    )
                    if single_stage:
                        return next(iter(stage_outputs.values()))

                    combined_outputs = consolidate_stage_outputs(stage_outputs)
                    if combined_outputs is None:
                        raise KeyError(f"No data produced for {method_name} via DVC.")
                    return combined_outputs
            except (DvcException, FileNotFoundError) as e:
                print(f"DVC unavailable for {method_name}: {e}. Falling back to manual aggregation.", file=stderr)
                return load_from_waves(waves)
            finally:
                if repo is not None:
                    repo.close()

        def load_with_dvc_cache(method_name):
            if method_name in JSON_CACHE_METHODS:
                return load_json_cache(method_name)
            return load_dataframe_with_dvc(method_name)

        df: Any = None
        # Select build backend: "dvc" (default) validates caches via DVC,
        # "make" bypasses DVC and builds directly with Make/wave loaders.
        build_backend = os.getenv('LSMS_BUILD_BACKEND', 'dvc').lower()
        use_dvc = build_backend == 'dvc'
        prefer_make_backend = materialize_backend == "make"
        resources = self.resources
        data_scheme = resources.get('Data Scheme') if isinstance(resources, dict) else {}
        has_data_scheme_entry = isinstance(data_scheme, dict) and data_scheme.get(method_name) is not None

        if not use_dvc and method_name in JSON_CACHE_METHODS:
            df = load_from_waves(waves)
        else:
            try:
                if use_dvc:
                    df = load_with_dvc_cache(method_name)
                elif has_data_scheme_entry and not prefer_make_backend:
                    df = load_from_waves(waves)
                else:
                    df = run_make_target(method_name)
            except Exception as error:
                _log_issue(self.name, method_name, waves, error)
                raise
        return self._finalize_result(df, scheme_entry, method_name)

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
            print(f"Panel IDs not found in {self.name}.",file=stderr)
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

    def cached_datasets(self) -> list[str]:
        """
        List dataset names currently cached for this country.
        """
        cache_files = []
        var_dir = data_root(self.name) / "var"
        underscore_dir = data_root(self.name) / "_"

        if var_dir.exists():
            cache_files.extend(var_dir.glob("*.parquet"))
        if underscore_dir.exists():
            cache_files.extend(underscore_dir.glob("*.json"))
            cache_files.extend(underscore_dir.glob("*.parquet"))

        datasets = []
        for path in cache_files:
            if path.suffix == ".json" and path.stem not in JSON_CACHE_METHODS:
                continue
            datasets.append(path.stem)
        return sorted(set(datasets))

    def clear_cache(
        self,
        methods: list[str] | None = None,
        waves: list[str] | None = None,
        dry_run: bool = False,
    ) -> list[Path]:
        """
        Remove cached files for this country.
        If methods is None, all cached datasets are removed.
        If waves is provided, removes only caches tied to those waves (for DVC outputs under build/).
        Returns list of deleted file paths.
        """
        removed: list[Path] = []
        targets = list(dict.fromkeys(methods or self.cached_datasets()))

        for method in targets:
            json_cache = data_root(self.name) / "_" / f"{method}.json"
            parquet_cache = data_root(self.name) / "_" / f"{method}.parquet"
            var_cache = data_root(self.name) / "var" / f"{method}.parquet"

            for candidate in (json_cache, parquet_cache, var_cache):
                if candidate.suffix == ".json" and candidate.stem not in JSON_CACHE_METHODS:
                    continue
                if candidate.exists():
                    removed.append(candidate)
                    if not dry_run:
                        candidate.unlink()

        build_removed: list[Path] = []
        if waves:
            try:
                stage_map = _load_materialize_stage_map(str(self.file_path.parent))
            except FileNotFoundError:
                stage_map = {}
            for method in targets:
                for wave in waves:
                    info = stage_map.get((self.name, wave, method))
                    if info:
                        build_path = (self.file_path.parent / info.output_path).resolve()
                        if build_path.exists():
                            build_removed.append(build_path)
                            if not dry_run:
                                build_path.unlink()

        removed.extend(build_removed)
        return removed
    

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
    
    def test_all_data_schemes(self, waves=None):
        """
        Test whether all method_names in obj.data_scheme can be successfully built.
        Falls back to Makefile if not in data_scheme.
        """
        all_methods = set(self.data_scheme)
        print(f"Testing all methods in {self.name} data scheme: {sorted(all_methods)}")

        failed_methods = {}
        
        for method_name in sorted(all_methods):
            print(f"\n>>> Testing method: {method_name}")
            try:
                df = self._aggregate_wave_data(waves=waves, method_name=method_name)

                # If it's JSON, it'll return a dict
                if isinstance(df, dict):
                    print(f"Loaded JSON for {method_name}: {len(df)} entries")
                elif isinstance(df, pd.DataFrame):
                    if df.empty:
                        print(f"Empty DataFrame for {method_name}")
                    else:
                        print(f"DataFrame loaded for {method_name}: {df.shape}")
                else:
                    print(f"❓ Unexpected return type for {method_name}: {type(df)}")
            except Exception as e:
                print(f"Failed to load {method_name}: {str(e)}")
                failed_methods[method_name] = str(e)

        print("\n=== Summary ===")
        if failed_methods:
            print(f"{len(failed_methods)} methods failed:")
            for method, error in failed_methods.items():
                print(f" - {method}: {error}")
        else:
            print("All methods loaded successfully!")

        return failed_methods





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
def _declared_index_levels(schema_entry: dict[str, Any] | None) -> list[str]:
    """Parse the declared index metadata from a data scheme entry."""
    if not schema_entry:
        return []

    declared_levels: Iterable[str] | None = schema_entry.get("index")
    if isinstance(declared_levels, str):
        cleaned = declared_levels.strip()
        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = cleaned[1:-1]
        declared_levels = [token.strip() for token in cleaned.split(",") if token.strip()]
    if not declared_levels:
        return []

    return [str(level) for level in declared_levels]


def _normalize_dataframe_index(
    df: pd.DataFrame,
    schema_entry: dict[str, Any],
    wave: str | None,
) -> pd.DataFrame:
    """
    Reorder and reduce a dataframe's MultiIndex to match the declared schema.

    - Reorders index levels to match the declared order.
    - Drops unexpected index levels.
    - Synthesizes missing 't' levels for wave-specific tables.
    - Collapses duplicate entries via first-observation aggregation.
    """

    if not isinstance(df, pd.DataFrame) or not isinstance(df.index, pd.MultiIndex):
        return df

    declared = _declared_index_levels(schema_entry)
    if not declared:
        return df

    current_names = list(df.index.names)

    # Add synthetic levels when declared but missing (e.g., 't' for wave outputs)
    missing_levels = [lvl for lvl in declared if lvl not in current_names]
    if missing_levels:
        df = df.reset_index()
        for level in missing_levels:
            if level == "t" and wave is not None:
                df[level] = wave
        df = df.set_index(declared)
        current_names = list(df.index.names)

    # Reorder levels to match declaration
    present_declared = [lvl for lvl in declared if lvl in current_names]
    if present_declared:
        remaining = [lvl for lvl in current_names if lvl not in present_declared]
        try:
            df = df.reorder_levels(present_declared + remaining)
        except Exception:
            pass

    # Drop any unexpected index levels
    extra_levels = [lvl for lvl in df.index.names if lvl not in declared]
    if extra_levels:
        df = df.droplevel(extra_levels)

    # Aggregate duplicates if any remain
    if not df.index.is_unique:
        df = df.groupby(level=declared).first()

    return df
