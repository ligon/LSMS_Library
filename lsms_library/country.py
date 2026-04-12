#!/usr/bin/env python3
"""Country-level aggregation of LSMS-ISA survey data.

This module defines the library's two core runtime classes:

- :class:`Country` — the primary user-facing interface. Exposes the
  country's waves, data scheme, and one method per table registered in
  ``{Country}/_/data_scheme.yml`` (plus derived tables dispatched via
  ``__getattr__`` using :data:`_ROSTER_DERIVED` / :data:`_FOOD_DERIVED`).
  Table methods aggregate across waves, consult the parquet cache under
  ``data_root()``, and return a DataFrame through :meth:`_finalize_result`
  — which applies kinship expansion, canonical spelling enforcement,
  dtype coercion, and the :meth:`_join_v_from_sample` cluster-index
  augmentation.

- :class:`Wave` — a view into a single wave of a single country. Used
  internally by :class:`Country` to drive wave-level extraction via
  :func:`~lsms_library.local_tools.df_data_grabber`.

The module also defines :class:`StageInfo`, a small dataclass describing
a single DVC stage entry discovered from the stage layer's ``dvc.yaml``
files (Uganda, Senegal, Malawi, Togo, Kazakhstan, Serbia, GhanaLSS).

Cache behavior is documented in ``CLAUDE.md`` — in brief, v0.7.0+ does a
best-effort cache read at the top of :func:`load_dataframe_with_dvc`
before consulting DVC; set ``LSMS_NO_CACHE=1`` to bypass it. The cache
stores pre-transformation data, and all harmonization happens at API
read time inside :meth:`Country._finalize_result`.
"""
from __future__ import annotations

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
import logging
import os
import warnings
from pathlib import Path
import subprocess
import json
from dataclasses import dataclass
from functools import lru_cache
from sys import stderr
import sys
from dvc.repo import Repo
from dvc.exceptions import DvcException, PathMissingError
from datetime import datetime
from typing import Any, Callable, Iterable
from contextlib import contextmanager, redirect_stdout

logger = logging.getLogger(__name__)

JSON_CACHE_METHODS = {'panel_ids', 'updated_ids'}


class DeprecatedFeatureError(AttributeError):
    """Raised when a removed or deprecated table method is called on Country.

    Subclasses AttributeError so that hasattr()-based probes and generic
    try/except AttributeError patterns degrade gracefully; callers who need
    migration guidance get the full message in the exception's args.
    """


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
    """Pointer to a single materialize stage in a country's ``dvc.yaml``.

    Collected by :func:`_load_materialize_stage_map` from every
    ``dvc.yaml`` under the countries directory. Drives the legacy stage
    layer used by the 7 stage-layer countries (Uganda, Senegal, Malawi,
    Togo, Kazakhstan, Serbia, GhanaLSS); retires with v0.8.0.

    Attributes
    ----------
    stage_key : str
        Unique key within the ``materialize.foreach`` block.
    stage_ref : str
        Fully qualified stage reference
        (``{yaml_rel}:materialize@{stage_key}``) usable with
        ``dvc repro``.
    country, wave, table : str | None
        Identifiers extracted from the stage's parameters.
    fmt : str
        Output format, usually ``parquet``.
    output_path : Path
        Resolved absolute path where the stage writes its output.
    """
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


def _issues_log_path() -> Path:
    """Return the path for the auto-generated issues log.

    Resolution order:
      1. ``LSMS_ISSUES_LOG`` environment variable (absolute path override).
      2. ``platformdirs.user_cache_path("lsms_library") / "issues.log"``
         (per-user default, outside the source tree).
      3. Hardcoded fallback ``~/.cache/lsms_library/issues.log`` when
         ``platformdirs`` is not installed.
    """
    override = os.environ.get("LSMS_ISSUES_LOG", "").strip()
    if override:
        return Path(override).expanduser()
    try:
        import platformdirs
        return platformdirs.user_cache_path("lsms_library") / "issues.log"
    except ImportError:
        return Path.home() / ".cache" / "lsms_library" / "issues.log"


def _log_issue(country: str, method: str, waves, error: Exception) -> None:
    """
    Append an issue entry to the user-cache issues log.

    Writes to ``~/.cache/lsms_library/issues.log`` (or the path returned by
    ``_issues_log_path()``) so that the source-tree ``lsms_library/ISSUES.md``
    is never auto-modified.  Set the ``LSMS_ISSUES_LOG`` environment variable
    to redirect to a different file.
    """
    log_path = _issues_log_path()
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    wave_info = ", ".join(waves) if isinstance(waves, (list, tuple)) and waves else "All waves"
    entry = [
        f"## {timestamp} {country} – {method}",
        "",
        f"- Waves: {wave_info}",
        f"- Error: `{type(error).__name__}: {error}`",
        "",
    ]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(entry))

def _property_value(instance, prop_name):
    """Retrieve a property directly from the class descriptor, bypassing __getattr__."""
    prop = getattr(type(instance), prop_name, None)
    if isinstance(prop, property):
        return prop.__get__(instance, type(instance))
    raise AttributeError(f"'{type(instance).__name__}' has no attribute '{prop_name}'")


def _rebuild_failure_error(country_name: str, method_name: str) -> RuntimeError:
    """Construct a clear RuntimeError for exhausted build fallbacks.

    Used at every site where the library could not materialize a
    table via any path.  Message enumerates the common root causes
    so callers can diagnose without reading the library source.
    """
    var_path = data_root(country_name) / "var" / f"{method_name}.parquet"
    return RuntimeError(
        f"Could not materialize {country_name}/{method_name}: no wave-level "
        f"build succeeded and no cached parquet was found at {var_path}.\n\n"
        f"Common causes:\n"
        f"  - installed via `pip install git+https://...` without the .dvc "
        f"metadata needed to rebuild from source;\n"
        f"  - LSMS_DATA_DIR points to an empty or stale cache location;\n"
        f"  - DVC credentials are missing or misconfigured;\n"
        f"  - the raw .dta source files have not been dvc-pulled.\n\n"
        f"See README.org for supported install and data-access patterns."
    )


class Wave:
    """A single survey wave within a country.

    Typically obtained via bracket notation on a
    :class:`Country`::

        wave = uga['2019-20']
        df = wave.household_roster()

    Parameters
    ----------
    year : str
        Wave label (e.g. ``'2019-20'``).
    wave_folder : str
        Subdirectory name on disk (may differ from *year* for
        multi-round surveys).
    country : Country
        Parent country instance.
    """

    def __init__(self, year: str, wave_folder: str, country: Country) -> None:
        self.year = year
        self.country = country
        self.name = f"{self.country.name}/{self.year}"
        self.folder = f"{self.country.name}/{wave_folder}"
        self.wave_folder = wave_folder


    def __getattr__(self, method_name):
        '''Dynamically create data-access methods for attributes in the data_scheme.

        For example, `wave.food_acquired()` will call `self.grab_data('food_acquired')`
        if 'food_acquired' is in the data scheme but not an existing method.
        '''
        # Allow direct data_scheme access via the property descriptor
        if method_name == 'data_scheme':
            return _property_value(self, 'data_scheme')

        # Reentrancy guard: if we're already inside __getattr__, don't recurse
        if self.__dict__.get('_in_getattr'):
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{method_name}'")

        # Resolve data_scheme via the property descriptor directly (avoids __getattr__)
        wave_scheme = _property_value(self, 'data_scheme')
        country_scheme = _property_value(self.country, 'data_scheme')

        self.__dict__['_in_getattr'] = True
        try:
            if method_name in wave_scheme or method_name in country_scheme:
                def method():
                    return self.grab_data(method_name)
                method.__doc__ = f"Return {method_name} for wave {self.year}."
                method.__name__ = method_name
                return method
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{method_name}'")
        finally:
            self.__dict__.pop('_in_getattr', None)
        
    @property
    def file_path(self) -> Path:
        return files("lsms_library") / "countries" / self.folder

    @property
    def resources(self) -> dict[str, Any]:
        """Load the data_info.yml that describes table structure, merges, etc."""
        info_path = self.file_path / "_" / "data_info.yml"
        if not info_path.exists():
            # warnings.warn(f"File not found: {info_path}")
            return {}
        with open(info_path, 'r') as file:
            return load_yaml(file)

    @property
    def data_scheme(self) -> list[str]:
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
    def formatting_functions(self) -> dict[str, Callable[..., Any]]:
        function_dic = self.country.formatting_functions
        for file in {f"{self.wave_folder}.py", "mapping.py"}:
            file_path = self.file_path / "_" / file
            if file_path.exists():
                function_dic.update(
                    get_formatting_functions(file_path,
                                             name=f"formatting_{self.wave_folder}"))
        return function_dic

    def column_mapping(self, request: str, data_info: dict[str, Any] | None = None) -> dict[str, Any]:
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
                # Accept both 'mapping' and 'mappings' (Mali uses the plural form)
                if value[-1].get('mapping') or value[-1].get('mappings'):
                    mapping_steps = value[-1].get('mapping') or value[-1].get('mappings')
                    # given a direct mapping dictionary like {Male: M, Female: F}
                    if isinstance(mapping_steps, dict):
                        return (value[:-1], mapping_steps)
                    # given a single string which is a function nuame:
                    elif isinstance(mapping_steps, str):
                        return (value[:-1], formatting_functions[mapping_steps])
                    #given a list requiring a categorical_mapping table ['harmonize_food', 'original_key', 'mapped_key']
                    elif isinstance(mapping_steps, list) and all(not isinstance(step, list) for step in mapping_steps):
                        cat_table = self.categorical_mapping[mapping_steps[0]]
                        mapping_dic = cat_table.set_index(mapping_steps[1])[mapping_steps[2]].to_dict()
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
                                cat_table = self.categorical_mapping[i[0]]
                                mapping = mapping + (cat_table.set_index(i[1])[i[2]].to_dict(),)
                                break

                        return (value[:-1], mapping)    
                else:
                    return tuple(value)
            if var_name in formatting_functions:
                # Named formatting functions for idxvars (e.g., Benin's `i()`)
                # expect composite keys (list values like [grappe, menage]).
                # When the value is a simple string (single column), use
                # format_id instead to avoid passing scalars to a function
                # that expects a Series row.
                if not (format_id_function and isinstance(value, str)):
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
    def categorical_mapping(self) -> dict[str, pd.DataFrame]:
        org_fn = self.file_path / "_" / "categorical_mapping.org"
        dic = dict(self.country.categorical_mapping)
        if not org_fn.exists():
            return dic if dic else {}
        else:
            dic.update(all_dfs_from_orgfile(org_fn))
            return dic

    @property
    def license(self) -> str:
        license_path = self.file_path / "Documentation" / "LICENSE.org"
        if license_path.exists():
            with open(license_path, 'r') as file:
                return file.read()
        warnings.warn(f"License file not found: {license_path}")
        return ""

    @property
    def data_source(self) -> str:
        source_path = self.file_path / "Documentation" / "SOURCE.org"
        if source_path.exists():
            with open(source_path, 'r') as file:
                return file.read()
        warnings.warn(f"Data source not found: {source_path}")
        return ""

    @property
    def mapping(self) -> dict[str, Any]:
        return {**self.categorical_mapping, **self.formatting_functions}

    def grab_data(self, request: str) -> pd.DataFrame:
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
            # When multiple files are listed, allow missing columns (filled with NaN)
            multiple_files = len(mapping_info) > 1
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
                        df = df_data_grabber(candidate, mappings['idxvars'], **mappings['myvars'], convert_categoricals=convert_cat, missing_ok=multiple_files)
                        break
                    except (FileNotFoundError, PathMissingError) as error:
                        last_error = error
                        continue
                else:
                    if last_error is not None:
                        raise last_error
                    raise FileNotFoundError(f"Unable to locate data file for {file} in {self.file_path}")
                df = check_adding_t(df)
                # Oddity with large number for missing code.
                # Some Stata files use a huge sentinel like 1.7e100 to mark
                # missing.  Detect and replace with NaN.  Wrap in errstate to
                # suppress numpy's cast warning on the >1e99 comparison.
                with np.errstate(over='ignore'):
                    na = df.select_dtypes(exclude=['object', 'datetime64[ns]', 'category']).max().max()
                    if pd.notna(na) and float(na) > 1e99:
                        warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
                        df = df.replace(na, np.nan)
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
                dfs_list = data_info.get('dfs')
                for idx, i in enumerate(dfs_list):
                    sub_data_info = data_info.get(i)
                    sub_mapping_details = self.column_mapping(i, sub_data_info)
                    try:
                        sub_df = get_data(sub_data_info, sub_mapping_details)
                        merge_dfs.append(sub_df.reset_index())
                    except (FileNotFoundError, PathMissingError, DvcException) as exc:
                        if idx == 0:
                            # The primary sub-df is required; re-raise
                            raise
                        # Secondary sub-dfs (e.g. geo files) are optional
                        sub_file = sub_data_info.get('file', i)
                        warnings.warn(
                            f"{self.name}/{request}: could not load "
                            f"sub-df '{i}' (file: {sub_file}); "
                            f"proceeding without it. ({exc})"
                        )
                if not merge_dfs:
                    warnings.warn(f"No data loaded for {request} in {self.name}")
                    df = pd.DataFrame()
                elif len(merge_dfs) == 1:
                    df = merge_dfs[0]
                    # Use only the index columns that are present
                    available_idx = [c for c in idxvars_list if c in df.columns]
                    df = df.set_index(available_idx)
                else:
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
            logger.info("Attempting to generate using Makefile...")
            #cluster features in the old makefile is called 'other_features'
            # if request =='cluster_features': request = 'other_features'
            # Use in-tree path for Make target, but look for output at data_root too
            intree_parquet = self.file_path / "_" / f"{request}.parquet"
            country_name = self.country.name
            external_parquet = data_root(country_name) / self.wave_folder / "_" / f"{request}.parquet"

            # Check if the parquet already exists before invoking Make
            parquet_fn = None
            for candidate in [external_parquet, intree_parquet]:
                if candidate.exists():
                    parquet_fn = candidate
                    break

            if parquet_fn is None:
                makefile_path = self.file_path.parent /'_'/ "Makefile"
                if not makefile_path.exists():
                    warnings.warn(f"Makefile not found in {makefile_path.parent}. Unable to generate required data.")
                    return pd.DataFrame()

                cwd_path = self.file_path.parent / "_"
                relative_parquet_path = intree_parquet.relative_to(cwd_path.parent)
                env = os.environ.copy()
                env["LSMS_DATA_DIR"] = str(data_root())
                bin_dir = os.path.dirname(sys.executable)
                env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
                subprocess.run(["make", "-s", '../' + str(relative_parquet_path)], cwd=cwd_path, check=True, env=env)
                logger.info(f"Makefile executed successfully for {self.name}. Rechecking for parquet file...")

                for candidate in [external_parquet, intree_parquet]:
                    if candidate.exists():
                        parquet_fn = candidate
                        break

            if parquet_fn is None:
                logger.warning(f"Parquet file for {request} still missing after running Makefile.")
                return pd.DataFrame()

            df = pd.read_parquet(parquet_fn)

        if isinstance(df, pd.DataFrame):
            df = map_index(df)
        
        df = check_adding_t(df)
        df = df[df.index.get_level_values('t') == self.year]
        return df

    # This cluster_features method is explicitly defined because additional processing is required after calling grab_data.
    def cluster_features(self) -> pd.DataFrame:
        df = self.grab_data('cluster_features')
        # if cluster_feature data is from old other_features.parquet file, region is called 'm' so we need to rename it
        if 'm' in df.index.names:
            df = df.reset_index(level = 'm').rename(columns = {'m':'Region'})
        if 'm' in df.columns:
            df = df.rename(columns = {'m':'Region'})
        return df
    
    # Food acquired method is explicitly defined because potentially categorical mapping is required after calling grab_data.
    def food_acquired(self) -> pd.DataFrame:
        df = self.grab_data('food_acquired')
        if df.empty:
            return df
        # if food_acquired data is loaded from a parquet file, we assume its unit and food label are already mapped.
        # Check both in-tree and data_root locations (wave scripts write to data_root).
        intree_parquet = self.file_path / "_" / "food_acquired.parquet"
        external_parquet = data_root(self.country.name) / self.wave_folder / "_" / "food_acquired.parquet"
        if intree_parquet.exists() or external_parquet.exists():
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
    """Primary interface to a single country's LSMS survey data.

    Provides access to all survey waves, standardized tables, and panel data.
    Tables listed in ``data_scheme`` are available as callable attributes
    (e.g. ``country.food_expenditures()``).

    Parameters
    ----------
    country_name : str
        Directory name under ``lsms_library/countries/``
        (e.g. ``'Uganda'``, ``'Tanzania'``).
    preload_panel_ids : bool
        If True, compute panel ID mappings eagerly at construction time.
        Default is False (lazy).
    verbose : bool
        Enable verbose logging.
    assume_cache_fresh : bool
        If True, read existing cached Parquet files directly, bypassing DVC
        and the normal build pipeline.  Use this when you know the cache is
        up-to-date and want to skip all existence / staleness checks.
        ``_finalize_result`` (kinship expansion, canonical spelling, dtype
        coercion, ``_join_v_from_sample``) still runs on every read — only
        the cache-lookup / DVC layer is bypassed.  Useful on clusters where
        the parquet cache has been pre-built.  Ignores ``LSMS_NO_CACHE``.
    trust_cache : bool
        Deprecated alias for ``assume_cache_fresh``.  Will be removed in
        v0.8.0.

    Examples
    --------
    >>> import lsms_library as ll
    >>> uga = ll.Country('Uganda')
    >>> uga.waves
    ['2005-06', '2009-10', ...]
    >>> uga.data_scheme
    ['food_acquired', 'household_roster', ...]
    >>> food = uga.food_expenditures()
    """

    required_list = ['food_acquired', 'household_roster', 'cluster_features',
                 'interview_date', 'household_characteristics',
                 'food_expenditures', 'food_quantities', 'food_prices',
                 'fct', 'nutrition','name','_panel_ids_cache', 'panel_ids']

    def __init__(self, country_name: str, preload_panel_ids: bool = False, verbose: bool = False, assume_cache_fresh: bool = False, trust_cache: bool = False) -> None:
        # Validate country name: reject path traversal attempts
        countries_dir = Path(__file__).resolve().parent / "countries"
        country_dir = (countries_dir / country_name).resolve()
        if not country_dir.is_relative_to(countries_dir):
            raise ValueError(f"Invalid country name {country_name!r}: path traversal not allowed")
        if not country_dir.is_dir():
            warnings.warn(f"Country directory not found for {country_name!r}")
        self.name = country_name
        self._panel_ids_cache = None
        self._updated_ids_cache = None
        self.wave_folder_map = {}
        if trust_cache:
            warnings.warn(
                "trust_cache is deprecated and will be removed in v0.8.0. "
                "Use assume_cache_fresh=True instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            assume_cache_fresh = True
        self.assume_cache_fresh = assume_cache_fresh
        scheme_map = self.resources.get("Data Scheme") if isinstance(self.resources, dict) else {}
        has_panel_ids = isinstance(scheme_map, dict) and "panel_ids" in scheme_map
        if preload_panel_ids and has_panel_ids:
            logger.info(f"Preloading panel_ids for {self.name}...")
            #ignore all the warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                _ = self._compute_panel_ids()

    @property
    def file_path(self) -> Path:
        return Path(__file__).resolve().parent / "countries" / self.name

    @property
    def resources(self) -> dict[str, Any]:
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
    def formatting_functions(self) -> dict[str, Callable[..., Any]]:
        function_dic = {}
        for file in [f"{self.name.lower()}.py", 'mapping.py']:
            general_mod_path = self.file_path/ "_"/ file
            function_dic.update(get_formatting_functions(general_mod_path, f"formatting_{self.name}"))

        return function_dic

    @property
    def categorical_mapping(self) -> dict[str, pd.DataFrame]:
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
    def mapping(self) -> dict[str, Any]:
        return {**self.categorical_mapping, **self.formatting_functions}

    def _resolve_materialize_stages(self, method_name: str, waves: list[str]) -> list[StageInfo]:
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
    def waves(self) -> list[str]:
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
            self.wave_folder_map = self.resources.get('wave_folder_map') or {}
            return sorted(self.resources.get('Waves'))
        #Otherwise, we will check the directory for subdirectories that contain 'Documentation' and 'SOURCE'.
        waves = [
            f.name for f in self.file_path.iterdir()
            if f.is_dir() and ((self.file_path / f.name / 'Documentation' / 'SOURCE.org').exists() or (self.file_path / f.name / 'Documentation' / 'SOURCE').exists())
        ]
        return sorted(waves)

    @property
    def data_scheme(self) -> list[str]:
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

    def __getitem__(self, year: str) -> Wave:
        # Ensure the year is one of the available waves
        if year in self.waves:
            wave_folder = self.wave_folder_map.get(year, year)
            return Wave(year, wave_folder, self)
        else:
            raise KeyError(f"{year} is not a valid wave for {self.name}")

    def _market_lookup(self, column: str = 'Region') -> pd.DataFrame:
        """Return a cached (t, v) -> m mapping derived from cluster_features.

        Parameters
        ----------
        column : str
            Which cluster_features column to use as the market identifier
            (e.g. ``'Region'``, ``'District'``).

        Returns
        -------
        pd.DataFrame
            With columns ``t``, ``v``, ``m``.
        """
        cache_key = f"_market_lookup_cache_{column}"
        cached = getattr(self, cache_key, None)
        if cached is not None:
            return cached

        cf_parts = []
        for wave in self.waves:
            try:
                w = self[wave]
                c = w.cluster_features()
                if not c.empty:
                    cf_parts.append(c.reset_index())
            except Exception:
                continue

        if not cf_parts:
            raise RuntimeError("cluster_features returned no data for any wave.")

        cf = pd.concat(cf_parts, ignore_index=True)

        # Wave-level cluster_features() returns raw data that has not
        # been through _apply_categorical_mappings().  Apply the
        # country's categorical mapping here so that market labels are
        # normalised (e.g. "DODOMA" -> "Dodoma") before the lookup is
        # cached and joined onto every downstream table.
        cat_maps = self.categorical_mapping
        if cat_maps:
            lower_lookup_cm = {name.lower(): name for name in cat_maps}
            key = lower_lookup_cm.get(column.lower())
            if key is not None:
                table = cat_maps[key]
                if "Preferred Label" in table.columns:
                    source_cols = [c for c in table.columns if c != "Preferred Label"]
                    if source_cols:
                        rdict = table.set_index(source_cols[0])["Preferred Label"].to_dict()
                        cf[column] = cf[column].replace(rdict)

        if column not in cf.columns:
            available = [c for c in cf.columns if c not in ('i', 't', 'v')]
            raise KeyError(
                f"'{column}' not in cluster_features columns. "
                f"Available: {available}"
            )

        lookup = (cf[['t', 'v', column]]
                  .dropna(subset=[column])
                  .drop_duplicates(subset=['t', 'v']))
        lookup = lookup.rename(columns={column: 'm'})
        lookup['m'] = lookup['m'].astype(str).str.strip()
        lookup = lookup[lookup['m'] != 'nan']

        setattr(self, cache_key, lookup)
        return lookup

    def _join_v_from_sample(self, df: pd.DataFrame) -> pd.DataFrame:
        """Join cluster identity ``v`` from sample() onto *df*.

        Only call when ``sample`` is in ``data_scheme`` and *df* has
        ``i`` and ``t`` in its index but not ``v``.  The sample()
        result is cached on the Country instance for reuse across
        features.
        """
        cache = getattr(self, '_sample_v_cache', None)
        if cache is None:
            try:
                s = self.sample()
                cache = s[['v']].copy()
                self._sample_v_cache = cache
            except Exception:
                self._sample_v_cache = False
                return df
        if cache is False:
            return df

        idx_names = list(df.index.names)
        flat = df.reset_index()
        # Coerce join keys to string for safe matching
        for key in ['i', 't']:
            if key in flat.columns:
                flat[key] = flat[key].astype(str)
        v_lookup = cache.reset_index()
        v_lookup['i'] = v_lookup['i'].astype(str)
        v_lookup['t'] = v_lookup['t'].astype(str)

        flat = flat.merge(v_lookup, on=['i', 't'], how='left')

        # Insert v after t in the index
        new_idx = []
        for n in idx_names:
            new_idx.append(n)
            if n == 't':
                new_idx.append('v')
        if 'v' not in new_idx:
            new_idx.append('v')
        result = flat.set_index(new_idx)
        # pandas merge() and set_index() both drop DataFrame.attrs.
        # Preserve them so flags like id_converted survive the v-join —
        # otherwise _finalize_result runs id_walk a second time on
        # already-converted data, causing ID-chain collisions in
        # countries with transitive panel linkage (#140 BF case).
        result.attrs = dict(df.attrs)
        return result

    def _add_market_index(self, df: pd.DataFrame, column: str = 'Region') -> pd.DataFrame:
        """Join a market identifier ``m`` from cluster_features onto *df*.

        Joins on ``(t, v)`` from ``_market_lookup(column)``.  If *df*
        lacks ``v`` in its index (e.g. a derived table that bypassed
        ``_finalize_result``), join ``v`` from ``sample()`` first.

        Returns *df* with ``m`` added and ``v`` removed from the index.
        """
        lookup = self._market_lookup(column)

        # Ensure v is present before doing the (t, v) -> m join
        if 'v' not in (df.index.names if isinstance(df.index, pd.MultiIndex) else [df.index.name]):
            if 'sample' in self.data_scheme:
                df = self._join_v_from_sample(df)
            else:
                warnings.warn(
                    f"_add_market_index: cannot join market {column!r} — "
                    f"v not in index and no sample() table available for {self.name}"
                )
                return df

        idx_names = list(df.index.names)
        flat = df.reset_index()

        def _normalize_v(series):
            """Normalize cluster IDs to strings, stripping .0 from floats."""
            def _norm(x):
                if pd.isna(x) or x == '':
                    return pd.NA
                try:
                    return str(int(float(x)))
                except (ValueError, OverflowError):
                    return str(x).strip()
            return series.map(_norm)

        # Join on (t, v) — v is now guaranteed present
        flat['t'] = flat['t'].astype(str)
        flat['v'] = _normalize_v(flat['v'])
        lkup = lookup.copy()
        lkup['t'] = lkup['t'].astype(str)
        lkup['v'] = _normalize_v(lkup['v'])
        flat = flat.merge(lkup, on=['t', 'v'], how='left')

        flat = flat.dropna(subset=['m'])
        # Drop village/cluster index (v) since m supersedes it
        if 'v' in idx_names:
            idx_names = [n for n in idx_names if n != 'v']
            if 'v' in flat.columns:
                flat = flat.drop(columns=['v'])
        # Insert m after t to match cfe convention (i, t, m, ...)
        new_idx = []
        for n in idx_names:
            new_idx.append(n)
            if n == 't':
                new_idx.append('m')
        if 'm' not in new_idx:
            new_idx.append('m')
        return flat.set_index(new_idx)

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
    

    def _apply_categorical_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Auto-apply categorical mappings where table names match columns or indices.

        For each column or index level in *df*, check whether
        ``self.categorical_mapping`` contains a table with the same name
        (case-insensitive).  If the table has a ``Preferred Label``
        column, build a replacement dictionary from the first other
        column → ``Preferred Label`` and apply it.
        """
        cat_maps = self.categorical_mapping
        if not cat_maps:
            return df

        # Build case-insensitive lookup
        lower_lookup = {name.lower(): name for name in cat_maps}

        def _build_replace_dict(table: pd.DataFrame) -> dict | None:
            if "Preferred Label" not in table.columns:
                return None
            source_cols = [c for c in table.columns if c != "Preferred Label"]
            if not source_cols:
                return None
            return table.set_index(source_cols[0])["Preferred Label"].to_dict()

        # Apply to columns
        for col in df.columns:
            key = lower_lookup.get(col.lower())
            if key is None:
                continue
            rdict = _build_replace_dict(cat_maps[key])
            if rdict:
                if hasattr(df[col], 'str'):
                    df[col] = df[col].str.strip()
                df[col] = df[col].replace(rdict)

        # Apply to index levels
        if isinstance(df.index, pd.MultiIndex):
            for level_name in df.index.names:
                if level_name is None:
                    continue
                key = lower_lookup.get(level_name.lower())
                if key is None:
                    continue
                rdict = _build_replace_dict(cat_maps[key])
                if rdict:
                    df = df.rename(index=rdict, level=level_name)

        return df

    def _finalize_result(self, df: Any, scheme_entry: dict[str, Any], method_name: str) -> pd.DataFrame | dict[str, Any]:
        """
        Apply final harmonization steps (index augmentation, normalization, id walk)
        before returning a dataset to callers.
        """
        if isinstance(df, dict):
            return df

        if isinstance(df, pd.DataFrame):
            df = self._augment_index_from_related_tables(df, scheme_entry, None)
            df = _normalize_dataframe_index(df, scheme_entry, None)

            # Join v from sample() for household-level tables that lack it.
            # Skip if v is already in the index OR already present as a
            # column (a legacy script may have written v alongside other
            # columns; joining again would create v_x/v_y name conflicts).
            current_names = list(df.index.names) if isinstance(df.index, pd.MultiIndex) else [df.index.name]
            v_already_present = ('v' in current_names
                                 or (isinstance(df, pd.DataFrame) and 'v' in df.columns))
            _no_v_join = {'sample', 'cluster_features', 'panel_ids', 'updated_ids'}
            if (not v_already_present
                    and 'i' in current_names
                    and 't' in current_names
                    and method_name not in _no_v_join
                    and 'sample' in self.data_scheme):
                df = self._join_v_from_sample(df)

            if isinstance(df.index, pd.MultiIndex):
                index_names = list(df.index.names)
                preferred = ['i', 't', 'v', 'm']
                desired_order = [name for name in preferred if name in index_names]
                desired_order += [name for name in index_names if name not in desired_order]
                if desired_order != index_names:
                    try:
                        df = df.reorder_levels(desired_order)
                    except ValueError as exc:
                        warnings.warn(
                            f"Could not reorder index levels for {method_name}: {exc}"
                        )

            if (
                'i' in df.index.names
                and not df.attrs.get('id_converted')
                and method_name not in ['panel_ids', 'updated_ids']
                and self._updated_ids_cache is not None
            ):
                df = id_walk(df, self.updated_ids)

            # Normalise "Relation" -> "Relationship" so kinship expansion fires
            if "Relation" in df.columns and "Relationship" not in df.columns:
                df = df.rename(columns={"Relation": "Relationship"})

            # Expand Relationship -> Generation, Distance, Affinity
            if "Relationship" in df.columns:
                df = _expand_kinship(df)

            # Auto-apply categorical mappings where table name matches
            # a column or index name (issue #49)
            df = self._apply_categorical_mappings(df)

            # Normalise variant spellings to canonical forms
            if method_name:
                df = _enforce_canonical_spellings(df, method_name)

            # Enforce rejected column-name spellings (e.g. Effected→Affected)
            df = _enforce_rejected_column_spellings(df)

            # Enforce declared dtypes from data_scheme.yml
            if isinstance(scheme_entry, dict):
                _enforce_declared_dtypes(df, scheme_entry)

        return df

    def _aggregate_wave_data(self, waves: list[str] | None = None, method_name: str | None = None) -> pd.DataFrame | dict[str, Any]:
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

        prefer_parquet_cache = bool(getattr(self, "assume_cache_fresh", False)) and method_name not in JSON_CACHE_METHODS
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
            except (FileNotFoundError, KeyError, ValueError):
                pass  # updated_ids not available for this country

        def safe_concat_dataframe_dict(df_dict):
            # Use the superset of all index levels as the reference order,
            # preserving declaration order from the first DataFrame.
            all_names: list[str] = []
            for df in df_dict.values():
                for name in df.index.names:
                    if name not in all_names:
                        all_names.append(name)
            reference_order = all_names

            aligned_dfs = {}
            for key, df in df_dict.items():
                current = list(df.index.names)
                missing = [lvl for lvl in reference_order if lvl not in current]
                if missing:
                    df = df.reset_index()
                    for lvl in missing:
                        df[lvl] = np.nan
                    df = df.set_index(reference_order)
                elif current != reference_order:
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
            # Resolve the wave folder for path construction (e.g. '2008-09' -> '2008-15')
            wave_folder = self.wave_folder_map.get(wave, wave) if wave else None
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
                    output_candidates.append(data_root(self.name) / wave_folder / "_" / f"{method_name}.parquet")
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
                        make_targets.append(data_root(self.name) / wave_folder / "_" / f"{method_name}.parquet")
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
                        logger.info(f"Makefile executed successfully for {self.name}/{wave or 'ALL'}. Rechecking for {method_name}...")
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
                    logger.info(f"Python fallback executed for {script.parent.name}.{method_name}.")
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
                logger.warning(f"Data file {target_path} still missing after running fallbacks.")
                return None

            if output_path.suffix == ".json":
                with open(output_path, "r", encoding="utf-8") as json_file:
                    return json.load(json_file)

            df_local = get_dataframe(str(output_path))
            df_local = map_index(df_local)
            return df_local

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
            if isinstance(country_fallback, pd.DataFrame) and not country_fallback.empty:
                return country_fallback

            raise _rebuild_failure_error(self.name, method_name)

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
                    logger.debug(f"Reading {method_name} from cache {candidate}")
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
                logger.warning(f"Makefile build failed for {method_name}: {error}. Falling back to wave aggregation.")
                result = load_from_waves(waves)
            else:
                result = run_make_target(method_name)
                if result is None:
                    raise _rebuild_failure_error(self.name, method_name)

            if isinstance(result, dict):
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, 'w') as json_file:
                    json.dump(result, json_file)
                logger.debug(f"Writing {method_name} to cache {cache_path}")
            elif isinstance(result, pd.DataFrame):
                result = _enforce_rejected_column_spellings(result)
                parquet_path = data_root(self.name) / "_" / f"{method_name}.parquet"
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                to_parquet(result, parquet_path)
                if cache_path.exists():
                    try:
                        cache_path.unlink()
                    except OSError:
                        pass
                logger.debug(f"Writing {method_name} to cache {parquet_path}")
            return result

        def load_dataframe_with_dvc(method_name):
            """
            Load data using DVC materialize stages. Falls back to load_from_waves if stages are missing.
            """
            cache_path = data_root(self.name) / "var" / f"{method_name}.parquet"
            cache_exists = cache_path.exists()

            # v0.7.0: best-effort cache read.  If a parquet exists at
            # cache_path, read it and return without consulting DVC, the
            # stage layer, or the wave loaders.  No staleness check is
            # performed -- contributors editing source data are expected
            # to clear the cache (`lsms-library cache clear --country X`)
            # or set LSMS_NO_CACHE=1 in the environment.  Hash-based
            # invalidation is deferred to v0.8.0.
            #
            # This block fixes the write-only-cache bug at the
            # `if not stage_infos:` branch below (which wrote a parquet
            # but never read it back) AND closes the same gap for the
            # outer exception handler that catches DvcException, which
            # also writes through `load_from_waves` after the v0.7.0
            # cache-write fix in the `except` block at the bottom of
            # this function.  See SkunkWorks/dvc_object_management.org
            # for the full design and bench/results/ for empirical
            # confirmation.  The returned DataFrame is intentionally
            # pre-finalize: `_aggregate_wave_data` calls
            # `_finalize_result` on it before returning to the user, so
            # kinship expansion, spelling normalization, and the
            # `_join_v_from_sample` augmentation still apply.
            no_cache = os.environ.get("LSMS_NO_CACHE", "").lower() in {"1", "true", "yes"}
            if cache_exists and not no_cache:
                try:
                    cached_df = get_dataframe(cache_path)
                    cached_df = map_index(cached_df)
                    logger.debug(
                        f"v0.7.0 cache read: {method_name} from {cache_path}"
                    )
                    return cached_df
                except Exception as cache_read_error:
                    logger.debug(
                        f"v0.7.0 cache read failed for {method_name} "
                        f"({cache_read_error!r}); rebuilding from source"
                    )

            dvc_root = self.file_path.parent

            repo: Repo | None = None
            try:
                with _working_directory(dvc_root):
                    repo = Repo(str(dvc_root))

                    stage_infos = self._resolve_materialize_stages(method_name, waves)
                    if not stage_infos:
                        df = load_from_waves(waves)
                        if isinstance(df, pd.DataFrame):
                            df = _enforce_rejected_column_spellings(df)
                            cache_path.parent.mkdir(parents=True, exist_ok=True)
                            to_parquet(df, cache_path)
                            logger.debug(f"Writing {method_name} to cache {cache_path}")
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
                        combined_df = _enforce_rejected_column_spellings(combined_df)
                        cache_path.parent.mkdir(parents=True, exist_ok=True)
                        to_parquet(combined_df, cache_path)
                        logger.debug(f"Writing {method_name} to cache {cache_path}")
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
                    except (OSError, ValueError, KeyError) as status_error:
                        warnings.warn(
                            f"DVC status failed for {method_name}: {status_error!r}. Assuming dirty outputs."
                        )
                        dirty = True

                    if cache_exists and not dirty:
                        try:
                            logger.debug(f"Reading {method_name} from cache {cache_path}")
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
                                except (OSError, ValueError, RuntimeError) as reproduce_error:
                                    warnings.warn(f"DVC reproduce failed for {stage.addressing}: {reproduce_error!r}. Falling back to legacy loaders.")
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
                logger.warning(f"DVC unavailable for {method_name}: {e}. Falling back to manual aggregation.")
                # v0.7.0: write the rebuild result to cache_path so the
                # next call hits the top-of-function cache read above.
                # Mirrors the `if not stage_infos:` branch that already
                # writes after load_from_waves.  Without this, dvc.yaml
                # countries (Uganda, Senegal, etc.) whose stages fail at
                # reproduce never populate the cache and rebuild from
                # source on every call.
                df = load_from_waves(waves)
                if isinstance(df, pd.DataFrame):
                    df = _enforce_rejected_column_spellings(df)
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    to_parquet(df, cache_path)
                    logger.debug(
                        f"v0.7.0 cache write (DVC fallback): {method_name} to {cache_path}"
                    )
                return df
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
                    if df is None:
                        raise _rebuild_failure_error(self.name, method_name)
            except Exception as error:
                _log_issue(self.name, method_name, waves, error)
                raise
        return self._finalize_result(df, scheme_entry, method_name)

    def _compute_panel_ids(self) -> None:
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
            logger.warning(f"Panel IDs not found in {self.name}.")
            self._panel_ids_cache = None
            self._updated_ids_cache = None

    @property
    def panel_ids(self) -> dict[str, Any] | None:
        """Raw panel-ID tables keyed by wave.  Computed lazily on first access."""
        if self._panel_ids_cache is None or self._updated_ids_cache is None:
            self._compute_panel_ids()
        return self._panel_ids_cache

    @property
    def updated_ids(self) -> dict[str, dict[str, str]] | None:
        """Mapping ``{old_id: new_id}`` per wave for ID harmonization.  Computed lazily."""
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
    
    def panel_attrition(self, waves=None, index='i', return_ids=False, split_households_new_sample=True):
        """
        Produce an upper-triangular matrix showing the number of households 
        that transition between rounds.
        
        Args:
            waves: List of waves to analyze (default: all waves)
            index: Index variable to track (default: 'i' for household)
            return_ids: If True, return both matrix and IDs dict
            split_households_new_sample: If True, treat split households as new sample
            
        Returns:
            DataFrame showing panel structure, optionally with IDs dict
        """
        from lsms_library.local_tools import panel_attrition as _panel_attrition
        
        if waves is None:
            waves = self.waves
        
        # Get household roster across all waves
        df = self.household_roster(waves=waves)
        
        return _panel_attrition(df, waves=waves, index=index, 
                               return_ids=return_ids, 
                               split_households_new_sample=split_households_new_sample)

    # Tables that can be derived from food_acquired via transformations
    _FOOD_DERIVED = {
        'food_expenditures': 'food_expenditures_from_acquired',
        'food_prices': 'food_prices_from_acquired',
        'food_quantities': 'food_quantities_from_acquired',
    }

    # Tables that can be derived from household_roster via transformations
    _ROSTER_DERIVED = {
        'household_characteristics': 'roster_to_characteristics',
    }

    # Deprecated table names: name -> deprecation message
    _DEPRECATED = {
        'locality': (
            "Country('Uganda').locality() is deprecated and will be removed "
            "in a future release. The data it carries is now first-class in "
            "two separate tables:\n\n"
            "  * (i, t) -> v  (household -> cluster)     via  Country(X).sample()\n"
            "  * (t, v) -> Region, Rural, District, ... via  Country(X).cluster_features()\n\n"
            "For callers who need the legacy (i, t, m) -> v shape, a "
            "compatibility shim is available:\n\n"
            "    from lsms_library.transformations import legacy_locality\n"
            "    loc = legacy_locality(Country('Uganda'))\n\n"
            "See docs/migration/locality.md for details."
        ),
    }

    def __getattr__(self, name):
        '''
        This method is triggered when an attribute is not found in the instance, but exists in the `data_scheme`.
        It dynamically generates a method to aggregate data for the requested attribute.

        For example, if a user calls `country_instance.food_acquired()` and `food_acquired` is part of the `data_scheme` but not an existing method,
        the method will dynamically create a function to handle data aggregation for `food_acquired`.

        For derived food tables (food_expenditures, food_prices, food_quantities),
        if no parquet/script exists but food_acquired is available, the table is
        derived automatically via transformations.  Similarly, household_characteristics
        can be derived from household_roster via roster_to_characteristics.

        Deprecated tables (listed in _DEPRECATED) emit a DeprecationWarning and
        return the compatibility shim output rather than raising AttributeError.
        The deprecation check fires before the data_scheme check so it takes
        effect even if the entry was accidentally left in data_scheme.yml.
        '''
        # Deprecated tables: warn and delegate to shim before anything else
        if name in self._DEPRECATED:
            dep_msg = self._DEPRECATED[name]

            def method(*args, **kwargs):
                warnings.warn(dep_msg, DeprecationWarning, stacklevel=2)
                from .transformations import legacy_locality
                _shims = {'locality': legacy_locality}
                return _shims[name](self)

            method.__name__ = name
            method.__qualname__ = f"Country.{name}"
            method.__module__ = self.__class__.__module__
            method.__doc__ = (
                f"[DEPRECATED] {dep_msg}\n\n"
                "This method will be removed in a future release."
            )
            try:
                object.__setattr__(self, name, method)
            except (AttributeError, TypeError):
                pass
            return method

        if name in self.data_scheme or name in self._FOOD_DERIVED or name in self._ROSTER_DERIVED:
            def method(waves=None, market=None):
                # For derived food tables, try deriving from food_acquired first
                # before falling back to wave-level scripts / make
                if (name in self._FOOD_DERIVED
                        and 'food_acquired' in self.data_scheme):
                    from .transformations import (food_expenditures_from_acquired,
                                                  food_prices_from_acquired,
                                                  food_quantities_from_acquired)
                    transform_fn = {
                        'food_expenditures': food_expenditures_from_acquired,
                        'food_prices': food_prices_from_acquired,
                        'food_quantities': food_quantities_from_acquired,
                    }[name]
                    try:
                        fa = self._aggregate_wave_data(waves, 'food_acquired')
                        if isinstance(fa, pd.DataFrame) and not fa.empty:
                            result = transform_fn(fa)
                            scheme_entry = self._materialization_entry(name)
                            result = self._finalize_result(result, scheme_entry, name)
                            if market is not None:
                                result = self._add_market_index(result, column=market)
                            return result
                    except Exception:
                        pass  # Fall through to normal aggregation

                # Derive household_characteristics from household_roster
                if (name in self._ROSTER_DERIVED
                        and 'household_roster' in self.data_scheme):
                    from .transformations import roster_to_characteristics
                    try:
                        roster = self._aggregate_wave_data(waves, 'household_roster')
                        if isinstance(roster, pd.DataFrame) and not roster.empty:
                            # Determine final_index from available index levels
                            idx = list(roster.index.names)
                            final_index = [n for n in ['t', 'v', 'i', 'm'] if n in idx]
                            if not final_index:
                                final_index = [n for n in idx if n != 'pid']
                            result = roster_to_characteristics(
                                roster, drop='pid', final_index=final_index)
                            if market is not None:
                                result = self._add_market_index(result, column=market)
                            return result
                    except Exception:
                        pass  # Fall through to normal aggregation

                result = self._aggregate_wave_data(waves, name)
                if market is not None and isinstance(result, pd.DataFrame) and not result.empty:
                    result = self._add_market_index(result, column=market)
                return result
            method.__doc__ = (
                f"Return {name} as a DataFrame, aggregated across *waves*.\n\n"
                "Parameters\n----------\n"
                "waves : list of str, optional\n"
                "    Subset of waves to include.  Defaults to all available.\n"
                "market : str, optional\n"
                "    Column from cluster_features (e.g. 'Region') to add as\n"
                "    an ``m`` index level for demand estimation.\n"
            )
            method.__name__ = name
            method.__qualname__ = f"Country.{name}"
            method.__module__ = self.__class__.__module__
            # Cache on the instance so repeated lookups return the same object
            # and IPython's pinfo can find it reliably.
            try:
                object.__setattr__(self, name, method)
            except (AttributeError, TypeError):
                pass
            return method
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __dir__(self):
        """Extend dir() to include dynamically generated table methods.

        IPython's ``?`` operator and tab completion rely on ``dir()`` to
        discover attributes.  Without this override, methods generated by
        ``__getattr__`` are invisible to introspection tools even though they
        work fine when called directly.
        """
        base = list(super().__dir__())
        dynamic = (
            list(self.data_scheme)
            + list(self._FOOD_DERIVED.keys())
            + list(self._ROSTER_DERIVED.keys())
            + list(self._DEPRECATED.keys())
        )
        return base + [n for n in dynamic if n not in base]
    
    def test_all_data_schemes(self, waves: list[str] | None = None) -> dict[str, str]:
        """
        Test whether all method_names in obj.data_scheme can be successfully built.
        Falls back to Makefile if not in data_scheme.
        """
        all_methods = set(self.data_scheme)
        print(f"Testing all methods in {self.name} data scheme: {sorted(all_methods)}")

        failed_methods = {}
        
        for method_name in sorted(all_methods):
            # Skip deprecated tables: they are handled by the _DEPRECATED shim
            # and should not be built as normal data_scheme entries.
            if method_name in self._DEPRECATED:
                print(f"\n>>> Skipping deprecated method: {method_name}")
                continue
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
            except Exception as e:  # broad catch intentional: diagnostic method
                print(f"Failed to load {method_name}: {e!r}")
                failed_methods[method_name] = str(e)

        print("\n=== Summary ===")
        if failed_methods:
            print(f"{len(failed_methods)} methods failed:")
            for method, error in failed_methods.items():
                print(f" - {method}: {error}")
        else:
            print("All methods loaded successfully!")

        return failed_methods



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


_SCHEME_DTYPE_MAP = {
    'bool': pd.BooleanDtype(),
    'int': pd.Int64Dtype(),
    'float': pd.Float64Dtype(),
    'str': pd.StringDtype(),
    'string': pd.StringDtype(),
    'datetime': 'to_datetime',
    'date': 'to_datetime',
    'timestamp': 'to_datetime',
}

_SCHEME_SKIP_KEYS = frozenset({'index', 'materialize', 'backend'})


@lru_cache(maxsize=1)
def _load_kinship_map() -> dict[str, tuple[int, int, str]]:
    """Load the kinship dictionary from kinship.yml.

    Returns a dict mapping relationship label strings to
    ``(Generation, Distance, Affinity)`` tuples.
    """
    kinship_path = files("lsms_library") / "categorical_mapping" / "kinship.yml"
    with open(kinship_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {label: tuple(vals) for label, vals in data.items()}


def _expand_kinship(df: pd.DataFrame) -> pd.DataFrame:
    """Expand a Relationship column into Generation, Distance, Affinity.

    Uses the dictionary in ``categorical_mapping/kinship.yml``.
    Unrecognised labels produce NA values and a warning listing the
    unknown strings so they can be added to kinship.yml.
    """
    if "Relationship" not in df.columns:
        return df

    kinship = _load_kinship_map()

    gen = []
    dist = []
    aff = []
    rel = []
    unknown = set()

    # Exact-match NA sentinels.  Do NOT lowercase-fold: "Nan" is British
    # English for grandmother in some contexts.  Explicit kinship.yml
    # mappings are checked FIRST, so if a survey legitimately uses "Nan"
    # as a label, adding it to kinship.yml overrides this sentinel list.
    _NA_SENTINELS = ('', '<NA>', 'nan', 'Nan', 'NaN', 'NAN', 'None', 'NaT')
    for val in df["Relationship"]:
        # True NA (not a string): always NA.
        if pd.isna(val):
            gen.append(pd.NA); dist.append(pd.NA); aff.append(pd.NA)
            rel.append(pd.NA)
            continue
        label = str(val).strip().title()
        stripped = str(val).strip()
        # Explicit mapping wins over sentinel list.
        tup = kinship.get(label) or kinship.get(stripped)
        if tup is not None:
            gen.append(tup[0]); dist.append(tup[1]); aff.append(tup[2])
            rel.append(val)
            continue
        # Unmapped and matches an NA sentinel: treat as NA silently.
        if stripped in _NA_SENTINELS:
            gen.append(pd.NA); dist.append(pd.NA); aff.append(pd.NA)
            rel.append(pd.NA)
            continue
        # Unmapped and not a sentinel: warn and NA.
        unknown.add(stripped)
        gen.append(pd.NA); dist.append(pd.NA); aff.append(pd.NA)
        rel.append(val)

    df["Relationship"] = pd.array(rel, dtype=pd.StringDtype())
    df["Generation"] = pd.array(gen, dtype=pd.Int64Dtype())
    df["Distance"] = pd.array(dist, dtype=pd.Int64Dtype())
    df["Affinity"] = pd.array(aff, dtype=pd.StringDtype())
    # Keep Relationship alongside the decomposed columns — the analyst
    # can see both the original survey label and the structured kinship.

    if unknown:
        warnings.warn(
            f"Unknown relationship labels (add to kinship.yml): "
            f"{sorted(unknown)}",
            stacklevel=2,
        )

    return df


@lru_cache(maxsize=1)
def _load_canonical_spellings() -> dict[str, dict[str, dict[str, str]]]:
    """Load spelling maps from data_info.yml Columns section.

    Returns ``{table: {column: {variant: canonical}}}`` built from the
    ``spellings`` inverse dictionaries.
    """
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    result: dict[str, dict[str, dict[str, str]]] = {}
    for table, cols in data.get("Columns", {}).items():
        for col, meta in cols.items():
            if not isinstance(meta, dict):
                continue
            spellings = meta.get("spellings")
            if not spellings:
                continue
            variant_map: dict[str, str] = {}
            for canonical, variants in spellings.items():
                for v in (variants or []):
                    variant_map[v] = canonical
            if variant_map:
                result.setdefault(table, {})[col] = variant_map
    return result


def _enforce_canonical_spellings(df: pd.DataFrame, method_name: str) -> pd.DataFrame:
    """Replace variant spellings with canonical forms per data_info.yml.

    Looks up the ``spellings`` dictionaries for the table being loaded
    and applies them to matching columns.
    """
    all_spellings = _load_canonical_spellings()
    table_spellings = all_spellings.get(method_name, {})
    for col, variant_map in table_spellings.items():
        if col in df.columns:
            df[col] = df[col].replace(variant_map)
        elif isinstance(df.index, pd.MultiIndex) and col in df.index.names:
            df = df.rename(index=variant_map, level=col)
    return df


@lru_cache(maxsize=1)
def _load_rejected_column_spellings() -> dict[str, str]:
    """Load the ``Rejected Spellings`` table from ``data_info.yml``.

    Returns ``{rejected_substring: canonical_substring}`` — e.g.
    ``{"Effected": "Affected"}``.  Applied to **column names**, not values.
    """
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("Rejected Spellings", {}) or {}


def _enforce_rejected_column_spellings(df: pd.DataFrame) -> pd.DataFrame:
    """Rename DataFrame columns that contain a rejected spelling substring.

    Uses the ``Rejected Spellings`` section of ``data_info.yml`` to replace
    substrings in column names — e.g. ``EffectedIncome`` → ``AffectedIncome``.
    """
    rejected = _load_rejected_column_spellings()
    if not rejected:
        return df
    rename_map: dict[str, str] = {}
    for col in df.columns:
        new_col = col
        for bad, good in rejected.items():
            if bad in new_col:
                new_col = new_col.replace(bad, good)
        if new_col != col:
            rename_map[col] = new_col
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _enforce_declared_dtypes(df: pd.DataFrame, scheme_entry: dict[str, Any]) -> None:
    """Cast DataFrame columns to the types declared in data_scheme.yml.

    Modifies *df* in place.  List declarations (e.g. ``[Male, Female]``)
    are treated as constrained string columns and cast to ``StringDtype``;
    validation of the actual values is left to ``diagnostics._check_value_constraints``.
    """
    for col, declared_type in scheme_entry.items():
        if col in _SCHEME_SKIP_KEYS or col not in df.columns:
            continue
        try:
            if isinstance(declared_type, list):
                df[col] = df[col].astype(pd.StringDtype())
            elif isinstance(declared_type, str) and declared_type in _SCHEME_DTYPE_MAP:
                target = _SCHEME_DTYPE_MAP[declared_type]
                if target == 'to_datetime':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif target in (pd.Int64Dtype(), pd.Float64Dtype(), pd.BooleanDtype()):
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(target)
                else:
                    df[col] = df[col].astype(target)
        except (ValueError, TypeError):
            pass  # best-effort; don't break loading


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

    if not isinstance(df, pd.DataFrame):
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
        # Only set_index with declared levels that actually exist in the DataFrame
        available = [lvl for lvl in declared if lvl in df.columns]
        if available:
            df = df.set_index(available)
        current_names = list(df.index.names)

    # Reorder levels to match declaration
    present_declared = [lvl for lvl in declared if lvl in current_names]
    if present_declared:
        remaining = [lvl for lvl in current_names if lvl not in present_declared]
        try:
            df = df.reorder_levels(present_declared + remaining)
        except (ValueError, TypeError):
            pass  # level count mismatch or non-hierarchical index; keep original order

    # Drop any unexpected index levels (but keep at least one, and only on MultiIndex)
    extra_levels = [lvl for lvl in df.index.names if lvl not in declared]
    if (
        extra_levels
        and isinstance(df.index, pd.MultiIndex)
        and len(df.index.names) > len(extra_levels)
    ):
        try:
            df = df.droplevel(extra_levels)
        except ValueError:
            pass  # Cannot drop levels; keep original index

    # Aggregate duplicates if any remain
    if not df.index.is_unique:
        present_levels = [lvl for lvl in declared if lvl in df.index.names]
        # Convert unordered categoricals to strings so groupby.first() works
        for col in df.columns:
            if hasattr(df[col], 'cat') and not df[col].cat.ordered:
                df[col] = df[col].astype(str).replace({'nan': pd.NA, 'None': pd.NA, '<NA>': pd.NA})
        df = df.groupby(level=present_levels, observed=True).first()

    return df
