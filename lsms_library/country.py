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
from collections import defaultdict
from .local_tools import df_data_grabber, format_id, get_categorical_mapping, get_dataframe, map_index, get_formatting_functions, panel_ids, id_walk, all_dfs_from_orgfile, to_parquet
from .local_tools import (
    LSMS_CACHE_SCHEMA,
    cached_file_hash,
    source_fingerprint,
    scan_script_data_refs,
    read_parquet_cache_hash,
    cache_freshness,
    stamp_parquet_hash,
    read_parquet_grain_audit,
    _collect_file_paths_from_block,
)
from .paths import data_root, countries_root
from .yaml_utils import load_yaml
from .currency import attach_currency, is_monetary_table
from .errors import LabelUnavailableError
from ._build_registry import build_transform, build_transforms_fingerprint, framework_imports_fingerprint
import importlib.util
import hashlib
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
from pyarrow.lib import ArrowInvalid
from datetime import datetime
from typing import Any, Callable, Iterable
from contextlib import contextmanager, redirect_stdout

logger = logging.getLogger(__name__)

JSON_CACHE_METHODS = {'panel_ids', 'updated_ids'}

# Reserved values on the ``u`` index level that are framework sentinels, not
# survey unit labels: ``'kg'`` is the kg-conversion tag emitted by
# food_quantities/food_prices; ``'Value'`` marks LCU-only goods.  A country's
# ``#+name: u`` categorical table must not remap these on *derived* food
# tables (GH #361) — see ``_apply_categorical_mappings(protect_u_sentinels=)``.
_RESERVED_U_SENTINELS = frozenset({'kg', 'Value'})

# Derived food tables whose ``u`` level can carry the reserved sentinels.
_U_SENTINEL_PROTECTED_METHODS = frozenset({'food_quantities', 'food_prices'})


def _augment_numeric_code_keys(rdict: dict) -> dict:
    """Add int/float-string variants of integer-valued keys to a replace dict.

    A categorical_mapping keyed on numeric codes (e.g. ``Code`` = 1, 2, 3)
    won't match index data that arrives as float-strings (``'1.0'``) — the
    "format_id applied to idxvars but not myvars" gotcha that leaks raw unit
    codes into ``food_acquired``'s ``u`` level (GH #223 Layer 2).  For every
    key whose value is an integer (whether ``1``, ``'1'``, or ``'1.0'``),
    register the ``'1'`` and ``'1.0'`` string variants pointing at the same
    Preferred Label.  Purely additive — the original keys win on collision,
    and non-numeric labels (``'Tas'``) are left untouched.
    """
    extra: dict = {}
    for k, v in rdict.items():
        ks = str(k).strip()
        try:
            f = float(ks)
        except (TypeError, ValueError):
            continue
        if f != int(f):
            continue  # genuine non-integer; not a unit code
        i = int(f)
        for variant in (str(i), f"{i}.0"):
            extra.setdefault(variant, v)
    if not extra:
        return rdict
    # Original keys take precedence over synthesized variants.
    return {**extra, **rdict}


# Categorical tables whose global (lsms_library/categorical_mapping/) and
# per-country versions are merged ROW-additively rather than full-override
# (GH #223 Layer 2 / DESIGN_u_consolidation).  For these, a country inherits
# the global rows (e.g. universal metric units in a global u.org) and only
# needs to declare its country-specific rows; a country row wins on a
# source-label key collision.  Every other table keeps the historical
# full-table override.  Keep this allow-list small and explicit.
#
# ``harmonize_assets`` (GH #168): the asset conceptual space is small and
# largely universal ("a bicycle is a bicycle"), so a global
# categorical_mapping/harmonize_assets.org carries the shared vocabulary and a
# country need only declare its country-specific raw labels (numeric codes,
# French/Portuguese spellings, per-wave typos) as overrides on top of the
# global base -- inherit-and-override, not re-list-everything.
#
# ``harmonize_education`` (GH #171): a global ordinal-level vocabulary
# (categorical_mapping/harmonize_education.org) is the shared base; per-country
# tables add only their country-specific attainment labels (English grade
# names, French/Portuguese levels, numeric grade codes) as overrides on top.
_ADDITIVE_CATEGORICAL_TABLES = frozenset({'u', 'harmonize_assets', 'harmonize_education'})


def _categorical_key_column(table: "pd.DataFrame") -> str | None:
    """The source-label column of a categorical table (first non-Preferred)."""
    cols = [c for c in table.columns if c != "Preferred Label"]
    return cols[0] if cols else None


def _row_union_categorical(global_t: "pd.DataFrame",
                           country_t: "pd.DataFrame") -> "pd.DataFrame":
    """Row-union a global and a country categorical table; country row wins.

    Identity is the source-label key column (first non-``Preferred Label``
    column).  Country rows override global rows with the same key; new
    country keys are added; global keys absent from the country table are
    kept.  Columns are outer-unioned (e.g. per-wave columns), with country
    values winning on overlap.  Falls back to the country table wholesale
    when the two key columns don't agree (can't align).
    """
    gk = _categorical_key_column(global_t)
    ck = _categorical_key_column(country_t)
    if gk is None or ck is None or gk != ck:
        return country_t
    # country first + keep='first' => country rows win on key collision;
    # concat outer-joins columns (NaN-filled), so wave columns survive.
    combined = pd.concat([country_t, global_t], ignore_index=True, sort=False)
    combined = combined.drop_duplicates(subset=[ck], keep="first")
    return combined.reset_index(drop=True)


def _merge_categorical_tables(global_maps: dict, country_maps: dict) -> dict:
    """Merge global and per-country categorical tables.

    Default is full-table override (country replaces global by name).  For
    names in :data:`_ADDITIVE_CATEGORICAL_TABLES`, the two are row-unioned
    (see :func:`_row_union_categorical`) when both exist, so the country
    inherits global rows and overrides only the keys it redeclares.
    """
    merged = dict(global_maps)
    for name, ctab in country_maps.items():
        if name in _ADDITIVE_CATEGORICAL_TABLES and name in merged:
            merged[name] = _row_union_categorical(merged[name], ctab)
        else:
            merged[name] = ctab
    return merged


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

    ``LSMS_MAKE_JOBS`` overrides the default (``cpu_count // 2``).  A
    country-level build fans out to one ``python <table>.py`` per wave, so
    ``-jN`` runs N wave builds -- and thus N concurrent large-blob S3 fetches --
    in parallel.  On a host where concurrent multipart S3 reads occasionally
    corrupt a TLS record under load (see ``local_tools._get_file_with_retry``,
    which retries those), set ``LSMS_MAKE_JOBS=1`` to serialize the fetches.
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


# Module-level parse cache for data_info.yml, keyed on the file's CONTENT
# hash (not mtime).  Re-parsing YAML on every wave on every cache-hash
# computation was ~3.7 ms/wave and dominated the L2 read gate; Wave
# objects are recreated on each ``country[wave]`` access so an
# instance-level cache wouldn't persist.  Keying on the content hash
# keeps this correct under edits (an edit changes the hash -> cache miss
# -> reparse) while making repeat reads ~free.
_DATA_INFO_CACHE: dict[str, dict[str, Any]] = {}

# ``.org`` files in a country/wave ``_/`` dir that are build-time inputs
# (food_items.org, categorical_mapping.org, unit_labels.org, ...) belong
# in the cache hash: a script reads them at build time and bakes the
# result into the parquet, so an edit must invalidate.  We hash *all*
# ``*.org`` in the relevant ``_/`` dir except the ones below, which are
# pure documentation never read by a build step -- hashing them would
# cause a spurious full-country rebuild on a docs edit.
_ORG_HASH_SKIP = {"CONTENTS.org"}
# Build-input file suffixes content-hashed from a country/wave _/ dir (helper
# module bodies + data/conversion tables).  .org is handled separately (skip
# list above).  Build OUTPUTS go to the cache (data_root()), not _/, so every
# matching _/ file is a committed build input (GH #522, rounds 7-8).
_BUILD_INPUT_SUFFIXES = {".py", ".csv", ".json", ".txt", ".tab", ".tsv"}


def _parse_data_info_cached(path: Path, content_hash: str | None) -> dict[str, Any]:
    """Parse a ``data_info.yml`` once per distinct content hash."""
    if content_hash is None:
        return {}
    cached = _DATA_INFO_CACHE.get(content_hash)
    if cached is not None:
        return cached
    try:
        with open(path, "r") as fh:
            parsed = load_yaml(fh)
    except (OSError, yaml.YAMLError):
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    _DATA_INFO_CACHE[content_hash] = parsed
    return parsed


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
        return countries_root() / self.folder

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
        # A wave with no ``_/`` directory simply declares no tables
        # (e.g. a partially-wired survey where a wave is auto-discovered
        # from ``Documentation/SOURCE.org`` but not yet wired -- and the
        # empty ``_/`` dir is absent on a fresh checkout since git does
        # not track empty directories).  Treat as empty rather than
        # raising FileNotFoundError, which otherwise poisons every
        # Country-level ``load_from_waves`` call (GH #274).
        _wave_dir = self.file_path / "_"
        if not _wave_dir.is_dir():
            # GH #329: a missing _/ is tolerated (an auto-discovered wave not
            # yet wired legitimately has an empty, git-untracked _/), but it is
            # indistinguishable from a BROKEN checkout (failed dvc pull, sparse
            # / shallow clone, interrupted git checkout) where a wave that
            # should contribute data silently contributes nothing -- a
            # quiet-wrong-answer (fewer waves than expected, no error) that is
            # worse than a loud one for a completeness-sensitive data library.
            # Surface it via logger.warning so the absence is at least visible
            # when debugging an incomplete table, without spamming a
            # UserWarning for every legitimately-unwired stub on every call.
            # (Marking intentional stubs to silence even this is a deferred
            # refinement; see GH #329.)
            logger.warning(
                "no _/ directory for wave %s -- this wave declares no tables. "
                "If this wave is expected to be wired, check for a partial "
                "checkout or a failed dvc pull (GH #329).",
                self.file_path,
            )
            return []
        wave_data = [f.stem for f in _wave_dir.iterdir() if f.suffix == '.py' and f.stem not in [f'{self.wave_folder}']]
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

    def _input_hash(self, table: str) -> str | None:
        """Content hash of every input that determines this wave's
        contribution to *table*'s harmonized parquet.

        Used by the L2 cache layer (v0.8.0) to detect stale wave-level
        parquets.  Returns ``None`` when the wave has no ``_/`` directory
        (nothing to verify -> caller preserves "read if present").

        Inputs hashed (all small files; the multi-MB ``.dta`` is never
        read -- its DVC sidecar md5 stands in via
        :func:`source_fingerprint`):

        - ``LSMS_CACHE_SCHEMA`` (library-wide extraction-logic version);
        - the wave's ``data_info.yml`` (column maps / merges / derived);
        - the wave-module formatting functions
          (``{wave_folder}.py``, ``mapping.py``);
        - YAML-path: the DVC fingerprint of each declared source file;
        - script-path: the ``_/{table}.py`` text plus the fingerprints of
          any data files it references as string literals.

        Deliberately excluded: post-read transforms (kinship, spellings,
        categorical mappings, ``_join_v_from_sample``) -- they re-run on
        every read and never touch the cached parquet.
        """
        wave_dir = self.file_path / "_"
        if not wave_dir.is_dir():
            return None
        di_hash = cached_file_hash(wave_dir / "data_info.yml")
        parts = [
            f"schema={LSMS_CACHE_SCHEMA}",
            f"wave={self.year}",
            f"table={table}",
            "data_info=" + (di_hash or "none"),
        ]
        for mod in (f"{self.wave_folder}.py", "mapping.py"):
            parts.append(f"mod:{mod}=" + (cached_file_hash(wave_dir / mod) or "none"))
        # Build-time .org inputs in this wave's _/ (e.g. wave-local
        # food_items.org / mapping tables).  CONTENTS.org and friends are
        # skipped (docs, never read by a build step).
        for org in sorted(wave_dir.glob("*.org")):
            if org.name in _ORG_HASH_SKIP:
                continue
            parts.append(f"org:{org.name}=" + (cached_file_hash(org) or "none"))

        data_info = _parse_data_info_cached(wave_dir / "data_info.yml", di_hash)
        block = data_info.get(table) if isinstance(data_info, dict) else None
        if block:
            # YAML path: resolve each declared file exactly as
            # Wave.grab_data does (``file_path / "Data" / fn`` then
            # normpath), which handles both bare names (``foo.dta``) and
            # ``../Data/foo.dta``-style entries.
            for fn in sorted(set(_collect_file_paths_from_block(block))):
                data_path = Path(os.path.normpath(self.file_path / "Data" / fn))
                parts.append("src:" + source_fingerprint(data_path))
        else:
            # Script path: the script text is the primary input; literal
            # data refs are folded in best-effort (see CRITICAL-1 in the
            # design doc for the residual dynamic-path gap).
            script = wave_dir / f"{table}.py"
            parts.append(f"script:{table}.py=" + (cached_file_hash(script) or "none"))
            for ref in sorted(set(scan_script_data_refs(script))):
                data_path = Path(os.path.normpath(self.file_path / "Data" / ref))
                parts.append("sref:" + source_fingerprint(data_path))

        # Every build module in the wave + country _/ dirs contributes to the
        # parquet via DYNAMIC dispatch (out-of-process scripts, df_edit hooks,
        # spec_from_file_location LOCAL helpers like Nigeria _age_helpers.py).
        # Two layers (GH #522): (1) file-hash each module's OWN body -- so e.g.
        # _age_helpers.py's _clean_year/apply_age_handler is versioned; (2) fold
        # the lsms_library-import CLOSURES those modules reach (age_handler,
        # conversion_table_matching_global, ...).  Best-effort; never raises.
        try:
            cdir = self.country.file_path / "_"
            wave_pys = tuple(sorted(str(p) for p in wave_dir.glob("*.py"))) if wave_dir.is_dir() else ()
            country_pys = tuple(sorted(str(p) for p in cdir.glob("*.py"))) if cdir.is_dir() else ()
            # (1) content-hash EVERY build INPUT in the wave + country _/ dirs --
            # .py helper bodies AND .csv/.json/.txt conversion tables (e.g. Malawi
            # ihs3_conversions.csv -> cfactor -> Quantity_kg, baked and not
            # re-applied on read) -- keyed by filename -> machine-independent
            # (GH #522, rounds 7-8).  Outputs go to the cache, not _/, so every
            # such file is a committed build input.
            for d in (wave_dir, self.country.file_path / "_"):
                if not d.is_dir():
                    continue
                for p in sorted(d.glob("*")):
                    if p.suffix.lower() in _BUILD_INPUT_SUFFIXES:
                        parts.append(f"binp:{p.name}=" + (cached_file_hash(p) or "none"))
            # (2) import-closure fold; two calls so the country tuple memoises
            # ONCE across waves (a combined tuple differs per wave).
            fw_w = framework_imports_fingerprint(wave_pys)
            fw_c = framework_imports_fingerprint(country_pys)
            if fw_w or fw_c:
                parts.append(f"fwimp={fw_w}:{fw_c}")
        except Exception:
            pass

        # Build-path framework transform CODE (GH #522 / cache step 2): version
        # the @build_transform closure relevant to this table, so editing e.g.
        # apply_derived / food_acquired_to_canonical invalidates the L2-wave
        # parquet.  Degrades to "none" (closure unversioned -> read-if-present),
        # never raises out of the hash.  See lsms_library/_build_registry.py.
        try:
            parts.append("btf=" + build_transforms_fingerprint(table))
        except Exception:
            parts.append("btf=none")

        return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()

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

        # A function whose name matches a declared data_scheme table is that
        # table's df_edit hook (dispatched per-table as
        # ``final_mapping['df_edit']`` below), NOT a per-cell column formatter.
        # Guard against a myvar that happens to share a name with such a table
        # hook -- e.g. the legacy ``interview_date`` cover-page myvar inside an
        # EHCVM ``household_roster`` colliding with the country-module
        # ``interview_date(df)`` visit-melt hook.  Auto-binding the df-level
        # hook as a per-cell formatter applies it to a scalar string and raises
        # ``AttributeError`` (masked downstream as a misleading ``KeyError``),
        # breaking the roster build / silently dropping waves (GH #476).  Such
        # a myvar is treated as a plain column rename, exactly as it was before
        # the table hook existed; the table's own df_edit dispatch is
        # unaffected.
        declared_tables = set(
            ((self.country.resources or {}).get('Data Scheme') or {}).keys()
        )

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
            if var_name in formatting_functions and var_name not in declared_tables:
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
    def documentation_path(self) -> Path:
        """Path to ``<wave>/Documentation/``."""
        return self.file_path / "Documentation"

    @property
    def license(self) -> str:
        """Verbatim contents of ``<wave>/Documentation/LICENSE.org``.

        Whatever's in the file --- a URL, full terms text, or both ---
        is what the user gets.  No format-dependent fork.  Empty
        string if the file is missing (with a warning).
        """
        license_path = self.documentation_path / "LICENSE.org"
        if license_path.exists():
            with open(license_path, 'r') as file:
                return file.read()
        warnings.warn(f"License file not found: {license_path}")
        return ""

    @property
    def data_source(self) -> str:
        """Verbatim contents of ``<wave>/Documentation/SOURCE.org``.

        Typically a one-line catalog URL, but the API doesn't promise
        that --- whatever's in the file is what gets returned.  Empty
        string if the file is missing (with a warning).
        """
        source_path = self.documentation_path / "SOURCE.org"
        if source_path.exists():
            with open(source_path, 'r') as file:
                return file.read()
        warnings.warn(f"Data source not found: {source_path}")
        return ""

    @property
    def mapping(self) -> dict[str, Any]:
        return {**self.categorical_mapping, **self.formatting_functions}

    @build_transform()  # body is build-path: check_adding_t, >1e99 sentinel, dfs merge, map_index (#522)
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

        # --- L2-wave cache check (YAML-path only) ---
        # Script-path tables (data_info is None) already get L2-wave parquets
        # from their _/<table>.py scripts via local_tools.to_parquet().  The
        # YAML path returns in-memory DataFrames without writing, so every
        # per-wave iterator (e.g. Country._market_lookup) pays the full
        # df_data_grabber cost on every call.  Cache the extracted+formatted
        # wave frame the first time it's produced.  Stored pre-transform:
        # categorical mapping, kinship expansion, and _finalize_result all
        # run downstream on every read.  Staleness semantics match L2-country
        # (see CLAUDE.md "Cache Behavior"): no automatic invalidation; force
        # a rebuild with LSMS_NO_CACHE=1 or by deleting the parquet.
        cache_path = None
        if data_info:
            cache_path = (data_root(self.country.name) / self.year / '_'
                          / f'{request}.parquet')
            if cache_path.exists() and not os.environ.get('LSMS_NO_CACHE'):
                # v0.8.0 content-hash staleness for the YAML-path L2-wave
                # parquet (same classification as L2-country; see
                # load_dataframe_with_dvc).  Stale -> fall through and
                # re-extract from source below.
                expected_wave_hash = self._input_hash(request)
                if cache_freshness(cache_path, expected_wave_hash) != "stale":
                    try:
                        df_cached = pd.read_parquet(cache_path)
                    except (OSError, ArrowInvalid):
                        df_cached = None
                    if df_cached is not None:
                        if (expected_wave_hash is not None
                                and read_parquet_cache_hash(cache_path) is None):
                            stamp_parquet_hash(cache_path, expected_wave_hash)
                        return df_cached

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
                    except (FileNotFoundError, PathMissingError, DvcException,
                            KeyError) as exc:
                        if idx == 0:
                            # The primary sub-df is required; re-raise
                            raise
                        # Secondary sub-dfs (e.g. geo files) are optional.
                        # ``KeyError`` covers the case where the source file
                        # exists but lacks the requested columns -- e.g. a
                        # df_geo whose YAML asks for ``lat_dd_mod``/``lon_dd_mod``
                        # that the file doesn't carry (or carries under a
                        # different casing/name).  df_data_grabber raises
                        # ``KeyError`` for a missing column when ``missing_ok``
                        # is False, and a single-file sub-df gets
                        # ``missing_ok=False``.  Treat it like a missing file:
                        # drop the optional sub-df rather than abort the whole
                        # cluster_features table and lose the Region/District
                        # that the primary df_main provides.  GH #515.
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
                # Apply any `derived:` transformers declared in the YAML.  These
                # run on the merged-and-indexed frame, before the per-request
                # Python hook, so they see the full multi-source result and
                # anything the hook does afterwards builds on the derived
                # columns.  See transformations.apply_derived for the contract.
                derived_spec = data_info.get('derived')
                if derived_spec:
                    from .transformations import apply_derived
                    df = apply_derived(df, derived_spec)
                # `drop:` removes the temporary columns that existed only to
                # feed the `derived:` transformers.  Listed columns that aren't
                # present are silently skipped so YAML authors don't have to
                # keep the list in lock-step with conditional transformers.
                drop_cols = data_info.get('drop') or []
                if drop_cols:
                    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
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

            # Check if the parquet already exists before invoking Make.
            # v0.8.0: skip a candidate whose embedded hash is STALE so a
            # script/source edit forces Make to rebuild it (closes the
            # "stale L2-wave parquet shadows a source-script fix" gap
            # documented in CLAUDE.md "Cache Behavior").
            expected_wave_hash = self._input_hash(request)
            parquet_fn = None
            for candidate in [external_parquet, intree_parquet]:
                if candidate.exists():
                    if cache_freshness(candidate, expected_wave_hash) == "stale":
                        logger.debug(
                            f"v0.8.0 wave cache STALE: {request} at {candidate}; "
                            f"will rebuild via Make"
                        )
                        continue
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
                # Mirror ``run_make_target`` (country.py ~line 1856): include
                # ``_make_jobs_flag()`` so this wave-level legacy fallback can
                # exploit the cores it was given.  Without this, every
                # script-path table without a YAML data_info entry built
                # serially regardless of cpu_count() -- the source of the
                # cold-build slowness observed when calling derived features
                # (e.g. Uganda's food_expenditures, which routes here per
                # wave for food_acquired).
                make_cmd = ["make", "-s"]
                jobs_flag = _make_jobs_flag()
                if jobs_flag:
                    make_cmd.append(jobs_flag)
                make_cmd.append('../' + str(relative_parquet_path))
                subprocess.run(make_cmd, cwd=cwd_path, check=True, env=env)
                logger.info(f"Makefile executed successfully for {self.name}. Rechecking for parquet file...")

                for candidate in [external_parquet, intree_parquet]:
                    if candidate.exists():
                        parquet_fn = candidate
                        break

            if parquet_fn is None:
                logger.warning(f"Parquet file for {request} still missing after running Makefile.")
                return pd.DataFrame()

            df = pd.read_parquet(parquet_fn)
            # NOTE: we deliberately do NOT trust-once-stamp script-path
            # wave parquets here.  They are written hashless by the
            # _/<table>.py scripts, so stamping the current hash onto a
            # parquet we merely *read* would (a) mask a real staleness on
            # the first edit and (b) bump the file mtime past the edited
            # source, defeating Make's timestamp rebuild (v0.8.0
            # CRITICAL-2).  Script-path staleness is instead governed by
            # the L2-country hash gate, which evicts hashless wave
            # parquets (see Country._evict_hashless_wave_caches) so this
            # branch re-runs Make on the next stale read.

        if isinstance(df, pd.DataFrame):
            df = map_index(df)

        df = check_adding_t(df)
        df = df[df.index.get_level_values('t') == self.year]

        # --- L2-wave cache write (YAML-path only) ---
        # See the matching read at the top of this method for rationale.
        # Write failures warn but don't propagate — the caller gets a
        # correct DataFrame either way.
        if (cache_path is not None
                and not os.environ.get('LSMS_NO_CACHE')
                and isinstance(df, pd.DataFrame)
                and not df.empty):
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                df = to_parquet(df, cache_path, absolute_path=True,
                                cache_hash=self._input_hash(request))
            except (OSError, ValueError, ArrowInvalid) as exc:
                # Disk full / permission denied / parquet serialization
                # failure -> warn and continue with the in-memory df.
                # Programmer bugs propagate.
                warnings.warn(
                    f'L2-wave cache write failed for '
                    f'{self.country.name}/{self.year}/{request}: {exc}'
                )

        return df

    # This cluster_features method is explicitly defined because additional processing is required after calling grab_data.
    def cluster_features(self) -> pd.DataFrame:
        df = self.grab_data('cluster_features')
        # Some countries declare ``i: <HHID>`` in cluster_features
        # ``idxvars`` so the YAML can merge a household-level df_geo
        # for GPS.  The result then has HH grain (one row per
        # household) instead of the canonical ``(t, v)`` cluster
        # grain documented in ``data_scheme.yml``, so it has to be
        # projected back onto ``(t, v)``.  GH #161.
        #
        # GH #323 SITE 2.  That projection used to be justified by a
        # PROSE comment -- "Region/Rural/District are invariant within
        # a cluster by construction of the LSMS-ISA sampling design" --
        # and prose is not enforcement.  The claim is FALSE wherever a
        # cluster code is unique only *within* a district: two real
        # clusters then merge, and ``.first()`` keeps one district's
        # Region and silently discards the other's.  That is WRONG data,
        # not merely lost data.  The invariant is now CHECKED rather than
        # asserted -- see ``_collapse_to_cluster_grain``.
        if 'i' in df.index.names:
            keep_levels = [lvl for lvl in df.index.names if lvl != 'i']
            df = _collapse_to_cluster_grain(
                df, keep_levels,
                country=getattr(self.country, 'name', None), wave=self.year,
            )
        # ``i`` may also leak in as a *column* when df_geo's idxvars
        # include it (the merge step turns the duplicate into a column).
        # Drop it: cluster_features is keyed on ``(t, v)``, the
        # household identifier has no place in the result.  GH #161.
        #
        # GH #323: NOTE WHAT THIS LINE ACTUALLY DOES.  When ``i`` is a COLUMN the
        # frame is still HOUSEHOLD grain -- it just doesn't look like it, because
        # the index is already ``(t, v)`` with one duplicate tuple per household.
        # Dropping ``i`` LAUNDERS that: an honest household-grain table becomes a
        # cluster-grain table with unexplained duplicates, which
        # ``_normalize_dataframe_index`` (Site 1) then collapses.  So this is
        # Site 2's twin, and the ONLY reason it is not silent is that Site 1's
        # audit happens to catch the wreckage downstream (Uganda's seven
        # ``i``-as-column waves: 9,890 rows destroyed, 934 deleted on a NaN key --
        # all of it reported by #614, none of it by the audit above).
        #
        # It is deliberately NOT rerouted through ``_collapse_to_cluster_grain``
        # here, because it does not need to be: Site 1 reduces these frames with the
        # same ``.first()`` and audits them with the same instrument, so the loss is
        # reported either way.  (Before the GPS ``.mean()`` was retired this
        # asymmetry also silently decided whether a country's cluster coordinates
        # came out a CENTROID or ONE HOUSEHOLD'S FIX -- on nothing more principled
        # than whether its YAML put ``i`` in ``idxvars`` or left it in a merge
        # block.  That incoherence was one of the arguments that retired the
        # ``.mean()``; with core no longer aggregating anywhere, both paths now
        # agree.)
        if 'i' in df.columns:
            df = df.drop(columns='i')
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

        .. note::
           This is the one escape that *also* skips the v0.8.0 content-hash
           staleness check — it is the explicit "I promise the cache
           matches current sources" mode.  With the hash check now the
           default, ``assume_cache_fresh`` is strictly weaker than the
           default path and is a candidate for deprecation (see
           SkunkWorks/dvc_object_management.org "Rethink trust_cache").
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
        countries_dir = countries_root()
        country_dir = (countries_dir / country_name).resolve()
        if not country_dir.is_relative_to(countries_dir):
            raise ValueError(f"Invalid country name {country_name!r}: path traversal not allowed")
        if not country_dir.is_dir():
            warnings.warn(f"Country directory not found for {country_name!r}")
        self.name = country_name
        self._panel_ids_cache = None
        self._updated_ids_cache = None
        # Tristate guard: distinguishes "never tried" from "tried and got
        # nothing".  Without this, _compute_panel_ids would re-fire (and
        # re-emit warnings) on every property access for countries that
        # don't declare panel_ids in data_scheme.yml.
        self._panel_ids_attempted = False
        self.wave_folder_map = {}
        if trust_cache:
            warnings.warn(
                "trust_cache is deprecated and will be removed in a future "
                "release. Use assume_cache_fresh=True instead.",
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
        return countries_root() / self.name

    @property
    def resources(self) -> dict[str, Any]:
        if '_resources_cache' not in self.__dict__:
            var = self.file_path / "_" / "data_scheme.yml"
            if var.exists():
                with open(var) as f:
                    self.__dict__['_resources_cache'] = load_yaml(f)
            else:
                self.__dict__['_resources_cache'] = {}
        return self.__dict__['_resources_cache']

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
        Also merges global .org files from lsms_library/categorical_mapping/ (GH #168).
        Global tables are loaded first; per-country tables override on name
        collision -- except names in ``_ADDITIVE_CATEGORICAL_TABLES`` (e.g.
        ``u``), which are row-unioned so a country inherits global rows and
        overrides only the keys it redeclares (DESIGN_u_consolidation).
        '''
        if '_categorical_mapping_cache' not in self.__dict__:
            # Load global mappings from lsms_library/categorical_mapping/*.org
            global_maps: dict[str, pd.DataFrame] = {}
            global_cm_dir = files("lsms_library") / "categorical_mapping"
            try:
                global_cm_path = Path(str(global_cm_dir))
                for org_file in sorted(global_cm_path.glob("*.org")):
                    try:
                        global_maps.update(all_dfs_from_orgfile(org_file))
                    except (ValueError, OSError, pd.errors.ParserError) as exc:
                        logger.info("Skipping global categorical mapping %s: %s", org_file.name, exc)
            except (FileNotFoundError, OSError) as exc:
                logger.info("Global categorical_mapping directory not accessible: %s", exc)

            # Load per-country mappings (override global on name collision)
            country_maps: dict[str, pd.DataFrame] = {}
            for rel in ['./', '../']:
                org_fn = Path(self.file_path / rel / "_" / "categorical_mapping.org")
                if org_fn.exists():
                    country_maps = all_dfs_from_orgfile(org_fn)
                    break

            self.__dict__['_categorical_mapping_cache'] = _merge_categorical_tables(
                global_maps, country_maps)
        return self.__dict__['_categorical_mapping_cache']

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

    def provenance(self) -> pd.DataFrame:
        """Tabular survey of source + license per wave.

        Reads ``<wave>/Documentation/SOURCE.org`` and
        ``<wave>/Documentation/LICENSE.org`` for every wave in
        :attr:`waves`.  Useful for AER-style data-editor reviews where
        the reviewer wants programmatic provenance verification
        rather than poking at the filesystem.

        Returns
        -------
        pandas.DataFrame
            Indexed by wave label ``t``.  Columns:

            - ``source`` — verbatim contents of ``SOURCE.org``,
              or ``pd.NA`` if the file is missing.  Typically a
              World Bank Microdata catalog URL.
            - ``license`` — verbatim contents of ``LICENSE.org``,
              or ``pd.NA``.  May be a URL, full terms text, or both,
              depending on what's recorded for that wave.
            - ``documentation_path`` — filesystem path to the wave's
              ``Documentation/`` directory.

        Reads silently --- no warnings for waves missing one or both
        files.  Use :attr:`Wave.license` / :attr:`Wave.data_source`
        directly if you want the warning side effect on miss.
        """
        rows = []
        for t in self.waves:
            wave = self[t]
            doc = wave.documentation_path
            src_path = doc / "SOURCE.org"
            lic_path = doc / "LICENSE.org"
            rows.append({
                't': t,
                'source': src_path.read_text() if src_path.exists() else pd.NA,
                'license': lic_path.read_text() if lic_path.exists() else pd.NA,
                'documentation_path': str(doc),
            })
        if not rows:
            return pd.DataFrame(
                columns=['source', 'license', 'documentation_path']
            ).rename_axis('t')
        return pd.DataFrame(rows).set_index('t')

    @property
    def data_scheme(self) -> list[str]:
        """List of data objects available for country.

        Includes derived tables (e.g. food_expenditures, household_characteristics)
        when their source table is present, even if they are not explicitly
        registered in data_scheme.yml.
        """
        data_info = self.resources
        data_list = list(data_info.get('Data Scheme', {}).keys()) if data_info else []
        # Surface derived tables when their source is present
        _derived_sources = {
            **{k: 'food_acquired' for k in self._FOOD_DERIVED},
            **{k: 'household_roster' for k in self._ROSTER_DERIVED},
        }
        for derived, source in _derived_sources.items():
            if source in data_list and derived not in data_list:
                data_list.append(derived)
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
            except (FileNotFoundError, KeyError, ValueError) as exc:
                # Surface as a warning rather than an info-level log so that
                # YAML / data-shape config bugs don't silently drop entire
                # waves from market-keyed lookups.  Pre-2026-05-05 this was a
                # logger.info that never reached users without explicit log
                # configuration; the silent drop hid the Malawi 2013-14 /
                # 2016-17 df_geo schema bug for years.
                warnings.warn(
                    f"_market_lookup: skipping cluster_features for "
                    f"{self.name}/{wave} ({type(exc).__name__}: {exc}). "
                    f"Rows from this wave will be absent from any "
                    f"{column!r} lookup; check that this wave's "
                    f"cluster_features YAML matches its source files.",
                    RuntimeWarning,
                    stacklevel=2,
                )
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
            except (FileNotFoundError, KeyError, ValueError, AttributeError) as exc:
                logger.info("sample() unavailable for v-join on %s: %s", self.name, exc)
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

        # Silent-skip guard (GH #256).  A wave whose roster `i` does not
        # intersect sample's `i` for the same `t` produces rows with NaN v
        # after the left-merge.  Downstream groupby() in
        # ``roster_to_characteristics`` and the food-derivation pipeline
        # drops NaN-keyed rows by default, silently swallowing the entire
        # wave.  Tajikistan/1999 pre-PR #253 was the worked example:
        # ``Country('Tajikistan').household_characteristics()`` returned
        # ``(10306, 15)`` -- 3 waves, with 1999 silently missing -- when
        # the roster declared ``i: hhid`` but sample declared
        # ``i: [pop_pt, hhid]``.  Warn here so the next such drift
        # surfaces immediately rather than via manual universe scan.
        if 't' in flat.columns and 'v' in flat.columns:
            nan_v_per_wave = flat.groupby('t')['v'].apply(
                lambda s: s.isna().mean() if len(s) else 0.0
            )
            fully_nan = sorted(
                str(t) for t, frac in nan_v_per_wave.items() if frac == 1.0
            )
            if fully_nan:
                warnings.warn(
                    f"{self.name}: waves {fully_nan} have 100% NaN v after "
                    f"_join_v_from_sample; these will be silently dropped "
                    f"from derived tables (household_characteristics, "
                    f"food_expenditures, food_prices, food_quantities) by "
                    f"the downstream groupby.  Check i-key compatibility "
                    f"between the roster source and the sample table -- "
                    f"compare wave-level data_info.yml's `idxvars.i` on "
                    f"each side.  See GH #256 for context and PRs #244 / "
                    f"#253 / #255 for the established fix pattern.",
                    UserWarning,
                    stacklevel=2,
                )

        # Normalize v to canonical pd.StringDtype.  sample() may return v as
        # int64, float64 (left-merge with NaN converts int→float), or plain
        # object; mixed dtypes across waves cause pyarrow serialisation failures
        # when the caller does df.to_parquet().  Enforce a uniform string dtype
        # here so Feature('housing')([...]) concatenation is always clean.
        # Fixes GH #142.
        if 'v' in flat.columns:
            def _v_to_str(x):
                if pd.isna(x) or x == '':
                    return pd.NA
                try:
                    return str(int(float(x)))
                except (ValueError, OverflowError):
                    return str(x).strip()
            flat['v'] = flat['v'].map(_v_to_str).astype(pd.StringDtype())

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
        # We preserve them across the v-join as a *performance* hint:
        # the surviving id_converted flag lets _finalize_result skip a
        # redundant id_walk pass.  Correctness no longer depends on
        # this — id_walk is idempotent by construction (see
        # _close_id_map in local_tools.py and tests/test_id_walk.py).
        # Historical context: commit 4db41a27 added this line as a
        # correctness fix for the Burkina Faso 2021-22 chain-collision
        # bug, before id_walk itself was made safe.
        result.attrs = dict(df.attrs)
        return result

    def _add_market_index(self, df: pd.DataFrame, column: str = 'Region') -> pd.DataFrame:
        """Join a market identifier ``m`` onto *df*, preferring the HH-level
        source.

        Looks ``column`` up first from ``sample()`` (HH-level, keyed on
        ``(i, t)``) and falls back to ``cluster_features()`` (cluster-level,
        keyed on ``(t, v)``) for rows where sample has no value.  Prior
        implementations had the opposite priority, which systematically
        reassigned HH whose own survey-reported region differed from the
        cluster's modal label — for Uganda that affected ~145 HH per wave
        relative to the retired ``other_features()`` path.

        ``cluster_features`` is retained as a fallback so countries whose
        ``sample()`` doesn't expose ``column`` still get a market label
        (via the historical cluster-level path).

        Returns *df* with ``m`` added and ``v`` removed from the index.
        """
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

        # --- primary: HH-level lookup from sample() on (i, t) ---
        flat['m'] = pd.NA
        if 'sample' in self.data_scheme:
            try:
                s = self.sample()
            except (OSError, KeyError, ValueError, AttributeError,
                    DvcException) as exc:
                # Sample-table missing / mis-shaped / DVC-fetch failure ->
                # fall back to wave-level region scan.  Programmer bugs
                # (TypeError, NameError) surface unchanged.
                logger.info(
                    "_add_market_index: could not load sample() for HH-level "
                    "lookup of column %r on %s: %s",
                    column, self.name, exc,
                )
                s = None
            if isinstance(s, pd.DataFrame) and column in s.columns:
                hh_lookup = s[column].rename('m_hh')
                if 'v' in hh_lookup.index.names:
                    hh_lookup = hh_lookup.droplevel('v')
                hh_lookup = hh_lookup[~hh_lookup.index.duplicated(keep='first')]
                flat = flat.merge(
                    hh_lookup, how='left', left_on=['i', 't'], right_index=True
                )
                # HH-level value takes priority; for rows where it's NA we
                # fall through to the cluster-level lookup below.
                flat['m'] = flat['m'].fillna(flat['m_hh'])
                flat = flat.drop(columns=['m_hh'])

        # --- fallback: cluster-level lookup on (t, v) for still-missing m ---
        missing = flat['m'].isna()
        if missing.any():
            # Ensure v is available for the cluster-level merge.
            if 'v' not in flat.columns:
                if 'sample' in self.data_scheme:
                    df_with_v = self._join_v_from_sample(df)
                    # ``drop_duplicates`` on (i, t) is load-bearing for tables
                    # whose canonical index has more levels than (i, t) --
                    # e.g. ``shocks`` (one row per (i, t, Shock)) or any
                    # future table with similar shape.  Without it, a tall
                    # input frame produces *k* identical (i, t, v) rows for
                    # a HH-period with *k* extra-level rows, and the merge
                    # below becomes a Cartesian product yielding *k^2* output
                    # rows per HH-period.  See GH #266 for the worked example
                    # (Uganda shocks: 14,457 rows -> 27,268 with 12,812 dup).
                    # The parallel logic in ``_location_lookup`` already
                    # carries the same dedup; this restores the symmetry.
                    v_lookup = (df_with_v.reset_index()[['i', 't', 'v']]
                                .drop_duplicates(subset=['i', 't']))
                    flat = flat.merge(v_lookup, on=['i', 't'], how='left')
                else:
                    warnings.warn(
                        f"_add_market_index: {missing.sum()} row(s) have NaN "
                        f"{column!r} at HH level and v is unavailable for "
                        f"cluster-level fallback on {self.name}"
                    )
            if 'v' in flat.columns:
                lookup = self._market_lookup(column).copy()
                flat['t'] = flat['t'].astype(str)
                flat['v'] = _normalize_v(flat['v'])
                lookup['t'] = lookup['t'].astype(str)
                lookup['v'] = _normalize_v(lookup['v'])
                lookup = lookup.rename(columns={'m': 'm_cluster'})
                flat = flat.merge(lookup, on=['t', 'v'], how='left')
                flat['m'] = flat['m'].fillna(flat['m_cluster'])
                flat = flat.drop(columns=['m_cluster'])

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
        result = flat.set_index(new_idx)
        result.attrs = dict(df.attrs)  # set_index drops attrs (pandas 2.x)
        return result

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
    

    def _apply_categorical_mappings(self, df: pd.DataFrame,
                                    protect_u_sentinels: bool = False) -> pd.DataFrame:
        """Auto-apply categorical mappings where table names match columns or indices.

        For each column or index level in *df*, check whether
        ``self.categorical_mapping`` contains a table with the same name
        (case-insensitive).  If the table has a ``Preferred Label``
        column, build a replacement dictionary from the first other
        column → ``Preferred Label`` and apply it.

        ``protect_u_sentinels`` (GH #361): when True, the country
        ``#+name: u`` table is not allowed to remap the framework's reserved
        ``u`` conversion sentinels (:data:`_RESERVED_U_SENTINELS` — ``'kg'``,
        ``'Value'``).  Set for *derived* food tables (food_quantities /
        food_prices), whose ``u`` level carries the lowercase ``'kg'`` tag
        produced by the kg conversion.  A country whose table happens to map
        e.g. ``kg → Kg`` (Burkina, to unify raw food_acquired spellings)
        would otherwise corrupt that sentinel, so cross-country
        ``xs('kg', level='u')`` silently misses it.  food_acquired itself
        passes ``protect_u_sentinels=False`` so its raw unit-label
        canonicalization is unaffected.
        """
        cat_maps = self.categorical_mapping
        if not cat_maps:
            return df

        # Build case-insensitive lookup
        lower_lookup = {name.lower(): name for name in cat_maps}

        def _build_replace_dict(table: pd.DataFrame, *,
                                drop_keys: set | None = None) -> dict | None:
            if "Preferred Label" not in table.columns:
                return None
            source_cols = [c for c in table.columns if c != "Preferred Label"]
            if not source_cols:
                return None
            rdict = table.set_index(source_cols[0])["Preferred Label"].to_dict()
            # Numeric-code tables (e.g. a `Code` column 1, 2, 3) don't match
            # data that arrives as float-strings ('1.0') -- the documented
            # "format_id is applied to idxvars but not myvars" gotcha, which
            # leaks raw unit codes into food_acquired's `u` level (GH #223
            # Layer 2).  Additively register int/float-string variants of
            # every integer-valued key so '1', '1.0' both resolve.  Purely
            # additive: existing keys win, non-numeric labels are untouched.
            rdict = _augment_numeric_code_keys(rdict)
            if drop_keys:
                # Never remap a reserved sentinel (e.g. the 'kg' conversion
                # tag) away from itself.
                rdict = {k: v for k, v in rdict.items() if k not in drop_keys}
            return rdict

        # Apply to columns
        for col in df.columns:
            key = lower_lookup.get(col.lower())
            if key is None:
                continue
            drop = _RESERVED_U_SENTINELS if (protect_u_sentinels and col == 'u') else None
            rdict = _build_replace_dict(cat_maps[key], drop_keys=drop)
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
                drop = (_RESERVED_U_SENTINELS
                        if (protect_u_sentinels and level_name == 'u') else None)
                rdict = _build_replace_dict(cat_maps[key], drop_keys=drop)
                if rdict:
                    df = df.rename(index=rdict, level=level_name)

        return df

    def _relabel_j(self, df: pd.DataFrame, labels: str | None, *, reaggregate: bool) -> pd.DataFrame:
        """Rename the ``j`` index level using a column of the country's food label table.

        ``labels='Preferred'`` (or ``None``) is a no-op.  For any other value
        ``X``, look for a column named ``'X Label'`` (falling back to ``'X'``)
        in ``food_items`` or ``harmonize_food`` under ``categorical_mapping``,
        keyed on ``'Preferred Label'`` (the current ``j`` values).
        """
        if labels in (None, 'Preferred'):
            return df
        if not isinstance(df, pd.DataFrame) or 'j' not in (df.index.names or []):
            raise KeyError(
                f"Cannot apply labels={labels!r}: result has no 'j' index level"
            )
        cat_maps = self.categorical_mapping or {}
        table = cat_maps.get('food_items')
        if table is None:
            table = cat_maps.get('harmonize_food')
        if table is None:
            # Country never curated a food-label table -> it cannot honour the
            # relabel.  LabelUnavailableError (a KeyError subclass) lets Feature
            # degrade gracefully while direct callers still catch KeyError.
            raise LabelUnavailableError(
                f"No food label table ('food_items' or 'harmonize_food') "
                f"on {self.name!r} for labels={labels!r}"
            )
        if 'Preferred Label' not in table.columns:
            # Malformed table (has a food-label table but no key column) -> a
            # genuine data defect, NOT a missing-curation case.  Plain KeyError
            # so it surfaces loudly rather than being degraded-over by Feature.
            raise KeyError(
                f"Food label table on {self.name!r} has no 'Preferred Label' column"
            )
        target = f"{labels} Label" if f"{labels} Label" in table.columns else labels
        if target not in table.columns:
            available = [c for c in table.columns if c != 'Preferred Label']
            # Country curates a food-label table but not THIS label column
            # (e.g. labels='Aggregate' against an EHCVM 'Original Label'-only
            # table) -> missing curation, degradable by Feature.
            raise LabelUnavailableError(
                f"Column {target!r} not in food label table on {self.name!r}; "
                f"available: {available}"
            )
        rdict = (table[['Preferred Label', target]]
                 .dropna()
                 .set_index('Preferred Label')[target]
                 .to_dict())
        result = df.rename(index=rdict, level='j')
        if reaggregate:
            numeric = result.select_dtypes(include='number')
            if not numeric.empty:
                result = numeric.groupby(list(result.index.names), dropna=False).sum(min_count=1)
        # Carry attrs across the rename / groupby: a performance hint
        # so id_converted survives and _finalize_result can skip a
        # redundant id_walk.  Idempotence in id_walk makes this
        # non-load-bearing for correctness; see _join_v_from_sample
        # above and tests/test_id_walk.py.
        result.attrs = dict(df.attrs)
        return result

    def _finalize_result(self, df: Any, scheme_entry: dict[str, Any], method_name: str,
                         currency: str | None = None) -> pd.DataFrame | dict[str, Any]:
        """
        Apply final harmonization steps (index augmentation, normalization, id walk)
        before returning a dataset to callers.

        ``currency`` (``'index'`` / ``'column'`` / ``None``) optionally attaches
        the ISO 4217 currency label to monetary tables; see
        :func:`lsms_library.currency.attach_currency`.  Applied last so it sits
        on the fully-finalized frame and is preserved by the downstream
        ``_relabel_j`` / ``_add_market_index`` steps in the caller.
        """
        if isinstance(df, dict):
            return df

        if isinstance(df, pd.DataFrame):
            df = self._augment_index_from_related_tables(df, scheme_entry, None)
            df = _normalize_dataframe_index(df, scheme_entry, None, method_name,
                                            country=self.name)

            # Join v from sample() for household-level tables that lack it.
            # Skip if v is already in the index OR already present as a
            # column (a legacy script may have written v alongside other
            # columns; joining again would create v_x/v_y name conflicts).
            current_names = list(df.index.names) if isinstance(df.index, pd.MultiIndex) else [df.index.name]
            v_already_present = ('v' in current_names
                                 or (isinstance(df, pd.DataFrame) and 'v' in df.columns))
            # Tables whose canonical index does NOT include v; joining v from
            # sample() would produce a non-canonical shape.  The set is declared
            # in lsms_library/data_info.yml ("Join v from sample" + index_info),
            # NOT hardcoded here (GH #436), so adding such a feature needs no
            # framework-code edit.  See _no_v_join_tables() below.
            if (not v_already_present
                    and 'i' in current_names
                    and 't' in current_names
                    and method_name not in _no_v_join_tables()
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
            # a column or index name (issue #49).  For derived food tables,
            # protect the reserved 'kg'/'Value' u-sentinels from a country's
            # #+name: u remap (GH #361).
            df = self._apply_categorical_mappings(
                df,
                protect_u_sentinels=method_name in _U_SENTINEL_PROTECTED_METHODS,
            )

            # Apply ``harmonize_<method_name>`` mapping to the ``j`` index
            # level when such a categorical_mapping table exists (GH #180,
            # #168).  Runs after :meth:`_apply_categorical_mappings` because
            # the table name (``harmonize_assets``, ``harmonize_food``, …)
            # does not match the index level name (``j``) so the auto-dispatch
            # above cannot fire.  Generalises the original assets-only hook
            # so future ``harmonize_education``, ``harmonize_housing``, etc.
            # tables auto-apply by convention.
            if (method_name
                    and isinstance(df.index, pd.MultiIndex)
                    and 'j' in df.index.names):
                ha_table = self.categorical_mapping.get(f'harmonize_{method_name}')
                if ha_table is not None and 'Preferred Label' in ha_table.columns:
                    src_cols = [c for c in ha_table.columns if c != 'Preferred Label']
                    if src_cols:
                        rdict = ha_table.set_index(src_cols[0])['Preferred Label'].to_dict()
                        df = df.rename(index=rdict, level='j')

            # Normalise variant spellings to canonical forms
            if method_name:
                df = _enforce_canonical_spellings(df, method_name)

            # Enforce rejected column-name spellings (e.g. Effected→Affected)
            df = _enforce_rejected_column_spellings(df)

            # Enforce declared dtypes from data_scheme.yml
            if isinstance(scheme_entry, dict):
                _enforce_declared_dtypes(df, scheme_entry)

            # Enforce canonical dtypes from data_info.yml (wins over country-level
            # declarations; e.g. Albania's Age: float → Int64 per canonical schema)
            if method_name:
                _enforce_canonical_dtypes(df, method_name)

            # Defensive: drop any row where every non-index column is NaN.
            # Wave-level scripts can leak hollow rows (e.g. survey questionnaire
            # entries where every food/quantity/expenditure field is missing
            # but a unit code happened to be filled in); plain dropna(how='all')
            # only fires when literally every column is NaN, which is rarely
            # the case once unit/cfactor columns enter the picture.  Wave
            # scripts that need the stricter "drop unless any DATA column is
            # non-null" form should use ``subset=`` themselves; this step is
            # the universal safety-net.
            #
            # GH #645: say how many rows it took.  This step is where a *value*
            # corruption upstream turns into a silent *deletion* -- a nulled
            # `Educational Attainment` is a deleted person, because the table
            # has exactly one column.  30% of Guatemala's individual_education
            # vanished here and nothing said a word (the cell was still graded
            # `sane`).  INFO, not a warning: legitimately-hollow rows are
            # common and this must not cry wolf on every table.  Whether it
            # should escalate above some rate is a judgement for the
            # maintainer, deliberately not made here.
            n_before = len(df)
            df = df.dropna(how='all')
            if len(df) < n_before and n_before:
                n_dropped = n_before - len(df)
                logger.info(
                    "%s/%s: dropna(how='all') removed %d of %d rows (%.1f%%) "
                    "-- every dropped row had NO non-index data at all",
                    self.name, method_name or "?", n_dropped, n_before,
                    100.0 * n_dropped / n_before)

            # Attach the ISO 4217 currency label (last, so it rides through the
            # caller's _relabel_j / _add_market_index without being dropped).
            if currency is not None and method_name and not df.empty:
                df = attach_currency(df, self.name, method_name, mode=currency)

            # Record the country so a later standalone convert() can recover it
            # (Feature output instead carries `country` in the index).
            df.attrs['country'] = self.name

        return df

    def _table_cache_hash(self, method_name: str, waves: list[str]) -> str | None:
        """Composite content hash for the L2-country parquet of
        *method_name* across *waves* (v0.8.0 cache invalidation).

        Composed from each wave's :meth:`Wave._input_hash`, so editing a
        single wave's source / config / script busts the country hash and
        triggers a rebuild that re-reads only the changed wave's L2-wave
        parquet.  Returns ``None`` (``unverifiable`` -> read-if-present)
        when no wave can produce an input hash, so tables the scheme
        can't introspect never regress the v0.7.0 fast path.
        """
        try:
            wave_hashes: list[str] = []
            any_hash = False
            for w in sorted(waves):
                try:
                    wh = self[w]._input_hash(method_name)
                except (KeyError, AttributeError, OSError):
                    wh = None
                wave_hashes.append(f"{w}={wh or 'none'}")
                if wh is not None:
                    any_hash = True
            # Country-level inputs that wave hashes miss: the country-level
            # concatenator script ``{country}/_/{table}.py`` (e.g.
            # Uganda/_/food_acquired.py), the country module
            # ``{country}/_/{name.lower()}.py`` (e.g. uganda.py, whose
            # helpers the wave scripts import), and the country mapping.
            # Without these, editing the concatenator or the country
            # module leaves the hash unchanged and serves stale data --
            # exactly the food_acquired_to_canonical class of bug.
            cdir = self.file_path / "_"
            country_parts = [
                "ctbl=" + (cached_file_hash(cdir / f"{method_name}.py") or "none"),
                "cmod=" + (cached_file_hash(cdir / f"{self.name.lower()}.py") or "none"),
                "cmap=" + (cached_file_hash(cdir / "mapping.py") or "none"),
                # data_scheme.yml declares the table registry and the
                # `materialize: make` flags (which decide YAML-path vs
                # script-path builds) -- build-relevant config, so an edit
                # must invalidate.  Since #436/#455 it also carries
                # per-feature `join_v` / derived declarations; those are
                # read-time transforms, so hashing the whole file
                # over-invalidates harmlessly on such edits (same coarse
                # whole-file approach as data_info.yml).
                "cscheme=" + (cached_file_hash(cdir / "data_scheme.yml") or "none"),
                # The Makefile defines how every script-path (`!make`) table
                # in this country is built, so an edit to it must invalidate.
                # Country-level (one per country); affects all the country's
                # tables (YAML-path included -- a harmless over-invalidation,
                # since the Makefile is edited rarely).
                "cmake=" + (cached_file_hash(cdir / "Makefile") or "none"),
            ]
            # Content-hash EVERY country-level build INPUT (not just ctbl/cmod/cmap):
            # .py helper bodies (e.g. _age_helpers.py) AND .csv/.json/.txt data
            # tables -- so a LOCAL helper or conversion table is versioned
            # (GH #522, rounds 7-8).
            for p in sorted(cdir.glob("*")):
                if p.suffix.lower() in _BUILD_INPUT_SUFFIXES:
                    country_parts.append(f"cbinp:{p.name}=" + (cached_file_hash(p) or "none"))
            # Country-level build-time .org inputs (food_items.org,
            # categorical_mapping.org, unit_labels.org, ...), CONTENTS.org
            # excluded.  These are read by the country-level concatenator /
            # harmonization helpers at build time.
            for org in sorted(cdir.glob("*.org")):
                if org.name in _ORG_HASH_SKIP:
                    continue
                country_parts.append(f"corg:{org.name}=" + (cached_file_hash(org) or "none"))
            if any(not p.endswith("=none") for p in country_parts):
                any_hash = True
            if not any_hash:
                return None
            # Build-path framework transform CODE (GH #522 / cache step 2):
            # folded in AFTER the any_hash gate so it never makes an otherwise
            # unintrospectable table falsely "verifiable"; degrades to "none".
            try:
                btf = build_transforms_fingerprint(method_name)
            except Exception:
                btf = "none"
            # Country-level build modules run framework helpers via dynamic
            # dispatch (concatenator scripts, df_edit hooks in the country
            # module).  Fold the lsms_library-import closures of every build
            # module in the country _/ dir (GH #522, both routes).  Per-wave
            # script tables also get this via each wave's _input_hash.
            try:
                country_pys = tuple(sorted(str(p) for p in cdir.glob("*.py"))) if cdir.is_dir() else ()
                fwimp = framework_imports_fingerprint(country_pys)
            except Exception:
                fwimp = ""
            payload = (f"schema={LSMS_CACHE_SCHEMA}\x1ftable={method_name}\x1f"
                       + f"btf={btf}\x1f"
                       + (f"fwimp={fwimp}\x1f" if fwimp else "")
                       + "\x1f".join(country_parts) + "\x1f"
                       + "\x1f".join(wave_hashes))
            return hashlib.sha256(payload.encode()).hexdigest()
        except Exception:
            return None

    def _evict_hashless_wave_caches(self, method_name: str) -> None:
        """Delete L2-wave parquets for *method_name* that carry NO embedded
        hash (i.e. written by a ``_/{table}.py`` script, which can't
        self-invalidate).

        Called when the L2-country hash is STALE.  Script-path wave
        parquets are hashless, so trusting them on the rebuild descent
        would re-serve stale data (and a read-time stamp would mask it
        permanently -- the v0.8.0 CRITICAL-2 bug).  Deleting them forces
        the descent's ``grab_data`` to re-run Make/the script.  Stamped
        (YAML-path) wave parquets are LEFT ALONE: they self-invalidate
        per-wave via their own hash gate, so deleting them would discard
        the per-wave granularity for unchanged waves.
        """
        country_root = data_root(self.name)
        if not country_root.exists():
            return
        # Glob matches round-name dirs too (Nigeria 2012Q3/, 2013Q1/),
        # mirroring clear_cache's discovery.
        for p in country_root.glob(f"*/_/{method_name}.parquet"):
            if p.parent.parent.name in ("_", "var"):
                continue
            try:
                if read_parquet_cache_hash(p) is None:
                    p.unlink()
                    logger.debug(f"v0.8.0 evicted hashless stale wave cache: {p}")
            except OSError:
                pass

    def _assert_built_required_columns(self, df: Any, method_name: str,
                                       scheme_entry: Any,
                                       is_script_path: bool) -> None:
        """Fail loudly if a script-path table is missing a required declared
        column -- the signature of a stale hashless wave parquet shadowing a
        wave-script fix (residual cache hazard, GH #479).

        Script-path wave parquets are hashless and historically could silently
        union a divergent per-era schema (e.g. GhanaLSS ``food_acquired`` served
        without its declared ``Quantity`` column).  This converts that silent
        wrong-data outcome into a loud, actionable error.

        Scope and safety:
        - Only *script-path* tables are checked (built by a ``_/{table}.py``
          wave script / country concatenator or ``materialize: make``); pure
          YAML-path tables self-invalidate per-wave and are skipped.
        - Called POST-``_finalize_result`` (see the call site), so kinship-
          decomposed columns (``Generation``/``Distance``/``Affinity``, added
          from ``Relationship`` by ``_expand_kinship``) and the joined ``v``
          level are already present -- no false positives from finalize-derived
          fields.
        - ``optional: true`` columns are exempt (genuinely-unavailable data).
        A correct build always emits every required declared column -- the same
        contract ``test_declared_columns_present`` enforces -- so this never
        breaks a healthy build; it only fires on a malformed (stale-cache) one.
        """
        if not is_script_path or not isinstance(df, pd.DataFrame):
            return
        if not isinstance(scheme_entry, dict):
            return
        # Parse declared (non-optional) columns exactly as diagnostics /
        # test_declared_columns_present do (skip index/materialize/etc. keys).
        _skip = {"index", "materialize", "backend", "aggregation"}
        required = [
            k for k, v in scheme_entry.items()
            if isinstance(k, str) and k not in _skip
            and not (isinstance(v, dict) and v.get("optional"))
        ]
        present = set(map(str, df.columns)) | set(
            n for n in df.index.names if n is not None
        )
        missing = [c for c in required if c not in present]
        if missing:
            raise RuntimeError(
                f"{self.name}/{method_name}: freshly-built table is missing "
                f"required declared column(s) {missing} (built columns: "
                f"{sorted(map(str, df.columns))}). For a script-path table "
                f"this almost always means a STALE script-written wave "
                f"parquet shadowed a wave-script fix (residual cache hazard, "
                f"GH #479) -- run `lsms-library cache clear --country "
                f"{self.name}` and rebuild. If the column is genuinely "
                f"unavailable for this country, mark it `optional: true` in "
                f"{self.name}/_/data_scheme.yml."
            )

    @build_transform()  # orchestrator: nested safe_concat_dataframe_dict / load_from_waves bake cross-wave
                        # alignment+concat into the parquet, not re-applied on read (#522, round-6)
    def _aggregate_wave_data(self, waves: list[str] | None = None, method_name: str | None = None,
                             currency: str | None = None) -> pd.DataFrame | dict[str, Any]:
        """Aggregates data across multiple waves using a single dataset method.

        If the required `.parquet` file is missing, it requests `Makefile` to
        generate only that file.

        ``currency`` is forwarded to :meth:`_finalize_result` to optionally
        attach the ISO 4217 currency label (monetary tables only).
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
                return self._finalize_result(df_cached, scheme_entry, method_name, currency=currency)

        if (
            not self._panel_ids_attempted
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
                    except (ValueError, KeyError, TypeError) as e:
                        # reorder_levels raises ValueError on level mismatch;
                        # KeyError/TypeError on bad arg shapes.  Re-raise
                        # with a richer message so the offending key is
                        # surfaced.
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

            # Pre-warm the DVC cache for this feature's source files
            # before invoking make / the wave script.  See
            # ``_warm_dvc_cache_for_feature`` for design notes; the
            # short version is that one batched ``Repo.fetch`` here
            # amortises DVC's ~93s graph-walk cost (Lustre metadata-
            # bound) over arbitrarily many targets, so the make -j12
            # subprocess that follows sees a warm cache and never
            # contends on ``.dvc/tmp/lock``.  Best-effort: any failure
            # falls through to the per-process retry loop in
            # ``_ensure_dvc_pulled`` (defense in depth).
            try:
                from .local_tools import _warm_dvc_cache_for_feature
                _warm_dvc_cache_for_feature(self, method_name, wave=wave)
            except Exception:
                # Pre-warm must never break a build.  The retry loop
                # downstream is the safety net.
                pass

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
            # Hashless (script-written) L2-wave parquets can't self-invalidate,
            # so evict them at the start of every wave-rebuild descent; the loop
            # below then re-runs Make/the script from source rather than reusing
            # a stale wave parquet.  Closes residual F1 (GH #479) for the
            # partial-cache (country parquet absent) and LSMS_NO_CACHE paths,
            # which previously fell through to a wave rebuild WITHOUT eviction
            # (eviction formerly fired only in the `freshness == "stale"`
            # branch, reachable only when the L2-country parquet exists).  This
            # lives INSIDE load_from_waves -- not earlier in the rebuild descent
            # -- so it touches only the wave-rebuild path and never the DVC
            # materialize-stage read path (`collect_stage_outputs`), whose stage
            # outputs share the {wave}/_/{table}.parquet location and must not be
            # evicted before being read.  Selective (deletes only hashless
            # parquets; stamped YAML waves self-invalidate per-wave); never runs
            # on a warm hit (the cache read returns before any rebuild descent).
            self._evict_hashless_wave_caches(method_name)
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
                    # GH #535: run_make_target reads the whole shared folder's
                    # script output, so for a country whose ``waves`` splits one
                    # folder into several quarter-waves (Nigeria PP/PH share one
                    # folder, e.g. 2010Q3 + 2011Q1 -> 2010-11) every quarter
                    # re-injects the same script-tagged rows -- an exact doubling
                    # at concat that surfaces as a false-positive GH #323
                    # "duplicate tuple(s)" warning (plus a wasted 2x build).
                    # Mirror grab_data's per-wave t-filter (~country.py:1092):
                    # keep only the rows whose ``t`` matches the requested wave
                    # ``w``.  No-op for single-t-per-folder tables and for
                    # Tanzania's multi-round folder (``t == wave`` holds there);
                    # ``w`` is never None on this in-loop path.
                    if (isinstance(wave_result, pd.DataFrame)
                            and not wave_result.empty
                            and 't' in (wave_result.index.names or [])):
                        wave_result = wave_result[
                            wave_result.index.get_level_values('t') == w
                        ]

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
                    wave_result = _normalize_dataframe_index(wave_result, scheme_entry, w, method_name,
                                                            country=self.name)

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

            # GH #323: start this table's grain ledger clean.  It is repopulated
            # either by the cold build (the collapse audit) or by replaying the
            # stamp on a warm read -- so `grain_reports()` describes THIS load, and
            # a stale report from an earlier call in the same process can never be
            # re-stamped into a parquet it does not belong to.
            _GRAIN_LEDGER.pop((self.name, method_name), None)

            # v0.7.0/v0.8.0: best-effort cache read.  If a parquet exists
            # at cache_path AND is not stale, read it and return without
            # consulting DVC, the stage layer, or the wave loaders.
            #
            # v0.8.0 content-hash staleness: the parquet carries an
            # embedded ``lsms_cache_hash`` (see local_tools.to_parquet);
            # we recompute the expected hash from this country's per-wave
            # inputs and compare.  Classifications:
            #   - unverifiable (no computable hash) -> read (v0.7.0 behavior)
            #   - fresh        -> read
            #   - legacy       -> read + trust-once re-stamp (migration)
            #   - stale        -> fall through and rebuild from source
            # Editing a wave's data_info.yml / wave module / _/<table>.py
            # script or a source .dta (via its DVC sidecar md5) flips the
            # hash and forces a rebuild.  ``assume_cache_fresh`` /
            # ``LSMS_NO_CACHE`` short-circuit this (handled elsewhere).
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
            cache_expected_hash = None
            if cache_exists and not no_cache:
                cache_expected_hash = self._table_cache_hash(method_name, waves)
                freshness = cache_freshness(cache_path, cache_expected_hash)
                if freshness == "stale":
                    logger.debug(
                        f"v0.8.0 cache STALE: {method_name} at {cache_path}; "
                        f"rebuilding from source"
                    )
                    # Eviction of hashless (script-written) wave parquets now
                    # happens once, just before the rebuild descent below, so
                    # it also covers the country-parquet-absent and
                    # LSMS_NO_CACHE paths (residual F1, GH #479).
                else:
                    try:
                        cached_df = get_dataframe(cache_path)
                        cached_df = map_index(cached_df)
                        # GH #323: this parquet was written POST-collapse, so its
                        # index is unique and _normalize_dataframe_index below can
                        # never re-detect the loss.  Replay what the cold build that
                        # produced it recorded -- otherwise the warning is a
                        # cold-build-only event and the loss is invisible on every
                        # warm read, which is exactly how #323 survived its first fix.
                        _replay_grain_audit(
                            read_parquet_grain_audit(cache_path),
                            self.name, method_name,
                        )
                        if freshness == "legacy" and cache_expected_hash is not None:
                            # Trust-once-then-stamp: parquet predates
                            # hashing; assume it matches current sources
                            # (same assumption v0.7.0 already made) and
                            # stamp it so the next read is guarded.
                            stamp_parquet_hash(cache_path, cache_expected_hash)
                        logger.debug(
                            f"v0.8.0 cache read ({freshness}): {method_name} "
                            f"from {cache_path}"
                        )
                        return cached_df
                    except (OSError, ArrowInvalid) as cache_read_error:
                        # Stale / corrupted cache parquet -> rebuild from source.
                        # Surface to the user (not just debug log) so a silent
                        # cache-miss isn't mistaken for a healthy build.
                        # Programmer bugs (TypeError, AttributeError) propagate.
                        warnings.warn(
                            f"v0.7.0 cache read failed for {method_name} "
                            f"({cache_read_error!r}); rebuilding from source",
                            category=UserWarning,
                            stacklevel=2,
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
                            to_parquet(df, cache_path,
                                       cache_hash=self._table_cache_hash(method_name, waves),
                                       grain_audit=_GRAIN_LEDGER.get((self.name, method_name)))
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
                                df_wave = _normalize_dataframe_index(df_wave, scheme_entry, stage.wave, method_name,
                                                                    country=self.name)
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
                        to_parquet(combined_df, cache_path,
                                   cache_hash=self._table_cache_hash(method_name, waves),
                                   grain_audit=_GRAIN_LEDGER.get((self.name, method_name)))
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
                            # GH #323: the OTHER warm-read path.  Same argument as at
                            # the v0.8.0 fast-path read above -- this parquet is
                            # post-collapse, so nothing downstream can re-detect the
                            # loss; replay what the cold build stamped.  (Largely dead
                            # since the DVC stage layer was retired in v0.7.0, but a
                            # silent warm read is exactly the hole being closed, so it
                            # does not get to stay open on a technicality.)
                            _replay_grain_audit(
                                read_parquet_grain_audit(cache_path),
                                self.name, method_name,
                            )
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
                                method_name,
                                country=self.name,
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
                    to_parquet(df, cache_path,
                               cache_hash=self._table_cache_hash(method_name, waves))
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
                # broad catch intentional: tee-into-log then re-raise so
                # _log_issue() captures every materialization failure for
                # the issues tracker before the exception propagates.
                _log_issue(self.name, method_name, waves, error)
                raise
        result = self._finalize_result(df, scheme_entry, method_name, currency=currency)
        # Loud schema gate (GH #479): for script-path tables (whose hashless
        # wave parquets can shadow a wave-script fix), fail with an actionable
        # message if a required declared column is missing post-finalize, rather
        # than silently returning wrong data.  ``materialize_backend`` is an
        # unreliable signal -- GhanaLSS food_acquired is script-built via the
        # wave-script fallback + a ``_/food_acquired.py`` concatenator yet
        # declares no ``materialize: make`` -- so we also treat the presence of
        # a country-level ``_/{table}.py`` concatenator as script-path.
        is_script_path = (
            materialize_backend == "make"
            or (self.file_path / "_" / f"{method_name}.py").exists()
        )
        self._assert_built_required_columns(result, method_name, scheme_entry,
                                            is_script_path)
        return result

    def _compute_panel_ids(self) -> None:
        """
        Compute and cache both panel_ids and updated_ids.

        Records the attempt via ``self._panel_ids_attempted`` regardless of
        outcome so the property accessors don't re-run the miss path on
        every access.  Countries that don't declare ``panel_ids`` in their
        data_scheme are silently absent (no warning); countries that
        declare it but fail to materialize trigger the loud warning, which
        is the case worth surfacing.
        """
        # Mark the attempt up front so any return path counts.
        self._panel_ids_attempted = True

        # Declared-absent: no panel_ids in data_scheme means the country
        # is intentionally cross-section / non-panel.  Don't even try to
        # aggregate — that would emit a "Data scheme does not contain
        # panel_ids" warning from _aggregate_wave_data.
        if 'panel_ids' not in self.data_scheme:
            self._panel_ids_cache = None
            self._updated_ids_cache = None
            return

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
            # Declared but materialization failed — this IS noteworthy.
            logger.warning(
                f"Panel IDs declared in data_scheme but not materialized "
                f"for {self.name}."
            )
            self._panel_ids_cache = None
            self._updated_ids_cache = None

    @property
    def panel_ids(self) -> dict[str, Any] | None:
        """Raw panel-ID tables keyed by wave.  Computed lazily on first access.

        Gated on ``_panel_ids_attempted`` rather than the cache value so a
        legitimate negative result (country without panel design) is
        cached as ``None`` without re-running ``_compute_panel_ids``.
        """
        if not self._panel_ids_attempted:
            self._compute_panel_ids()
        return self._panel_ids_cache

    @property
    def updated_ids(self) -> dict[str, dict[str, str]] | None:
        """Mapping ``{old_id: new_id}`` per wave for ID harmonization.  Computed lazily."""
        if not self._panel_ids_attempted:
            self._compute_panel_ids()
        return self._updated_ids_cache

    def cached_datasets(self) -> list[str]:
        """
        List dataset names currently cached for this country.

        Discovers caches at three locations under ``data_root()``
        (DVC blob cache L1 lives under ``dvc-cache/`` and is not enumerated
        by this method):

        - **L2-country**: ``data_root(country)/var/*.parquet``.
        - **Country-level companion**: ``data_root(country)/_/*.{parquet,json}``.
        - **L2-wave**: ``data_root(country)/{wave}/_/*.parquet`` for
          every wave subdirectory (excluding the special ``_/`` and
          ``var/`` directories above).  This catches script-path tables
          (Nigeria's PP/PH ``household_roster``, Tanzania's multi-round
          tables) whose only cache lives at the wave level.
        """
        cache_files: list[Path] = []
        country_root = data_root(self.name)
        var_dir = country_root / "var"
        underscore_dir = country_root / "_"

        if var_dir.exists():
            cache_files.extend(var_dir.glob("*.parquet"))
        if underscore_dir.exists():
            cache_files.extend(underscore_dir.glob("*.json"))
            cache_files.extend(underscore_dir.glob("*.parquet"))

        # L2-wave: any direct subdir of the country root with a
        # ``_/`` containing parquets, excluding the two special dirs
        # already handled above.
        if country_root.exists():
            for wave_dir in country_root.iterdir():
                if not wave_dir.is_dir() or wave_dir.name in ("_", "var"):
                    continue
                wave_underscore = wave_dir / "_"
                if wave_underscore.exists():
                    cache_files.extend(wave_underscore.glob("*.parquet"))

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

        Clears two parquet tiers (the L1 DVC blob cache is left alone;
        evict it manually with ``rm -rf {data_root}/dvc-cache`` if you
        really mean to re-fetch from S3) plus DVC build artifacts:

        - **L2-country** at ``data_root(country)/var/{method}.parquet``
          and the JSON-companion ``data_root(country)/_/{method}.parquet|json``.
        - **L2-wave** at ``data_root(country)/{wave}/_/{method}.parquet``
          for every wave the country knows about (via :attr:`waves`).
          Both YAML-path tables (cached on first ``Wave.grab_data`` call,
          since the 2026-04-15 L2-wave addition) and script-path
          tables (written by ``_/{method}.py`` via
          :func:`local_tools.to_parquet`) live here.
        - **DVC build artifacts** under ``lsms_library/countries/{country}/...``
          when ``waves=`` is passed (resolved via the materialize stage map).

        If ``methods`` is None, all cached datasets are removed.
        Returns list of deleted file paths.
        """
        removed: list[Path] = []
        targets = list(dict.fromkeys(methods or self.cached_datasets()))

        # Discover L2-wave caches by globbing the country root.
        # We *cannot* derive directory names from ``self.waves`` because
        # countries with per-round ``t`` values write under the round
        # name, not the wave-folder name (e.g. Nigeria's
        # ``2012-13/_/household_roster.py`` writes to
        # ``data_root/Nigeria/{2012Q3,2013Q1}/_/household_roster.parquet``).
        # The country root may not exist if nothing has been cached yet.
        country_root = data_root(self.name)
        l2_caches_by_method: dict[str, list[Path]] = {}
        if country_root.exists():
            for method in targets:
                # Match any *direct* subdir of the country root that has
                # an _/{method}.parquet — excludes ``_/`` (companion) and
                # ``var/`` (L2-country) which are already handled below.
                hits = [
                    p for p in country_root.glob(f"*/_/{method}.parquet")
                    if p.parent.parent.name not in ("_", "var")
                ]
                if hits:
                    l2_caches_by_method[method] = hits

        for method in targets:
            json_cache = country_root / "_" / f"{method}.json"
            parquet_cache = country_root / "_" / f"{method}.parquet"
            var_cache = country_root / "var" / f"{method}.parquet"
            l2_caches = l2_caches_by_method.get(method, [])

            for candidate in (json_cache, parquet_cache, var_cache, *l2_caches):
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
        'area_output': (
            "Country('EthiopiaRHS').area_output() is deprecated and will be "
            "removed in a future release. The HH-level (t, i) wide crop table "
            "is subsumed by the item-level crop_production (t, i, j) feature "
            "(GH #438), which covers more waves (R1-R4 + R6 + R7):\n\n"
            "    Country('EthiopiaRHS').crop_production()  # (t, i, v, j, u)\n\n"
            "For callers who need the legacy wide (t, i) shape, a compatibility "
            "shim is available:\n\n"
            "    from lsms_library.transformations import legacy_area_output\n"
            "    ao = legacy_area_output(Country('EthiopiaRHS'))"
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
                from .transformations import legacy_locality, legacy_area_output
                _shims = {'locality': legacy_locality,
                          'area_output': legacy_area_output}
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
            def method(waves=None, market=None, labels='Preferred', age_cuts=None,
                       units=None, volume_as_mass=True, currency=None, numeraire=None,
                       basis=None):
                if age_cuts is not None and name not in self._ROSTER_DERIVED:
                    raise TypeError(
                        f"{name}() got an unexpected keyword argument 'age_cuts'; "
                        "only roster-derived tables (e.g. household_characteristics) "
                        "accept age_cuts."
                    )
                if units is not None and name not in {'food_prices', 'food_quantities'}:
                    raise TypeError(
                        f"{name}() got an unexpected keyword argument 'units'; "
                        "only 'food_prices' and 'food_quantities' accept units."
                    )
                if basis is not None:
                    if name != 'food_expenditures':
                        raise TypeError(
                            f"{name}() got an unexpected keyword argument 'basis'; "
                            "only 'food_expenditures' accepts basis."
                        )
                    # Validate early: the derive path's broad except would
                    # otherwise swallow the transform's ValueError and surface
                    # a confusing "could not materialize" instead (#575).
                    if basis not in {'purchased', 'total'}:
                        raise ValueError(
                            f"food_expenditures() basis= must be 'purchased' or "
                            f"'total'; got {basis!r}"
                        )
                if (volume_as_mass is not True
                        and name not in {'food_prices', 'food_quantities'}):
                    raise TypeError(
                        f"{name}() got an unexpected keyword argument 'volume_as_mass'; "
                        "only 'food_prices' and 'food_quantities' accept it."
                    )
                if currency is not None:
                    if currency not in {'index', 'column'}:
                        raise ValueError(
                            f"{name}() currency= must be 'index', 'column', or None; "
                            f"got {currency!r}"
                        )
                    if not is_monetary_table(name, self.name):
                        raise TypeError(
                            f"{name}() got an unexpected keyword argument 'currency'; "
                            "only tables with monetary columns accept it."
                        )
                if numeraire is not None:
                    if currency is not None:
                        raise ValueError(
                            f"{name}(): pass either currency= or numeraire=, not both."
                        )
                    if not is_monetary_table(name, self.name):
                        raise TypeError(
                            f"{name}() got an unexpected keyword argument 'numeraire'; "
                            "only tables with monetary columns accept it."
                        )
                    from .conversion import conversion_targets
                    if numeraire not in conversion_targets():
                        raise ValueError(
                            f"{name}(): unknown numeraire {numeraire!r}; "
                            f"available: {conversion_targets()}"
                        )
                    # Attach the LCU label as an index level so the post-step
                    # convert() can relabel it to the target basis.
                    currency = 'index'
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
                    transform_kwargs = {}
                    if units is not None and name in {'food_prices', 'food_quantities'}:
                        transform_kwargs['units'] = units
                    if name in {'food_prices', 'food_quantities'}:
                        transform_kwargs['volume_as_mass'] = volume_as_mass
                    if name == 'food_expenditures' and basis is not None:
                        transform_kwargs['basis'] = basis
                    derived = None
                    try:
                        fa = self._aggregate_wave_data(waves, 'food_acquired')
                        if isinstance(fa, pd.DataFrame) and not fa.empty:
                            derived = transform_fn(fa, **transform_kwargs)
                            scheme_entry = self._materialization_entry(name)
                            derived = self._finalize_result(derived, scheme_entry, name,
                                                            currency=currency)
                    except (FileNotFoundError, KeyError, ValueError, RuntimeError) as exc:
                        if units is not None:
                            # Caller explicitly asked for canonical units= behaviour;
                            # the legacy fall-through path can't honour that, so
                            # surface the failure rather than silently returning
                            # un-units-aware data.
                            raise
                        logger.info(
                            "Deriving %s from food_acquired failed (%s); "
                            "falling back to legacy aggregation", name, exc)
                        derived = None
                    if derived is not None:
                        # Relabel is outside the except so user-facing KeyErrors
                        # (bad labels=) propagate instead of silently falling
                        # through to the legacy aggregation path.
                        reagg = name in {'food_expenditures', 'food_quantities'}
                        derived = self._relabel_j(derived, labels, reaggregate=reagg)
                        if market is not None:
                            derived = self._add_market_index(derived, column=market)
                        if numeraire is not None:
                            from .conversion import convert as _convert
                            derived = _convert(derived, to=numeraire, country=self.name)
                        return derived

                # Derive household_characteristics from household_roster
                if (name in self._ROSTER_DERIVED
                        and 'household_roster' in self.data_scheme):
                    from .transformations import roster_to_characteristics
                    derived = None
                    try:
                        roster = self._aggregate_wave_data(waves, 'household_roster')
                        if isinstance(roster, pd.DataFrame) and not roster.empty:
                            # Determine final_index from available index levels
                            idx = list(roster.index.names)
                            final_index = [n for n in ['t', 'v', 'i', 'm'] if n in idx]
                            if not final_index:
                                final_index = [n for n in idx if n != 'pid']
                            rc_kwargs = {'drop': 'pid', 'final_index': final_index}
                            if age_cuts is not None:
                                rc_kwargs['age_cuts'] = tuple(age_cuts)
                            derived = roster_to_characteristics(roster, **rc_kwargs)
                    except (FileNotFoundError, KeyError, ValueError, RuntimeError) as exc:
                        logger.info(
                            "Deriving %s from household_roster failed (%s); "
                            "falling back to legacy aggregation", name, exc)
                        derived = None
                    if derived is not None:
                        # _add_market_index lives OUTSIDE the except so a
                        # user-facing KeyError (a market= column this country
                        # lacks) propagates, instead of being misread as a
                        # derivation failure and falling through to the legacy
                        # make/DVC path -- which has no rule for the derived
                        # household_characteristics table and so emits spurious
                        # "Makefile execution failed" + a misleading "could not
                        # materialize" error (GH #518).  Mirrors the food-derived
                        # path above.
                        if market is not None:
                            derived = self._add_market_index(derived, column=market)
                        return derived

                result = self._aggregate_wave_data(waves, name, currency=currency)
                # Apply relabeling to any table with a j index level
                if (isinstance(result, pd.DataFrame) and not result.empty
                        and 'j' in (result.index.names or [])):
                    reagg = name in {'food_expenditures', 'food_quantities'}
                    result = self._relabel_j(result, labels, reaggregate=reagg)
                if market is not None and isinstance(result, pd.DataFrame) and not result.empty:
                    result = self._add_market_index(result, column=market)
                if numeraire is not None and isinstance(result, pd.DataFrame) and not result.empty:
                    from .conversion import convert as _convert
                    result = _convert(result, to=numeraire, country=self.name)
                return result
            doc_parts = [
                f"Return {name} as a DataFrame, aggregated across *waves*.\n\n",
                "Parameters\n----------\n",
                "waves : list of str, optional\n",
                "    Subset of waves to include.  Defaults to all available.\n",
                "market : str, optional\n",
                "    Column from cluster_features (e.g. 'Region') to add as\n",
                "    an ``m`` index level for demand estimation.\n",
                "labels : str, optional\n",
                "    Label column to use for the ``j`` index on derived food\n",
                "    tables (food_expenditures, food_quantities, food_prices).\n",
                "    Defaults to ``'Preferred'`` (current behaviour).  Other\n",
                "    values (e.g. ``'Aggregate'``) select a same-named column\n",
                "    from the country's ``food_items`` / ``harmonize_food``\n",
                "    table; ``food_expenditures`` and ``food_quantities`` are\n",
                "    re-aggregated after renaming.  Raises ``KeyError`` if the\n",
                "    column is absent.\n",
            ]
            if name in {'food_prices', 'food_quantities'}:
                if name == 'food_prices':
                    doc_parts.append(
                        "units : {'kgvalue', 'kgprice', 'unitvalue', 'unitprice'}, optional\n"
                        "    Price basis.  Default ``'kgvalue'`` (Expenditure /\n"
                        "    Quantity_kg, currency per kg).  Other modes:\n"
                        "    ``'unitvalue'`` (Expenditure / Quantity, per native\n"
                        "    u; gives 1 = Kwacha-per-Kwacha for u='Value' rows),\n"
                        "    ``'kgprice'`` (reported Price × kg_factor),\n"
                        "    ``'unitprice'`` (reported Price as-is).  See\n"
                        "    :func:`lsms_library.transformations.food_prices_from_acquired`\n"
                        "    and ``slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org``.\n"
                    )
                else:  # food_quantities
                    doc_parts.append(
                        "units : {'kgs', 'units'}, optional\n"
                        "    Quantity basis.  Default ``'kgs'`` (kilograms\n"
                        "    where the unit is convertible; native quantity\n"
                        "    carried with native u-tag where it isn't).\n"
                        "    ``'units'`` returns native Quantity per (t,v,i,j,u,s).\n"
                        "    See\n"
                        "    :func:`lsms_library.transformations.food_quantities_from_acquired`\n"
                        "    and ``slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org``.\n"
                    )
                doc_parts.append(
                    "volume_as_mass : bool, optional\n"
                    "    When ``True`` (default), treat ``1 litre = 1 kg`` for\n"
                    "    fluid units (litre, ml, cl) -- a specific-gravity-1\n"
                    "    approximation, roughly right for water-based foods\n"
                    "    and moderately wrong for cooking oil and alcohol.\n"
                    "    Pass ``False`` to drop fluid units from the hand-coded\n"
                    "    factor map and from the explicit-metric label parser;\n"
                    "    their kg conversion (if any) then comes purely from\n"
                    "    data-driven price-ratio inference.  See GH #231.\n"
                )
            if name in self._ROSTER_DERIVED:
                doc_parts.append(
                    "age_cuts : tuple of positive numbers, optional\n"
                    "    Interior breakpoints between age buckets, passed to\n"
                    "    :func:`lsms_library.transformations.roster_to_characteristics`.\n"
                    "    Partitions ages into ``len(age_cuts) + 1`` half-open\n"
                    "    buckets ``[0, c_0), [c_0, c_1), ..., [c_{n-1}, inf)``.\n"
                    "    Defaults to ``(4, 9, 14, 19, 31, 51)``, producing the\n"
                    "    compact labels ``00-03``, ``04-08``, …, ``51+``.\n"
                    "    Fractional breakpoints (e.g. ``(0.5, 1, 5)``) are\n"
                    "    allowed and trigger explicit ``[lo, hi)`` labels.\n"
                )
            if is_monetary_table(name, self.name):
                doc_parts.append(
                    "currency : {'index', 'column'}, optional\n"
                    "    Attach the ISO 4217 currency code (resolved per wave,\n"
                    "    e.g. UGX, NGN, XOF; GhanaLSS 2005-06 -> GHC vs 2016-17\n"
                    "    -> GHS) to this monetary table.  ``'index'`` appends a\n"
                    "    ``currency`` index level; ``'column'`` adds a column.\n"
                    "    Defaults to ``None`` (omit) for single-country calls;\n"
                    "    ``Feature(...)`` defaults to ``'index'``.  See\n"
                    "    :func:`lsms_library.currency.attach_currency`.\n"
                )
                doc_parts.append(
                    "numeraire : str, optional\n"
                    "    Convert the monetary columns to a comparable basis -- a\n"
                    "    target column of ``conversion_factors.org`` (e.g.\n"
                    "    ``'PPP-2017'``, ``'FX'``, ``'USD-real-2017'``).  Mutually\n"
                    "    exclusive with ``currency``.  Pre-reform redenomination\n"
                    "    waves and missing factors yield ``NaN``.  See\n"
                    "    :func:`lsms_library.conversion.convert`.\n"
                )
            method.__doc__ = "".join(doc_parts)
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

# Maps string representations of booleans to Python booleans.
# Needed because pyarrow serialises Python bool values in object-dtype
# Series as the strings 'True' / 'False', so cached parquets that were
# written before an explicit BooleanDtype was enforced at write time
# arrive back as string columns.  pd.to_numeric('True') returns NaN,
# so naive numeric coercion silently wipes every value.
_STR_TO_BOOL: dict[str, bool] = {
    'True': True, 'False': False,
    'true': True, 'false': False,
    '1': True,    '0': False,
    'yes': True,  'no': False,
    'Yes': True,  'No': False,
    'YES': True,  'NO': False,
}


def _coerce_to_boolean(series: pd.Series) -> pd.Series:
    """Safely cast *series* to nullable BooleanDtype.

    Handles three source formats produced by the YAML build path:

    * Object-dtype with Python bool or string values  (``'True'``/``'False'``)
      — these are what pyarrow writes when a YAML mapping returns Python
      booleans into an ``object`` column.
    * Numeric (int/float 0/1) — legacy scripts that stored 0/1 integers.
    * Already-boolean — passthrough with a cheap re-cast.
    """
    if pd.api.types.is_bool_dtype(series) or isinstance(series.dtype, pd.BooleanDtype):
        return series.astype(pd.BooleanDtype())
    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        mapped = series.map(
            lambda x: _STR_TO_BOOL.get(str(x), None) if pd.notna(x) else None
        )
        return mapped.astype(pd.BooleanDtype())
    # numeric (int/float) path
    return pd.to_numeric(series, errors='coerce').astype(pd.BooleanDtype())

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

    Vectorized implementation: uses pd.Series.map() over the whole column
    instead of a row-by-row Python loop.
    """
    if "Relationship" not in df.columns:
        return df

    kinship = _load_kinship_map()

    # Exact-match NA sentinels.  Do NOT lowercase-fold: "Nan" is British
    # English for grandmother in some contexts.  Explicit kinship.yml
    # mappings are checked FIRST, so if a survey legitimately uses "Nan"
    # as a label, adding it to kinship.yml overrides this sentinel list.
    _NA_SENTINELS = frozenset(('', '<NA>', 'nan', 'Nan', 'NaN', 'NAN', 'None', 'NaT'))

    # Cast to nullable string so pd.NA propagates through .str accessors.
    rel_str = df["Relationship"].astype("string")

    stripped = rel_str.str.strip()
    titled = stripped.str.title()

    # Try title-case first, fall back to raw stripped (mirrors original logic).
    mapped = titled.map(kinship).fillna(stripped.map(kinship))

    def _component(series, i):
        return series.map(lambda x: x[i] if isinstance(x, (list, tuple)) else pd.NA)

    df = df.copy()
    df["Generation"] = pd.array(_component(mapped, 0), dtype=pd.Int64Dtype())
    df["Distance"]   = pd.array(_component(mapped, 1), dtype=pd.Int64Dtype())
    df["Affinity"]   = pd.array(_component(mapped, 2), dtype=pd.StringDtype())

    # Preserve original Relationship string for mapped rows; NA for sentinels
    # and true NAs; original value for unknowns (consistent with old behaviour).
    is_sentinel = stripped.isin(_NA_SENTINELS) & mapped.isna()
    rel_out = rel_str.where(~is_sentinel, pd.NA)
    df["Relationship"] = pd.array(rel_out, dtype=pd.StringDtype())
    # Keep Relationship alongside the decomposed columns — the analyst
    # can see both the original survey label and the structured kinship.

    # Warn on unknowns (non-sentinel, unmapped, non-NA).
    unknown_mask = mapped.isna() & ~is_sentinel & rel_str.notna()
    unknown = set(stripped[unknown_mask].dropna().unique().tolist())
    if unknown:
        warnings.warn(
            f"Unknown relationship labels (add to kinship.yml): "
            f"{sorted(unknown)}",
            stacklevel=2,
        )

    return df


# Historical hardcoded _no_v_join set (pre-GH-#436, code/config separation).
# Used as a fallback if data_info.yml cannot be read or omits the declarative
# "Join v from sample" section, so the framework never silently starts
# fabricating v on tables that never carried it.
_NO_V_JOIN_FALLBACK = frozenset({
    'sample', 'cluster_features', 'panel_ids', 'updated_ids',
    'shocks', 'assets', 'livestock', 'income',
})


def _compute_no_v_join(data: dict) -> frozenset[str]:
    """Pure helper: derive the no-v-join set from a parsed data_info.yml dict.

    A table is skipped when its canonical index (``Index Info > index_info``)
    omits ``v``, or when it is listed under ``Join v from sample > skip_extra``.
    Returns :data:`_NO_V_JOIN_FALLBACK` when the ``Join v from sample`` section
    is absent (version skew against an older/foreign data_info.yml), so v-join
    is never silently re-enabled on a table that never carried it.
    """
    section = data.get("Join v from sample") if isinstance(data, dict) else None
    if not isinstance(section, dict):
        return _NO_V_JOIN_FALLBACK
    skip: set[str] = set()
    index_info = (data.get("Index Info", {}) or {}).get("index_info", {}) or {}
    for table, spec in index_info.items():
        if not isinstance(spec, str):
            continue
        # Strip exactly one surrounding paren pair (mirrors
        # feature._canonical_index_levels) so a level name containing a paren
        # is not mangled, then split on commas into whole level tokens.
        cleaned = spec.strip()
        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = cleaned[1:-1]
        levels = [tok.strip() for tok in cleaned.split(",") if tok.strip()]
        if "v" not in levels:
            skip.add(table)
    skip.update(str(t) for t in (section.get("skip_extra") or []))
    return frozenset(skip)


@lru_cache(maxsize=1)
def _no_v_join_tables() -> frozenset[str]:
    """Tables that ``_finalize_result`` must NOT join ``v`` onto.

    Declarative source: ``lsms_library/data_info.yml`` (see
    :func:`_compute_no_v_join`).  Falls back to the historical hardcoded set
    (:data:`_NO_V_JOIN_FALLBACK`) when the file is missing or malformed.
    """
    try:
        info_path = files("lsms_library") / "data_info.yml"
        with open(info_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return _compute_no_v_join(data)
    except Exception:
        return _NO_V_JOIN_FALLBACK


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
            if hasattr(df[col], "cat"):
                df[col] = df[col].astype("string").replace(variant_map)
            else:
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
                elif target == pd.Int64Dtype():
                    # Round before casting so float values (e.g. 59.41 from
                    # age_handler) survive the safe-cast check.
                    df[col] = pd.to_numeric(df[col], errors='coerce').round().astype(target)
                elif target == pd.BooleanDtype():
                    # NOT pd.to_numeric: cached parquets store bools as the
                    # strings 'True'/'False', and pd.to_numeric('True') -> NaN
                    # would silently wipe every value (GH #386).
                    df[col] = _coerce_to_boolean(df[col])
                elif target == pd.Float64Dtype():
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(target)
                else:
                    df[col] = df[col].astype(target)
        except (ValueError, TypeError):
            pass  # best-effort; don't break loading


@lru_cache(maxsize=1)
def _load_canonical_dtypes() -> dict[str, dict[str, str]]:
    """Load column type declarations from data_info.yml Columns section.

    Returns ``{table: {column: type_str}}`` built from the ``type`` field of
    each column entry.  Only columns with an explicit ``type`` declaration are
    included.
    """
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    result: dict[str, dict[str, str]] = {}
    for table, cols in data.get("Columns", {}).items():
        for col, meta in cols.items():
            if not isinstance(meta, dict):
                continue
            type_decl = meta.get("type")
            if type_decl and isinstance(type_decl, str):
                result.setdefault(table, {})[col] = type_decl
    return result


def _enforce_canonical_dtypes(df: pd.DataFrame, method_name: str) -> None:
    """Cast DataFrame columns to the types declared in data_info.yml Columns.

    Modifies *df* in place.  This runs after ``_enforce_declared_dtypes`` so
    the canonical schema (data_info.yml) wins over any country-level override
    (e.g. ``Age: float`` in Albania's data_scheme.yml is overridden to Int64
    because data_info.yml declares ``Age: type: int``).

    Non-numeric strings in int/float columns are coerced to ``pd.NA`` via
    ``pd.to_numeric(..., errors='coerce')`` before the dtype cast.
    """
    all_dtypes = _load_canonical_dtypes()
    table_dtypes = all_dtypes.get(method_name, {})
    for col, type_decl in table_dtypes.items():
        if col not in df.columns:
            continue
        if type_decl not in _SCHEME_DTYPE_MAP:
            continue
        target = _SCHEME_DTYPE_MAP[type_decl]
        try:
            if target == 'to_datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif target == pd.Int64Dtype():
                # Round before casting so that float ages like 59.41 (from
                # age_handler's date-arithmetic path) survive the safe-cast
                # check in pandas' IntegerArray.__from_sequence__.
                df[col] = pd.to_numeric(df[col], errors='coerce').round().astype(target)
            elif target == pd.BooleanDtype():
                # NOT pd.to_numeric: cached parquets store bools as the strings
                # 'True'/'False', and pd.to_numeric('True') -> NaN would
                # silently wipe every value (GH #386).
                df[col] = _coerce_to_boolean(df[col])
            elif target == pd.Float64Dtype():
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(target)
            else:
                df[col] = df[col].astype(target)
        except (ValueError, TypeError):
            pass  # best-effort; don't break loading


# ---------------------------------------------------------------------------
# GH #323 -- the grain-collapse audit.
#
# THE CLASS OF BUG.  `_normalize_dataframe_index` reduces a non-unique DECLARED
# index with `groupby(...).first()`.  Where the duplicate rows DISAGREE, the rows
# it drops are real data (distinct people, distinct shocks) and they vanished with
# no signal.  #323 was closed once on a warning that could not fire (below); #500,
# #501 and #514 were each closed on a single INSTANCE while the class survived.
#
# WHY THE OLD WARNING COULD NOT FIRE.  It was gated on `not df.index.is_unique`,
# and the L2-country parquet is WRITTEN POST-COLLAPSE -- so on every warm read the
# index is already unique and the gate is False.  The bug hid behind the cache that
# the bug poisoned.  Two fixes follow from that, and both matter:
#   (1) audit BEFORE the collapse, while the evidence still exists;
#   (2) PERSIST the finding into the parquet and re-emit it on the warm read
#       (`local_tools._GRAIN_AUDIT_KEY`), so the signal outlives the destruction.
#
# WHY NOT "just declare an aggregation policy".  Because duplicates on a declared
# index almost never mean "reduce me" -- they mean the IDENTIFIER IS BROKEN or a
# LEVEL IS MISSING.  Mali's `household_roster` declares `(t, i, pid)`, but `pid` is
# a *household* id stamped onto every member (5,149 distinct values across 37,175
# rows), so `first()` keeps ONE PERSON PER HOUSEHOLD and 32,026 people disappear.
# No reducer is correct there: `first` keeps one person, `sum` is meaningless on
# `Sex`.  Declaring `aggregation: {pid: first}` would only put a signature on the
# corpse.  So the core does NOT aggregate -- consistent with the NO-AGGREGATION-IN-
# CORE contract in SkunkWorks/grain_aggregation_policy.org -- it reports, and (in
# strict mode) refuses.  The one genuine reduction policy we have,
# `_ADDITIVE_MEASURE_COLUMNS` (food_acquired), stays and is lossless.
# ---------------------------------------------------------------------------

class GrainCollapseError(RuntimeError):
    """A declared index was non-unique and collapsing it would destroy rows.

    Raised instead of the default warning when ``LSMS_GRAIN_STRICT`` is set.
    """


class GrainCollapseWarning(RuntimeWarning):
    """A declared index was non-unique; collapsing it destroyed rows (GH #323).

    Its own class (rather than a bare ``RuntimeWarning``) so callers and CI can
    target it precisely: ``warnings.simplefilter("error", GrainCollapseWarning)``.
    """


def _grain_strict() -> bool:
    """Whether a destructive grain collapse should RAISE rather than warn.

    Default is warn: making it fatal out of the box breaks ~30 countries at once
    and gets reverted, and a revert is how the class survives.  Strict mode is
    what lets tests and CI ratchet the census down to zero without a
    known-bad allowlist (an allowlist is the same disease with a registry).
    """
    return os.environ.get("LSMS_GRAIN_STRICT", "").lower() in {"1", "true", "yes"}


def _audit_index_collapse(
    df: pd.DataFrame, levels: list[str],
) -> dict[str, Any] | None:
    """Measure what collapsing *df* onto *levels* would destroy.

    Returns ``None`` when the collapse is provably lossless, else a report dict.

    THE DISTINCTION THAT MAKES THIS SIGNAL READABLE.  Across the 40 countries,
    ~7.5M rows sit on a duplicated declared index -- but 6.46M of them are EXACT
    duplicates of a row that survives (a cluster attribute repeated once per
    household in the cluster, say).  Collapsing those loses nothing.  Only ~542k
    rows sit in groups whose rows actually DISAGREE, and those are the real data
    loss.  Warning on the raw duplicate count would bury the 542k under 6.5M false
    alarms -- and a warning nobody reads is exactly how this bug survived.  So:

    - a duplicate group whose rows are all mutually identical -> lossless dedup,
      not reported;
    - a group containing any two rows that differ -> DESTRUCTIVE; every row it
      drops is counted, including a row that happens to duplicate another inside
      that group (two identical roster rows are still two distinct PEOPLE, and a
      household's size is wrong if you drop one).

    Missing values count as values: two rows that differ only in *whether* a field
    is recorded are different rows.  That is deliberately conservative -- it
    over-reports rather than under-reports, and it is what catches
    ``Burkina_Faso/shocks``, where ``first()`` keeps an all-``<NA>`` row and throws
    away the row that has the real answers.

    ``nan_key_rows`` is a SEPARATE loss riding along in the same operation:
    ``groupby()`` defaults to ``dropna=True``, so a row with NaN in a declared
    index level is DELETED OUTRIGHT by the collapse, not merely merged into it.
    """
    if not levels:
        return None
    try:
        n_nan_key_rows = int(df.index.to_frame().isna().any(axis=1).sum())
    except (TypeError, ValueError):
        n_nan_key_rows = 0

    # If the audit itself cannot run we must NOT return None -- None means
    # "provably lossless", and an instrument that fails silently and reports
    # clean is the exact disease this whole change exists to cure.  Say so
    # instead, loudly, and let the caller decide (strict mode makes it fatal).
    #
    # Cost note: the stringify below is O(rows) and is the expensive part
    # (~3 s on the largest cell, Mali/cluster_features/2021-22 at 4.7M rows).
    # It runs ONLY when the index is already known to be non-unique, and ONLY on
    # the cold build path -- a warm read pays ~1 ms to read the stamp instead.
    try:
        # Stringify to make every column hashable (categoricals, lists, pd.NA)
        # and to make NA compare equal to NA rather than to nothing.
        flat = df.reset_index().astype(str)
        size = flat.groupby(levels, dropna=False, observed=True).size()
        distinct = (flat.drop_duplicates()
                        .groupby(levels, dropna=False, observed=True).size())
    except (TypeError, ValueError, KeyError) as exc:
        return {
            "levels": list(levels),
            "rows": int(len(df)),
            "dropped": int(df.index.duplicated().sum()),
            "destroyed": 0,
            "conflicting_groups": 0,
            "nan_key_rows": n_nan_key_rows,
            "unauditable": f"{type(exc).__name__}: {exc}",
        }

    n_dropped = int(size.sum() - len(size))
    conflicting = distinct.index[distinct > 1]
    n_destroyed = int((size.loc[conflicting] - 1).sum()) if len(conflicting) else 0

    if not n_destroyed and not n_nan_key_rows:
        return None  # provably lossless: nothing to report
    return {
        "levels": list(levels),
        "rows": int(len(df)),
        "dropped": n_dropped,
        "destroyed": n_destroyed,
        "conflicting_groups": int(len(conflicting)),
        "nan_key_rows": n_nan_key_rows,
    }


def _format_grain_report(report: dict[str, Any]) -> str:
    country = report.get("country") or "?"
    table = report.get("table") or "?"
    wave = report.get("wave")
    where = f"{country}/{table}" + (f"/{wave}" if wave else "")
    levels = ", ".join(report.get("levels") or [])
    site = report.get("site")
    duplicated = bool(report.get("dropped") or report.get("destroyed")
                      or report.get("unauditable"))
    # GH #323 Site 2: the household -> cluster projection.  Not a *declared*-index
    # collapse -- the index is deliberately narrowed from (t, v, i) to (t, v) -- so
    # say what actually happened rather than borrowing Site 1's sentence.
    if site == 'Wave.cluster_features':
        bits = [
            f"{where}: cluster_features was projected from HOUSEHOLD grain onto the "
            f"cluster grain ({levels}), and the households in a cluster DISAGREE."
        ]
    elif duplicated:
        bits = [f"{where}: declared index ({levels}) is NOT UNIQUE."]
    else:
        # missing-level-only report: the index is unique, but only because it was
        # silently narrowed.
        bits = [f"{where}: declared index was SILENTLY NARROWED to ({levels})."]
    if report.get("unauditable"):
        bits.append(
            f"The collapse COULD NOT BE AUDITED ({report['unauditable']}), so it is "
            f"NOT known to be safe: {report.get('dropped', 0):,} row(s) were dropped "
            f"by groupby().first() and may carry data. Treat as data loss until shown "
            f"otherwise."
        )
    if report.get("destroyed"):
        bits.append(
            f"Collapsing it with groupby().first() DESTROYED {report['destroyed']:,} "
            f"of {report['rows']:,} rows whose values DISAGREE "
            f"({report['conflicting_groups']:,} conflicting index tuples). "
            f"These rows are gone from the returned data."
        )
    if report.get("missing_levels"):
        bits.append(
            f"Declared index level(s) {report['missing_levels']} are ABSENT from the "
            f"data, so the index was silently narrowed -- that is very likely what "
            f"manufactured these duplicates."
        )
    if report.get("nan_key_rows"):
        bits.append(
            f"Additionally {report['nan_key_rows']:,} row(s) carry NaN in a declared "
            f"index level and are DELETED OUTRIGHT by the collapse (groupby dropna)."
        )
    if site == 'Wave.cluster_features':
        bits.append(
            "cluster_features is reduced with groupby().first(), which skips NA "
            "per column -- so a conflicting cluster does not even yield one of its "
            "households' rows, it yields a COMPOSITE. The comment that used to "
            "license this ('Region/Rural/District are invariant within a cluster by "
            "construction of the LSMS-ISA sampling design') is false here: this "
            "cluster id is NOT unique at the grain it is being used at. Fix the "
            "cluster key (e.g. make v a composite of district+cluster code), do not "
            "declare a reducer: the core does not aggregate "
            "(SkunkWorks/grain_aggregation_policy.org). "
            "Set LSMS_GRAIN_STRICT=1 to make this fatal. GH #323 (Site 2), GH #161."
        )
    else:
        bits.append(
            "Duplicates on a declared index almost always mean the IDENTIFIER IS BROKEN "
            "or an index LEVEL IS MISSING -- fix the index (source a real id, or declare "
            "the level the survey actually varies over). Do NOT declare a reducer: the "
            "core does not aggregate (SkunkWorks/grain_aggregation_policy.org). "
            "Set LSMS_GRAIN_STRICT=1 to make this fatal. GH #323."
        )
    return " ".join(bits)


# Reports produced during a build, keyed by (country, table).  The collapse
# happens deep in a per-wave call but must be stamped into the L2-country parquet
# written much later by a different function, and pandas drops ``df.attrs`` across
# merge/set_index/groupby (see CLAUDE.md) -- so routing the report through attrs
# would silently lose it, which is the very failure mode being fixed here.
_GRAIN_LEDGER: dict[tuple[str, str], list[dict[str, Any]]] = {}


def _record_grain_report(report: dict[str, Any]) -> None:
    """Emit a grain-collapse report and file it for the cache writer."""
    key = (report.get("country") or "?", report.get("table") or "?")
    existing = _GRAIN_LEDGER.setdefault(key, [])
    if report not in existing:
        existing.append(report)
    _emit_grain_report(report)


def _emit_grain_report(report: dict[str, Any]) -> None:
    """Raise (strict) or warn (default).  The single choke point for the signal."""
    msg = _format_grain_report(report)
    if _grain_strict():
        raise GrainCollapseError(msg)
    warnings.warn(msg, GrainCollapseWarning, stacklevel=2)


def grain_reports(country: str | None = None, table: str | None = None) -> list[dict]:
    """Grain-collapse reports filed during this process (GH #323).

    Public read-only accessor, for tests / the audit harness / a user who wants to
    assert their analysis lost nothing.
    """
    out: list[dict[str, Any]] = []
    for (c, t), reports in _GRAIN_LEDGER.items():
        if country is not None and c != country:
            continue
        if table is not None and t != table:
            continue
        out.extend(reports)
    return out


def _replay_grain_audit(reports: Any, country: str, table: str) -> None:
    """Re-emit reports stamped into an L2 parquet by the cold build (GH #323).

    THIS IS THE LINE THAT MAKES THE FIX REAL.  Without it every warning is a
    cold-build-only event, and since practically all reads are warm, the loss goes
    back to being invisible the moment the cache is populated -- which is precisely
    how #323 survived its first fix.
    """
    if not isinstance(reports, list):
        return
    for report in reports:
        if not isinstance(report, dict):
            continue
        report = {**report, "country": country, "table": table, "from_cache": True}
        # _record_ (not just _emit_): grain_reports() must be a reliable account of
        # what a session lost, whether the loss happened during THIS process's cold
        # build or during some earlier one whose parquet we are now serving.
        _record_grain_report(report)


# ---------------------------------------------------------------------------
# GH #323 -- SITE 2: the household -> cluster projection in Wave.cluster_features.
#
# A SECOND, hardcoded grain collapse, entirely separate from the declared-index
# one in `_normalize_dataframe_index` (Site 1).  Countries that declare
# ``i: <HHID>`` in their `cluster_features` idxvars (17 of them, so the YAML can
# merge a household-level GPS frame) hand `Wave.cluster_features` a
# HOUSEHOLD-grain table, which is then reduced to the declared ``(t, v)`` cluster
# grain -- BEFORE `_normalize_dataframe_index` ever sees it.  So Site 1's audit
# cannot see this loss: by the time it runs, the rows are already gone.
#
# The reduction was justified by a comment, and by nothing else:
#
#     "Region/Rural/District are invariant within a cluster by construction of
#      the LSMS-ISA sampling design."
#
# Prose is not enforcement.  The claim fails exactly where a cluster code is
# unique only WITHIN a district (or a region, or an enumeration area): two
# genuinely different clusters collide on one ``v``, and ``.first()`` then keeps
# one of their Regions and throws the other away.  The output is not a lossy
# summary of the input -- it is a WRONG ROW, attributing one cluster's district to
# another's households.  Under Design B (SkunkWorks/grain_aggregation_policy.org:
# NO AGGREGATION IN CORE) the answer is not to teach core a better reducer; it is
# to CHECK the invariant the comment merely asserted, and be loud when it fails.
# Same machinery as Site 1: `_audit_index_collapse` + `_record_grain_report`, so
# the finding is stamped into the L2 parquet and replayed on every warm read.
#
# GPS: THE LAST EXCEPTION, AND IT IS GONE (Ethan, 2026-07-13).
# `Latitude`/`Longitude` used to be reduced with `.mean()` -- a cluster centroid --
# on the theory that household GPS is genuinely per-household and so varies within
# a cluster by design, making it a false positive for the audit and a legitimate
# thing for core to average.  The corpus says otherwise.  Measured across every cell
# where the `.mean()` could fire:
#
#   Malawi 2010-11   768 clusters    0 averaged   <- no-op
#   Malawi 2013-14   204 clusters  188 averaged
#   Malawi 2016-17   881 clusters    0 averaged   <- no-op
#   Malawi 2019-20   819 clusters    0 averaged   <- no-op
#   Niger  2021-22   555 clusters    0 averaged   <- no-op
#
# In FOUR OF FIVE cells the `.mean()` is a provable no-op: the published GPS *is*
# the cluster's (displaced) fix, stamped onto each household -- it was never
# household GPS at all.  In the fifth it averages points a median of 148 km and up
# to 783 km apart, which is not a centroid, it is a BROKEN CLUSTER KEY -- and that
# same cell is already warning for Region (93), District (165) and Rural (131).
# The averaging was not summarising a cluster; it was smearing two clusters
# together and hiding the evidence.
#
# So GPS is now audited and reduced exactly like every other column, and core
# performs NO aggregation here at all.  Measured cost of the flip: ZERO new warning
# cells -- every cell whose count it raises was already warning, and both silent GPS
# cells stay silent.  The NO-AGGREGATION-IN-CORE contract
# (SkunkWorks/grain_aggregation_policy.org) now has no exception left in it.
#
# (An analyst who genuinely wants a cluster centroid computes one -- that is what
# transformations.py is for.  A country whose survey really does record per-household
# GPS has put a household-level column in a cluster-grain table; the fix is to move
# the column, not to teach core to average.)
# ---------------------------------------------------------------------------


def _collapse_to_cluster_grain(
    df: pd.DataFrame,
    keep_levels: list[str],
    country: str | None = None,
    wave: str | None = None,
) -> pd.DataFrame:
    """Project household-grain ``cluster_features`` onto the ``(t, v)`` grain.

    Audits the projection BEFORE performing it -- one line later the evidence is
    gone, and the parquet that gets cached is written from the collapsed frame,
    which is why no downstream instrument (Site 1's audit included) can see this
    loss.  Every column is treated alike: audited for destruction, then reduced
    with ``.first()``.  Core does not aggregate -- not even GPS (see above).

    ``.first()`` here is worse than it looks, and worth naming: pandas'
    ``groupby().first()`` skips NA *per column*, so where households in a cluster
    disagree it does not even return one of the source rows -- it assembles a row
    out of the first non-null value of each column INDEPENDENTLY.  The result can
    be a household composite that exists nowhere in the survey.  Hence: audit.
    """
    if not keep_levels:
        return df

    dropped_levels = [lvl for lvl in df.index.names if lvl not in keep_levels]

    # AUDIT BEFORE DESTROYING -- on a frame from which the levels being PROJECTED
    # AWAY have already been removed.  That removal is load-bearing and is the one
    # subtlety here: ``_audit_index_collapse`` compares WHOLE ROWS via
    # ``reset_index()``, and ``i`` is DISTINCT BY CONSTRUCTION within a cluster.
    # Leave it in and every cluster with two households looks "destructive" --
    # ~100% false positives, and the 11 Uganda clusters that genuinely disagree on
    # Region are buried in the noise.  (Not hypothetical: the first cut of this
    # patch did exactly that, and reported 2,304 destroyed rows for Uganda 2019-20
    # instead of 947.  It was caught only by the test asserting that a LOSSLESS
    # projection stays silent.)
    #
    # What is left is the real question: do the households of one cluster disagree
    # about the CLUSTER'S OWN attributes?  If they do, the cluster id is not a
    # cluster id.
    audit_frame = df.droplevel(dropped_levels) if dropped_levels else df
    report = _audit_index_collapse(audit_frame, keep_levels)
    if report is not None:
        report.update(country=country, table='cluster_features', wave=wave,
                      site='Wave.cluster_features')
        _record_grain_report(report)

    return df.groupby(level=keep_levels, observed=True).first()


@build_transform()
def _normalize_dataframe_index(
    df: pd.DataFrame,
    schema_entry: dict[str, Any],
    wave: str | None,
    table_name: str | None = None,
    country: str | None = None,
) -> pd.DataFrame:
    """
    Reorder and reduce a dataframe's MultiIndex to match the declared schema.

    - Reorders index levels to match the declared order.
    - Drops unexpected index levels.
    - Synthesizes missing 't' levels for wave-specific tables.
    - Collapses duplicate entries: SUMs the additive measure columns for
      tables in ``_ADDITIVE_MEASURE_COLUMNS`` (``table_name``), else keeps the
      first row per group (the historical default).
    - GH #323: AUDITS that collapse first, while the pre-collapse frame still
      exists, and reports any destroyed rows loudly (or fatally, under
      ``LSMS_GRAIN_STRICT``).  ``country`` is carried only so the report can name
      the cell; it does not affect the transformation.
    """

    if not isinstance(df, pd.DataFrame):
        return df

    declared = _declared_index_levels(schema_entry)
    if not declared:
        return df

    current_names = list(df.index.names)

    # Undo map_index()'s legacy j->i swap when the schema actually declares 'j'.
    # map_index() renames a 'j' index level to 'i' whenever a table has no
    # household 'i' level (a legacy old-parquet convention).  For a cluster-
    # level item feature (e.g. community_prices, index (t, v, j, u) with no
    # household i), that rename turns the declared 'j' into an *undeclared* 'i',
    # which the level-drop + duplicate-collapse below then discards entirely --
    # silent data loss (the food item collapses, leaving one price per cluster).
    # When the schema wants 'j' (not 'i') and the frame carries the swapped 'i'
    # but no 'j', restore the name so the level survives.  Household tables
    # (declared 'i') and tables declaring both i and j are untouched.
    if ('j' in declared and 'i' not in declared
            and 'i' in current_names and 'j' not in current_names):
        df = df.rename_axis(index={'i': 'j'})
        current_names = list(df.index.names)

    # Add synthetic levels when declared but missing (e.g., 't' for wave outputs)
    missing_levels = [lvl for lvl in declared if lvl not in current_names]
    absent_levels: list[str] = []
    if missing_levels:
        df = df.reset_index()
        for level in missing_levels:
            if level == "t" and wave is not None:
                df[level] = wave
        # Only set_index with declared levels that actually exist in the DataFrame
        available = [lvl for lvl in declared if lvl in df.columns]
        # GH #323: a declared level that is NEITHER an index level NOR a column is
        # silently dropped here -- the index is narrowed behind the caller's back,
        # which MANUFACTURES the duplicate tuples that the collapse below then
        # destroys.  Two chained silent failures.  Record it so the collapse report
        # can name it as the root cause (and so it is loud even when, by luck, the
        # narrowed index stays unique).  Measured occurrences today: zero.
        absent_levels = [lvl for lvl in declared if lvl not in df.columns]
        if available:
            df = df.set_index(available)
        current_names = list(df.index.names)

    # Report the narrowing even when the narrowed index happens to stay UNIQUE --
    # a silently narrowed index is a defect regardless of whether it also
    # manufactured duplicates this time.  (When it DID manufacture them, the
    # collapse report below names these levels as the likely root cause.)
    if absent_levels and df.index.is_unique:
        _record_grain_report({
            "levels": [lvl for lvl in declared if lvl in df.index.names],
            "rows": int(len(df)), "dropped": 0, "destroyed": 0,
            "conflicting_groups": 0, "nan_key_rows": 0,
            "missing_levels": absent_levels,
            "country": country, "table": table_name, "wave": wave,
        })

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
        # Convert unordered categoricals to strings so the groupby below works
        for col in df.columns:
            if hasattr(df[col], 'cat') and not df[col].cat.ordered:
                df[col] = df[col].astype(str).replace({'nan': pd.NA, 'None': pd.NA, '<NA>': pd.NA})
        # GH #514/#323: collapsing a non-unique canonical index with .first()
        # silently DISCARDS the dropped rows.  For additive-measure tables
        # (food_acquired, whose source legitimately records the same item across
        # several transactions per (t,v,i,j,u,s)) SUM the additive columns and
        # re-derive any per-unit Price from the summed totals -- no data lost.
        # Single source of truth for the additive column map lives in feature.py
        # (imported lazily to avoid an import cycle).
        from .feature import _ADDITIVE_MEASURE_COLUMNS
        additive = _ADDITIVE_MEASURE_COLUMNS.get(table_name) if table_name else None
        present_additive = [c for c in (additive or ()) if c in df.columns]

        # GH #323: AUDIT BEFORE DESTROYING.  This is the only moment at which the
        # evidence exists -- one line further down the dropped rows are gone, and
        # the parquet we cache is written from the collapsed frame, which is why
        # every previous instrument (the old warning, diagnostics'
        # _check_duplicate_index, any scan of var/) reported "clean".
        report = _audit_index_collapse(df, present_levels)
        if report is not None and present_additive and not report.get("unauditable"):
            # The additive SUM is lossless over the measure columns, so a
            # disagreement among them is expected and is NOT destruction.  Only a
            # NaN-key deletion (groupby drops those rows outright) is real loss here.
            # An UNAUDITABLE report is never silenced here -- "we could not check"
            # must not be downgraded to "it is fine".
            report = (dict(report, destroyed=0, conflicting_groups=0)
                      if report.get("nan_key_rows") else None)

        if present_additive:
            agg = {c: ('sum' if c in present_additive else 'first') for c in df.columns}
            df = df.groupby(level=present_levels, observed=True).agg(agg)
            if 'Price' in df.columns and {'Expenditure', 'Quantity'} <= set(df.columns):
                df['Price'] = df['Expenditure'] / df['Quantity'].where(df['Quantity'] != 0)
        else:
            df = df.groupby(level=present_levels, observed=True).first()

        if report is not None:
            report.update(country=country, table=table_name, wave=wave,
                          missing_levels=absent_levels or None,
                          additive=bool(present_additive))
            _record_grain_report(report)

    return df
