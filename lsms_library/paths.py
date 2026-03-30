"""
Centralized data-path resolution for LSMS Library.

All materialized data (parquets, JSON caches) is written under a
platform-appropriate user-data directory, keeping the installed package
tree read-only.

Resolution order for the data root:
  1. ``LSMS_DATA_DIR`` environment variable (explicit override)
  2. ``platformdirs.user_data_path("lsms_library")`` (per-user default)

The ``var_path`` helper maps a bare table filename to the correct
country ``var/`` subdirectory by inspecting the caller's location in
the source tree.
"""

from __future__ import annotations

import inspect
import os
from functools import lru_cache
from pathlib import Path


COUNTRIES_ROOT = Path(__file__).resolve().parent / "countries"


@lru_cache(maxsize=None)
def data_root(country: str | None = None) -> Path:
    """Return the root directory for materialized data.

    Parameters
    ----------
    country : str, optional
        If given, append the country subdirectory.

    Returns
    -------
    Path
    """
    from . import config as _config
    _override = _config.data_dir()
    if _override:
        base = Path(_override)
    else:
        try:
            import platformdirs
            base = platformdirs.user_data_path("lsms_library")
        except ImportError:
            # Fallback when platformdirs is not installed
            base = Path.home() / ".local" / "share" / "lsms_library"
    return base / country if country else base


def _caller_country_and_wave(stack_depth: int = 2) -> tuple[str | None, str | None]:
    """Inspect the call stack to infer country and wave from the caller's file path.

    Returns (country, wave) where wave may be None for country-level scripts.
    """
    try:
        frame = inspect.stack()[stack_depth]
        caller = Path(frame.filename).resolve()
    except (IndexError, ValueError):
        return None, None

    try:
        rel = caller.relative_to(COUNTRIES_ROOT)
    except ValueError:
        return None, None

    parts = rel.parts  # e.g. ('Uganda', '_', 'food_acquired.py')
                        #   or ('Uganda', '2005-06', '_', 'shocks.py')
    if len(parts) < 2:
        return None, None

    country = parts[0]

    # Wave-level: Country/YYYY-YY/_/script.py
    if len(parts) >= 3 and parts[1] != "_":
        wave = parts[1]
        return country, wave

    return country, None


def var_path(name: str, country: str | None = None, wave: str | None = None) -> Path:
    """Resolve a table filename to its ``var/`` location under data_root.

    When *country* is not given, it is inferred from the calling script's
    position in the source tree.

    Parameters
    ----------
    name : str
        Bare filename, e.g. ``"food_acquired.parquet"``.
    country : str, optional
        Explicit country name.
    wave : str, optional
        Explicit wave (unused for country-level ``var/``; reserved for future use).

    Returns
    -------
    Path
    """
    if country is None:
        country, wave = _caller_country_and_wave(stack_depth=2)
    if country is None:
        # Cannot resolve; return the legacy relative path
        return Path("../var") / name
    return data_root(country) / "var" / name


def wave_data_path(name: str, country: str | None = None, wave: str | None = None) -> Path:
    """Resolve a bare filename for a wave-level parquet.

    Wave-level scripts write parquets like ``shocks.parquet`` into their own
    directory.  This helper redirects to the data tree.

    Parameters
    ----------
    name : str
        Bare filename, e.g. ``"shocks.parquet"``.
    country, wave : str, optional
        If not given, inferred from the call stack.

    Returns
    -------
    Path
    """
    if country is None or wave is None:
        inferred_country, inferred_wave = _caller_country_and_wave(stack_depth=2)
        country = country or inferred_country
        wave = wave or inferred_wave
    if country is None:
        return Path(name)
    if wave is None:
        return data_root(country) / "_" / name
    return data_root(country) / wave / "_" / name
