"""
Centralized data-path resolution for LSMS Library.

All materialized data (parquets, JSON caches) is written under a
user-data directory, keeping the installed package tree read-only.

Resolution order for the data root:
  1. ``LSMS_DATA_DIR`` environment variable (explicit override)
  2. ``data_dir`` in ``~/.config/lsms_library/config.yml``
  3. ``~/.local/share/lsms_library`` (XDG-style default, all platforms)

The default is deliberately XDG-style on every platform rather than the
OS-native location that ``platformdirs.user_data_path()`` would pick.
On macOS, that function returns ``~/Library/Application Support/lsms_library``
— a path containing a literal space that GNU make cannot parse, because
make splits target names on whitespace.  Several wave-level builds shell
out to ``make`` (see ``country.py::run_make_target``), so a space in
``data_root()`` would break every Makefile-backed feature (Uganda
``food_expenditures``, Nigeria / GhanaLSS ``food_acquired``, etc.).
Using ``~/.local/share/lsms_library`` everywhere keeps the path
space-free and matches the location documented in ``CLAUDE.md``.

The ``var_path`` helper maps a bare table filename to the correct
country ``var/`` subdirectory by inspecting the caller's location in
the source tree.
"""

from __future__ import annotations

import inspect
import os
import warnings
from functools import lru_cache
from pathlib import Path


COUNTRIES_ROOT = Path(__file__).resolve().parent / "countries"


_WHITESPACE_WARNED: set[str] = set()


def _warn_on_whitespace(base: Path, source: str) -> None:
    """Warn once per path when ``data_root`` resolves to a path with whitespace.

    GNU make splits target names on whitespace, so every Makefile-backed
    wave build (Uganda ``food_expenditures``, etc.) fails when the data
    root contains a space.  We can't stop the user from pointing
    ``LSMS_DATA_DIR`` at ``/Users/alice/My Data/``, but we can surface
    the problem loudly instead of letting make emit an opaque flood of
    "overriding recipe for target" warnings.
    """
    key = str(base)
    if " " in key and key not in _WHITESPACE_WARNED:
        _WHITESPACE_WARNED.add(key)
        warnings.warn(
            f"LSMS Library data root {base!s} contains whitespace "
            f"(source: {source}).  GNU make cannot handle targets with "
            "spaces, so Makefile-backed wave builds (e.g. Uganda "
            "food_expenditures) will fail.  Point `LSMS_DATA_DIR` or "
            "`data_dir` in ~/.config/lsms_library/config.yml at a "
            "space-free directory such as ~/.local/share/lsms_library.",
            RuntimeWarning,
            stacklevel=3,
        )


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
        _warn_on_whitespace(base, "LSMS_DATA_DIR or config.yml data_dir")
    else:
        # XDG-style default on every platform.  See module docstring for
        # why we don't use ``platformdirs.user_data_path`` here (macOS
        # returns ``~/Library/Application Support/lsms_library`` — a
        # path with a space that breaks GNU make).
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
