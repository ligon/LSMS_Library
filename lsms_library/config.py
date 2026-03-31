"""User configuration for LSMS Library.

Reads settings from a YAML file in the platform-appropriate user config
directory, with environment variables taking precedence.

Config file location (Linux example)::

    ~/.config/lsms_library/config.yml

Contents::

    microdata_api_key: your_key_here
    # data_dir: /path/to/override     # same as LSMS_DATA_DIR env var

Lookup order for each setting: environment variable → config file → None.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any


def _config_dir() -> Path:
    """Return the user config directory for lsms_library."""
    try:
        import platformdirs
        return platformdirs.user_config_path("lsms_library")
    except ImportError:
        return Path.home() / ".config" / "lsms_library"


def _config_file() -> Path:
    return _config_dir() / "config.yml"


@lru_cache(maxsize=1)
def _load_config() -> dict[str, Any]:
    """Load the config file, returning an empty dict if absent."""
    path = _config_file()
    if not path.exists():
        return {}
    try:
        import yaml
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get(key: str, *, env_var: str | None = None, default: Any = None) -> Any:
    """Look up a config value: env var → config file → default.

    Parameters
    ----------
    key : str
        Key in the config file (e.g. ``"microdata_api_key"``).
    env_var : str, optional
        Environment variable name to check first.  If not given,
        derived from *key* by upper-casing (e.g. ``MICRODATA_API_KEY``).
    default : Any
        Value returned when the key is not found anywhere.
    """
    if env_var is None:
        env_var = key.upper()
    val = os.environ.get(env_var, "").strip()
    if val:
        return val
    return _load_config().get(key, default)


# Convenience accessors for common settings

def microdata_api_key() -> str | None:
    """Return the World Bank Microdata Library API key, or None."""
    return get("microdata_api_key") or None


def data_dir() -> str | None:
    """Return the data directory override, or None."""
    return get("data_dir", env_var="LSMS_DATA_DIR") or None


def config_path() -> Path:
    """Return the path to the config file (may not exist yet)."""
    return _config_file()
