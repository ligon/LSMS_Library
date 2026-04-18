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
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Any


def _config_dir() -> Path:
    """Return the user config directory for lsms_library.

    Honors the ``LSMS_CONFIG_DIR`` environment variable if set, allowing
    users to relocate the config directory (e.g. into a synced /
    git-crypted dotfiles repo) without changing global ``XDG_CONFIG_HOME``.
    Falls back to ``platformdirs.user_config_path()`` (which itself
    honors ``XDG_CONFIG_HOME``), or a hardcoded default if platformdirs
    is unavailable.
    """
    override = os.environ.get("LSMS_CONFIG_DIR", "").strip()
    if override:
        return Path(override).expanduser()
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
    import yaml
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}
    except (yaml.YAMLError, OSError) as exc:
        warnings.warn(
            f"Ignoring malformed config at {path}: {exc}",
            stacklevel=2,
        )
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


def s3_creds_path() -> Path:
    """Return the user-writable path for decrypted S3 credentials.

    Precedence: ``$LSMS_S3_CREDS`` env var → ``<config_dir>/s3_creds``.
    Does NOT create the file or its parent directory.

    This path is where :func:`lsms_library.data_access._auto_unlock_s3`
    writes the plaintext S3 reader credentials after decrypting
    ``s3_reader_creds.gpg`` with the obfuscated passphrase, and where
    :func:`lsms_library.dvc_permissions.authenticate` writes them in
    the interactive fallback.  Moving the write target out of the
    package tree makes the library safe to install into a read-only
    site-packages directory (e.g. a pip-installed wheel).
    """
    override = os.environ.get("LSMS_S3_CREDS", "").strip()
    if override:
        return Path(override).expanduser()
    return _config_dir() / "s3_creds"
