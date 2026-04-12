"""LSMS Library: a harmonized interface to Living Standards Measurement Study data.

The library provides a uniform API across 40+ LSMS-ISA and related
household surveys from the World Bank Microdata Library.  Its design
principle is to harmonize the *interface*, not the data: surveys differ
in structure, and the library preserves survey-specific detail while
offering a common way to read it.

Typical usage
-------------
Instantiate a :class:`Country` and call its table methods as attributes.
Table names (``food_acquired``, ``household_roster``, ``shocks``, ...)
come from the country's ``data_scheme.yml``.

.. code-block:: python

    import lsms_library as ll

    uga = ll.Country('Uganda')
    uga.waves              # ['2005-06', '2009-10', ...]
    uga.data_scheme        # ['household_roster', 'food_acquired', ...]

    roster = uga.household_roster()    # harmonized DataFrame across waves
    food   = uga.food_expenditures()   # derived from food_acquired
    chars  = uga.household_characteristics()  # derived from household_roster

:class:`Feature` assembles the same table across every country that
declares it, with a ``country`` index level prepended:

.. code-block:: python

    df = ll.Feature('household_roster')()               # every country
    df = ll.Feature('shocks')(['Ethiopia', 'Niger'])    # subset

Returned DataFrames are indexed by some subset of ``(country, t, v, i,
pid, j)`` — wave (``t``), cluster (``v``), household (``i``), person
(``pid``), item (``j``) — depending on the table.

Public API
----------
- :class:`Country` — top-level per-country interface. Exposes ``waves``,
  ``data_scheme``, and one method per table in the scheme. Derived
  tables (``household_characteristics``, ``food_expenditures``,
  ``food_prices``, ``food_quantities``) are dispatched via
  ``__getattr__`` without needing to be in ``data_scheme.yml``.
- :class:`Wave` — single-wave view of a country, used internally by
  :class:`Country` but occasionally handy for debugging.
- :class:`Feature` — cross-country aggregator for a given table name.
- ``tools`` (alias for :mod:`lsms_library.local_tools`) — ``get_dataframe``
  for reading ``.dta``/``.csv``/``.parquet`` with local → DVC → WB NADA
  fallback, ``to_parquet`` for writing caches under ``data_root()``, and
  the ``df_data_grabber`` helper used by wave-level scripts.
- :func:`authenticate` — interactive DVC/S3 credential unlock. Usually
  runs automatically on import when a World Bank Microdata API key is
  available.
- :mod:`lsms_library.transformations` — derivation helpers for
  roster-to-characteristics and food-to-expenditures.

Data access and caching
-----------------------
Reads use a fallback chain: local file on disk → DVC filesystem → World
Bank NADA download. Parquet caches are written under ``data_root()``,
which defaults to ``~/.local/share/lsms_library/{Country}/`` and can be
overridden via ``LSMS_DATA_DIR`` or the ``data_dir`` key in
``~/.config/lsms_library/config.yml``.

As of v0.7.0, :meth:`Country` methods check the parquet cache before
consulting DVC, giving 10–17× cross-process speedups. The cache has no
automatic staleness check — set ``LSMS_NO_CACHE=1`` or run
``lsms-library cache clear --country {Country}`` after editing source
configs or scripts.

Environment variables
---------------------
- ``MICRODATA_API_KEY`` — World Bank Microdata Library API key (also
  readable from ``~/.config/lsms_library/config.yml``). Required for
  direct downloads; also unlocks the S3 read cache at import time.
- ``LSMS_DATA_DIR`` — override the parquet cache root.
- ``LSMS_NO_CACHE`` — disable the v0.7.0 top-of-function cache read.
- ``LSMS_BUILD_BACKEND=make`` — force rebuild from source, bypassing
  both the cache and the DVC stage layer.
- ``LSMS_SKIP_AUTH`` — suppress the import-time authentication attempt.

Further reading
---------------
- ``CLAUDE.md`` at the repository root for contributor-facing design
  notes, the canonical schema (``lsms_library/data_info.yml``), and the
  list of cross-cutting gotchas.
- ``.claude/skills/add-feature/`` for adding a new table to a country,
  with sub-skills for ``sample``, ``shocks``, ``assets``,
  ``panel-ids``, ``food-acquired``, and ``pp-ph`` (post-planting /
  post-harvest countries).
- ``CONTRIBUTING.org`` for the new-wave workflow.

Data terms of use
-----------------
Underlying microdata must be obtained from the World Bank Microdata
Library (https://microdata.worldbank.org/) under their terms of use.
The library's S3 bucket is a read cache over the authoritative WB NADA
downloads, unlocked automatically when a valid API key is present.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("LSMS_Library")
except PackageNotFoundError:
    __version__ = "0.0.0"

from pathlib import Path
import os
import warnings

import logging
from sys import stderr
from . import country
from .country import Country, Wave
from .feature import Feature
from . import local_tools as tools
from . import transformations
from .dvc_permissions import authenticate
try:
    from dvc.ui import ui as dvc_ui
    from dvc.logger import LoggerHandler, setup as dvc_log_setup
    from functools import wraps
    import sys

    if hasattr(dvc_ui, "rich_console"):
        try:
            dvc_ui.rich_console.file = stderr
        except Exception:
            pass
    if hasattr(dvc_ui, "error_console"):
        try:
            dvc_ui.error_console.file = stderr
        except Exception:
            pass
    original_init = LoggerHandler.__init__

    @wraps(original_init)
    def _patched_logger_handler_init(self, stream, *args, **kwargs):
        if stream is sys.stdout:
            stream = stderr
        original_init(self, stream, *args, **kwargs)

    LoggerHandler.__init__ = _patched_logger_handler_init  # type: ignore[assignment]

    dvc_log_setup()
    for logger_name in ("dvc", "dvc_objects", "dvc_data", ""):
        logger_obj = logging.getLogger(logger_name)
        for handler in logger_obj.handlers:
            if isinstance(handler, LoggerHandler) and getattr(handler, "stream", None) is not stderr:
                handler.stream = stderr
except Exception:
    # DVC optional; if missing, proceed without UI tweaks
    pass

from .config import s3_creds_path as _s3_creds_path
creds_file = _s3_creds_path()

SKIP_AUTH = os.getenv("LSMS_SKIP_AUTH", "").lower() in {"1", "true", "yes"}

if not SKIP_AUTH and not creds_file.exists():
    # Try non-interactive S3 unlock first.  This only decrypts the local
    # s3_reader_creds.gpg — no network call required.  WB API key validation
    # (which requires a network round-trip to microdata.worldbank.org) is
    # deferred until the first actual data access via permissions() in
    # data_access.py.  This keeps ``import lsms_library`` fast.
    try:
        from .data_access import _auto_unlock_s3 as _unlock
        _unlocked = _unlock()
        if not _unlocked:
            # Local GPG decryption failed or no .gpg file — fall back to
            # interactive passphrase prompt.
            raise RuntimeError("auto-unlock did not produce S3 credentials")
    except Exception:
        try:
            authenticate()
        except Exception as exc:
            warnings.warn(
                f"Automatic DVC authentication failed: {exc}. "
                "Set LSMS_SKIP_AUTH=1 to suppress, or set "
                "MICRODATA_API_KEY for non-interactive access."
            )
