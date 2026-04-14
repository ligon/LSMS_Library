"""Project-level pytest configuration.

Two-tier test strategy
----------------------
- **Fast tier** (default ``pytest``): uses the L2 parquet cache at
  ``~/.local/share/lsms_library/``.  Tests the API surface and
  ``_finalize_result()`` transformations but does NOT exercise the
  wave-level build pipeline.
- **Full rebuild tier** (``pytest --rebuild`` or
  ``LSMS_NO_CACHE=1 pytest``): forces every table to rebuild from
  ``.dta`` source through ``load_from_waves``.  Catches regressions in
  ``data_info.yml``, formatting functions, and wave scripts.

Use ``make test`` for the fast tier and ``make test-full`` for the
rebuild tier.
"""

import os

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--rebuild",
        action="store_true",
        default=False,
        help="Force cold-cache rebuilds (sets LSMS_NO_CACHE=1).",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "rebuild: mark test as requiring a cold-cache rebuild from source.",
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow (requires network or full data loading).",
    )
    if config.getoption("--rebuild", default=False):
        os.environ["LSMS_NO_CACHE"] = "1"
