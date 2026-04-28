"""Project-level pytest configuration.

Three-tier test strategy
------------------------
- **Fast tier** (default ``pytest``): uses both L1 (country-level) and
  L2 (wave-level) parquet caches at ``~/.local/share/lsms_library/``.
  Tests the API surface and ``_finalize_result()`` transformations
  but does NOT exercise the wave-level build pipeline.
- **Soft rebuild** (``pytest --rebuild`` or ``LSMS_NO_CACHE=1 pytest``):
  sets ``LSMS_NO_CACHE=1``.  The framework bypasses L1 reads and L2
  reads on the YAML path, but **script-path L2 parquets** (written by
  ``_/{table}.py`` scripts via ``local_tools.to_parquet()``) are read
  through ``run_make_target`` which does not consult the env var, so
  stale wave parquets can still shadow source-script fixes.  Use this
  tier when you want a moderately cold rebuild but trust the L2 wave
  caches to be fresh.
- **Hard rebuild** (``pytest --rebuild-caches``): physically deletes
  every ``data_root()`` cache file for every country we have a checkout
  for, then sets ``LSMS_NO_CACHE=1``.  This is the only tier that
  catches the failure mode where a source script fix has shipped but
  the wave-level L2 parquet predates it (Nigeria/Senegal age sentinel
  case, 2026-04-25).

Use ``make test`` for the fast tier and ``make test-full`` (now passes
``--rebuild-caches``) for a fully cold rebuild.
"""

from __future__ import annotations

import os
import shutil
import warnings


def pytest_addoption(parser):
    parser.addoption(
        "--rebuild",
        action="store_true",
        default=False,
        help="Soft cold-cache rebuilds (sets LSMS_NO_CACHE=1; does not "
             "delete script-path L2 wave parquets).",
    )
    parser.addoption(
        "--rebuild-caches",
        action="store_true",
        default=False,
        help="Hard cold-cache rebuilds: physically delete every L1 and "
             "L2 parquet under data_root() before the session, then set "
             "LSMS_NO_CACHE=1.  Use this when a source-script fix has "
             "shipped but cached parquets predate it.",
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

    rebuild_caches = config.getoption("--rebuild-caches", default=False)
    if rebuild_caches:
        _purge_data_root_caches()
        os.environ["LSMS_NO_CACHE"] = "1"
    elif config.getoption("--rebuild", default=False):
        os.environ["LSMS_NO_CACHE"] = "1"
    else:
        # Always clear Uganda's parquet cache at session start.  v0.7.1
        # added country-level ``v`` and ``District`` formatters to
        # Uganda/_/uganda.py and a framework-level grain fix in
        # ``Country.cluster_features()``; both run only at extraction
        # time, so cached parquets predating those changes mask the
        # fix and produce spurious test failures (float-stringified v,
        # missing Region column, household-grain cluster_features).
        # The cost of a fresh build is small (~5-10s for Uganda's
        # tables) and runs in the master process before xdist workers
        # spawn, so there's no race.  Tracked as part of GH #196 /
        # #197 / #161.  A no-op on a clean tree.
        _purge_country_caches("Uganda")


def _purge_country_caches(country_name: str) -> None:
    """Targeted single-country variant of :func:`_purge_data_root_caches`.

    Used as a session-start hook to flush parquets that predate a
    recently-shipped extraction-layer change.  Idempotent on a clean
    tree.  Failures are warnings, not errors --- a missing data tree
    or import failure shouldn't stop the test session.
    """
    try:
        from lsms_library.paths import COUNTRIES_ROOT
        if not (COUNTRIES_ROOT / country_name / "_" / "data_scheme.yml").exists():
            return
        from lsms_library import Country
        Country(country_name, verbose=False).clear_cache()
    except Exception as exc:  # noqa: BLE001
        warnings.warn(
            f"_purge_country_caches({country_name!r}) failed ({exc!r}); "
            f"tests may see stale parquets.  Run "
            f"`lsms-library cache clear --country {country_name}` manually."
        )


def _purge_data_root_caches() -> None:
    """Delete every parquet/json under ``data_root()`` for every cached
    country.

    Uses ``Country.clear_cache`` per country so the L2 wave-level
    sweep (and the existing CLI logic) stays in one place.  Falls back
    to a recursive parquet/json glob if the Country construction fails
    (avoids a bad-config ever blocking a rebuild).

    Skips non-country directories that may live alongside real
    countries under ``data_root()`` — e.g. ``TestCountry`` fixtures
    or ``dvc-cache`` artefacts — by checking for the canonical
    ``data_scheme.yml`` under ``lsms_library/countries/{name}/_/``.
    """
    from lsms_library.paths import COUNTRIES_ROOT, data_root

    root = data_root()
    if not root.exists():
        return

    purged = 0
    for country_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        country_name = country_dir.name
        # Skip directories that aren't real LSMS countries.
        if not (COUNTRIES_ROOT / country_name / "_" / "data_scheme.yml").exists():
            continue

        try:
            from lsms_library import Country  # lazy: heavy import

            removed = Country(country_name, verbose=False).clear_cache()
            purged += len(removed)
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"--rebuild-caches: Country({country_name!r}).clear_cache "
                f"failed ({exc!r}); falling back to recursive parquet sweep."
            )
            for path in country_dir.rglob("*.parquet"):
                path.unlink()
                purged += 1
            for path in country_dir.rglob("*.json"):
                path.unlink()
                purged += 1

    if purged:
        print(f"\n[--rebuild-caches] purged {purged} cache files under {root}")
