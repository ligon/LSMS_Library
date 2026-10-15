"""Project-level pytest configuration.

Three-tier test strategy
------------------------
- **Fast tier** (default ``pytest``): uses both L1 (country-level) and
  L2 (wave-level) parquet caches at ``~/.local/share/lsms_library/``.
  Tests the API surface and ``_finalize_result()`` transformations
  but does NOT exercise the wave-level build pipeline.  As a session-
  start safety hook, Uganda's L1+L2+in-tree caches are purged each run
  (Uganda is where extraction-layer iteration happens; staleness here
  has bitten GH #196 / #197 / #161).  Cost: ~5-15 min per session for
  Uganda's table rebuilds.
- **Skip-purge sub-mode** (``pytest --no-purge``): same as the fast
  tier but without the Uganda auto-purge.  Use during iterative
  diagnosis loops where the failures under investigation are known
  not to be Uganda-extraction-related (e.g. framework bugs, import
  errors, test-expectation tweaks).  Re-run without ``--no-purge``
  for the final pre-merge / pre-release verification.
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

**Collection is a dry run** (GH #719).  Under ``--collect-only`` / ``--co`` no
purge of any tier runs, because such an invocation executes no tests and must
therefore have no side effects.  This matters beyond tidiness: the purge target
resolves through ``data_root()``, which is a *shared* scratch path, so asking
"what tests exist?" used to evict parquets belonging to other sessions -- and
under ``--rebuild-caches`` it took every country, not just Uganda.  The purge
itself is unchanged and still fires on every real run.
"""

from __future__ import annotations

import os
import shutil
import warnings


def _addoption_once(parser, *names, **kwargs):
    """``parser.addoption`` that tolerates a second registration (GH #734).

    Inside a git worktree the main checkout's ``lsms_library.pth`` puts the
    main checkout on ``sys.path`` too, so pytest loads BOTH root
    ``conftest.py`` files under the same module name and this hook runs
    twice -- the second ``addoption`` raised ``ValueError: option names
    {'--rebuild'} already added`` and no test could run from a worktree.
    The option already exists with the same meaning, so the duplicate is a
    no-op.  Only the duplicate-name ValueError is swallowed; the message is
    checked so a genuine argparse error still propagates.
    """
    try:
        parser.addoption(*names, **kwargs)
    except ValueError as e:
        if "already added" not in str(e):
            raise


def pytest_addoption(parser):
    _addoption_once(
        parser,
        "--rebuild",
        action="store_true",
        default=False,
        help="Soft cold-cache rebuilds (sets LSMS_NO_CACHE=1; does not "
             "delete script-path L2 wave parquets).",
    )
    _addoption_once(
        parser,
        "--rebuild-caches",
        action="store_true",
        default=False,
        help="Hard cold-cache rebuilds: physically delete every L1 and "
             "L2 parquet under data_root() before the session, then set "
             "LSMS_NO_CACHE=1.  Use this when a source-script fix has "
             "shipped but cached parquets predate it.",
    )
    _addoption_once(
        parser,
        "--no-purge",
        action="store_true",
        default=False,
        help="NO-OP since 2026-09-22.  The default-mode Uganda auto-purge it "
             "suppressed has been retired (v0.8.0's content hash and GH #803 "
             "do both of its jobs, and it silently excluded Uganda from "
             "tests/test_table_structure.py and hollowed out "
             "tests/test_uganda_invariance.py).  Still accepted so existing "
             "invocations keep working.  --rebuild-caches still purges.",
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

    if getattr(config.option, "collectonly", False):
        # GH #719: collection is a DRY RUN.  ``--collect-only`` (and ``--co``)
        # answers "what tests exist?" and executes none of them, so it must
        # have no side effects -- yet every branch below deletes cached
        # parquets out of ``data_root()``.
        #
        # That path is resolved from *config*, so on a shared checkout it is
        # shared state: since the 2026-08-22 migration it points at the FCA
        # scratch cache used by other sessions and other `fc_jevons` members,
        # not at a private `~/.local/share`.  A `--collect-only` run would
        # evict their warm parquets, and with `--rebuild-caches` it would take
        # *every country*, not just Uganda.
        #
        # The purge itself is right and is deliberately left alone (stale
        # parquets predating the v0.7.1 extraction fixes really do mask them --
        # GH #196 / #197 / #161).  Only the TRIGGER is wrong.  `--no-purge` is
        # not the answer here: it asks the operator to opt out of a correct
        # default to dodge an incorrect side effect.
        #
        # Markers are registered above, before this return, because collection
        # needs them.
        return

    rebuild_caches = config.getoption("--rebuild-caches", default=False)
    if rebuild_caches:
        _purge_data_root_caches()
        os.environ["LSMS_NO_CACHE"] = "1"
    elif config.getoption("--rebuild", default=False):
        os.environ["LSMS_NO_CACHE"] = "1"
    elif config.getoption("--no-purge", default=False):
        # Iterative-diagnosis escape hatch.  Skip the Uganda auto-purge
        # when the operator knows the failure under investigation is not
        # Uganda-extraction-related.  Restore (re-run without the flag)
        # before merging to master / cutting a release.
        pass
    else:
        # RETIRED 2026-09-22.  This branch used to run
        # ``_purge_country_caches("Uganda")`` on every session.
        #
        # Its two jobs are both done by the library now:
        #
        #   1. "cached parquets predating an extraction-time change mask the
        #      fix" (v0.7.1, GH #196/#197/#161) -- v0.8.0's content hash does
        #      this for EVERY country automatically.  ``Country._table_cache_hash``
        #      folds the country module, so editing ``Uganda/_/uganda.py`` moves
        #      the hash and the parquet grades stale.  Measured 2026-09-22:
        #      58feecca8a82 -> 745cc5a5f599 on a one-line edit.
        #   2. deleting in-tree ``Uganda/var/*.parquet`` -- GH #803 made the
        #      library refuse to read an in-tree parquet at all
        #      (``InTreeParquetWarning``, country.py:645).  There are none.
        #
        # What it cost, which its own comment never stated and
        # ``Uganda/_/CONTENTS.org`` recorded as a TODO on 2026-08-20: the purge
        # runs in ``pytest_configure``, BEFORE collection, and
        # ``tests/test_table_structure.py`` parametrizes over the
        # ``var/<table>.parquet`` files that exist at collection time.  So
        # Uganda -- 18 script-path tables, the most idiosyncratic country in the
        # corpus -- was absent from that module on every run.  Worse,
        # ``tests/test_uganda_invariance.py`` only READS parquets, so the purge
        # left all 53 of its entries skipping unless some alphabetically earlier
        # module happened to rebuild the table it pins.  A regression net whose
        # coverage depends on test ordering is not a regression net.
        #
        # ``--rebuild-caches`` still purges everything: that is an explicit
        # request, not a default.  ``--no-purge`` is kept as an accepted no-op
        # so existing invocations do not start erroring.
        pass


def _purge_country_caches(country_name: str) -> None:
    """Targeted single-country variant of :func:`_purge_data_root_caches`.

    Used as a session-start hook to flush parquets that predate a
    recently-shipped extraction-layer change.  Idempotent on a clean
    tree.  Failures are warnings, not errors --- a missing data tree
    or import failure shouldn't stop the test session.
    """
    try:
        from lsms_library.paths import countries_root
        country_dir = countries_root() / country_name
        if not (country_dir / "_" / "data_scheme.yml").exists():
            return
        from lsms_library import Country
        Country(country_name, verbose=False).clear_cache()
        # Also delete any **in-tree** ``{country}/var/*.parquet`` build
        # artefacts.  These are gitignored legacy outputs from before
        # the data_root migration; ``test_uganda_invariance.py`` (and a
        # handful of others) read in-tree first and only fall through to
        # ``data_root()`` when in-tree is missing, so a stale in-tree
        # parquet masks the data_root rebuild that
        # ``Country.clear_cache()`` just primed.  Surfaced as the
        # housing.parquet ``[27741, 2] != [24290, 2]`` shape mismatch.
        # Files are gitignored (``lsms_library/countries/*/var/*``) --
        # safe to delete.
        intree_var = country_dir / "var"
        if intree_var.is_dir():
            for parquet in intree_var.glob("*.parquet"):
                parquet.unlink()
    except Exception as exc:  # noqa: BLE001
        warnings.warn(
            f"_purge_country_caches({country_name!r}) failed ({exc!r}); "
            f"tests may see stale parquets.  Run "
            f"`lsms-library cache clear --country {country_name}` "
            f"and "
            f"`rm -f lsms_library/countries/{country_name}/var/*.parquet` "
            f"manually."
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
    from lsms_library.paths import countries_root, data_root

    root = data_root()
    if not root.exists():
        return

    purged = 0
    for country_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        country_name = country_dir.name
        # Skip directories that aren't real LSMS countries.
        if not (countries_root() / country_name / "_" / "data_scheme.yml").exists():
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
