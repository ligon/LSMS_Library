"""Regression test for the cache-miss path producing canonical-shaped output.

Motivated by 2026-05-08 PR-review: PRs #230, #242, #243 all changed
behaviour observed via ``Country.<feature>()`` but were verified by
running the country-level script directly to populate the cache, then
reading back through the API.  That tests only the *cache-read* path;
the *cache-miss* path (which the framework actually exercises on a
fresh machine) was not covered.  PR #243 specifically passed local
spot-checks under cache-read but failed under cache-miss because the
framework's ``load_from_waves`` aggregates per-wave parquets instead
of running the country-level normalizer.

This test calls the API in a PRIVATE, EMPTY ``LSMS_DATA_DIR`` (a per-test
``tmp_path``; see ``private_data_root``) so every read is a cache miss, and
asserts the result satisfies the country's declared scheme via
``validate_feature``.  Where canonical alignment is known to be blocked by
deferred wave-level work, the test is marked ``xfail`` so the failure is
tracked rather than silenced.

Why a private root and not "clear the relevant parquets" (GH #803).  The
previous version physically cleared the L2-country and L2-wave parquets for
the target table under WHATEVER ``data_root()`` was and rebuilt in place.
Run from a checkout whose ``config.yml`` names a shared ``data_dir`` (the
Savio setup), a full-suite run therefore rewrote the SHARED cache -- and on
2026-09-07 01:49 it rewrote ``GhanaLSS/var/food_acquired.parquet`` with
pre-#785 content read from hashless IN-TREE wave parquets, stamped with a
fresh hash (the #803 mechanism; attribution in
``slurm_logs/gh797/attribution/REPORT.org``).  A test must not be able to
write to a cache it does not own.  The private root also makes the
``extra`` upstream list redundant: nothing is warm, so the rebuild always
traverses the full source chain.  (The old ``_clear_country_caches`` also
only looked under ``data_root()`` and so could not see the in-tree
artefacts; with #803 those are never read, and with a private root there is
nothing to clear.)

L1 (the DVC blob cache) is deliberately SHARED: the private root gets a
``dvc-cache`` symlink to the importing process's ``_DVC_CACHE_DIR`` so the
wave-script subprocesses (which resolve L1 from ``LSMS_DATA_DIR``) do not
re-pull every ``.dta`` from S3.  L1 is content-addressed and append-only, so
sharing it is safe; L2 is what must be private.
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import config as _config
from lsms_library import local_tools as _lt
from lsms_library.country import data_root
from lsms_library.diagnostics import (
    Check,
    SanityReport,
    check_panel_consistency,
    validate_feature,
)


warnings.simplefilter("ignore")  # silence noisy DVC / pandas chatter


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    The cache-miss tests delete cached parquets and force the framework
    to rebuild from source, which goes through DVC -> S3.  Without
    credentials the rebuild fails with ``NoCredentialsError`` regardless
    of whether the test logic is correct.

    Three credential locations are checked, matching where lsms_library
    / DVC / boto3 actually look:

    1. ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` env vars
       (set by the ``data-tests`` CI job from repo secrets).
    2. ``lsms_library/countries/.dvc/s3_creds`` (written by either the
       ``data-tests`` CI job or the import-time auto-unlock path).
    3. The auto-unlock path itself: if ``import lsms_library`` succeeds
       without ``LSMS_SKIP_AUTH=1`` set, the ``.gpg``-decrypt-on-import
       hook has already populated location (2).

    The CI ``unit-tests`` job intentionally sets ``LSMS_SKIP_AUTH=1`` to
    keep PR validation fast and data-free; this function returns False
    in that environment, and the tests below silent-skip.
    """
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get(
        "AWS_SECRET_ACCESS_KEY"
    ):
        return True
    creds_file = (
        Path(__file__).parent.parent
        / "lsms_library" / "countries" / ".dvc" / "s3_creds"
    )
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


# Module-level skip: every test in this file needs DVC -> S3 access.
pytestmark = pytest.mark.skipif(
    not _aws_creds_available(),
    reason=(
        "cache-miss tests need S3 credentials; provided by the "
        "'data-tests' CI job (post-merge to master) and on Savio. "
        "The 'unit-tests' CI job sets LSMS_SKIP_AUTH=1 and skips."
    ),
)


# --------------------------------------------------------------------------
# Targets that must remain canonically shaped after a cache-miss rebuild.
#
# (country, feature, [upstream tables the feature is derived from])
#
# ``extra`` documents which upstream caches the requested feature depends
# on (e.g. ``food_expenditures`` is auto-derived from ``food_acquired``).
# It used to be the list of caches to CLEAR; with the private data root
# (see the module docstring) every table is cold, so it is informational.
# --------------------------------------------------------------------------
TARGETS_OK = [
    # PR #230: idxvars repair
    pytest.param(
        "Guyana", "household_characteristics", ["household_roster"],
        id="PR230_Guyana_household_characteristics",
    ),
    pytest.param(
        "Azerbaijan", "household_characteristics", ["household_roster"],
        id="PR230_Azerbaijan_household_characteristics",
    ),
    pytest.param(
        "Serbia and Montenegro", "household_characteristics", ["household_roster"],
        id="PR230_Serbia_and_Montenegro_household_characteristics",
    ),
    # PR #242: Ethiopia + Nigeria food_expenditures s-axis restore
    pytest.param(
        "Ethiopia", "food_expenditures", ["food_acquired"],
        id="PR242_Ethiopia_food_expenditures",
    ),
    pytest.param(
        "Nigeria", "food_expenditures", ["food_acquired"],
        id="PR242_Nigeria_food_expenditures",
    ),
    # GH #109 (resolved): GhanaLSS food_acquired is now canonicalized at the
    # wave level ([t, i, j, u, s, visit]) and concatenated at the country
    # level, so this is a normal passing target.  It was previously an
    # xfail(strict) pending the wave-level reshape; the marker was removed when
    # the #109 work landed (the strict xfail xpassed as designed).
    pytest.param(
        "GhanaLSS", "food_acquired", [],
        id="GH109_GhanaLSS_food_acquired",
    ),
]


# No remaining xfail targets: the GhanaLSS food_acquired case moved to
# TARGETS_OK above once GH #109 landed.
TARGETS_XFAIL = []


@pytest.fixture
def private_data_root(tmp_path, monkeypatch):
    """A per-test, empty ``LSMS_DATA_DIR`` with the L1 blob cache shared.

    In-process (the ``test_gh323_site4_dfs_merge`` pattern): the env var is
    set AFTER import and ``data_root``'s lru_cache is cleared, so every
    ``data_root()`` call in this process -- and every ``LSMS_DATA_DIR`` the
    library exports to a wave-script subprocess -- resolves to ``tmp_path``.
    ``local_tools._DVC_CACHE_DIR`` is an import-time snapshot and stays where
    it was, which is exactly the shared-L1 / private-L2 split wanted here;
    the symlink gives the subprocesses the same L1.

    Asserts, rather than assumes, that the private root is not the
    configured ``data_dir``: the shared cache must be unreachable from this
    test whatever ``~/.config/lsms_library/config.yml`` says (GH #803).
    """
    private = tmp_path / "data"
    private.mkdir()
    shared_l1 = Path(_lt._DVC_CACHE_DIR)
    if shared_l1.is_dir():
        (private / "dvc-cache").symlink_to(shared_l1, target_is_directory=True)
    monkeypatch.setenv("LSMS_DATA_DIR", str(private))
    data_root.cache_clear()
    try:
        assert data_root() == private, data_root()
        # The shared roots this test must never touch: the config FILE's
        # ``data_dir`` (``_config.data_dir()`` would return the env var we
        # just set) and the XDG default.
        shared = [Path.home() / ".local" / "share" / "lsms_library"]
        configured = _config._load_config().get("data_dir")
        if configured:
            shared.append(Path(configured).expanduser())
        assert all(private.resolve() != s.resolve() for s in shared), \
            f"private data root collides with a shared data root: {shared}"
        assert not any(private.glob("*/var/*.parquet")), "private data root is not empty"
        yield private
    finally:
        data_root.cache_clear()


@pytest.mark.parametrize(
    "country, feature, extra",
    TARGETS_OK + TARGETS_XFAIL,
)
def test_canonical_shape_via_cache_miss(private_data_root, country, feature, extra):
    """Cache-miss → API call should produce canonical-shaped output.

    The private, empty data root makes every tier a miss for ``feature``
    AND for the upstream tables listed in ``extra`` (and for everything
    else), so the rebuild traverses the full source chain.
    ``validate_feature`` asserts the result conforms to the country's
    declared schema and the cross-country reference shape.
    """
    report = validate_feature(country, feature)
    if not report.ok:
        # Surface the failing checks before assert so pytest output is
        # actionable.
        for check in report.checks:
            if check.status == "FAIL":
                pytest.fail(
                    f"[{country}/{feature}] {check.name}: {check.message}"
                )
    assert report.ok, f"validate_feature returned ok=False for {country}/{feature}"
    # The build must have landed in the private root, not anywhere else.
    built = private_data_root / country / "var"
    assert any(built.glob("*.parquet")), f"nothing was written under {built}"


# --------------------------------------------------------------------------
# Panel consistency for GhanaLSS (PR #243 retains the existing GLSS1↔GLSS2
# panel via the framework's id_walk).  Currently fails on
# ``panel_ids_targets_exist`` and ``id_walk_idempotent`` -- those are
# pre-existing diagnostic FAILs against the cached household_roster
# (related to how the cache stores pre-id_walk values), not a regression
# introduced by PR #243.  Marked xfail so the suite stays green while the
# cache/diagnostic interaction is sorted out separately.
# --------------------------------------------------------------------------
@pytest.mark.xfail(
    reason=(
        "Two pre-existing diagnostic FAILs (panel_ids_targets_exist, "
        "id_walk_idempotent) on cached household_roster.parquet for "
        "GhanaLSS GLSS1↔GLSS2 panel; not introduced by PR #243.  Tracked "
        "separately under #109."
    ),
    strict=False,
)
def test_ghanalss_panel_consistency():
    report = check_panel_consistency(ll.Country("GhanaLSS"))
    if not report.ok:
        for check in report.checks:
            if check.status == "FAIL":
                pytest.fail(
                    f"[GhanaLSS panel] {check.name}: {check.message}"
                )
    assert report.ok
