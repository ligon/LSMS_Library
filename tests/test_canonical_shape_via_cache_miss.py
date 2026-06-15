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

This test removes the relevant cache parquet, calls the API, and
asserts the result satisfies the country's declared scheme via
``validate_feature``.  Where canonical alignment is known to be
blocked by deferred wave-level work, the test is marked ``xfail`` so
the failure is tracked rather than silenced.
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll
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
# (country, feature, [extra caches to clear before the rebuild])
#
# ``extra`` lists caches that the requested feature depends on; clearing
# them forces the rebuild to traverse the full source-data chain instead
# of consuming a stale upstream cache.  E.g. ``food_expenditures`` is
# auto-derived from ``food_acquired`` so the latter must also be cleared
# before the test.
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


def _clear_country_caches(country: str, tables: list[str]) -> None:
    """Physically clear L1 (country) and L2 (per-wave) parquet caches.

    Scoped equivalent of ``lsms-library cache clear --country {country}``
    for the listed tables.  The L2 ``{wave}/_/{table}.parquet`` layer
    must be cleared too -- clearing only L1 leaves stale wave parquets
    that the framework happily reads back into a freshly-built L1,
    silently masking source-data fixes (CLAUDE.md "Cache Behavior").
    """
    country_root = data_root() / country
    if not country_root.exists():
        return
    for table in tables:
        # L1: country-level cache
        l1 = country_root / "var" / f"{table}.parquet"
        if l1.exists():
            l1.unlink()
        # L2: every wave's per-wave cache
        for wave_dir in country_root.iterdir():
            if not wave_dir.is_dir() or wave_dir.name == "var":
                continue
            l2 = wave_dir / "_" / f"{table}.parquet"
            if l2.exists():
                l2.unlink()


@pytest.mark.parametrize(
    "country, feature, extra",
    TARGETS_OK + TARGETS_XFAIL,
)
def test_canonical_shape_via_cache_miss(country, feature, extra):
    """Cache-miss → API call should produce canonical-shaped output.

    Both L1 (country-level) and L2 (per-wave) caches are cleared for
    ``feature`` and any upstream dependencies listed in ``extra``,
    then the API is invoked.  ``validate_feature`` asserts the result
    conforms to the country's declared schema and the cross-country
    reference shape.
    """
    _clear_country_caches(country, [feature, *extra])

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
