"""Regression tests for Uganda v-encoding and cluster_features grain.

These codify the invariants restored by:

* GH #196: ``Country('Uganda').sample()['v']`` and
  ``cluster_features().v`` / ``household_characteristics().v`` use the
  same canonical string encoding (no float-stringification mismatch).
* GH #197: ``household_characteristics()`` warns loudly when roster
  rows are dropped due to NaN ``v``; the warning names the per-wave
  count.  Since GH #1120 D2 Uganda has no such rows, so the test now
  asserts the warning does NOT fire.
* GH #161: ``cluster_features()`` is at ``(t, v)`` grain with no
  duplicate keys, no stray ``i`` column, and ``District`` values are
  not float-stringified (no ``"101.0"`` -> the canonical ``"101"``).

Tests skip if Uganda data isn't available (no DVC / no ``.dta``
files on disk).
"""
from __future__ import annotations

import warnings
from typing import Any

import pandas as pd
import pytest

pytestmark = pytest.mark.requires_s3


def _uganda_or_skip():
    """Return ``Country('Uganda')`` or skip if data is unavailable."""
    try:
        import lsms_library as ll
        return ll.Country('Uganda')
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Uganda not available: {exc!r}")


@pytest.fixture(scope='module')
def uga():
    return _uganda_or_skip()


@pytest.fixture(scope='module')
def uga_sample(uga):
    try:
        return uga.sample()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"sample() failed: {exc!r}")


@pytest.fixture(scope='module')
def uga_cf(uga):
    try:
        return uga.cluster_features()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"cluster_features() failed: {exc!r}")


@pytest.fixture(scope='module')
def uga_hc(uga):
    """``household_characteristics()`` and the warnings it emits, built once.

    Skips the dependent tests cleanly on environments without DVC/S3
    credentials (where ``household_characteristics()`` triggers a
    wave-level rebuild that needs the raw .dta files).
    """
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            hc = uga.household_characteristics()
        return hc, list(caught)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"household_characteristics() failed: {exc!r}")


# ---------------------------------------------------------------------------
# GH #196: cross-table v encoding
# ---------------------------------------------------------------------------


class TestVEncodingMatchesAcrossTables:
    """``sample().v`` and ``cluster_features().v`` must use the same
    canonical string form for every wave with cluster data."""

    def test_no_float_stringified_v_in_sample(self, uga_sample):
        """``'10120402.0'``-style float-stringification must not appear."""
        v = uga_sample.reset_index()['v'].dropna().astype(str)
        bad = [s for s in v.unique() if s.endswith('.0')]
        assert not bad, f"sample().v has float-stringified entries: {bad[:5]}"

    @pytest.mark.parametrize('wave', [
        '2005-06', '2010-11', '2011-12', '2013-14', '2015-16',
        '2018-19', '2019-20',
    ])
    def test_sample_v_matches_cluster_features_v(self, uga_sample, uga_cf, wave):
        """Every cluster present in cluster_features should also be in
        sample for the same wave (or be a documented synthetic ``@``
        cluster from the 2009-10 mover/split-off fallback)."""
        sw = set(uga_sample.reset_index().query('t == @wave')['v'].dropna())
        cw = set(uga_cf.reset_index().query('t == @wave')['v'].dropna())
        assert cw, f"cluster_features has no v entries for {wave}"
        # cluster_features's v must all be in sample's v -- the
        # sample is the authoritative cluster list.
        missing = cw - sw
        assert not missing, (
            f"{wave}: {len(missing)} cluster_features v values missing "
            f"from sample().v -- {sorted(missing)[:5]}"
        )


# ---------------------------------------------------------------------------
# GH #197: silent HH drop loud warning
# ---------------------------------------------------------------------------


class TestNoRosterHouseholdFallsIntoTheMoverBucket:
    """GH #197 / #268 made the NaN-``v`` roster rows loud: they are kept under
    the SHARED ``'Mover'`` sentinel and a per-wave warning names them.

    Since GH #1120 D2 (@ligon 2026-10-23) Uganda has no such rows: every
    household ``sample()`` holds has a ``v`` -- a cluster, or its own
    ``moved-{hhid}`` singleton -- and the frame holds every roster household
    (2018-19's 66 GSEC2/AGSEC1-only households included).  So the warning
    must NOT fire, and no household may land in the shared bucket, where it
    would be welded to every other unlocated household.  Before #1120 this
    class asserted the opposite (that the warning fires): 2009-10 110 /
    2013-14 7 / 2018-19 346 / 2019-20 3 roster rows had no ``v``.
    """

    def test_no_sentinel_warning(self, uga_hc):
        _, caught = uga_hc
        relevant = [
            w for w in caught
            if 'household_characteristics' in str(w.message)
            and 'sentinel' in str(w.message)
        ]
        n = len(relevant)
        assert n == 0, f"{n} 'Mover' sentinel warning(s): a roster row has no v"

    def test_no_household_in_the_mover_bucket(self, uga_hc):
        hc, _ = uga_hc
        n = int((pd.Series(hc.index.get_level_values('v')) == 'Mover').sum())
        assert n == 0, f"{n} household(s) in the shared 'Mover' bucket"


# ---------------------------------------------------------------------------
# GH #161: cluster_features grain + District format
# ---------------------------------------------------------------------------


class TestClusterFeaturesGrain:
    """``cluster_features`` is per-cluster, not per-household."""

    def test_index_is_t_v(self, uga_cf):
        assert list(uga_cf.index.names) == ['t', 'v']

    def test_no_duplicate_keys(self, uga_cf):
        n_dup = int(uga_cf.index.duplicated().sum())
        assert n_dup == 0, f"cluster_features has {n_dup} duplicate (t, v) keys"

    def test_no_stray_i_column(self, uga_cf):
        assert 'i' not in uga_cf.columns, (
            f"cluster_features leaks the household ``i`` column: "
            f"{list(uga_cf.columns)}"
        )

    def test_district_no_float_stringified(self, uga_cf):
        """``District`` from numeric Stata columns must not stringify
        to ``'101.0'``-style values."""
        if 'District' not in uga_cf.columns:
            pytest.skip("no District column")
        dist = uga_cf['District'].dropna().astype(str)
        bad = [s for s in dist.unique() if s.endswith('.0')]
        assert not bad, (
            f"District has float-stringified entries: {bad[:5]}.  "
            f"format_id should strip the trailing .0"
        )
