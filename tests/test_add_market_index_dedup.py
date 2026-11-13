"""
Regression test for GH #266 -- ``_add_market_index`` cluster-fallback
must not multiply rows for tall input tables.

Bug shape (Uganda v0.7.2): ``Country('Uganda').shocks(market='Region')``
returns 27,268 rows for the same 14,457-row input that
``Country('Uganda').shocks()`` produces -- each (i, t)-household with
*k* shocks gets *k^2* output rows instead of *k* because the
fallback ``v_lookup`` merge inside ``_add_market_index`` does a
Cartesian product on (i, t) against an un-deduplicated lookup frame.

Root cause: line 1420 of country.py built ``v_lookup`` as
``df_with_v.reset_index()[['i', 't', 'v']]`` without
``.drop_duplicates(subset=['i', 't'])``.  For a tall input one row
per (i, t, Shock), the lookup ends up with *k* copies of every
(i, t, v) tuple, and the merge on (i, t) yields *k^2* outputs.

The parallel ``_location_lookup`` already carried the dedup; this
just restores symmetry.
"""
import warnings

import pandas as pd
import pytest

import lsms_library as ll

pytestmark = pytest.mark.requires_s3


@pytest.fixture(scope="module")
def uga_shocks_bare():
    """``Country('Uganda').shocks()`` -- the baseline shape with no
    market index.  Skips if the underlying microdata isn't reachable
    in this environment (CI without DVC creds, etc.)."""
    try:
        return ll.Country("Uganda").shocks()
    except Exception as exc:  # noqa: BLE001 - broad catch intentional
        pytest.skip(f"Uganda shocks unavailable: {exc}")


@pytest.fixture(scope="module")
def uga_shocks_market():
    """``Country('Uganda').shocks(market='Region')`` -- the path
    exercising ``_add_market_index``'s cluster-level fallback."""
    try:
        return ll.Country("Uganda").shocks(market="Region")
    except Exception as exc:  # noqa: BLE001 - broad catch intentional
        pytest.skip(f"Uganda shocks(market='Region') unavailable: {exc}")


class TestAddMarketIndexDedup:
    """All assertions target the GH #266 bug surface in
    ``Country._add_market_index``.  Uganda shocks is the canonical
    worked example: 14,457 input rows; the bug previously produced
    27,268 (12,812 spurious duplicates)."""

    def test_no_duplicate_index_rows(self, uga_shocks_market):
        """The market-indexed result must have no duplicate index rows.
        Pre-fix: ``index.duplicated().sum() == 12_812``."""
        ndup = int(uga_shocks_market.index.duplicated().sum())
        assert ndup == 0, (
            f"Uganda shocks(market='Region') has {ndup} duplicate index rows. "
            f"This is the GH #266 Cartesian-product bug in "
            f"_add_market_index's cluster-level fallback merge."
        )

    def test_row_count_not_inflated(self, uga_shocks_bare, uga_shocks_market):
        """Adding the market index neither multiplies rows nor drops them.
        Pre-fix #266 Uganda: 27,268 vs 14,457 -> 88% inflation.  This used to
        be ``<=`` because ``_add_market_index`` silently deleted rows with no
        market label; it now keeps them with ``m`` NA and warns, so the row
        count is exactly preserved."""
        n_bare = len(uga_shocks_bare)
        n_market = len(uga_shocks_market)
        assert n_market == n_bare, (
            f"shocks(market='Region') = {n_market} rows vs "
            f"shocks() = {n_bare}.  _add_market_index must neither "
            f"multiply rows (GH #266: ~1.88x via Cartesian product on tall "
            f"input) nor drop them (an unlabelled row is kept with m NA)."
        )

    def test_shock_distribution_preserved(self, uga_shocks_bare, uga_shocks_market):
        """The number of shock rows per (i, t) HH-period in the market
        version should match (or be a subset of) the bare version.
        Pre-fix: each k-shock HH-period became k^2.  The k=2 bucket
        had 1,999 HH-periods pre-fix, 3,998 post-bug (2x).  Post-fix
        the (k -> count) distribution should equal or be a subset of
        the bare distribution."""
        bare_dist = (
            uga_shocks_bare.reset_index()
            .groupby(['i', 't'])
            .size()
            .value_counts()
            .sort_index()
            .to_dict()
        )
        market_dist = (
            uga_shocks_market.reset_index()
            .groupby(['i', 't'])
            .size()
            .value_counts()
            .sort_index()
            .to_dict()
        )
        # The distributions must be IDENTICAL.  This used to allow a subset
        # (market_n <= bare_n) because _add_market_index silently dropped
        # rows with no market label; those rows are now kept with m NA.
        # Pre-fix #266 the market counts were precisely the squares
        # (k=2: 1999 -> 3998, k=3: 525 -> 1575, etc.).
        assert market_dist == bare_dist, (
            f"shocks(market='Region') (k -> HH-periods) {market_dist} != "
            f"shocks() {bare_dist}.  Pre-fix #266 inflated each k to k^2; "
            f"since the market-label fix no HH-period may be dropped either."
        )
