"""Regression tests for Guatemala's cluster (PSU) identity -- GH #323.

Before this landed, ``Guatemala/2000/_/data_info.yml`` declared ``v: region``
(8 values) for both ``sample`` and ``cluster_features``, and additionally
leaked ``i: hogar`` into ``cluster_features``' idxvars.  The wave frame was
therefore household-level (7,276 rows) on a declared ``(t, v)`` index, and
``_normalize_dataframe_index`` silently collapsed 7,268 of those rows with
``groupby().first()``.

That collapse was not a dedup.  All 8 regions contain BOTH urban and rural
households, so ``Rural`` is not a function of ``region``: ``.first()`` made an
ARBITRARY pick (by row order), stamping 7 regions "Urban" and one "Rural" and
leaving 3,591 of 7,276 households -- 49.4% -- in a cluster whose ``Rural`` flag
contradicted their own.  Silently WRONG, not merely missing.

ENCOVI 2000 does identify a PSU; it just lives in ``CONSUMO5.DTA`` (the
``ECV*``/``HOGARES``/``PERSONAS`` files carry only region+area).  ``v`` is now
the composite ``depto-mupio-sector-segmento`` -- 1,065 clusters, each wholly
inside one region and wholly urban or wholly rural.

These invariants hold on a warm cache too: the pre-fix L2-country parquet was
itself written post-collapse (8 rows), so a stale cache does not mask them.

Tests skip if Guatemala data isn't available (no DVC / no ``.dta`` on disk).
"""
from __future__ import annotations

import pandas as pd
import pytest


def _guatemala_or_skip():
    try:
        import lsms_library as ll
        return ll.Country('Guatemala')
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Guatemala not available: {exc!r}")


@pytest.fixture(scope='module')
def gtm():
    return _guatemala_or_skip()


@pytest.fixture(scope='module')
def cf(gtm):
    try:
        return gtm.cluster_features()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Guatemala cluster_features unavailable: {exc!r}")


@pytest.fixture(scope='module')
def sample(gtm):
    try:
        return gtm.sample()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Guatemala sample unavailable: {exc!r}")


def test_cluster_features_is_at_t_v_grain(cf):
    """(t, v), one row per cluster, no duplicate keys, no stray household id."""
    assert list(cf.index.names) == ['t', 'v']
    assert cf.index.is_unique, (
        f"cluster_features has {int(cf.index.duplicated().sum())} duplicate "
        "(t, v) tuples -- the declared index is non-unique and will be "
        "silently collapsed (GH #323)."
    )
    assert 'i' not in cf.index.names and 'i' not in cf.columns


def test_cluster_is_finer_than_region(cf):
    """The PSU must be finer than the 8 regions.

    FAILS PRE-FIX: v was `region`, so cluster_features had exactly 8 rows --
    one per region -- and 7,268 household rows had been discarded to get there.
    """
    assert len(cf) > 8, (
        f"cluster_features has only {len(cf)} rows. `v` has collapsed to "
        "something region-coarse; the ENCOVI 2000 PSU has 1,065 clusters."
    )
    # Each region must contain many clusters (metropolitana alone has ~100+).
    per_region = cf.groupby('Region', observed=True).size()
    assert (per_region > 1).all(), (
        "Some region maps to a single cluster -- v is not a real PSU:\n"
        f"{per_region.to_string()}"
    )


def test_v_is_not_the_float32_corrupted_upm(cf):
    """Guard the float32 trap.

    CONSUMO5.DTA ships a `upm` column, but Stata stored it as float32 and every
    value exceeds 2**24, so the digits encoding `segmento` are rounded away:
    the 1,065 real PSUs collapse to 847 distinct stored values, with 201 of them
    conflating genuinely different PSUs.  Anyone "simplifying" the composite key
    to `v: upm` would silently merge 218 clusters -- this test stops that.
    """
    assert len(cf) >= 1065, (
        f"Only {len(cf)} clusters. The uncorrupted PSU composite "
        "(depto-mupio-sector-segmento) yields 1,065; 847 is the signature of "
        "reading the float32-damaged `upm` column directly."
    )


def test_rural_is_a_function_of_the_cluster(cf, sample):
    """THE core #323 invariant: a household's Rural must match its cluster's.

    FAILS PRE-FIX with 3,591 mismatches of 7,276 households (49.4%), because
    groupby(region).first() picked each region's Rural flag arbitrarily.

    Checked on every household -- not only those where the answer was already
    determined.
    """
    hh = sample.reset_index()[['i', 'v', 'Rural']]
    clust = cf.reset_index()[['v', 'Rural']]
    j = hh.merge(clust, on='v', how='inner', suffixes=('_hh', '_clust'))

    assert len(j) == len(hh), (
        f"{len(hh) - len(j)} household(s) reference a `v` absent from "
        "cluster_features -- sample.v and cluster_features.v are out of step."
    )

    bad = j[j.Rural_hh.astype(str) != j.Rural_clust.astype(str)]
    assert len(bad) == 0, (
        f"{len(bad)} of {len(j)} households sit in a cluster whose Rural flag "
        "contradicts their own. `Rural` is not a function of `v`, so collapsing "
        "cluster_features to one row per v discards real variation (GH #323).\n"
        f"{bad.head(10).to_string()}"
    )


def test_every_cluster_is_wholly_urban_or_wholly_rural(sample):
    """The property that makes the one-row-per-cluster collapse legitimate."""
    per_v = sample.groupby('v', observed=True)['Rural'].nunique(dropna=False)
    mixed = per_v[per_v > 1]
    assert mixed.empty, (
        f"{len(mixed)} cluster(s) contain both urban and rural households, so "
        "cluster_features' Rural column is ill-defined:\n"
        f"{mixed.head(10).to_string()}"
    )


def test_every_cluster_lies_in_exactly_one_region(cf, sample):
    """Region must likewise be a function of v (market='Region' relies on it)."""
    assert 'Region' in cf.columns
    assert cf.Region.notna().all()
    per_v = cf.reset_index().groupby('v', observed=True)['Region'].nunique()
    assert (per_v == 1).all()


def test_sample_keeps_every_household_and_has_a_cluster_for_each(sample):
    """No household lost, and none left without a PSU."""
    assert len(sample) == 7276, f"expected 7,276 households, got {len(sample)}"
    assert sample['v'].notna().all(), (
        f"{int(sample['v'].isna().sum())} household(s) have no cluster id."
    )
    assert sample['v'].nunique() == 1065
