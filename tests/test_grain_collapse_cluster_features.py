"""GH #323, SITE 2 -- the household -> cluster projection in ``Wave.cluster_features``.

A SECOND grain collapse, hardcoded, and entirely separate from the declared-index
one that ``tests/test_grain_collapse.py`` covers.  Seventeen countries declare
``i: <HHID>`` in their ``cluster_features`` idxvars (so the YAML can merge a
household-level GPS frame), which hands ``Wave.cluster_features`` a HOUSEHOLD-grain
table; it reduces that to the ``(t, v)`` cluster grain with ``.first()`` --
*before* ``_normalize_dataframe_index`` ever runs.  Site 1's audit therefore cannot
see this loss: by the time it fires, the rows are already gone.

The reduction was licensed by a comment and by nothing else --

    "Region/Rural/District are invariant within a cluster by construction of the
     LSMS-ISA sampling design."

-- and prose is not enforcement.  Measured against the real corpus the claim is
false: in Uganda 2019-20 alone, 11 clusters disagree on ``Region``, 23 on
``District`` and 125 on ``Rural``.  ``.first()`` keeps one at random.  That is not
a lossy summary, it is a WRONG ROW.

Three properties, each of which FAILS on pre-fix code:

1.  A destructive projection is LOUD (and fatal under ``LSMS_GRAIN_STRICT``).
2.  A lossless projection stays SILENT -- this is what Cambodia / Tajikistan /
    GhanaLSS were actually asking for, and they get it without any YAML.
3.  The finding lands in the ledger under ``(country, 'cluster_features')``, which
    is what the L2-country writer stamps into the parquet -- so it survives the
    cache, exactly as at Site 1.

Plus one that PINS A DECISION rather than a behaviour: the ``.mean()`` on
Latitude/Longitude is an aggregation in core, and it is GRANDFATHERED.  If someone
later makes it loud, or rips it out, these tests must be edited to say so -- it
does not get to change by accident.
"""
from __future__ import annotations

import types
import warnings

import pandas as pd
import pytest

from lsms_library import local_tools as lt
from lsms_library.country import (
    GrainCollapseError,
    GrainCollapseWarning,
    Wave,
    _CLUSTER_GPS_COLUMNS,
    _GRAIN_LEDGER,
    _collapse_to_cluster_grain,
    _gps_averaging_stats,
    _replay_grain_audit,
    grain_reports,
)


@pytest.fixture(autouse=True)
def _clean_ledger(monkeypatch):
    monkeypatch.delenv("LSMS_GRAIN_STRICT", raising=False)
    _GRAIN_LEDGER.clear()
    yield
    _GRAIN_LEDGER.clear()


def _hh_grain(rows: list[tuple], columns: list[str]) -> pd.DataFrame:
    """Build a household-grain cluster_features frame: index (t, v, i)."""
    idx = pd.MultiIndex.from_tuples([r[:3] for r in rows], names=["t", "v", "i"])
    return pd.DataFrame([r[3:] for r in rows], index=idx, columns=columns)


def _conflicting() -> pd.DataFrame:
    """Cluster ``v1`` is really TWO clusters -- a code unique only within a district.

    This is the shape the "invariant by construction" comment denied could exist.
    """
    return _hh_grain(
        [("2020", "v1", "h1", "North", "Rural", "Gulu"),
         ("2020", "v1", "h2", "South", "Urban", "Mbale"),
         ("2020", "v2", "h3", "North", "Rural", "Gulu")],
        ["Region", "Rural", "District"],
    )


def _redundant() -> pd.DataFrame:
    """A cluster attribute repeated once per household -- collapsing loses nothing."""
    return _hh_grain(
        [("2020", "v1", "h1", "North", "Rural"),
         ("2020", "v1", "h2", "North", "Rural"),
         ("2020", "v2", "h3", "South", "Urban")],
        ["Region", "Rural"],
    )


def _fake_wave(df: pd.DataFrame, country: str = "Testland", year: str = "2020"):
    """A Wave stub thin enough to call the REAL ``Wave.cluster_features`` on.

    The method only ever touches ``grab_data``, ``country.name`` and ``year``, so we
    exercise the shipped code rather than a re-implementation of it.
    """
    stub = types.SimpleNamespace(
        grab_data=lambda request: df,
        country=types.SimpleNamespace(name=country),
        year=year,
    )
    return stub


# --------------------------------------------------------------------------
# 1. a destructive projection is LOUD
# --------------------------------------------------------------------------

def test_conflicting_cluster_attributes_are_reported_not_asserted_away():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = Wave.cluster_features(_fake_wave(_conflicting()))

    # the projection still HAPPENS -- this fix reports, it does not aggregate and
    # it does not retain (D1/D2).
    assert list(out.index.names) == ["t", "v"]
    assert len(out) == 2

    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert len(grain) == 1, "a cluster whose households DISAGREE must warn"

    msg = str(grain[0].message)
    assert "Testland/cluster_features/2020" in msg, "the report must NAME the cell"
    assert "HOUSEHOLD grain" in msg
    assert "DESTROYED 1" in msg
    assert "invariant within a cluster" in msg, "name the false claim being retired"
    assert "GH #323" in msg

    (report,) = grain_reports(country="Testland", table="cluster_features")
    assert report["site"] == "Wave.cluster_features", (
        "the site must be distinguishable from Site 1 in the same table's ledger"
    )
    assert report["destroyed"] == 1
    assert report["conflicting_groups"] == 1
    assert report["levels"] == ["t", "v"]


def test_strict_mode_raises_so_ci_can_ratchet():
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LSMS_GRAIN_STRICT", "1")
        with pytest.raises(GrainCollapseError, match="cluster_features"):
            Wave.cluster_features(_fake_wave(_conflicting()))


def test_the_row_first_returns_can_exist_in_no_household(recwarn):
    """Why this is worse than 'lossy': ``first()`` skips NA PER COLUMN.

    So a conflicting cluster does not even collapse to one of its households -- it
    collapses to a COMPOSITE assembled from the first non-null value of each column
    independently, a household that the survey never interviewed.  Pinned here
    because it is the strongest argument for auditing rather than tolerating.
    """
    df = _hh_grain(
        [("2020", "v1", "h1", None, "Rural"),
         ("2020", "v1", "h2", "South", None)],
        ["Region", "Rural"],
    )
    out = Wave.cluster_features(_fake_wave(df))
    assert out.loc[("2020", "v1")].to_dict() == {"Region": "South", "Rural": "Rural"}
    assert [w for w in recwarn if issubclass(w.category, GrainCollapseWarning)], (
        "a composite row is a destroyed row and must be reported"
    )


# --------------------------------------------------------------------------
# 2. a lossless projection stays SILENT
# --------------------------------------------------------------------------

def test_lossless_projection_is_silent():
    """What the Design-A branches actually wanted, and now get for free.

    Cambodia / Tajikistan / GhanaLSS were not asking core to aggregate; they were
    asking to say "this household -> cluster projection is lossless, and I checked".
    A provably lossless collapse is already silent -- no YAML, no country code.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = Wave.cluster_features(_fake_wave(_redundant()))

    assert not [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert not grain_reports(country="Testland", table="cluster_features")
    assert out.loc[("2020", "v1"), "Region"] == "North"
    assert len(out) == 2


def test_a_cluster_grain_table_is_never_touched():
    """No ``i`` in the index -> no projection, no audit, no warning."""
    df = pd.DataFrame(
        {"Region": ["North", "South"]},
        index=pd.MultiIndex.from_tuples([("2020", "v1"), ("2020", "v2")],
                                        names=["t", "v"]),
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = Wave.cluster_features(_fake_wave(df))
    assert not [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    pd.testing.assert_frame_equal(out, df)


# --------------------------------------------------------------------------
# 3. D2 -- a NaN cluster key is DELETED, and that deletion is LOUD
# --------------------------------------------------------------------------

def test_nan_cluster_key_is_deleted_outright_and_reported():
    """``groupby`` defaults to ``dropna=True``: a household with no ``v`` is not
    merged into the cluster, it is DELETED.  Decision D2 (2026-07-13) keeps the
    deletion -- retaining would change returned data for every country -- so the
    obligation is that it be LOUD.
    """
    df = _hh_grain(
        [("2020", "v1", "h1", "North"),
         ("2020", None, "h2", "South")],
        ["Region"],
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = Wave.cluster_features(_fake_wave(df))

    assert len(out) == 1, "the NaN-key household is gone from the returned data (D2)"
    (report,) = grain_reports(country="Testland", table="cluster_features")
    assert report["nan_key_rows"] == 1
    msg = str([w for w in caught
               if issubclass(w.category, GrainCollapseWarning)][0].message)
    assert "DELETED OUTRIGHT" in msg


# --------------------------------------------------------------------------
# 4. the signal survives the cache (same mechanism as Site 1)
# --------------------------------------------------------------------------

def test_site2_report_is_stamped_into_the_parquet_and_replayed(tmp_path):
    """The load-bearing property.  The L2-country parquet is written POST-collapse,
    so on a warm read nothing at this site can re-detect the loss.  The report must
    land in the ledger under the key the cache writer stamps
    (``(country, 'cluster_features')``) and come back out on the warm read.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        collapsed = Wave.cluster_features(_fake_wave(_conflicting()))

    audit = _GRAIN_LEDGER[("Testland", "cluster_features")]
    assert audit, "the cache writer reads exactly this key -- an empty one is a silent cache"

    fn = tmp_path / "cluster_features.parquet"
    lt.to_parquet(collapsed, fn, absolute_path=True, cache_hash="deadbeef",
                  grain_audit=audit)

    # ... and the warm read gets it back and re-emits it.
    _GRAIN_LEDGER.clear()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _replay_grain_audit(lt.read_parquet_grain_audit(fn),
                            "Testland", "cluster_features")

    (w,) = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert "HOUSEHOLD grain" in str(w.message), (
        "the warm read must reproduce the SITE-2 message, not Site 1's"
    )
    (report,) = grain_reports(country="Testland", table="cluster_features")
    assert report["from_cache"] is True
    assert report["destroyed"] == 1


# --------------------------------------------------------------------------
# 5. the GPS `.mean()` -- a DECISION, pinned
# --------------------------------------------------------------------------
#
# Latitude/Longitude are genuinely household-level: every household has its own
# fix, so they are NEVER constant within a cluster.  Auditing them the way Region is
# audited would mark ~every cluster in ~every GPS country destructive -- a ~100%
# false-positive rate, and a warning nobody reads is how #323 survived its first
# fix.  So they are excluded from the destruction audit and still averaged.
#
# That `.mean()` is the last aggregation core performs at this site.  It is
# GRANDFATHERED, not endorsed.  These tests pin the decision so that changing it is
# a deliberate act with a diff, not a drift.

def _with_gps() -> pd.DataFrame:
    return _hh_grain(
        [("2020", "v1", "h1", "North", 1.0, 30.0),
         ("2020", "v1", "h2", "North", 3.0, 32.0)],
        ["Region", "Latitude", "Longitude"],
    )


def test_gps_is_averaged_to_a_centroid_not_first():
    out = Wave.cluster_features(_fake_wave(_with_gps()))
    assert out.loc[("2020", "v1"), "Latitude"] == 2.0   # mean, not first (1.0)
    assert out.loc[("2020", "v1"), "Longitude"] == 31.0


def test_gps_variation_alone_does_not_warn__GRANDFATHERED():
    """DECISION, not an accident.  Household GPS varies within a cluster by design;
    warning on it would drown the Region/District finding that this whole site
    exists to surface.  If this behaviour is ever changed (option (b): make it
    loud), THIS TEST is the thing to edit, and the edit is the record of the choice.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Wave.cluster_features(_fake_wave(_with_gps()))
    assert not [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert not grain_reports(country="Testland", table="cluster_features")


def test_gps_averaging_is_counted_whenever_a_report_is_filed():
    """Grandfathered, but not unmeasured: any report for this cell carries the census.
    This is the ``audited`` half of "grandfather it, documented and audited".
    """
    df = pd.concat([_with_gps(),
                    _hh_grain([("2020", "v1", "h3", "South", 5.0, 34.0)],
                              ["Region", "Latitude", "Longitude"])])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        Wave.cluster_features(_fake_wave(df))

    (report,) = grain_reports(country="Testland", table="cluster_features")
    assert report["destroyed"] == 2, "counted on the ATTRIBUTE columns only"
    assert report["gps_averaged_groups"] == 1
    assert report["gps_averaged_rows"] == 3
    assert report["gps_columns"] == ["Latitude", "Longitude"]


def test_gps_columns_are_excluded_from_the_destruction_count():
    """The false-positive that would have made this whole audit unreadable."""
    df = _hh_grain(
        [("2020", "v1", "h1", "North", 1.0, 30.0),
         ("2020", "v1", "h2", "North", 9.0, 39.0)],   # same Region, different fix
        ["Region", "Latitude", "Longitude"],
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Wave.cluster_features(_fake_wave(df))
    assert not [w for w in caught if issubclass(w.category, GrainCollapseWarning)], (
        "differing household GPS is not destruction -- it is what the centroid is FOR"
    )


def test_gps_stats_are_none_when_the_mean_is_a_no_op():
    df = _hh_grain(
        [("2020", "v1", "h1", 1.0, 30.0),
         ("2020", "v1", "h2", 1.0, 30.0)],
        ["Latitude", "Longitude"],
    )
    assert _gps_averaging_stats(df, ["t", "v"], list(_CLUSTER_GPS_COLUMNS)) is None


# --------------------------------------------------------------------------
# 6. the helper is not a second mechanism (D1)
# --------------------------------------------------------------------------

def test_site2_reuses_the_site1_ledger_and_warning_class():
    """D1: reuse #614's machinery, do not build a parallel one.  A caller that
    filters ``GrainCollapseWarning`` / reads ``grain_reports()`` sees BOTH sites.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _collapse_to_cluster_grain(_conflicting(), ["t", "v"],
                                   country="Testland", wave="2020")
    reports = grain_reports(country="Testland")
    assert [r["table"] for r in reports] == ["cluster_features"]
    assert reports[0]["site"] == "Wave.cluster_features"
