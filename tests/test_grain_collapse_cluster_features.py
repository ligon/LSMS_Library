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

Plus a group that pins the GPS decision (Ethan, 2026-07-13): the ``.mean()`` that
used to average Latitude/Longitude into a cluster centroid is **gone**.  It was the
last aggregation core performed at this site, and the corpus showed it earned its
keep nowhere -- a provable no-op in 4 of the 5 cells where it could fire, and in the
5th it was averaging points up to 783 km apart, i.e. smearing a broken cluster key
rather than summarising a cluster.  GPS is now audited and reduced exactly like
every other column, and NO-AGGREGATION-IN-CORE has no exception left in it.
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
    _GRAIN_LEDGER,
    _collapse_to_cluster_grain,
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
# 5. GPS: core does not average it either.  DECIDED, and pinned.
# --------------------------------------------------------------------------
#
# Latitude/Longitude used to be reduced with `.mean()` -- a cluster centroid -- on
# the theory that household GPS is genuinely per-household, therefore varies within
# a cluster by design, therefore is a false positive for the audit and a legitimate
# thing for core to average.  The corpus refuted every step of that.  In 4 of the 5
# cells where the `.mean()` could fire it is a provable NO-OP (the published GPS *is*
# the cluster's displaced fix, stamped on each household -- never household GPS at
# all).  In the 5th it averages points a median of 148 km and up to 783 km apart:
# not a centroid, a broken cluster key -- and that cell already warns for Region,
# District and Rural.
#
# Decision (Ethan, 2026-07-13): make it loud.  Core aggregates NOTHING here.
# Measured cost: zero new warning cells.  These tests pin that, so it cannot drift
# back to averaging without a deliberate diff.

def _with_gps() -> pd.DataFrame:
    return _hh_grain(
        [("2020", "v1", "h1", "North", 1.0, 30.0),
         ("2020", "v1", "h2", "North", 3.0, 32.0)],
        ["Region", "Latitude", "Longitude"],
    )


def test_gps_is_not_averaged__core_does_not_aggregate():
    """The flip.  Was `.mean()` (2.0 / 31.0); is now `.first()`, like every other
    column.  A cluster coordinate is now A HOUSEHOLD'S REPORTED FIX, not a synthetic
    centroid that core invented behind the caller's back.
    """
    out = Wave.cluster_features(_fake_wave(_with_gps()))
    assert out.loc[("2020", "v1"), "Latitude"] == 1.0    # first, NOT mean (2.0)
    assert out.loc[("2020", "v1"), "Longitude"] == 30.0  # first, NOT mean (31.0)


def test_gps_variation_now_warns():
    """The inverted assertion.  Households in a cluster reporting different fixes is
    a real disagreement about a cluster-grain fact, and `.first()` silently picks
    one.  It is reported like any other destruction -- no exception, no grandfather.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Wave.cluster_features(_fake_wave(_with_gps()))

    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert len(grain) == 1, "differing household GPS must now warn"
    (report,) = grain_reports(country="Testland", table="cluster_features")
    assert report["destroyed"] == 1
    assert report["site"] == "Wave.cluster_features"


def test_constant_gps_within_a_cluster_stays_silent():
    """The other half, and the reason the flip was free: where the survey stamps the
    cluster's own fix onto each household -- 4 of the 5 real cells -- the projection
    is lossless and stays SILENT.  This is why making GPS loud added zero new
    warning cells to the corpus.
    """
    df = _hh_grain(
        [("2020", "v1", "h1", "North", 1.0, 30.0),
         ("2020", "v1", "h2", "North", 1.0, 30.0)],
        ["Region", "Latitude", "Longitude"],
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = Wave.cluster_features(_fake_wave(df))

    assert not [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert out.loc[("2020", "v1"), "Latitude"] == 1.0


def test_no_column_is_special_cased_any_more():
    """The contract, stated as a test: `_collapse_to_cluster_grain` has no notion of
    a GPS column.  Latitude and a made-up column named Altitude get identical
    treatment -- audited, then `.first()`.  If someone reintroduces a per-column
    reducer, this fails.
    """
    df = _hh_grain(
        [("2020", "v1", "h1", 1.0, 100.0),
         ("2020", "v1", "h2", 3.0, 300.0)],
        ["Latitude", "Altitude"],
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = Wave.cluster_features(_fake_wave(df))
    assert out.loc[("2020", "v1"), "Latitude"] == 1.0
    assert out.loc[("2020", "v1"), "Altitude"] == 100.0


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
