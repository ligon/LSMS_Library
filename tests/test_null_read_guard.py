"""The silent all-null read: right shape, no content, nothing raised.

Covers both emission sites of ``lsms_library.null_read_audit``:

* **Site R** -- ``local_tools.get_dataframe``.  The mis-parse class: a reader
  that claims a file it cannot actually decode and hands back a frame of NaN.
* **Site B** -- ``Country._finalize_result``.  A required *declared* column that
  is present and empty.

The three motivating instances, and how each is exercised here:

===============================  ===========================================
instance                         test
===============================  ===========================================
Peru 1990 ``.SSP`` (#699)        ``test_xport_shaped_misparse_is_reported``
GhanaLSS ``COMM.DAT`` / ``.DCT`` ``test_fixed_width_parse_of_a_delimited_file
                                 _is_reported`` -- a REAL ``read_fwf``-vs-
                                 comma mismatch, run on a temp file
Niger 2014-15 ``Latitude``       ``test_niger_2014_15_latitude_is_reported``
                                 (real corpus, ``requires_s3``)
===============================  ===========================================

The synthetic frames are not invented: their shapes are the *measured* ones.
``pyreadstat.read_xport`` on the real ``N00A.SSP`` blob returns 1528 x 10 with
7 columns entirely NaN; a fixed-width parse of the real ``COMM.DAT`` per its own
``COMM.DCT`` returns 86 x 311 with 254 columns entirely NaN.  Both measured
2026-08-22 against the DVC blobs.

The two negative tests carry the most weight, because a guard that cries wolf is
a guard people learn to ignore -- which is how GH #323's first warning died:

* ``test_healthy_read_is_silent`` / ``test_scattered_empty_columns_are_silent``
  -- the corpus has **887** individually all-null raw columns across 153 files
  and only **3** of them are columns any table asks for.  A per-column trigger
  would fire 887 times; the frame-fraction trigger fires 0 times.
* ``test_empty_optional_column_is_silent`` -- ``optional: true`` is the config
  author's own statement that the column may be absent.
"""
from __future__ import annotations

import os
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import _build_registry as R
from lsms_library.null_read_audit import (
    NullReadError,
    NullReadWarning,
    _NULL_FRACTION_TRIGGER,
    _clear_null_read_reports,
    audit_declared_columns,
    audit_read,
    check_declared_columns,
    check_read,
    null_read_reports,
)


@pytest.fixture(autouse=True)
def _clean_ledger_and_env(monkeypatch):
    """The report ledger is process-wide; a test asserting on it must not
    inherit another test's findings.  Strict mode likewise must not leak."""
    monkeypatch.delenv("LSMS_READ_STRICT", raising=False)
    _clear_null_read_reports()
    yield
    _clear_null_read_reports()


def _frame(n_rows: int, n_cols: int, n_null: int) -> pd.DataFrame:
    """``n_cols`` columns of which the last ``n_null`` are entirely NaN."""
    data = {}
    for i in range(n_cols):
        if i >= n_cols - n_null:
            data[f"c{i}"] = np.full(n_rows, np.nan)
        else:
            data[f"c{i}"] = np.arange(n_rows, dtype=float)
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# Site R -- the mis-parse class
# ---------------------------------------------------------------------------

def test_xport_shaped_misparse_is_reported():
    """#699: ``pyreadstat.read_xport`` on Peru 1990 ``N00A.SSP`` -> 1528 x 10
    with 7 columns entirely NaN, and no exception.  ``pandas.read_sas`` reads
    all 10.  Measured on the real blob; the shape is reproduced here."""
    report = audit_read(_frame(1528, 10, 7), "Peru/1990/Data/N00A.SSP")
    assert report is not None
    assert report["null_fraction"] == 0.7
    assert len(report["null_columns"]) == 7
    assert report["rows"] == 1528


@pytest.mark.parametrize("rows,cols,nulls", [
    (1509, 5, 2),      # EXPEND.SSP -- HHSIZE, WT
    (3326, 4, 2),      # PANEL.SSP  -- PID85, PID90
    (1528, 10, 7),     # N00A.SSP
    (86, 311, 254),    # GhanaLSS COMM.DAT parsed fixed-width per COMM.DCT
])
def test_every_measured_known_bad_parse_is_caught(rows, cols, nulls):
    """The threshold must catch every parse we have actually measured going
    wrong.  The tightest of these is EXPEND.SSP at 40%; the corpus background
    tops out at 28.3%, which is the gap the threshold sits in."""
    assert audit_read(_frame(rows, cols, nulls), "blob") is not None


def test_fixed_width_parse_of_a_delimited_file_is_reported(tmp_path):
    """The GhanaLSS ``COMM.DAT`` class, reproduced with a REAL parse.

    ``COMM.DCT`` declares 311 fixed-width fields of width 12; ``COMM.DAT``
    actually ships comma-delimited with a header row.  ``read_fwf`` does not
    complain -- it returns the right shape full of NaN.  Here the same collision
    is staged on a temp file so the failure is the parser's, not a fixture's.
    """
    names = [f"V{i}" for i in range(12)]
    fn = tmp_path / "COMM.DAT"
    rows = ["\n".join(",".join(str(i * 100 + j) for j in range(12))
                      for i in range(40))]
    fn.write_text(",".join(names) + "\n" + rows[0] + "\n")

    # What the .DCT would say: 12 fields of width 12, i.e. columns 0-143.
    colspecs = [(i * 12, (i + 1) * 12) for i in range(12)]
    misparsed = pd.read_fwf(fn, colspecs=colspecs, names=names, skiprows=1)
    n_null = sum(misparsed[c].isna().all() for c in misparsed.columns)
    assert n_null / misparsed.shape[1] >= _NULL_FRACTION_TRIGGER, (
        "precondition: the fixed-width misread must actually produce nulls")
    assert audit_read(misparsed, fn) is not None

    # ...and the CORRECT parse of the very same bytes says nothing.
    assert audit_read(pd.read_csv(fn), fn) is None


def test_get_dataframe_reports_end_to_end_and_returns_the_data_unchanged(tmp_path):
    """Through the real reader.  Two assertions, and the second is the one that
    keeps this guard shippable: the returned frame is byte-identical to what the
    reader returned before the guard existed."""
    from lsms_library.local_tools import get_dataframe

    fn = tmp_path / "mostly_empty.csv"
    fn.write_text("a,b,c,d\n1,,,\n2,,,\n3,,,\n")
    expected = pd.read_csv(fn)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        got = get_dataframe(str(fn))

    fired = [w for w in caught if issubclass(w.category, NullReadWarning)]
    assert fired, "3 of 4 columns are all-null; the guard must say so"
    assert "mostly_empty.csv" in str(fired[0].message)
    pd.testing.assert_frame_equal(got, expected)


def test_healthy_read_is_silent(tmp_path):
    from lsms_library.local_tools import get_dataframe

    fn = tmp_path / "healthy.csv"
    fn.write_text("a,b,c,d\n1,2,3,4\n5,6,7,8\n")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        get_dataframe(str(fn))
    assert not [w for w in caught if issubclass(w.category, NullReadWarning)]


def test_scattered_empty_columns_are_silent():
    """The firehose test.  Albania 2004 ``w3_hh_basic.dta`` is the corpus
    maximum: 169 of 598 columns all-null (28.3%) in a perfectly healthy read.
    A per-column trigger would fire 169 times on this one file."""
    assert audit_read(_frame(1797, 598, 169), "Albania/2004/w3_hh_basic.dta") is None


def test_zero_row_frame_is_silent():
    """Every column of an empty table is vacuously all-null.  That is a
    different defect with a different fix, and reporting it here would make
    the signal mean two things."""
    assert audit_read(pd.DataFrame({"a": [], "b": []}), "empty.dta") is None


def test_empty_string_is_a_value_not_a_null():
    """A recorded blank is data the survey collected.  Calling it missing would
    be the guard inventing a defect."""
    df = pd.DataFrame({"a": ["", "", ""], "b": ["", "", ""], "c": [1, 2, 3]})
    assert audit_read(df, "blanks.csv") is None


def test_check_read_returns_its_input_object():
    df = _frame(10, 3, 3)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert check_read(df, "x") is df


# ---------------------------------------------------------------------------
# Site B -- a required declared column that is present and empty
# ---------------------------------------------------------------------------

def _built(waves_with_lat=("a",), waves_without=("b",), n=5):
    """A (t, v)-indexed cluster_features-shaped frame."""
    rows = []
    for t in list(waves_with_lat) + list(waves_without):
        has = t in waves_with_lat
        for k in range(n):
            rows.append({"t": t, "v": f"{k}", "Region": "R",
                         "Latitude": 1.5 if has else np.nan})
    return pd.DataFrame(rows).set_index(["t", "v"])


def test_declared_required_column_empty_in_one_wave_is_reported():
    reports = audit_declared_columns(
        _built(), ["Region", "Latitude"], country="Testland",
        table="cluster_features")
    assert len(reports) == 1
    (rep,) = reports
    assert rep["column"] == "Latitude"
    assert rep["scope"] == "waves"
    assert rep["waves"] == ["b"]


def test_declared_required_column_empty_everywhere_is_reported_as_whole():
    reports = audit_declared_columns(
        _built(waves_with_lat=(), waves_without=("a", "b")),
        ["Region", "Latitude"], country="Testland", table="cluster_features")
    assert [r["scope"] for r in reports] == ["whole"]
    assert reports[0]["column"] == "Latitude"


def test_empty_optional_column_is_silent():
    """``optional: true`` means the author has already said this column may be
    absent for this country.  ``_required_scheme_columns`` drops it, so it never
    reaches the audit -- exercised here through that real helper rather than by
    hand, so the two readings of 'required' cannot drift apart."""
    from lsms_library.country import _required_scheme_columns

    entry = {"index": "(t, v)", "Region": "str",
             "Latitude": {"type": "float", "optional": True}}
    required = _required_scheme_columns(entry)
    assert "Latitude" not in required
    assert audit_declared_columns(
        _built(waves_with_lat=(), waves_without=("a", "b")),
        required, country="Testland", table="cluster_features") == []


def test_a_populated_table_is_silent():
    assert audit_declared_columns(
        _built(waves_with_lat=("a", "b"), waves_without=()),
        ["Region", "Latitude"], country="Testland",
        table="cluster_features") == []


def test_a_column_absent_from_the_frame_is_not_this_guards_business():
    """Presence is ``_assert_built_required_columns``'s job and it RAISES.  This
    guard must not double-report it as emptiness, or the two guards will be read
    as disagreeing."""
    assert audit_declared_columns(
        _built(), ["Region", "Latitude", "NotThere"], country="Testland",
        table="cluster_features")[0]["column"] == "Latitude"


def test_check_declared_columns_returns_its_input_object():
    df = _built()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert check_declared_columns(
            df, ["Latitude"], country="T", table="x") is df


# ---------------------------------------------------------------------------
# Strict mode -- its own lever, same spelling as LSMS_GRAIN_STRICT
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes"])
def test_read_strict_spellings_are_fatal(monkeypatch, value):
    monkeypatch.setenv("LSMS_READ_STRICT", value)
    with pytest.raises(NullReadError):
        check_read(_frame(10, 3, 3), "x.dta")


@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_read_strict_off_spellings_only_warn(monkeypatch, value):
    monkeypatch.setenv("LSMS_READ_STRICT", value)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_read(_frame(10, 3, 3), "x.dta")
    assert [w for w in caught if issubclass(w.category, NullReadWarning)]


def test_read_strict_is_fatal_at_site_b_too(monkeypatch):
    monkeypatch.setenv("LSMS_READ_STRICT", "1")
    with pytest.raises(NullReadError, match="Latitude"):
        check_declared_columns(_built(), ["Latitude"], country="Testland",
                               table="cluster_features")


def test_grain_strict_does_not_arm_the_read_guard(monkeypatch):
    """Separate concerns, separate levers.  A maintainer ratcheting grain
    collapse to zero must not be handed a second, unrelated class of failure in
    the same flip."""
    monkeypatch.delenv("LSMS_READ_STRICT", raising=False)
    monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_read(_frame(10, 3, 3), "x.dta")
    assert [w for w in caught if issubclass(w.category, NullReadWarning)]


# ---------------------------------------------------------------------------
# The audit-harness surface
# ---------------------------------------------------------------------------

def test_null_read_reports_collects_both_sites():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        check_declared_columns(_built(), ["Latitude"], country="Testland",
                               table="cluster_features")
        check_read(_frame(10, 3, 3), "/tmp/whatever.dta")
    assert len(null_read_reports()) == 2
    assert len(null_read_reports(country="Testland")) == 1
    assert len(null_read_reports(country="Testland",
                                 table="cluster_features")) == 1


def test_a_report_is_filed_once_not_once_per_emission():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for _ in range(3):
            check_declared_columns(_built(), ["Latitude"], country="Testland",
                                   table="cluster_features")
    assert len(null_read_reports(country="Testland")) == 1


def test_the_message_names_country_table_column_and_waves():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_declared_columns(_built(), ["Latitude"], country="Testland",
                               table="cluster_features")
    msg = str([w for w in caught
               if issubclass(w.category, NullReadWarning)][0].message)
    for token in ("Testland", "cluster_features", "Latitude", "b",
                  "LSMS_READ_STRICT"):
        assert token in msg, f"{token!r} missing from: {msg}"


# ---------------------------------------------------------------------------
# Cache-hash neutrality -- measured, not assumed
# ---------------------------------------------------------------------------

def test_the_audit_module_is_not_folded_into_any_build_fingerprint():
    """Editing this guard must not cold-rebuild the corpus.

    ``check_read`` is called from ``get_dataframe``, which every build-path
    callable reaches, so without the ``_EXCLUDED_CALLABLES`` entries a reworded
    warning would move ``build_transforms_fingerprint`` for every table in every
    country.  Measured with a stub edit before this landed: 37 of 37 probed
    ``Country._table_cache_hash`` values moved.

    Asserted structurally (no source of ``null_read_audit`` in any fingerprint
    part) rather than by comparing two hex digests, so the test still means
    something after any legitimate hash change elsewhere.
    """
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    leaked = [p.split("=")[0] for p in parts if "null_read_audit" in p]
    assert not leaked, (
        "null_read_audit source leaked into the build fingerprint: "
        f"{leaked}. Add the callable to _build_registry._EXCLUDED_CALLABLES "
        "or every table in every country rebuilds on a docstring edit.")


def test_site_b_host_is_excluded_so_site_b_costs_no_invalidation():
    """Site B lives in ``Country._finalize_result``, which is read-path and
    already excluded.  Pinned because moving the call to
    ``_aggregate_wave_data`` (its obvious neighbour, and where
    ``_assert_built_required_columns`` is called from) WOULD invalidate the
    whole corpus -- that one is a tagged build orchestrator."""
    assert ("lsms_library.country.Country._finalize_result"
            in R._EXCLUDED_CALLABLES)


# ---------------------------------------------------------------------------
# The real corpus instance
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_niger_2014_15_latitude_is_reported():
    """Niger declares ``Latitude: float`` REQUIRED in ``_/data_scheme.yml`` and
    serves it 0-of-270 populated for 2014-15.  Every shape guard passes.

    ``Niger/_/CONTENTS.org`` records that this absence is *correct*: "2014-15
    genuinely ships no geovariables/offsets file of any kind ... so its
    Latitude/Longitude are honestly absent -- not mis-addressed.  It is NOT
    wired, and that is correct."  The guard still reports it, and should: the
    point is to make the emptiness visible, not to adjudicate it.  See the
    ledger's open question -- ``optional:`` is country-grain while this absence
    is wave-grain, so there is no way to record the judgement today.
    """
    from lsms_library.country import Country

    _clear_null_read_reports()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = Country("Niger").cluster_features()

    lat = df.xs("2014-15", level="t")["Latitude"]
    assert len(lat) == 270 and lat.notna().sum() == 0, (
        "precondition changed -- if Niger 2014-15 now has coordinates, "
        "this test should be deleted, not weakened")

    reports = null_read_reports(country="Niger", table="cluster_features")
    lat_reports = [r for r in reports if r["column"] == "Latitude"]
    assert lat_reports, f"guard stayed silent; reports={reports}"
    assert "2014-15" in (lat_reports[0]["waves"] or [])
