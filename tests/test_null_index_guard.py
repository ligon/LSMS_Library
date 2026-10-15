"""GH #847: a NaN on a declared index level is a deferred silent deletion.

The framework-side instance is Grain Collapse decision D2 ("delete-and-report":
count the NaN-key rows a collapse deletes, never silently merge them).  This
issue is the **read-path half**: where a wave's index happens to be unique, no
collapse runs and the NaN-keyed rows are served intact -- with no count
anywhere.

The synthetic frames reproduce the measured pre-GH-#842 Niger shape; the
corpus-wide census (``bench/null_index_census.py``, 2026-09-13) measured
19 firing cells of 384 warm L2 tables, the live instances being GhanaLSS
``food_acquired`` u=13,380 (2016-17 produced lines with no unit), GhanaLSS
``plot_features`` plot_id=13,944, CotedIvoire ``crop_production`` u=6,365,
Malawi ``crop_production`` u=8,535 + plot_id=40, GhanaSPS ``food_acquired``
u=13,903, and Niger's ``v=2`` on 2011-12 ``crop_production`` (GH #842's
sentinel covered the ``u`` axis, not the v-join's own sparse row).

===============================  ============================================
instance                         test
===============================  ============================================
synthetic (t, i, j, u) per-level   ``test_per_level_and_per_wave_counts``
partial / whole-scope findings
silent on a fully-populated index   ``test_fully_populated_index_is_silent``
Niger ``v`` NaN live instance      ``test_niger_unit_nan_is_reported_end_to_end``
===============================  ============================================

The two most weighty negative tests are pinned so the guard cannot become a
firehose:

* ``test_fully_populated_index_is_silent`` -- the census above is what the
  trigger must produce: 19 cells, not hundreds.  Any broader trigger would
  be a firehose, and any narrower one would falsify that corpus count.
* ``test_undeclared_level_is_silent`` -- a frame's non-declared level (an
  extra axis carried past the declared index) must not fire; the guard
  checks what the SCHEMA declared, and the country's ``data_scheme.yml``
  plus the canonical ``data_info.yml`` Index Info decide what it checked.
"""
from __future__ import annotations

import os
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import _build_registry as R
from lsms_library.null_index_audit import (
    NullIndexKeyError,
    NullIndexKeyWarning,
    _clear_null_index_reports,
    audit_index_levels,
    check_index_levels,
    null_index_reports,
)


@pytest.fixture(autouse=True)
def _clean_ledger_and_env(monkeypatch):
    """The report ledger is process-wide; a test asserting on it must not
    inherit another test's findings.  Strict mode likewise must not leak."""
    monkeypatch.delenv("LSMS_READ_STRICT", raising=False)
    _clear_null_index_reports()
    yield
    _clear_null_index_reports()


def _frame(rows: list[dict], levels: list[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame({c: [] for c in levels}).set_index(levels)
    return pd.DataFrame(rows).set_index(levels)


def _nan_frame() -> pd.DataFrame:
    """A (t, i, j, u)-indexed food_acquired-shaped frame, one wave carrying a
    NaN `u`, a second wave clean: the pre-#842 Niger shape."""
    rows = []
    for (t, u, bad) in [("a", "Kg", False), ("b", np.nan, True)]:
        for k in range(5 if not bad else 3):
            rows.append({"t": t, "i": f"h{k}", "j": "Millet",
                         "u": u if not bad else np.nan, "Quantity": 1.0})
    return _frame(rows, ["t", "i", "j", "u"])


# ---------------------------------------------------------------------------
# The audit's two grains -- per level, per wave
# ---------------------------------------------------------------------------

def test_per_level_and_per_wave_counts():
    reports = audit_index_levels(_nan_frame(), ["t", "i", "j", "u"],
                                 country="Testland", table="food_acquired")
    assert len(reports) == 1
    (rep,) = reports
    assert rep["rows"] == len(_nan_frame())
    (u,) = rep["findings"]
    assert u["level"] == "u"
    assert u["n_nan"] == 3
    assert u["waves"] == {"b": 3}


def test_two_null_levels_are_each_counted():
    df = _nan_frame()
    # blank out `j` on the same rows `u` is also blank: both findings.
    import pandas as _pd
    rows2 = [{"t": "a", "i": "h0", "j": np.nan, "u": np.nan,
              "Quantity": 1.0}]
    df2 = _frame(rows2, ["t", "i", "j", "u"])
    reports = audit_index_levels(df2, ["t", "i", "j", "u"],
                                 country="Testland", table="x")
    levels = {f["level"]: f["n_nan"] for f in reports[0]["findings"]}
    assert levels == {"j": 1, "u": 1}


def test_fully_populated_index_is_silent():
    rows = [{"t": "a", "i": f"h{k}", "u": "Kg", "Quantity": 1.0}
            for k in range(4)]
    reports = audit_index_levels(
        _frame(rows, ["t", "i", "u"]), ["t", "i", "u"],
        country="Testland", table="x")
    assert reports == []


def test_undeclared_level_is_silent():
    """A level the schema did NOT declare (an extra axis carried past
    normalization, a label axis) must not fire -- the guard checks the
    declared schema, not every level the frame happens to have."""
    rows = [{"t": "a", "i": "h0", "extra": np.nan, "Quantity": 1.0}]
    reports = audit_index_levels(
        _frame(rows, ["t", "i", "extra"]), ["t", "i"],
        country="Testland", table="x")
    assert reports == []


def test_absent_declared_level_is_not_reported():
    """A declared level that never made it onto the frame is the
    normalize-data-frame machinery's problem (it has its own report channel);
    this guard must not invent a row for it."""
    rows = [{"t": "a", "i": "h0", "Quantity": 1.0}]
    reports = audit_index_levels(
        _frame(rows, ["t", "i"]), ["t", "i", "u"],
        country="Testland", table="x")
    assert reports == []


def test_empty_frame_is_silent():
    df = _frame([], ["t", "i"])
    assert audit_index_levels(df, ["t", "i"], country="T", table="x") == []


def test_check_index_levels_returns_its_input_object():
    df = _nan_frame()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert check_index_levels(df, ["u"], country="T", table="x") is df


# ---------------------------------------------------------------------------
# Strict mode -- LSMS_READ_STRICT, the shared lever
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes"])
def test_read_strict_spellings_are_fatal(monkeypatch, value):
    monkeypatch.setenv("LSMS_READ_STRICT", value)
    with pytest.raises(NullIndexKeyError, match="'u'"):
        check_index_levels(_nan_frame(), ["u"], country="Testland",
                           table="food_acquired")


@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_read_strict_off_spellings_only_warn(monkeypatch, value):
    monkeypatch.setenv("LSMS_READ_STRICT", value)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_index_levels(_nan_frame(), ["u"], country="Testland",
                           table="food_acquired")
    assert [w for w in caught if issubclass(w.category, NullIndexKeyWarning)]


def test_grain_strict_does_not_arm_the_index_guard(monkeypatch):
    """Deliberately its own concern from ``LSMS_GRAIN_STRICT``: a maintainer
    ratcheting destroyed rows to zero must not be handed the null-content
    class in the same flip.  The guard shares ``LSMS_READ_STRICT`` with the
    all-null-column guard, and that is the decision, pinned here."""
    monkeypatch.delenv("LSMS_READ_STRICT", raising=False)
    monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_index_levels(_nan_frame(), ["u"], country="Testland",
                           table="x")
    assert [w for w in caught if issubclass(w.category, NullIndexKeyWarning)]


# ---------------------------------------------------------------------------
# The ledger and the message
# ---------------------------------------------------------------------------

def test_null_index_reports_collects_and_filters():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        check_index_levels(_nan_frame(), ["u"], country="Testland",
                           table="food_acquired")
        check_index_levels(_nan_frame(), ["u"], country="Elsewhere",
                           table="x")
    assert len(null_index_reports()) == 2
    assert len(null_index_reports(country="Testland")) == 1
    assert len(null_index_reports(country="Testland",
                                  table="food_acquired")) == 1


def test_a_report_is_filed_once_not_once_per_emission():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for _ in range(3):
            check_index_levels(_nan_frame(), ["u"], country="Testland",
                               table="x")
    assert len(null_index_reports(country="Testland")) == 1


def test_the_message_names_country_table_level_and_wave():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_index_levels(_nan_frame(), ["u"], country="Testland",
                           table="food_acquired")
    msg = str([w for w in caught
               if issubclass(w.category, NullIndexKeyWarning)][0].message)
    for token in ("Testland", "food_acquired", "'u'", "'b'", "3",
                  "LSMS_READ_STRICT"):
        assert token in msg, f"{token!r} missing from: {msg}"


# ---------------------------------------------------------------------------
# Cache-hash neutrality -- measured, not assumed
# ---------------------------------------------------------------------------

def test_the_audit_module_is_not_folded_into_any_build_fingerprint():
    """Editing this guard must not cold-rebuild the corpus.

    ``check_index_levels`` is called from ``Country._finalize_result``, which
    is read-path and excluded from every fingerprint.  Without the
    ``_EXCLUDED_CALLABLES`` entries the walk could still reach this module on
    a future call site and re-wording a warning would move
    ``build_transforms_fingerprint`` for every table in every country.
    Asserted structurally (no source of ``null_index_audit`` in any
    fingerprint part) rather than by comparing two hex digests, so the test
    still means something after any legitimate hash change elsewhere.
    """
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    leaked = [p.split("=")[0] for p in parts if "null_index_audit" in p]
    assert not leaked, (
        "null_index_audit source leaked into the build fingerprint: "
        f"{leaked}. Add the callable to _build_registry._EXCLUDED_CALLABLES "
        "or every table in every country rebuilds on a docstring edit.")


def test_site_i_host_is_excluded_so_site_i_costs_no_invalidation():
    """The call lives in ``Country._finalize_result``, which is read-path and
    already excluded.  Pinned because moving the call to
    ``_aggregate_wave_data`` (the tagged build orchestrator) WOULD invalidate
    the corpus."""
    assert ("lsms_library.country.Country._finalize_result"
            in R._EXCLUDED_CALLABLES)


# ---------------------------------------------------------------------------
# The real corpus instance -- the pre-GH-#842 Niger cell
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_niger_unit_nan_is_reported_end_to_end():
    """Niger's 2018-19 ``crop_production`` (t, i, plot, crop, u) index IS
    unique, so no collapse runs and its 441 NaN-``u`` rows were served intact
    -- the exact "served with no count" shape this guard exists to surface.
    GH #842's ``Unknown`` sentinel now fills that unit at build time, so the
    shipped table is clean: this test builds the guard's input behind a
    stripped level to keep the end-to-end shape pinned to the defect, not
    re-litigating the country fix.
    """
    from lsms_library.country import Country

    _clear_null_index_reports()
    c = Country("Niger")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = c.crop_production()

    # The country fix (GH #842) landed: the delivered table's declared `u`
    # level carries no NaN.  The one remaining NaN-keyed level is `v`, with
    # exactly two rows in 2011-12 -- a live, unresolved instance of the class
    # this guard reports, and therefore the case this test checks.
    declare = ["t", "v", "i", "plot_id", "crop", "u"]
    present = [l for l in declare if l in df.index.names]
    nan_levels = {
        l: int(df.index.get_level_values(l).isna().sum()) for l in present
        if df.index.get_level_values(l).isna().any()}
    assert nan_levels == {"v": 2}, (
        "precondition changed -- expected the only NaN-keyed declared level "
        f"to be v=2 rows, got {nan_levels}; the live instance moved")
    v_nan_waves = dict(
        pd.Series(np.asarray(df.index.get_level_values("t"))
                  [df.index.get_level_values("v").isna()])
        .value_counts().astype(int))
    assert v_nan_waves == {"2011-12": 2}, v_nan_waves

    # The guard fires, names the level and the wave, and reports the count.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_index_levels(df, declare, country="Niger",
                           table="crop_production")
    fired = [w for w in caught if issubclass(w.category, NullIndexKeyWarning)]
    assert fired, "guard stayed silent on the known NaN-v rows"
    msg = str(fired[0].message)
    assert "Niger" in msg and "crop_production" in msg
    assert "'v'" in msg and "'2011-12'" in msg and "2" in msg

    # And LSMS_READ_STRICT=1 makes the same table fatal -- exercised on a
    # synthetic frame so the test does not depend on the cache's state.
    os.environ["LSMS_READ_STRICT"] = "1"
    try:
        with pytest.raises(NullIndexKeyError):
            check_index_levels(_nan_frame(), ["u"], country="Niger",
                               table="crop_production")
    finally:
        os.environ.pop("LSMS_READ_STRICT", None)
