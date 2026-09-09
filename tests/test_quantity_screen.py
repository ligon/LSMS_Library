"""SITE Q -- the present, non-null, impossible value (GH #857).

What these tests pin, and why each one is here rather than being obvious:

* the rule fires on a LONE extreme and stays silent on a merely large one, on a
  thin cell, and on a cell with no scale (a zero reference);
* the audit **returns its input frame unchanged** -- the whole design rests on
  "count, never clip", so a mutation is a correctness failure, not a style one;
* the strict lever is its OWN, and neither of the other two arms it;
* the module is not folded into any build fingerprint, so re-wording a warning
  cannot cold-rebuild the corpus;
* and the real Tanzania 2020-21 rows GH #857 names actually fire -- skipped
  cleanly, never silently passed, when the warm parquet is not on this machine.

The synthetic frames are built to the shapes measured in the corpus, not to
shapes that make the rule look good.  ``tests/test_crop_kg_factor.py`` is the
sibling for the factor half of the same "count, never clip" contract.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import _build_registry as R
from lsms_library.quantity_audit import (
    QUANTITY_MIN_CELL,
    QUANTITY_OUTLIER_K,
    QUANTITY_REFERENCE_QUANTILE,
    QuantityImplausibleError,
    QuantityImplausibleWarning,
    _clear_quantity_reports,
    audit_quantities,
    check_quantities,
    quantity_reports,
)


@pytest.fixture(autouse=True)
def _clean_ledger_and_env(monkeypatch):
    """The report ledger is process-wide; a test asserting on it must not
    inherit another test's findings.  Strict mode likewise must not leak."""
    monkeypatch.delenv("LSMS_QUANTITY_STRICT", raising=False)
    _clear_quantity_reports()
    yield
    _clear_quantity_reports()


def _cell(n=60, wave="2020-21", unit="kg", crop="Coconuts", base=100.0,
          extras=()):
    """A ``crop_production``-shaped cell: *n* ordinary rows plus *extras*.

    Index ``(t, i, plot, j)`` with ``u`` as a COLUMN -- Tanzania's shape, one
    of the three the corpus actually ships (see ``_AXES``).
    """
    rows = [{"t": wave, "i": f"hh{k:04d}", "plot": "1", "j": crop,
             "u": unit, "Quantity": base * (1 + k % 10)} for k in range(n)]
    rows += list(extras)
    return pd.DataFrame(rows).set_index(["t", "i", "plot", "j"])


def _outlier(value, i="hh9999", crop="Coconuts", unit="kg", wave="2020-21"):
    return {"t": wave, "i": i, "plot": "1", "j": crop, "u": unit,
            "Quantity": value}


def _audit(df):
    return audit_quantities(df, "Quantity", country="Testland",
                            table="crop_production")


# ---------------------------------------------------------------------------
# Fires
# ---------------------------------------------------------------------------

def test_a_lone_extreme_in_a_healthy_cell_is_reported():
    """The Tanzania coconut shape, scaled down: a cell of ordinary harvests
    with one row four orders of magnitude above them."""
    reports = _audit(_cell(extras=[_outlier(7_500_000.0)]))
    assert len(reports) == 1
    report = reports[0]
    assert report["n_implausible"] == 1
    assert report["wave"] == "2020-21"
    offender = report["offenders"][0]
    assert offender["i"] == "hh9999" and offender["j"] == "Coconuts"
    assert offender["u"] == "kg" and offender["value"] == 7_500_000.0
    assert offender["ratio"] > QUANTITY_OUTLIER_K


def test_the_report_names_the_row_not_merely_the_count():
    """A count alone is not actionable: #857 asks for household, plot, crop,
    unit and wave, because the fix is always in one wave's source column."""
    report = _audit(_cell(extras=[_outlier(9e6, i="hh-named")]))[0]
    offender = report["offenders"][0]
    assert set(offender) >= {"i", "plot", "j", "u", "value", "reference",
                             "ratio", "rung"}
    assert offender["i"] == "hh-named"
    assert report["country"] == "Testland" and report["table"] == "crop_production"


def test_each_wave_gets_its_own_report():
    """Waves are graded separately everywhere else in the library; a finding
    that spans two of them is two findings."""
    df = pd.concat([_cell(extras=[_outlier(9e6)]),
                    _cell(wave="2019-20",
                          extras=[_outlier(9e6, wave="2019-20")])])
    waves = sorted(r["wave"] for r in _audit(df))
    assert waves == ["2019-20", "2020-21"]


def test_several_offenders_in_one_wave_are_one_report_not_one_each():
    """One warning per (country, table, wave).  A warning per row would bury
    the finding in exactly the way ``null_read_audit``'s per-column form did
    (884 of 887 firings were noise)."""
    df = _cell(extras=[_outlier(9e6, i="a"), _outlier(8e6, i="b")])
    reports = _audit(df)
    assert len(reports) == 1 and reports[0]["n_implausible"] == 2
    assert [o["i"] for o in reports[0]["offenders"]] == ["a", "b"]  # by ratio


# ---------------------------------------------------------------------------
# Does not fire
# ---------------------------------------------------------------------------

def test_a_merely_large_value_is_silent():
    """Ten times the 90th percentile is a big harvest, not an impossible one.
    The threshold is 100x and the guard must respect it."""
    assert _audit(_cell(extras=[_outlier(10_000.0)])) == []


def test_a_thin_cell_cannot_manufacture_a_reference():
    """Fewer than QUANTITY_MIN_CELL comparable rows at every rung of the
    ladder means no judgement -- silence, not a guess."""
    df = _cell(n=QUANTITY_MIN_CELL - 5, extras=[_outlier(9e9)])
    assert _audit(df) == []


def test_a_cell_with_no_scale_is_silent():
    """A cell whose 90th percentile is zero carries no scale, so it cannot
    support a plausibility judgement.  Measured cost of omitting this guard:
    six Malawi rows of 2-30 units fired at ratio ``inf`` -- a 2 kg cassava
    harvest named as implausible."""
    df = _cell(base=0.0, extras=[_outlier(30.0)])
    assert _audit(df) == []


def test_nulls_and_infinities_are_not_outliers():
    df = _cell(extras=[_outlier(np.nan), _outlier(np.inf, i="hh-inf")])
    reports = _audit(df)
    named = {o["i"] for r in reports for o in r["offenders"]}
    assert "hh9999" not in named  # the NaN row


def test_a_healthy_frame_is_silent():
    assert _audit(_cell()) == []


def test_a_frame_without_the_column_is_not_this_guards_business():
    df = _cell().drop(columns=["Quantity"])
    assert _audit(df) == []
    assert audit_quantities(pd.DataFrame(), "Quantity", country="T",
                            table="crop_production") == []


def test_an_unscreened_table_is_not_audited():
    """``_SCREENED_COLUMNS`` is a registry of what is LOOKED AT, and a table
    that is not in it costs one dictionary lookup."""
    df = _cell(extras=[_outlier(9e9)])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert check_quantities(df, country="T", table="food_acquired") is df
    assert not [w for w in caught
                if issubclass(w.category, QuantityImplausibleWarning)]


# ---------------------------------------------------------------------------
# Count, never clip
# ---------------------------------------------------------------------------

def test_the_audit_returns_its_input_object_and_changes_no_value():
    """The load-bearing contract.  ``_screen_reported_factors`` states it for
    the factor half: "A rejected value is NEVER clipped or rescaled -- inventing
    a weight is the failure this whole design exists to avoid.\""""
    df = _cell(extras=[_outlier(7.5e6)])
    before = df["Quantity"].to_numpy(dtype="float64", na_value=np.nan).copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = check_quantities(df, country="T", table="crop_production")
    assert out is df
    after = df["Quantity"].to_numpy(dtype="float64", na_value=np.nan)
    assert np.array_equal(before, after)
    assert df["Quantity"].max() == 7.5e6, "the offending value was altered"


def test_a_nullable_masked_dtype_does_not_break_the_comparison():
    """``Quantity`` arrives as ``Float64`` from the real parquets, and a bare
    ``>`` between a masked array and a float array yields ``pd.NA`` rather than
    ``False``.  Hit twice while measuring; pinned so it stays fixed."""
    df = _cell(extras=[_outlier(7.5e6)])
    df["Quantity"] = df["Quantity"].astype("Float64")
    reports = _audit(df)
    assert len(reports) == 1 and reports[0]["n_implausible"] == 1


# ---------------------------------------------------------------------------
# The reference statistic -- why p90 and not p99
# ---------------------------------------------------------------------------

def test_p90_survives_the_second_outlier_where_p99_does_not():
    """Tanzania's ``(2020-21, kg, Timber)`` cell, n=37, reproduced: p99 is
    2,632,000 (66% of the very row under test), p95 is 70,800 (dragged up by
    the SECOND outlier at 200,000) and p90 is 23,088 (clean).

    This is the measurement that chose ``QUANTITY_REFERENCE_QUANTILE``.  Under
    a p99 reference the 4,000,000 kg row does not fire at any threshold that
    leaves the rest of the corpus quiet.
    """
    tail = [3500., 3600., 10200., 15000., 16200., 18480., 30000., 38500.]
    values = ([600.0] * (37 - len(tail) - 2) + tail + [200_000., 4_000_000.])
    cell = pd.Series(values, dtype="float64")
    assert cell.quantile(0.99) > 2e6            # eaten by the row under test
    assert cell.quantile(0.95) > 5e4            # eaten by the second outlier
    assert cell.quantile(0.90) == pytest.approx(23_088.0)
    assert 4_000_000.0 / cell.quantile(QUANTITY_REFERENCE_QUANTILE) > QUANTITY_OUTLIER_K
    assert 4_000_000.0 / cell.quantile(0.99) < QUANTITY_OUTLIER_K


def test_the_ladder_falls_back_but_never_pools_across_waves():
    """A crop too thin to judge on its own borrows the wave's UNIT cell, not
    another wave's -- ``_survey_median_factors``' rule: a thin module does not
    borrow a thick one's licence."""
    df = pd.concat([
        _cell(n=60, crop="Common"),
        _cell(n=5, crop="Rare", extras=[_outlier(9e6, crop="Rare")]),
    ])
    report = _audit(df)[0]
    assert report["offenders"][0]["rung"] == "t,u"
    keys = {r["rung"] for rep in _audit(df) for r in rep["offenders"]}
    assert "u,j" not in keys, "a rung without t would pool across waves"


# ---------------------------------------------------------------------------
# Strict mode -- its own lever, same spelling as the other two
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes"])
def test_quantity_strict_spellings_are_fatal(monkeypatch, value):
    monkeypatch.setenv("LSMS_QUANTITY_STRICT", value)
    with pytest.raises(QuantityImplausibleError, match="Coconuts"):
        check_quantities(_cell(extras=[_outlier(9e6)]), country="T",
                         table="crop_production")


@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_quantity_strict_off_spellings_only_warn(monkeypatch, value):
    monkeypatch.setenv("LSMS_QUANTITY_STRICT", value)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_quantities(_cell(extras=[_outlier(9e6)]), country="T",
                         table="crop_production")
    assert [w for w in caught
            if issubclass(w.category, QuantityImplausibleWarning)]


@pytest.mark.parametrize("lever", ["LSMS_READ_STRICT", "LSMS_GRAIN_STRICT"])
def test_the_other_levers_do_not_arm_this_guard(monkeypatch, lever):
    """Three concerns, three levers.  A destroyed row, an empty column and an
    impossible value ratchet separately -- a maintainer turning one on in CI
    must not be handed the other two in the same flip."""
    monkeypatch.delenv("LSMS_QUANTITY_STRICT", raising=False)
    monkeypatch.setenv(lever, "1")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_quantities(_cell(extras=[_outlier(9e6)]), country="T",
                         table="crop_production")
    assert [w for w in caught
            if issubclass(w.category, QuantityImplausibleWarning)]


# ---------------------------------------------------------------------------
# The ledger and its accessor
# ---------------------------------------------------------------------------

def test_quantity_reports_drains_what_was_filed():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        check_quantities(_cell(extras=[_outlier(9e6)]), country="Testland",
                         table="crop_production")
    assert quantity_reports() and quantity_reports(country="Testland")
    assert quantity_reports(country="Elsewhere") == []
    assert quantity_reports(table="food_acquired") == []
    report = quantity_reports(country="Testland", table="crop_production")[0]
    assert report["offenders"][0]["value"] == 9e6


def test_a_report_is_filed_once_not_once_per_emission():
    df = _cell(extras=[_outlier(9e6)])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        check_quantities(df, country="Testland", table="crop_production")
        check_quantities(df, country="Testland", table="crop_production")
    assert len(quantity_reports(country="Testland")) == 1


def test_the_message_names_country_table_wave_and_the_lever():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_quantities(_cell(extras=[_outlier(9e6)]), country="Testland",
                         table="crop_production")
    msg = str(caught[0].message)
    for token in ("Testland", "crop_production", "2020-21", "Coconuts",
                  "LSMS_QUANTITY_STRICT", "NOTHING HAS BEEN CHANGED"):
        assert token in msg, f"{token!r} missing from the warning"


def test_a_broken_audit_never_breaks_a_read(monkeypatch):
    """The read is the job.  A failure inside the audit must warn, not raise --
    and must still hand the frame back.  An instrument that fails silently and
    reports clean is the disease this module exists to cure, so the failure is
    announced rather than swallowed."""
    import lsms_library.quantity_audit as qa

    def boom(*_a, **_k):
        raise RuntimeError("boom")

    monkeypatch.setattr(qa, "audit_quantities", boom)
    df = _cell()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = check_quantities(df, country="T", table="crop_production")
    assert out is df
    assert any("could not run" in str(w.message) for w in caught)
    assert any(issubclass(w.category, QuantityImplausibleWarning)
               for w in caught)


# ---------------------------------------------------------------------------
# Cache -- a reworded warning must not cold-rebuild the corpus
# ---------------------------------------------------------------------------

def test_the_audit_module_is_not_folded_into_any_build_fingerprint():
    """Asserted structurally (no source of ``quantity_audit`` in any
    fingerprint part) rather than by comparing two hex digests, so the test
    still means something after any legitimate hash change elsewhere.

    Measured when this landed: 0 of 12 probed ``Country._table_cache_hash``
    values and 0 of 5 ``build_transforms_fingerprint`` values moved.
    """
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    leaked = [p.split("=")[0] for p in parts if "quantity_audit" in p]
    assert not leaked, (
        "quantity_audit source leaked into the build fingerprint: "
        f"{leaked}. Add the callable to _build_registry._EXCLUDED_CALLABLES "
        "or every table in every country rebuilds on a docstring edit.")


def test_site_q_host_is_excluded_so_site_q_costs_no_invalidation():
    """Site Q lives in ``Country._finalize_result``, which is read-path and
    already excluded.  Moving the call to ``_aggregate_wave_data`` -- its
    obvious neighbour -- WOULD invalidate the whole corpus."""
    assert ("lsms_library.country.Country._finalize_result"
            in R._EXCLUDED_CALLABLES)
    assert "_QUANTITY_LEDGER" in R._EXCLUDED_CONSTANTS


# ---------------------------------------------------------------------------
# The real corpus instance
# ---------------------------------------------------------------------------

def test_tanzania_2020_21_coconut_and_timber_rows_fire():
    """GH #857's own numbers, against the warm wave parquet.

    Skipped -- never silently passed -- when the parquet is not on this
    machine: building it here would be a corpus rebuild inside a unit test.
    Rebuild it with ``Country('Tanzania').crop_production()`` if you want this
    to run.
    """
    from lsms_library.paths import data_root

    path = (data_root() / "Tanzania" / "2020-21" / "_"
            / "crop_production.parquet")
    if not path.exists():
        pytest.skip(f"warm Tanzania 2020-21 crop_production absent ({path})")

    df = pd.read_parquet(path)
    reports = audit_quantities(df, "Quantity", country="Tanzania",
                               table="crop_production")
    assert len(reports) == 1, [r["wave"] for r in reports]
    report = reports[0]
    named = {(o["j"], o["value"]): o for o in report["offenders"]}

    # The four rows the issue cites, with the households it names.
    assert named[("Coconuts", 7_500_000.0)]["i"].startswith("9640-001")
    assert named[("Timber", 4_000_000.0)]["i"].startswith("2010-001")
    assert ("Other Fruits", 2_400_000.0) in named
    assert ("Ripe Bananas", 1_505_400.0) in named

    # ... and a bounded total, so a future rule change that turns the screen
    # into a firehose fails here rather than in someone's analysis.
    assert report["n_implausible"] == 13, [
        (o["j"], o["value"], o["ratio"]) for o in report["offenders"]]
    assert report["rows"] == 11_125
