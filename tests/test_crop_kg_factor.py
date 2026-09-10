"""``crop_production.KgFactor`` and the layered factor in ``harvest_kg``.

``KgFactor`` is the kilograms-per-unit the INSTRUMENT reports for a harvest
row (UNPS ``a5?q6d``, "conversion factor into kg").  ``harvest_kg`` prefers
it over the library's inferred unit->kg factor, on the same principle by
which ``food_acquired``'s exact per-row ``Quantity_kg`` already beats an
inferred factor (``transformations.conversion_to_kgs`` lines ~866-872,
``food_quantities_from_acquired`` ~1306-1311).

The load-bearing test in this module is the FIRST one: a frame with no
``KgFactor`` column must reproduce the pre-change output exactly, because
every country in the corpus is such a frame today.  It is pinned twice --
structurally here against the old algorithm, and empirically against Uganda's
warm build at the bottom of the file.

Schema facts are READ from ``lsms_library/data_info.yml``, never hardcoded
(CLAUDE.md, "Canonical Schema").
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml
from pandas.testing import assert_frame_equal

from lsms_library.transformations import (
    KG_FACTOR_DISAGREEMENT_TOLERANCE,
    KG_FACTOR_LAYERS,
    KG_FACTOR_MAX,
    SURVEY_MEDIAN_MIN_REPORTS,
    U_UNKNOWN,
    _kg_factor_series,
    harvest_kg,
    harvest_kg_factors,
)

_DATA_INFO = Path(__file__).resolve().parent.parent / "lsms_library" / "data_info.yml"

# 'kg' is in KNOWN_METRIC (factor 1.0); 'sack' / 'basket' / 'drum' name no
# metric content, so the inferred layer cannot resolve them -- and with no
# Expenditure column the price-ratio branch never fires either.
KNOWN_UNIT = "kg"
UNKNOWN_UNITS = ("sack", "basket", "drum")
# 'pound' is in KNOWN_METRIC (0.453592) and is NOT the kilogram unit, so the
# reported and inferred layers can both have a value on the same row without
# tripping the "a Kg row must weigh 1" rule.
POUND, POUND_KG = "pound", 0.453592


def _frame(rows, *, with_kgfactor=True):
    """Build a minimal crop_production-shaped frame.

    Each row is ``(t, i, plot, j, u, condition, season, Quantity, KgFactor)``.
    """
    cols = ["t", "i", "plot", "j", "u", "condition", "season",
            "Quantity", "KgFactor"]
    df = pd.DataFrame(rows, columns=cols)
    # ``.to_numpy()`` matters: handing pandas a Series here would make it
    # ALIGN the Series' RangeIndex against the MultiIndex below and quietly
    # produce an all-NaN column, which turns several of these tests vacuous.
    data = {"Quantity": df["Quantity"].astype(float).to_numpy()}
    if with_kgfactor:
        data["KgFactor"] = df["KgFactor"].astype(float).to_numpy()
    return pd.DataFrame(
        data,
        index=pd.MultiIndex.from_frame(
            df[["t", "i", "plot", "j", "u", "condition", "season"]]),
    )


def _legacy_harvest_kg(df):
    """The pre-change algorithm, spelled out: Quantity x inferred factor."""
    qty = pd.to_numeric(df["Quantity"], errors="coerce")
    kg = qty * _kg_factor_series(df)
    out = pd.DataFrame({"Harvest_kg": kg}).replace(0, np.nan).dropna()
    return out.groupby(["t", "i", "plot", "j"]).sum()


# ---------------------------------------------------------------------------
# The invariant: no KgFactor column -> nothing changed
# ---------------------------------------------------------------------------

BASE_ROWS = [
    ("2019-20", "h1", "h1-1-1", "Maize", KNOWN_UNIT, "dried", "A", 40.0, np.nan),
    ("2019-20", "h1", "h1-1-1", "Beans", "sack", "dried", "A", 2.0, np.nan),
    ("2019-20", "h2", "h2-1-1", "Maize", "basket", "fresh", "B", 3.0, np.nan),
    ("2019-20", "h2", "h2-1-1", "Maize", KNOWN_UNIT, "dried", "B", 10.0, np.nan),
]


def test_frame_without_kgfactor_reproduces_the_old_algorithm():
    df = _frame(BASE_ROWS, with_kgfactor=False)
    assert "KgFactor" not in df.columns
    assert_frame_equal(harvest_kg(df), _legacy_harvest_kg(df))


def test_all_nan_kgfactor_column_changes_nothing():
    """Declaring the column but never filling it must be a no-op."""
    without = harvest_kg(_frame(BASE_ROWS, with_kgfactor=False))
    with_col = harvest_kg(_frame(BASE_ROWS, with_kgfactor=True))
    assert_frame_equal(without, with_col)


def test_only_the_inferred_layer_acts_without_reports():
    f = harvest_kg_factors(_frame(BASE_ROWS, with_kgfactor=False))
    assert f.attrs["kg_factor_sources"] == {
        "reported": 0, "shipped": 0, "survey_median": 0, "inferred": 2,
        "none": 2, "reported_implausible": 0, "shipped_implausible": 0,
        "shipped_matched": 0}


def test_returned_frame_carries_only_harvest_kg():
    """``yield_kg`` does ``harvest_kg(...).reset_index()`` then arithmetic.

    Provenance must therefore live on ``attrs`` / the companion function and
    never as an extra column on this frame.
    """
    res = harvest_kg(_frame(BASE_ROWS))
    assert list(res.columns) == ["Harvest_kg"]


# ---------------------------------------------------------------------------
# Layer (a): the row's own reported factor
# ---------------------------------------------------------------------------

def test_reported_factor_wins_and_converts_an_unknown_unit():
    rows = [
        # A unit the library cannot resolve, but the survey weighed it.
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 2.0, 100.0),
        # A unit the library CAN resolve; the survey's number still wins.
        # Not the KILOGRAM unit -- a Kg row reporting 3.0 is a mis-key and the
        # plausibility screen rejects it (see the screen tests below).
        ("2019-20", "h1", "h1-1-1", "Beans", POUND, "dried", "A", 5.0, 0.5),
        # No report -> the inferred layer.
        ("2019-20", "h2", "h2-1-1", "Maize", KNOWN_UNIT, "dried", "A", 7.0, np.nan),
    ]
    df = _frame(rows)
    f = harvest_kg_factors(df)
    assert list(f["KgFactorSource"]) == ["reported", "reported", "inferred"]
    assert list(f["kg_per_unit"]) == [100.0, 0.5, 1.0]

    res = harvest_kg(df)
    assert res.loc[("2019-20", "h1", "h1-1-1", "Maize"), "Harvest_kg"] == 200.0
    assert res.loc[("2019-20", "h1", "h1-1-1", "Beans"), "Harvest_kg"] == 2.5
    assert res.loc[("2019-20", "h2", "h2-1-1", "Maize"), "Harvest_kg"] == 7.0


@pytest.mark.parametrize("bad", [0.0, -3.0, np.inf, -np.inf, np.nan])
def test_unusable_reported_factor_falls_through(bad):
    """0, negative and non-finite are DISCARDED, not clipped or used."""
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", KNOWN_UNIT, "dried", "A", 4.0, bad),
        ("2019-20", "h2", "h2-1-1", "Beans", "sack", "dried", "A", 4.0, bad),
    ]
    f = harvest_kg_factors(_frame(rows))
    assert list(f["KgFactorSource"]) == ["inferred", "none"]
    assert f["kg_per_unit"].iloc[0] == 1.0
    assert pd.isna(f["kg_per_unit"].iloc[1])
    assert f.attrs["kg_factor_sources"]["reported"] == 0


# ---------------------------------------------------------------------------
# Layer (b): the median of reported factors for the same (u, condition)
# ---------------------------------------------------------------------------

def _sack_group(n_reports, *, n_gaps=1, factor=100.0, condition="dried"):
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", "sack", condition, "A",
             1.0, factor) for k in range(n_reports)]
    rows += [("2019-20", f"g{k}", f"g{k}-1-1", "Maize", "sack", condition, "A",
              1.0, np.nan) for k in range(n_gaps)]
    return _frame(rows)


def test_survey_median_fills_a_gap_when_enough_rows_report():
    df = _sack_group(SURVEY_MEDIAN_MIN_REPORTS)
    f = harvest_kg_factors(df)
    assert f["KgFactorSource"].iloc[-1] == "survey_median"
    assert f["kg_per_unit"].iloc[-1] == 100.0
    assert set(f["KgFactorSource"].iloc[:-1]) == {"reported"}


def test_survey_median_does_not_fill_when_too_few_report():
    df = _sack_group(SURVEY_MEDIAN_MIN_REPORTS - 1)
    f = harvest_kg_factors(df)
    assert f["KgFactorSource"].iloc[-1] == "none"
    assert pd.isna(f["kg_per_unit"].iloc[-1])


def test_min_reports_kwarg_moves_the_bar():
    df = _sack_group(2)
    assert harvest_kg_factors(df)["KgFactorSource"].iloc[-1] == "none"
    lowered = harvest_kg_factors(df, min_reports=2)
    assert lowered["KgFactorSource"].iloc[-1] == "survey_median"
    # and it reaches harvest_kg, not just the companion
    assert harvest_kg(df, min_reports=2).shape[0] == 3


def test_survey_median_falls_back_from_condition_to_unit_alone():
    """A thin ``(u, condition)`` group defers to the same wave's ``u``."""
    conditions = ["dried", "fresh", "green", "dried_grain", "dry_at_harvest"]
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", "drum", cond, "A",
             1.0, 50.0) for k, cond in enumerate(conditions)]
    rows.append(("2019-20", "g0", "g0-1-1", "Maize", "drum", "dried", "A",
                 1.0, np.nan))
    f = harvest_kg_factors(_frame(rows))
    # (t, 'drum', 'dried') holds one report -- under the bar; (t, 'drum')
    # holds five -- at it.
    assert f["KgFactorSource"].iloc[-1] == "survey_median"
    assert f["kg_per_unit"].iloc[-1] == 50.0


def test_survey_median_does_not_cross_a_wave():
    rows = [("2018-19", f"r{k}", f"r{k}-1-1", "Maize", "sack", "dried", "A",
             1.0, 100.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    rows.append(("2019-20", "g0", "g0-1-1", "Maize", "sack", "dried", "A",
                 1.0, np.nan))
    f = harvest_kg_factors(_frame(rows))
    assert f["KgFactorSource"].iloc[-1] == "none"


def test_survey_median_ignores_unusable_reports():
    """A 0 report neither shifts the median nor counts toward N."""
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", "sack", "dried", "A",
             1.0, 100.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS - 1)]
    rows.append(("2019-20", "z", "z-1-1", "Maize", "sack", "dried", "A",
                 1.0, 0.0))
    rows.append(("2019-20", "g0", "g0-1-1", "Maize", "sack", "dried", "A",
                 1.0, np.nan))
    f = harvest_kg_factors(_frame(rows))
    assert f["KgFactorSource"].iloc[-1] == "none"


# ---------------------------------------------------------------------------
# The plausibility screen on the reported layer
# ---------------------------------------------------------------------------
#
# Uganda's first wired wave: 86 rows with a reported factor > 200 contributed
# 46.97M of a 57.9M kg national total.  The top ones are mis-keys -- a
# Sorghum row with quantity 147 and factor 147 000, a u='Kg' row claiming
# 260 000, and 2 017 / 2 018 from the adjacent year column.


def test_kilogram_unit_must_weigh_one():
    rows = [
        # a Kg row whose factor is not 1 -> rejected, falls to inferred (1.0)
        ("2019-20", "h1", "h1-1-1", "SimSim", KNOWN_UNIT, "dried", "A",
         2.0, 260_000.0),
        # 1% slack is allowed
        ("2019-20", "h2", "h2-1-1", "Maize", KNOWN_UNIT, "dried", "A",
         2.0, 1.005),
    ]
    f = harvest_kg_factors(_frame(rows))
    assert list(f["KgFactorSource"]) == ["inferred", "reported"]
    assert list(f["kg_reported_rejected"]) == [True, False]
    assert f["kg_per_unit"].iloc[0] == 1.0          # NOT clipped, NOT rescaled
    assert f["kg_per_unit"].iloc[1] == 1.005


def test_factor_above_the_cap_is_rejected():
    rows = [
        ("2019-20", "h1", "h1-1-1", "Sorghum", "sack", "dried", "A",
         147.0, 147_000.0),
        ("2019-20", "h2", "h2-1-1", "Maize", "sack", "dried", "A",
         1.0, KG_FACTOR_MAX),          # exactly at the cap: kept
    ]
    f = harvest_kg_factors(_frame(rows))
    assert list(f["KgFactorSource"]) == ["none", "reported"]
    assert f.attrs["kg_factor_sources"]["reported_implausible"] == 1
    # the 147 000 row contributes NOTHING rather than 21.6 million kg
    assert len(harvest_kg(_frame(rows))) == 1


def test_calendar_year_on_a_non_kg_unit_is_rejected():
    rows = [
        ("2019-20", "h1", "h1-1-1", "Beans", "sack", "dried", "A", 1.0, 2_017.0),
        ("2019-20", "h2", "h2-1-1", "Soybean", "sack", "dried", "A", 1.0, 2_018.0),
    ]
    f = harvest_kg_factors(_frame(rows))
    assert list(f["kg_reported_rejected"]) == [True, True]
    assert set(f["KgFactorSource"]) == {"none"}


def test_a_rejected_report_does_not_enter_a_median():
    """Falls through 'exactly as if unreported' -- including out of the pool."""
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", "sack", "dried", "A",
             1.0, 147_000.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    rows.append(("2019-20", "g0", "g0-1-1", "Maize", "sack", "dried", "A",
                 1.0, np.nan))
    f = harvest_kg_factors(_frame(rows))
    assert f["kg_survey_median"].isna().all()
    assert set(f["KgFactorSource"]) == {"none"}


def test_the_screen_count_rides_beside_a_partition_that_still_sums():
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0, 100.0),
        ("2019-20", "h2", "h2-1-1", "Maize", "sack", "dried", "A", 1.0, 9_999.0),
        ("2019-20", "h3", "h3-1-1", "Maize", KNOWN_UNIT, "dried", "A", 1.0, np.nan),
        ("2019-20", "h4", "h4-1-1", "Maize", "basket", "dried", "A", 1.0, np.nan),
    ]
    df = _frame(rows)
    counts = harvest_kg_factors(df).attrs["kg_factor_sources"]
    assert counts["reported_implausible"] == 1
    # the four SERVING layers still partition the frame ...
    assert sum(counts[layer] for layer in KG_FACTOR_LAYERS) == len(df)
    # ... and the rejected row is served by one of them, not lost
    assert counts == {"reported": 1, "shipped": 0, "survey_median": 0,
                      "inferred": 1, "none": 2, "reported_implausible": 1,
                      "shipped_implausible": 0, "shipped_matched": 0}


def test_screen_is_inert_without_a_kgfactor_column():
    df = _frame(BASE_ROWS, with_kgfactor=False)
    f = harvest_kg_factors(df)
    assert f.attrs["kg_factor_sources"]["reported_implausible"] == 0
    assert not f["kg_reported_rejected"].any()
    assert_frame_equal(harvest_kg(df), _legacy_harvest_kg(df))


# ---------------------------------------------------------------------------
# The missing-unit sentinel is NOT a unit
# ---------------------------------------------------------------------------

def test_survey_median_refuses_the_missing_unit_sentinel():
    """``u='Unknown'`` means no unit was recorded, so there is no group.

    Reported rows still convert through layer (a) -- that is the whole point
    of ``KgFactor`` for Uganda's 2018-19 season A, which ships the factor and
    no unit code at all -- but their factors must not pool into a median, and
    the unreported rows must not receive one.  Otherwise the sentinel would
    manufacture weights out of containers of unrelated sizes.
    """
    reported = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", U_UNKNOWN, "dried",
                 "A", 2.0, 100.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    gaps = [("2019-20", f"g{k}", f"g{k}-1-1", "Maize", U_UNKNOWN, "dried",
             "A", 2.0, np.nan) for k in range(2)]
    f = harvest_kg_factors(_frame(reported + gaps))

    n = SURVEY_MEDIAN_MIN_REPORTS
    assert list(f["KgFactorSource"]) == ["reported"] * n + ["none"] * 2
    # No survey_median value is computed for the sentinel AT ALL -- not on
    # the rows that reported, and not on the rows that did not.
    assert f["kg_survey_median"].isna().all()
    assert f.attrs["kg_factor_sources"]["survey_median"] == 0
    # ... and layer (a) still converts the rows that reported.
    assert (f["kg_per_unit"].iloc[:n] == 100.0).all()
    assert f["kg_per_unit"].iloc[n:].isna().all()


def test_sentinel_rows_do_not_feed_a_real_unit_group():
    """A sentinel row's report must not leak into another unit's median."""
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", U_UNKNOWN, "dried",
             "A", 1.0, 100.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    rows.append(("2019-20", "g0", "g0-1-1", "Maize", "sack", "dried", "A",
                 1.0, np.nan))
    f = harvest_kg_factors(_frame(rows))
    assert f["KgFactorSource"].iloc[-1] == "none"


def test_nan_unit_is_treated_as_the_sentinel():
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", np.nan, "dried", "A",
             1.0, 100.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    rows.append(("2019-20", "g0", "g0-1-1", "Maize", np.nan, "dried", "A",
                 1.0, np.nan))
    f = harvest_kg_factors(_frame(rows))
    assert f["kg_survey_median"].isna().all()
    assert f["KgFactorSource"].iloc[-1] == "none"


def test_sentinel_rows_are_excluded_from_the_disagreement_audit():
    """A reported factor on a unit-less row has nothing to disagree with."""
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", U_UNKNOWN, "dried",
             "A", 1.0, 100.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    # one genuine unit, whose report is 100% off the inferred 1.0
    rows.append(("2019-20", "g0", "g0-1-1", "Maize", POUND, "dried", "A",
                 1.0, 1.0))
    d = harvest_kg_factors(_frame(rows)).attrs["kg_factor_disagreement"]
    assert d["reported_vs_survey_median"] == {"both": 0, "disagree": 0,
                                              "share": None}
    # only the real unit is in the denominator
    assert d["reported_vs_inferred"] == {"both": 1, "disagree": 1, "share": 1.0}


# ---------------------------------------------------------------------------
# Disclosure: the counts and the disagreement audit
# ---------------------------------------------------------------------------

def test_provenance_counts_sum_to_the_input_row_count():
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 2.0, 100.0),
        ("2019-20", "h2", "h2-1-1", "Maize", KNOWN_UNIT, "dried", "A", 2.0, np.nan),
        ("2019-20", "h3", "h3-1-1", "Maize", "basket", "dried", "A", 2.0, np.nan),
        # Counted by the layer that gave it a FACTOR, even though a zero
        # Quantity keeps it out of the sum.
        ("2019-20", "h4", "h4-1-1", "Maize", "sack", "dried", "A", 0.0, 100.0),
    ]
    df = _frame(rows)
    counts = harvest_kg_factors(df).attrs["kg_factor_sources"]
    # The four SERVING layers partition the frame; `reported_implausible` is a
    # screen count that rides alongside and is deliberately outside it.
    assert set(counts) == set(KG_FACTOR_LAYERS) | {"reported_implausible",
                                                   "shipped_implausible",
                                                   "shipped_matched"}
    assert sum(counts[layer] for layer in KG_FACTOR_LAYERS) == len(df)
    assert counts == {"reported": 2, "shipped": 0, "survey_median": 0,
                      "inferred": 1, "none": 1, "reported_implausible": 0,
                      "shipped_implausible": 0, "shipped_matched": 0}
    assert sum(counts[layer] for layer in KG_FACTOR_LAYERS) == len(df)
    # Two rows survive the sum: the zero-Quantity row drops despite being
    # counted `reported`, and the `none` row has no factor to apply.
    assert len(harvest_kg(df)) == 2


def test_harvest_kg_republishes_the_tallies_after_the_groupby():
    df = _frame(BASE_ROWS)
    res, f = harvest_kg(df), harvest_kg_factors(df)
    assert res.attrs["kg_factor_sources"] == f.attrs["kg_factor_sources"]
    assert res.attrs["kg_factor_disagreement"] == f.attrs["kg_factor_disagreement"]


def test_disagreement_reported_vs_inferred():
    """Both layers have a factor; how often do they differ by >10%?"""
    rows = [
        # ~3.5% off the inferred 0.4536 -> inside the band
        ("2019-20", "h1", "h1-1-1", "Maize", POUND, "dried", "A", 1.0, 0.47),
        # ~55% off -> outside it
        ("2019-20", "h2", "h2-1-1", "Maize", POUND, "dried", "A", 1.0, 1.0),
        # reported only: no inferred factor, so it is not in the denominator
        ("2019-20", "h3", "h3-1-1", "Maize", "sack", "dried", "A", 1.0, 9.0),
        # inferred only: likewise
        ("2019-20", "h4", "h4-1-1", "Maize", POUND, "dried", "A", 1.0, np.nan),
    ]
    d = harvest_kg_factors(_frame(rows)).attrs["kg_factor_disagreement"]
    assert d["tolerance"] == KG_FACTOR_DISAGREEMENT_TOLERANCE
    assert d["reported_vs_inferred"] == {"both": 2, "disagree": 1, "share": 0.5}


def test_disagreement_reported_vs_survey_median():
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", "sack", "dried", "A",
             1.0, 50.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    # one outlier report in the same group: median stays 50, this row is 150.
    # Both sit UNDER KG_FACTOR_MAX -- an outlier the screen would reject is a
    # different test (a rejected report never reaches the audit).
    rows.append(("2019-20", "z", "z-1-1", "Maize", "sack", "dried", "A",
                 1.0, 150.0))
    d = harvest_kg_factors(_frame(rows)).attrs["kg_factor_disagreement"]
    pair = d["reported_vs_survey_median"]
    assert pair["both"] == SURVEY_MEDIAN_MIN_REPORTS + 1
    assert pair["disagree"] == 1
    assert pair["share"] == pytest.approx(1 / (SURVEY_MEDIAN_MIN_REPORTS + 1))


def test_disagreement_is_empty_when_nothing_is_reported():
    d = harvest_kg_factors(_frame(BASE_ROWS, with_kgfactor=False)).attrs[
        "kg_factor_disagreement"]
    for pair in ("reported_vs_shipped", "reported_vs_survey_median",
                 "reported_vs_inferred"):
        assert d[pair] == {"both": 0, "disagree": 0, "share": None}


def test_companion_exposes_every_layer_for_auditing():
    """A maintainer must be able to see what each layer OFFERED, per unit."""
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", POUND, "dried",
             "A", 1.0, 2.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    f = harvest_kg_factors(_frame(rows))
    assert list(f.columns) == ["kg_per_unit", "KgFactorSource", "kg_reported",
                               "kg_reported_rejected", "kg_shipped",
                               "kg_shipped_rejected", "kg_shipped_source",
                               "kg_survey_median", "kg_inferred"]
    by_unit = f.groupby(f.index.get_level_values("u"))[
        ["kg_reported", "kg_inferred"]].median()
    assert by_unit.loc[POUND, "kg_reported"] == 2.0
    assert by_unit.loc[POUND, "kg_inferred"] == POUND_KG


def test_u_as_a_column_is_accepted():
    """Some countries carry ``u`` as a column, not an index level."""
    df = _frame(BASE_ROWS).reset_index("u")
    f = harvest_kg_factors(df)
    assert len(f) == len(df)
    assert f.index.equals(df.index)


# ---------------------------------------------------------------------------
# The canonical declaration
# ---------------------------------------------------------------------------

def test_kgfactor_is_declared_optional_in_the_canonical_schema():
    canonical = yaml.safe_load(_DATA_INFO.read_text(encoding="utf-8"))
    spec = canonical["Columns"]["crop_production"]["KgFactor"]
    assert spec["type"] == "float"
    assert spec["optional"] is True
    # Never required: no country must be forced to carry it, and the
    # null-read guard must not demand it (CLAUDE.md, "The Silent All-Null
    # Read" -- `optional:` columns are exempt at Site B).
    assert not spec.get("required", False)
    assert spec.get("note")


# ---------------------------------------------------------------------------
# Data-gated regression baseline
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_uganda_harvest_kg_baseline():
    """Pin Uganda's harvest_kg totals on the warm build.

    Re-measured 2026-09-09 after two deliberate moves: the 2018-19 ``KgFactor``
    wiring (#842/#849; the reported layer now serves 14,050 rows and the
    plausibility screen rejects 99) and the 2009-10 ``99999`` sentinel strip
    (#861; 3,077 sentinel-only rows leave, 133,683 -> 130,606).  The previous
    version of this test pinned the pre-wiring numbers and asked to be updated
    in the wiring PR; it was not, and failed on development for a day.  When a
    future change moves these numbers deliberately, update them here in the
    same PR rather than loosening the assertion.

    2026-09-09, the shipped-factor layer: the counts dict below gained
    ``shipped``, ``shipped_implausible`` and ``shipped_matched``, all 0,
    because ``shipped`` joined ``KG_FACTOR_LAYERS`` (whose counts partition
    the frame) and the two screen/join diagnostics ride beside it -- so every
    key is present on every call.  NO NUMBER MOVED; this call passes no
    ``shipped_factors`` table and Uganda has none to pass.
    """
    import lsms_library as ll

    cp = ll.Country("Uganda").crop_production()
    assert "KgFactor" in cp.columns
    res = harvest_kg(cp)
    assert len(cp) == 130_606
    assert len(res) == 36_095
    assert res["Harvest_kg"].sum() == pytest.approx(10_868_272.24500081, rel=1e-9)
    assert res.attrs["kg_factor_sources"] == {
        "reported": 14_050, "shipped": 0, "survey_median": 57,
        "inferred": 28_147, "none": 88_352, "reported_implausible": 99,
        "shipped_implausible": 0, "shipped_matched": 0}
