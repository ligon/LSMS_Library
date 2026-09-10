"""The ``shipped`` layer: an externally supplied kg-per-unit conversion table.

Ethiopia's ESS waves ship ``Crop_CF_Wave{2..5}.dta`` and Malawi's IHS5 ships
two crop-side conversion files (GH #852, #854): crop x unit x region tables
that no per-row ``KgFactor`` column can hold.  ``harvest_kg`` /
``harvest_kg_factors`` take one as ``shipped_factors=`` and serve it as a
layer ranked AFTER the row's own reported factor and BEFORE the survey median.

Three properties carry the design and each is pinned below:

* the layer is EXPLICIT -- nothing discovers a table, and omitting the kwarg
  is bit-for-bit inert (the whole of ``test_crop_kg_factor.py`` is the other
  half of that pin, including the Uganda baseline);
* an AMBIGUOUS table is REFUSED, never averaged or first()-ed -- Ethiopia's
  crop 74 / unit 62 duplicate is a decision for the loader;
* a shipped factor is screened by the SAME ``_screen_reported_factors`` as a
  reported one, and a rejection is COUNTED (``shipped_implausible``), never
  clipped.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    KG_FACTOR_LAYERS,
    KG_FACTOR_MAX,
    SHIPPED_FACTOR_JOIN_LEVELS,
    SURVEY_MEDIAN_MIN_REPORTS,
    U_UNKNOWN,
    ShippedFactorWarning,
    _shipped_factor_lookup,
    harvest_kg,
    harvest_kg_factors,
)

# 'pound' is in KNOWN_METRIC (0.453592) and is not the kilogram unit, so the
# inferred layer can serve it without the kg-must-weigh-1 rule firing.
POUND, POUND_KG = "pound", 0.453592
KNOWN_UNIT = "kg"


def _frame(rows, *, columns=None, index=None):
    """A minimal crop_production-shaped frame.

    ``columns`` names the tuple fields; every field not named in ``index``
    stays a column.  Defaults to the shape ``test_crop_kg_factor`` uses.
    """
    columns = columns or ["t", "i", "plot", "j", "u", "condition", "season",
                          "Quantity"]
    index = index or ["t", "i", "plot", "j", "u", "condition", "season"]
    df = pd.DataFrame(rows, columns=columns)
    data = {c: df[c].to_numpy() for c in columns if c not in index}
    data = {c: (v.astype(float) if c in ("Quantity", "KgFactor") else v)
            for c, v in data.items()}
    return pd.DataFrame(data,
                        index=pd.MultiIndex.from_frame(df[list(index)]))


def _cp(rows):
    """crop_production rows: (t, i, plot, j, u, condition, season, Quantity)."""
    return _frame(rows)


def _cp_with_reports(rows):
    """...plus a reported KgFactor column."""
    return _frame(rows,
                  columns=["t", "i", "plot", "j", "u", "condition", "season",
                           "Quantity", "KgFactor"])


def _table(rows, keys, *, source=False):
    """A shipped-factor table keyed on *keys*."""
    cols = list(keys) + ["KgFactor"] + (["Source"] if source else [])
    df = pd.DataFrame(rows, columns=cols)
    return df.set_index(list(keys))


# ---------------------------------------------------------------------------
# Layer order
# ---------------------------------------------------------------------------

def test_reported_wins_over_shipped():
    """The row's own enumerator beats a table.  A table is not this harvest."""
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 2.0,
             90.0)]
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])
    f = harvest_kg_factors(_cp_with_reports(rows), shipped_factors=table)
    assert list(f["KgFactorSource"]) == ["reported"]
    assert f["kg_per_unit"].iloc[0] == 90.0
    # ... and the table's offer is still DISCLOSED, so the two can be audited
    assert f["kg_shipped"].iloc[0] == 100.0


def test_shipped_wins_over_survey_median():
    """An external table outranks a pooling of the survey's own reports."""
    reported = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", "sack", "dried",
                 "A", 1.0, 50.0) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    gap = [("2019-20", "g0", "g0-1-1", "Maize", "sack", "dried", "A", 1.0,
            np.nan)]
    df = _cp_with_reports(reported + gap)
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])

    without = harvest_kg_factors(df)
    assert without["KgFactorSource"].iloc[-1] == "survey_median"

    f = harvest_kg_factors(df, shipped_factors=table)
    assert f["KgFactorSource"].iloc[-1] == "shipped"
    assert f["kg_per_unit"].iloc[-1] == 100.0
    # the median is still computed and disclosed; it just did not win
    assert f["kg_survey_median"].iloc[-1] == 50.0


def test_shipped_wins_over_inferred():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", POUND, "dried", "A", 2.0)]
    table = _table([("Maize", POUND, 0.5)], ["j", "u"])
    df = _cp(rows)
    assert harvest_kg_factors(df)["KgFactorSource"].iloc[0] == "inferred"
    f = harvest_kg_factors(df, shipped_factors=table)
    assert f["KgFactorSource"].iloc[0] == "shipped"
    assert f["kg_per_unit"].iloc[0] == 0.5
    assert f["kg_inferred"].iloc[0] == pytest.approx(POUND_KG)
    assert harvest_kg(df, shipped_factors=table)["Harvest_kg"].iloc[0] == 1.0


def test_shipped_reaches_a_unit_no_other_layer_can_serve():
    """The point of the layer: 'sack' resolves nowhere else."""
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 3.0)]
    df = _cp(rows)
    assert harvest_kg_factors(df)["KgFactorSource"].iloc[0] == "none"
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])
    assert harvest_kg(df, shipped_factors=table)["Harvest_kg"].iloc[0] == 300.0


def test_shipped_is_in_the_layer_vocabulary_at_rank_two():
    assert KG_FACTOR_LAYERS == ("reported", "shipped", "survey_median",
                                "inferred", "none")


# ---------------------------------------------------------------------------
# The join
# ---------------------------------------------------------------------------

def test_join_on_j_and_u_only():
    """A (j, u) table ignores t, condition and region entirely."""
    rows = [
        ("2018-19", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0),
        ("2019-20", "h2", "h2-1-1", "Maize", "sack", "fresh", "B", 1.0),
        ("2019-20", "h3", "h3-1-1", "Beans", "sack", "dried", "A", 1.0),
    ]
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert list(f["KgFactorSource"]) == ["shipped", "shipped", "none"]
    assert list(f["kg_per_unit"][:2]) == [100.0, 100.0]


def test_join_with_t_separates_the_waves():
    rows = [
        ("2018-19", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0),
        ("2019-20", "h2", "h2-1-1", "Maize", "sack", "dried", "A", 1.0),
    ]
    table = _table([("2018-19", "Maize", "sack", 100.0),
                    ("2019-20", "Maize", "sack", 120.0)], ["t", "j", "u"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert list(f["kg_per_unit"]) == [100.0, 120.0]

    # a wave the table does not cover is served by no shipped factor -- the
    # 2011-12 case of GH #852, which ships no Crop_CF file at all
    only_2018 = _table([("2018-19", "Maize", "sack", 100.0)], ["t", "j", "u"])
    g = harvest_kg_factors(_cp(rows), shipped_factors=only_2018)
    assert list(g["KgFactorSource"]) == ["shipped", "none"]


def test_join_on_condition_when_both_sides_carry_it():
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "shelled", "A", 1.0),
        ("2019-20", "h2", "h2-1-1", "Maize", "sack", "unshelled", "A", 1.0),
    ]
    table = _table([("Maize", "sack", "shelled", 100.0),
                    ("Maize", "sack", "unshelled", 60.0)],
                   ["j", "u", "condition"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert list(f["kg_per_unit"]) == [100.0, 60.0]


def test_a_table_without_condition_serves_every_condition():
    """Malawi's tree file has no condition axis -- trees have none."""
    rows = [
        ("2019-20", "h1", "h1-1-1", "Mango", "basket", "fresh", "A", 1.0),
        ("2019-20", "h2", "h2-1-1", "Mango", "basket", "dried", "A", 1.0),
    ]
    table = _table([("Mango", "basket", 25.0)], ["j", "u"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert list(f["kg_per_unit"]) == [25.0, 25.0]


def test_a_condition_keyed_table_on_a_frame_without_condition():
    """The key is dropped -- and if that leaves it ambiguous, it is refused.

    Both halves matter: a table whose conditions collapse to ONE factor still
    serves, and one whose conditions disagree raises rather than picking.
    """
    columns = ["t", "i", "plot", "j", "u", "season", "Quantity"]
    index = ["t", "i", "plot", "j", "u", "season"]
    df = _frame([("2019-20", "h1", "h1-1-1", "Maize", "sack", "A", 1.0)],
                columns=columns, index=index)
    assert "condition" not in (df.index.names or [])

    agreeing = _table([("Maize", "sack", "shelled", 100.0)],
                      ["j", "u", "condition"])
    # ... and it ANNOUNCES the dropped axis rather than going condition-blind
    # in silence, which is what the multi-row case has always done by raising.
    with pytest.warns(ShippedFactorWarning, match="condition"):
        f = harvest_kg_factors(df, shipped_factors=agreeing)
    assert f["kg_per_unit"].iloc[0] == 100.0

    disagreeing = _table([("Maize", "sack", "shelled", 100.0),
                          ("Maize", "sack", "unshelled", 60.0)],
                         ["j", "u", "condition"])
    with pytest.raises(ValueError, match="ambiguous"):
        harvest_kg_factors(df, shipped_factors=disagreeing)


def test_region_keyed_table_after_the_caller_resolved_region():
    """`region` is the CALLER's job to put on crop_production, and is exact.

    ``cluster_features`` ships ``Region`` (capital); this transform joins on
    ``region`` and never re-enters ``sample()`` / ``cluster_features()``.
    """
    columns = ["t", "i", "plot", "j", "u", "condition", "season", "region",
               "Quantity"]
    index = columns[:-2] + ["region"]
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", "Amhara",
         1.0),
        ("2019-20", "h2", "h2-1-1", "Maize", "sack", "dried", "A", "Oromia",
         1.0),
        ("2019-20", "h3", "h3-1-1", "Maize", "sack", "dried", "A", "Tigray",
         1.0),
    ]
    df = _frame(rows, columns=columns, index=index)
    table = _table([("Maize", "sack", "Amhara", 100.0),
                    ("Maize", "sack", "Oromia", 85.0)],
                   ["j", "u", "region"])
    f = harvest_kg_factors(df, shipped_factors=table)
    assert list(f["KgFactorSource"]) == ["shipped", "shipped", "none"]
    assert list(f["kg_per_unit"][:2]) == [100.0, 85.0]
    # The same table with the capital-R spelling joins on (j, u) only -- and
    # is therefore ambiguous, which is the loud failure we want rather than a
    # silent region-blind match.
    capital = table.rename_axis(index={"region": "Region"})
    with pytest.raises(ValueError, match="ambiguous"):
        harvest_kg_factors(df, shipped_factors=capital)


def test_region_as_a_column_on_both_sides_is_accepted():
    """`u` is a level in some countries and a column in others; so is region."""
    columns = ["t", "i", "plot", "j", "u", "condition", "season", "region",
               "Quantity"]
    index = ["t", "i", "plot", "j", "u", "condition", "season"]
    df = _frame([("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A",
                  "Amhara", 1.0)], columns=columns, index=index)
    table = pd.DataFrame({"j": ["Maize"], "u": ["sack"], "region": ["Amhara"],
                          "KgFactor": [100.0]})
    f = harvest_kg_factors(df, shipped_factors=table)
    assert f["kg_per_unit"].iloc[0] == 100.0


def test_join_keys_are_case_and_whitespace_insensitive():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "Sack", "dried", "A", 1.0)]
    table = _table([(" maize ", "SACK", 100.0)], ["j", "u"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert f["kg_per_unit"].iloc[0] == 100.0


def test_country_is_a_join_key_so_one_country_s_table_stays_its_own():
    """A cross-country Feature() frame carries `country`; fence on it."""
    columns = ["country", "t", "i", "plot", "j", "u", "condition", "season",
               "Quantity"]
    index = columns[:-1]
    rows = [
        ("Ethiopia", "2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A",
         1.0),
        ("Malawi", "2019-20", "h2", "h2-1-1", "Maize", "sack", "dried", "A",
         1.0),
    ]
    df = _frame(rows, columns=columns, index=index)
    table = _table([("Ethiopia", "Maize", "sack", 100.0)],
                   ["country", "j", "u"])
    f = harvest_kg_factors(df, shipped_factors=table)
    assert list(f["KgFactorSource"]) == ["shipped", "none"]
    assert "country" in SHIPPED_FACTOR_JOIN_LEVELS


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------

def test_an_ambiguous_table_raises_and_names_the_duplicate_keys():
    """Ethiopia's Crop_CF_Wave5 ships crop 74 / unit 62 twice (4.34, 6.125).

    De-duplicating is the loader's deliberate act; averaging or first()-ing
    here would be the grain collapse CLAUDE.md forbids.
    """
    rows = [("2019-20", "h1", "h1-1-1", "Enset", "gemel", "dried", "A", 1.0)]
    table = _table([("Enset", "gemel", 4.34), ("Enset", "gemel", 6.125)],
                   ["j", "u"])
    with pytest.raises(ValueError) as exc:
        harvest_kg_factors(_cp(rows), shipped_factors=table)
    message = str(exc.value)
    assert "ambiguous" in message
    assert "['j', 'u']" in message
    assert "enset" in message and "gemel" in message
    assert "LOADER" in message


def test_a_casefold_collision_is_refused_not_resolved():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "Kg", "dried", "A", 1.0)]
    table = _table([("Maize", "Kg", 1.0), ("Maize", "kg", 1.0)], ["j", "u"])
    with pytest.raises(ValueError, match="ambiguous"):
        harvest_kg_factors(_cp(rows), shipped_factors=table)


def test_a_table_keyed_on_nothing_joinable_raises():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0)]
    table = pd.DataFrame({"crop_code": [74], "unit_code": [62],
                          "KgFactor": [4.34]})
    with pytest.raises(ValueError, match="keyed on none of"):
        harvest_kg_factors(_cp(rows), shipped_factors=table)


def test_a_table_with_no_kgfactor_column_raises():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0)]
    table = pd.DataFrame({"j": ["Maize"], "u": ["sack"],
                          "conversion": [100.0]}).set_index(["j", "u"])
    with pytest.raises(ValueError, match="KgFactor"):
        harvest_kg_factors(_cp(rows), shipped_factors=table)


def test_a_non_dataframe_raises():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0)]
    with pytest.raises(ValueError, match="must be a DataFrame"):
        harvest_kg_factors(_cp(rows), shipped_factors={"sack": 100.0})


def test_a_table_keyed_only_on_a_level_the_frame_lacks():
    """Two guards, and the ORDER is worth pinning.

    A ``region``-only table trips the mandatory-``u`` rule first, because
    that rule is about the table alone and does not depend on the frame.  The
    "nothing to join on" guard below it is therefore unreachable through
    ``harvest_kg_factors`` -- a frame with no ``u`` raises even earlier, in
    ``_kg_factor_series`` -- so it is exercised directly, as the
    defence-in-depth it is.
    """
    columns = ["t", "i", "plot", "j", "u", "condition", "season", "Quantity"]
    df = _frame([("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A",
                  1.0)], columns=columns)
    with pytest.raises(ValueError, match="not on 'u'"):
        harvest_kg_factors(df, shipped_factors=_table([("Amhara", 100.0)],
                                                      ["region"]))

    no_u = df.reset_index("u").drop(columns="u")
    with pytest.raises(ValueError, match="expected a 'u' unit"):
        harvest_kg_factors(no_u, shipped_factors=_table(
            [("Maize", "sack", 100.0)], ["j", "u"]))
    with pytest.raises(ValueError, match="nothing to join on"):
        _shipped_factor_lookup(no_u, _table([("sack", 100.0)], ["u"]))


# ---------------------------------------------------------------------------
# The screen, and the sentinel
# ---------------------------------------------------------------------------

def test_an_implausible_shipped_factor_is_counted_not_applied():
    """The same three rules as a reported factor; count, never clip."""
    rows = [
        # over KG_FACTOR_MAX
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0),
        # a kilogram unit whose shipped factor is not 1
        ("2019-20", "h2", "h2-1-1", "Beans", KNOWN_UNIT, "dried", "A", 1.0),
        # an integral calendar year on a non-kg unit
        ("2019-20", "h3", "h3-1-1", "Millet", "sack", "dried", "A", 1.0),
    ]
    table = _table([("Maize", "sack", KG_FACTOR_MAX + 1),
                    ("Beans", KNOWN_UNIT, 50.0),
                    ("Millet", "sack", 2_017.0)], ["j", "u"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert list(f["kg_shipped_rejected"]) == [True, True, True]
    assert f["kg_shipped"].isna().all()
    # NOT clipped, NOT rescaled: each row falls through as if never offered
    assert list(f["KgFactorSource"]) == ["none", "inferred", "none"]
    assert f["kg_per_unit"].iloc[1] == 1.0
    counts = f.attrs["kg_factor_sources"]
    assert counts["shipped"] == 0
    assert counts["shipped_implausible"] == 3
    assert counts["reported_implausible"] == 0
    # the table DID key correctly -- all three rows matched; they were
    # rejected by the screen, which is a different fact and a different count
    assert counts["shipped_matched"] == 3


def test_an_unusable_shipped_value_is_discarded():
    """0, negative and non-finite are discarded on the same rule as a report."""
    rows = [("2019-20", "h1", "h1-1-1", f"c{k}", "sack", "dried", "A", 1.0)
            for k in range(4)]
    table = _table([("c0", "sack", 0.0), ("c1", "sack", -3.0),
                    ("c2", "sack", np.inf), ("c3", "sack", np.nan)],
                   ["j", "u"])
    # ... and the zero-match warning says WHY: the values, not the keys.
    with pytest.warns(ShippedFactorWarning, match="usable"):
        f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert set(f["KgFactorSource"]) == {"none"}
    assert f.attrs["kg_factor_sources"]["shipped"] == 0
    # discarded, not REJECTED -- the screen never saw a usable number
    assert not f["kg_shipped_rejected"].any()


def test_the_sentinel_refuses_the_shipped_layer_too():
    """u='Unknown' is the absence of a unit; a kg-PER-UNIT table has no answer.

    A loader keying a factor on the sentinel -- or shipping a table with no
    ``u`` key at all -- would otherwise fabricate a weight for every unit-less
    row, which is exactly what U_UNKNOWN exists to prevent.
    """
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", U_UNKNOWN, "dried", "A", 1.0),
        ("2019-20", "h2", "h2-1-1", "Maize", np.nan, "dried", "A", 1.0),
        ("2019-20", "h3", "h3-1-1", "Maize", "sack", "dried", "A", 1.0),
    ]
    table = _table([("Maize", U_UNKNOWN, 100.0), ("Maize", "sack", 100.0)],
                   ["j", "u"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert list(f["KgFactorSource"]) == ["none", "none", "shipped"]
    assert f["kg_shipped"].iloc[:2].isna().all()
    # excluded BEFORE the screen, so it is not counted as implausible either
    assert f.attrs["kg_factor_sources"]["shipped_implausible"] == 0

    # a j-only table cannot reach them either -- it is refused outright,
    # since a kg-per-unit factor with no unit is not a factor (see
    # test_a_table_not_keyed_on_u_is_refused).
    jonly = _table([("Maize", 100.0)], ["j"])
    with pytest.raises(ValueError, match="not on 'u'"):
        harvest_kg_factors(_cp(rows), shipped_factors=jonly)


def test_a_shipped_factor_does_not_enter_a_survey_median():
    """The median is of REPORTED factors.  A table is not a report."""
    rows = [("2019-20", f"r{k}", f"r{k}-1-1", "Maize", "sack", "dried", "A",
             1.0, np.nan) for k in range(SURVEY_MEDIAN_MIN_REPORTS)]
    rows.append(("2019-20", "g0", "g0-1-1", "Maize", "basket", "dried", "A",
                 1.0, np.nan))
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])
    f = harvest_kg_factors(_cp_with_reports(rows), shipped_factors=table)
    assert f["kg_survey_median"].isna().all()
    assert f["KgFactorSource"].iloc[-1] == "none"


# ---------------------------------------------------------------------------
# Disclosure
# ---------------------------------------------------------------------------

def test_the_counts_dict_gains_both_keys_and_still_partitions():
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0),
        ("2019-20", "h2", "h2-1-1", "Maize", KNOWN_UNIT, "dried", "A", 1.0),
        ("2019-20", "h3", "h3-1-1", "Beans", "basket", "dried", "A", 1.0),
        ("2019-20", "h4", "h4-1-1", "Millet", "drum", "dried", "A", 1.0),
    ]
    table = _table([("Maize", "sack", 100.0),
                    ("Millet", "drum", 9_999.0)], ["j", "u"])
    df = _cp(rows)
    counts = harvest_kg_factors(df, shipped_factors=table).attrs[
        "kg_factor_sources"]
    assert set(counts) == set(KG_FACTOR_LAYERS) | {"reported_implausible",
                                                   "shipped_implausible",
                                                   "shipped_matched"}
    assert sum(counts[layer] for layer in KG_FACTOR_LAYERS) == len(df)
    assert counts == {"reported": 0, "shipped": 1, "survey_median": 0,
                      "inferred": 1, "none": 2, "reported_implausible": 0,
                      "shipped_implausible": 1, "shipped_matched": 2}


def test_the_source_column_rides_through():
    """Malawi's ladder stamps a provenance string on every factor."""
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0),
        ("2019-20", "h2", "h2-1-1", "Beans", "sack", "dried", "A", 1.0),
        ("2019-20", "h3", "h3-1-1", "Millet", "sack", "dried", "A", 1.0),
    ]
    table = _table([("Maize", "sack", 100.0, "IHS 2020"),
                    ("Beans", "sack", 9_999.0, "National mean")],
                   ["j", "u"], source=True)
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert f["kg_shipped_source"].iloc[0] == "IHS 2020"
    # a REJECTED shipped factor carries no source: the column describes what
    # was actually offered post-screen, exactly as kg_shipped does
    assert pd.isna(f["kg_shipped_source"].iloc[1])
    assert pd.isna(f["kg_shipped_source"].iloc[2])


def test_a_table_without_a_source_column_is_fine():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0)]
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert pd.isna(f["kg_shipped_source"].iloc[0])
    assert f["kg_per_unit"].iloc[0] == 100.0


def test_the_disagreement_audit_gains_reported_vs_shipped():
    """GH #852's real question: does the WB table agree with the enumerator?"""
    rows = [
        # reported 100 vs shipped 105 -- inside the 10% band
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0, 100.0),
        # reported 100 vs shipped 60 -- outside it
        ("2019-20", "h2", "h2-1-1", "Beans", "sack", "dried", "A", 1.0, 100.0),
        # shipped only: not in the denominator
        ("2019-20", "h3", "h3-1-1", "Millet", "sack", "dried", "A", 1.0,
         np.nan),
    ]
    table = _table([("Maize", "sack", 105.0), ("Beans", "sack", 60.0),
                    ("Millet", "sack", 80.0)], ["j", "u"])
    d = harvest_kg_factors(_cp_with_reports(rows),
                           shipped_factors=table).attrs[
        "kg_factor_disagreement"]
    assert d["reported_vs_shipped"] == {"both": 2, "disagree": 1, "share": 0.5}


def test_harvest_kg_forwards_and_republishes():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 2.0)]
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])
    df = _cp(rows)
    res = harvest_kg(df, shipped_factors=table)
    f = harvest_kg_factors(df, shipped_factors=table)
    assert res["Harvest_kg"].iloc[0] == 200.0
    assert list(res.columns) == ["Harvest_kg"]
    assert res.attrs["kg_factor_sources"] == f.attrs["kg_factor_sources"]
    assert res.attrs["kg_factor_disagreement"] == f.attrs[
        "kg_factor_disagreement"]


# ---------------------------------------------------------------------------
# Inertness
# ---------------------------------------------------------------------------

def test_passing_no_table_is_inert():
    """The other half of this pin is the whole of test_crop_kg_factor.py."""
    rows = [
        ("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 2.0),
        ("2019-20", "h2", "h2-1-1", "Maize", KNOWN_UNIT, "dried", "A", 4.0),
    ]
    df = _cp(rows)
    f = harvest_kg_factors(df)
    assert f["kg_shipped"].isna().all()
    assert not f["kg_shipped_rejected"].any()
    assert (f["kg_shipped_source"].isna()).all()
    counts = f.attrs["kg_factor_sources"]
    assert counts["shipped"] == 0 and counts["shipped_implausible"] == 0
    assert counts["shipped_matched"] == 0
    assert f.attrs["kg_factor_disagreement"]["reported_vs_shipped"] == {
        "both": 0, "disagree": 0, "share": None}


def test_an_empty_table_serves_nothing_and_does_not_raise():
    """An EMPTY table is not a mis-keyed one, so it does not warn."""
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0)]
    table = _table([], ["j", "u"])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ShippedFactorWarning)
        f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert f.attrs["kg_factor_sources"]["shipped"] == 0
    assert f.attrs["kg_factor_sources"]["shipped_matched"] == 0


# ---------------------------------------------------------------------------
# The mis-keyed table: this layer's dominant failure mode, now loud
# ---------------------------------------------------------------------------

def test_a_non_empty_table_matching_nothing_warns_once():
    """The Ethiopia trap: raw WB crop codes against decoded `j` labels."""
    rows = [("2019-20", "h1", "h1-1-1", "Enset", "gemel", "dried", "A", 1.0),
            ("2019-20", "h2", "h2-1-1", "Maize", "sack", "dried", "A", 1.0)]
    table = _table([(74, "gemel", 4.34)], ["j", "u"])
    with pytest.warns(ShippedFactorWarning) as record:
        f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert len(record) == 1
    assert "matched 0 of 2 rows" in str(record[0].message)
    assert f.attrs["kg_factor_sources"]["shipped_matched"] == 0


def test_shipped_matched_is_the_tell_not_the_shipped_count():
    """A perfectly-keyed table outranked on every row still reads shipped: 0.

    This is why `counts['shipped']` cannot be the mis-keying tell, and why
    `shipped_matched` is taken before the sentinel, the screen and the rank.
    """
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0,
             90.0)]
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ShippedFactorWarning)
        f = harvest_kg_factors(_cp_with_reports(rows), shipped_factors=table)
    counts = f.attrs["kg_factor_sources"]
    assert counts["shipped"] == 0          # outranked by `reported`
    assert counts["shipped_matched"] == 1  # but the table keyed just fine


def test_a_sentinel_only_table_matched_but_served_nothing():
    """Distinguishable from a mis-keyed table, which was the point."""
    rows = [("2019-20", "h1", "h1-1-1", "Maize", U_UNKNOWN, "dried", "A",
             1.0)]
    table = _table([("Maize", U_UNKNOWN, 100.0)], ["j", "u"])
    f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    counts = f.attrs["kg_factor_sources"]
    assert counts["shipped"] == 0 and counts["shipped_implausible"] == 0
    assert counts["shipped_matched"] == 1


def test_a_table_not_keyed_on_u_is_refused():
    """The error message claimed this rule; now the code has it."""
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0)]
    table = _table([("Maize", 50.0)], ["j"])
    with pytest.raises(ValueError, match="not on 'u'"):
        harvest_kg_factors(_cp(rows), shipped_factors=table)


def test_a_dropped_region_key_warns_instead_of_going_national_in_silence():
    """The asymmetry the red-team found: one region used to be silent."""
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0)]
    table = _table([("Maize", "sack", "Amhara", 100.0)],
                   ["j", "u", "region"])
    with pytest.warns(ShippedFactorWarning, match="region"):
        f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    # it still JOINS -- the Ethiopia region case needs this until
    # cluster_features carries region
    assert f["kg_per_unit"].iloc[0] == 100.0
    assert f.attrs["kg_factor_sources"]["shipped_matched"] == 1


def test_a_correctly_keyed_table_warns_about_nothing():
    rows = [("2019-20", "h1", "h1-1-1", "Maize", "sack", "dried", "A", 1.0)]
    table = _table([("Maize", "sack", 100.0)], ["j", "u"])
    with warnings.catch_warnings():
        warnings.simplefilter("error", ShippedFactorWarning)
        f = harvest_kg_factors(_cp(rows), shipped_factors=table)
    assert f["kg_per_unit"].iloc[0] == 100.0
