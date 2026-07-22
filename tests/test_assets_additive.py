"""GH #323: `assets` Value is ADDITIVE across a per-unit roster.

Nigeria W2's `sect5b_plantingw2` is a per-unit asset roster -- one row per
individual unit owned, enumerated by `item_seq` (1..15), each carrying its own
reported Value.  The canonical assets grain is (t, i, j), so those rows arrive
as duplicates on the declared index and the historical `.first()` collapse kept
ONE UNIT and discarded the rest: N576,299,043 true -> N429,001,558 kept, i.e.
N147,297,485 (25.6%) destroyed, in EACH of t=2012Q3 and t=2013Q1.

Summing `Value` is exactly lossless.  `Quantity` and `Age` must NOT be summed:
  * Quantity comes from the separate, already-clean `sect5a` grid and is merely
    REPEATED across the item_seq rows by the wave's `dfs:` merge -- summing it
    would multiply the unit count by itself (4 beds -> 16).
  * Age is genuinely per-unit and has no lossless representation at (t, i, j).

These tests pin the per-column policy against a plausible "just sum the numeric
columns" regression.  They use synthetic frames -- no microdata required.
"""
import pandas as pd
import pytest

from lsms_library.feature import (
    _ADDITIVE_MEASURE_COLUMNS,
    _collapse_duplicate_index,
)


def _roster_frame():
    """Household 10001 owns 4 beds (sect5a Quantity=4, repeated), each unit
    carrying its own Age and Value -- the real W2 shape, at (t, i, j) grain
    after `item_seq` has been dropped."""
    idx = pd.MultiIndex.from_tuples(
        [("2012Q3", "10001", "Bed")] * 4, names=["t", "i", "j"]
    )
    return pd.DataFrame(
        {
            "Quantity": [4.0, 4.0, 4.0, 4.0],      # repeated, NOT per-unit
            "Age": [10.0, 6.0, 10.0, 6.0],         # per-unit
            "Value": [7000.0, 3000.0, 6000.0, 5000.0],   # per-unit, additive
        },
        index=idx,
    )


def test_assets_declares_value_additive():
    """The policy entry exists and names Value ONLY."""
    assert _ADDITIVE_MEASURE_COLUMNS.get("assets") == ("Value",)


def test_assets_value_is_summed_not_first():
    """The per-unit values sum to the household's holding of that item."""
    out = _collapse_duplicate_index(_roster_frame(), "assets")
    assert len(out) == 1
    # 7000 + 3000 + 6000 + 5000 -- NOT 7000, which is what .first() kept.
    assert out["Value"].iloc[0] == 21000.0


def test_assets_quantity_is_not_summed():
    """Quantity is repeated across the roster rows; summing it would give 16."""
    out = _collapse_duplicate_index(_roster_frame(), "assets")
    assert out["Quantity"].iloc[0] == 4.0


def test_assets_age_is_not_summed():
    """Age is per-unit; summing it is meaningless (would give 32)."""
    out = _collapse_duplicate_index(_roster_frame(), "assets")
    assert out["Age"].iloc[0] == 10.0


def test_table_without_additive_policy_still_uses_first():
    """The additive policy is per-table: a table with no entry is unchanged."""
    out = _collapse_duplicate_index(_roster_frame(), "shocks")
    assert out["Value"].iloc[0] == 7000.0   # historical .first()


def test_assets_collapse_is_a_no_op_on_a_unique_index():
    """Countries already at (t, i, j) grain -- i.e. every country but Nigeria --
    have a unique index, so the additive policy never fires for them."""
    idx = pd.MultiIndex.from_tuples(
        [("2016", "h1", "Bed"), ("2016", "h2", "Bed")], names=["t", "i", "j"]
    )
    df = pd.DataFrame({"Quantity": [1.0, 2.0], "Value": [100.0, 200.0]}, index=idx)
    out = _collapse_duplicate_index(df, "assets")
    pd.testing.assert_frame_equal(out, df)


@pytest.mark.parametrize("table", ["food_acquired", "assets"])
def test_additive_policy_columns_are_tuples(table):
    """Guard the shape of the registry (a bare string would iterate per-char)."""
    assert isinstance(_ADDITIVE_MEASURE_COLUMNS[table], tuple)


# --- GH #323: an all-NA group must be NA, not a fabricated 0.0 ----------------

def test_all_na_group_sums_to_NA_not_zero():
    import pandas as pd
    from lsms_library.country import _sum_min_count_1
    s = pd.Series([pd.NA, pd.NA], dtype='Float64')
    assert pd.isna(_sum_min_count_1(s)), "all-NA group must sum to NA, not 0.0"
    assert s.sum() == 0.0, "guard: bare .sum() IS the fabrication we are avoiding"


def test_partial_na_group_still_sums_the_observed_values():
    import pandas as pd
    from lsms_library.country import _sum_min_count_1
    s = pd.Series([7000, pd.NA, 3000], dtype='Float64')
    assert _sum_min_count_1(s) == 10000


def test_feature_site_uses_the_same_min_count_reducer():
    """The two collapse sites read ONE policy dict, so they must apply it
    identically -- an all-NA group is NA on the Feature path too, not 0.0."""
    idx = pd.MultiIndex.from_tuples(
        [("2012Q3", "10001", "Bed")] * 2, names=["t", "i", "j"])
    df = pd.DataFrame({"Value": pd.array([pd.NA, pd.NA], dtype="Float64"),
                       "Age": [10.0, 10.0]}, index=idx)
    out = _collapse_duplicate_index(df, "assets")
    assert pd.isna(out["Value"].iloc[0]), (
        "an all-NA group summed to a fabricated 0.0 on the Feature path")


# --- GH #323: registering a table as additive must not go on to silence a
# --- destruction the SUM does not actually reconcile ------------------------

def _collapse_country(df, table_name):
    """Run the Country-side collapse, returning (out, grain warnings)."""
    import warnings as _w
    from lsms_library.country import (_normalize_dataframe_index,
                                      GrainCollapseWarning)
    with _w.catch_warnings(record=True) as caught:
        _w.simplefilter("always")
        out = _normalize_dataframe_index(
            df, {"index": "(t, i, j)"}, wave="2012Q3",
            table_name=table_name, country="Testland")
    return out, [x for x in caught
                 if issubclass(x.category, GrainCollapseWarning)]


@pytest.mark.parametrize("collapse", ["country", "feature"])
def test_additive_does_not_silence_a_non_additive_destruction(collapse):
    """The trap this policy entry could have walked into.

    Wholesale silencing was safe only while `food_acquired` -- every column of
    which the collapse reconciles -- was the sole entry.  `assets` carries a
    per-unit `Age` that NO reducer preserves at (t, i, j).  Silencing the whole
    report would have bought a recovered `Value` total with a SILENT `Age`
    destruction: GH #323 reintroduced by its own fix.  Losslessness is per
    column, so the audit is too.
    """
    df = _roster_frame()                      # Age disagrees: 10, 6, 10, 6
    if collapse == "country":
        out, grain = _collapse_country(df, "assets")
    else:
        import warnings as _w
        from lsms_library.country import GrainCollapseWarning
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            out = _collapse_duplicate_index(df, "assets")
        grain = [x for x in caught
                 if issubclass(x.category, GrainCollapseWarning)]
    assert out["Value"].iloc[0] == 21000.0, "the recovery must survive"
    assert grain, "Age was destroyed and nobody said so"


@pytest.mark.parametrize("collapse", ["country", "feature"])
def test_a_disagreement_confined_to_the_additive_columns_stays_silent(collapse):
    """P3's silent half.  If the SUM reconciles every disagreement, warning
    would be pure noise -- that is the whole reason the additive path exists."""
    idx = pd.MultiIndex.from_tuples(
        [("2012Q3", "10001", "Bed")] * 3, names=["t", "i", "j"])
    df = pd.DataFrame({"Quantity": [4.0, 4.0, 4.0],       # agrees
                       "Age": [10.0, 10.0, 10.0],         # agrees
                       "Value": [7000.0, 3000.0, 6000.0]},  # disagrees; summed
                      index=idx)
    if collapse == "country":
        out, grain = _collapse_country(df, "assets")
    else:
        import warnings as _w
        from lsms_library.country import GrainCollapseWarning
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            out = _collapse_duplicate_index(df, "assets")
        grain = [x for x in caught
                 if issubclass(x.category, GrainCollapseWarning)]
    assert out["Value"].iloc[0] == 16000.0
    assert not grain, (
        "a disagreement the SUM fully reconciles warned; that noise is what "
        "buries the real signal")


@pytest.mark.parametrize("collapse", ["country", "feature"])
def test_food_acquired_stays_silent_when_only_price_disagrees(collapse):
    """`Price` is RE-DERIVED from the summed totals, so a disagreement in it is
    reconciled too -- food_acquired must not start warning on the visit-level
    collapse it was registered to fix (GH #501)."""
    idx = pd.MultiIndex.from_tuples(
        [("2018", "h1", "Rice")] * 2, names=["t", "i", "j"])
    df = pd.DataFrame({"Quantity": [2.0, 3.0],
                       "Expenditure": [10.0, 20.0],
                       "Price": [5.0, 6.6667]}, index=idx)
    if collapse == "country":
        out, grain = _collapse_country(df, "food_acquired")
    else:
        import warnings as _w
        from lsms_library.country import GrainCollapseWarning
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            out = _collapse_duplicate_index(df, "food_acquired")
        grain = [x for x in caught
                 if issubclass(x.category, GrainCollapseWarning)]
    assert out["Quantity"].iloc[0] == 5.0
    assert out["Expenditure"].iloc[0] == 30.0
    assert out["Price"].iloc[0] == pytest.approx(6.0)   # 30 / 5, re-derived
    assert not grain
