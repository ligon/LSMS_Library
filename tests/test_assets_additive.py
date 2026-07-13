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
