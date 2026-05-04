"""Regression tests for ``conversion_table_matching_global``.

Hardens the function against NaN / non-string entries on either side of
the matcher.  Reported in GH #213 (Malawi food_acquired): a NaN in the
conversion table's label column made ``difflib.get_close_matches`` raise
``TypeError: object of type 'float' has no len()`` from
``real_quick_ratio()``.
"""

import numpy as np
import pandas as pd

from lsms_library.local_tools import conversion_table_matching_global


def test_handles_nan_in_conversion_labels():
    """A NaN label in the conversion table must not crash the matcher."""
    df = pd.DataFrame({"i": ["maize", "rice", "beans"]})
    conversions = pd.DataFrame(
        {"item_name": ["Maize flour", np.nan, "Rice (white)", "Beans, dry"]}
    )

    matches, mapping = conversion_table_matching_global(
        df, conversions, conversion_label_name="item_name"
    )

    # NaN label must be dropped, not crash and not appear as a key.
    assert all(isinstance(k, str) for k in mapping)
    assert "Maize flour" in mapping
    assert "Rice (white)" in mapping


def test_handles_nan_in_df_items():
    """A NaN in df['i'] must not crash the matcher either."""
    df = pd.DataFrame({"i": ["maize", np.nan, "beans"]})
    conversions = pd.DataFrame({"item_name": ["Maize", "Beans"]})

    matches, mapping = conversion_table_matching_global(
        df, conversions, conversion_label_name="item_name"
    )

    # Both real items should match something.
    assert mapping["Maize"] == "Maize"
    assert mapping["Beans"] == "Beans"


def test_handles_non_string_labels():
    """Numeric / mixed-type labels are coerced to string rather than crashing."""
    df = pd.DataFrame({"i": ["100", "200", "rice"]})
    conversions = pd.DataFrame({"item_name": [100, 200, "Rice"]})

    matches, mapping = conversion_table_matching_global(
        df, conversions, conversion_label_name="item_name"
    )

    # Coerced numerics become string keys; no exception.
    assert "100" in mapping
    assert "200" in mapping
    assert mapping["Rice"] == "Rice"
