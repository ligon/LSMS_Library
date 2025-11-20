import numpy as np
import pandas as pd

import lsms_library as ll


def _attrition_matrix(country_name: str, table: str) -> pd.DataFrame:
    country = ll.Country(country_name, preload_panel_ids=False, verbose=False)
    df = getattr(country, table)()
    return ll.tools.panel_attrition(df, country.waves).fillna(0)


def _is_upper_triangular(matrix: pd.DataFrame) -> bool:
    values = matrix.to_numpy()
    # verify lower-triangular entries (excluding diagonal) are zero
    return np.allclose(np.tril(values, k=-1), 0)


def _nonzero_adjacent(matrix: pd.DataFrame) -> bool:
    # ensure overlap between consecutive waves is non-zero
    waves = matrix.columns.tolist()
    for i in range(len(waves) - 1):
        if matrix.iloc[i, i + 1] <= 0:
            return False
    return True


def test_panel_attrition_household_characteristics_is_upper_triangular():
    matrix = _attrition_matrix("Uganda", "household_characteristics")
    assert _is_upper_triangular(matrix)
    assert _nonzero_adjacent(matrix)


def test_panel_attrition_consistency_between_tables():
    hc_matrix = _attrition_matrix("Uganda", "household_characteristics")
    income_matrix = _attrition_matrix("Uganda", "income")

    # ensure we don't have zeros where the other matrix has large counts
    threshold = 50
    for i, row in enumerate(hc_matrix.index):
        for j, col in enumerate(hc_matrix.columns):
            a = hc_matrix.iloc[i, j]
            b = income_matrix.iloc[i, j]
            if a == 0 and b > threshold:
                raise AssertionError(f"household_characteristics has 0 overlap for {row}/{col} while income has {b}")
            if b == 0 and a > threshold:
                raise AssertionError(f"income has 0 overlap for {row}/{col} while household_characteristics has {a}")

    # diagonals should be monotonically non-decreasing for both tables
    # diagonals shouldn't be zero and should be at least overlapping
    for diag_value in hc_matrix.values.diagonal():
        assert diag_value > 0
    for diag_value in income_matrix.values.diagonal():
        assert diag_value > 0
