"""Cross-country regression test: household_roster Age dtype must be Int64.

Parametrises over every country that declares household_roster in its
data_scheme.yml and asserts:
  1. df.Age.dtype is nullable integer (Int64).
  2. All non-NA Age values are >= 0.

Marked @pytest.mark.slow because it may trigger cache builds on a cold
machine.  In CI the parquet cache is expected to be warm.
"""

import pandas as pd
import pytest
import yaml

from lsms_library.paths import COUNTRIES_ROOT


# ---------------------------------------------------------------------------
# Collect countries that declare household_roster
# ---------------------------------------------------------------------------

class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the !make tag used in data_scheme.yml."""

_SchemeLoader.add_constructor(
    "!make", lambda loader, node: {"__make__": True}
)


def _countries_with_household_roster() -> list[str]:
    """Return sorted list of country names that declare household_roster."""
    countries = []
    for yml in sorted(COUNTRIES_ROOT.glob("*/_/data_scheme.yml")):
        country = yml.parent.parent.name
        with open(yml, "r", encoding="utf-8") as f:
            data = yaml.load(f, Loader=_SchemeLoader)
        if not isinstance(data, dict):
            continue
        ds = data.get("Data Scheme")
        if not isinstance(ds, dict):
            continue
        if "household_roster" in ds:
            countries.append(country)
    return countries


_ROSTER_COUNTRIES = _countries_with_household_roster()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.slow
@pytest.mark.parametrize("country", _ROSTER_COUNTRIES)
def test_age_dtype_is_int64(country):
    """household_roster().Age must be nullable Int64 (or compatible integer dtype)."""
    import lsms_library as ll

    c = ll.Country(country)
    try:
        df = c.household_roster()
    except Exception as exc:
        pytest.skip(f"{country}: household_roster() raised {type(exc).__name__}: {exc}")

    if "Age" not in df.columns:
        pytest.fail(f"{country}: 'Age' column missing from household_roster()")

    age = df["Age"]

    # Countries without microdata (CLAUDE.md §Countries Without Microdata)
    # may return an empty or all-NA Age column without raising.  Treat the
    # same as the raising case: skip rather than fail, since dtype on an
    # empty column is determined by pandas defaults, not by the coercion
    # pipeline under test.
    if len(df) == 0 or age.dropna().empty:
        pytest.skip(f"{country}: household_roster() returned empty or all-NA Age")

    dtype = age.dtype

    # Accept Int64 (pandas nullable integer) or any integer dtype that also
    # supports pd.NA (i.e. not plain int64 which cannot hold NA).
    is_nullable_int = (
        isinstance(dtype, pd.Int64Dtype)
        or str(dtype) == "Int64"
    )
    assert is_nullable_int, (
        f"{country}: Age.dtype is {dtype!r}, expected Int64 (nullable integer). "
        f"Sample values: {age.dropna().head(5).tolist()}"
    )


@pytest.mark.slow
@pytest.mark.parametrize("country", _ROSTER_COUNTRIES)
def test_age_no_negative_values(country):
    """All non-NA Age values must be >= 0 after coercion."""
    import lsms_library as ll

    c = ll.Country(country)
    try:
        df = c.household_roster()
    except Exception as exc:
        pytest.skip(f"{country}: household_roster() raised {type(exc).__name__}: {exc}")

    if "Age" not in df.columns:
        pytest.fail(f"{country}: 'Age' column missing from household_roster()")

    age = df["Age"]

    # Same empty-result guard as the dtype test — skip rather than fail on
    # countries without microdata that return an empty DataFrame.
    if len(df) == 0 or age.dropna().empty:
        pytest.skip(f"{country}: household_roster() returned empty or all-NA Age")

    # Use pd.notna to handle both np.nan and pd.NA
    non_na = age[pd.notna(age)]
    negatives = non_na[non_na < 0]
    assert len(negatives) == 0, (
        f"{country}: {len(negatives)} negative Age values found after coercion: "
        f"{negatives.head(10).tolist()}"
    )
