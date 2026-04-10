"""
Tests for the sample() feature — sampling design metadata.

Tests that sample() returns a well-formed DataFrame with cluster (v),
weight, strata, and Rural columns, indexed by (i, t).
"""

import os
import pytest
import pandas as pd

# Allow overriding build backend via env; default to make to avoid DVC locks
os.environ.setdefault("LSMS_BUILD_BACKEND", "make")

import lsms_library as ll
from lsms_library.paths import COUNTRIES_ROOT
from lsms_library.yaml_utils import load_yaml


def _countries_with_sample() -> list[str]:
    """Discover countries whose data_scheme.yml declares a sample table."""
    countries = []
    for yml in sorted(COUNTRIES_ROOT.glob("*/_/data_scheme.yml")):
        data = load_yaml(yml)
        if not isinstance(data, dict):
            continue
        ds = data.get("Data Scheme", {})
        if isinstance(ds, dict) and "sample" in ds:
            countries.append(yml.parent.parent.name)
    return countries


SAMPLE_COUNTRIES = _countries_with_sample()


_sample_cache: dict[str, pd.DataFrame] = {}


def _get_sample(country_name: str) -> pd.DataFrame:
    """Build and cache sample() per country."""
    if country_name not in _sample_cache:
        c = ll.Country(country_name)
        _sample_cache[country_name] = c.sample()
    return _sample_cache[country_name]


@pytest.mark.parametrize("country_name", SAMPLE_COUNTRIES)
class TestSample:

    @pytest.fixture()
    def sample_df(self, country_name):
        """Build sample() once per country (cached across tests)."""
        df = _get_sample(country_name)
        assert isinstance(df, pd.DataFrame), f"{country_name}.sample() did not return a DataFrame"
        assert not df.empty, f"{country_name}.sample() returned an empty DataFrame"
        return df

    def test_index_is_i_t(self, country_name, sample_df):
        """sample() should be indexed by (i, t)."""
        assert sample_df.index.names == ["i", "t"], (
            f"{country_name} sample index is {sample_df.index.names}, expected ['i', 't']"
        )

    def test_has_v_column(self, country_name, sample_df):
        """sample() must have a cluster column v."""
        assert "v" in sample_df.columns, (
            f"{country_name} sample missing 'v' column; has {list(sample_df.columns)}"
        )

    def test_v_mostly_populated(self, country_name, sample_df):
        """v should be non-null for nearly all rows."""
        v_null_rate = sample_df["v"].isna().mean()
        assert v_null_rate < 0.01, (
            f"{country_name} sample has {v_null_rate:.1%} null v values"
        )

    def test_has_weight_column(self, country_name, sample_df):
        """sample() must have a weight column."""
        assert "weight" in sample_df.columns, (
            f"{country_name} sample missing 'weight' column; has {list(sample_df.columns)}"
        )

    def test_covers_all_waves(self, country_name, sample_df):
        """sample() should cover all waves the country declares."""
        c = ll.Country(country_name)
        expected_waves = set(c.waves)
        actual_waves = set(sample_df.index.get_level_values("t").unique())
        missing = expected_waves - actual_waves
        assert not missing, (
            f"{country_name} sample missing waves: {sorted(missing)}"
        )

    def test_no_duplicate_index(self, country_name, sample_df):
        """Each (i, t) should appear at most once."""
        dup_rate = sample_df.index.duplicated().mean()
        assert dup_rate < 0.01, (
            f"{country_name} sample has {dup_rate:.1%} duplicate (i, t) entries"
        )

    def test_reasonable_row_count(self, country_name, sample_df):
        """Each wave should have a plausible number of households."""
        counts = sample_df.groupby("t").size()
        for wave, n in counts.items():
            assert n >= 100, (
                f"{country_name} wave {wave} has only {n} households in sample"
            )

    def test_weight_nonnegative_where_present(self, country_name, sample_df):
        """Non-null weights should be non-negative (zero is allowed for
        non-response or dropped households)."""
        if "weight" not in sample_df.columns:
            pytest.skip("no weight column")
        weights = sample_df["weight"].dropna()
        if weights.empty:
            pytest.skip("all weights are null (may be expected for some waves)")
        assert (weights >= 0).all(), (
            f"{country_name} has {(weights < 0).sum()} negative weights"
        )
