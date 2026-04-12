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

# Countries whose source survey data does not include sampling weights.
# These are genuine data limitations, not library bugs — the underlying
# micro-data simply has no weight column.  test_has_weight_column is
# xfailed for these.
NO_WEIGHT_COUNTRIES = {"China", "Kazakhstan", "Pakistan"}


_sample_cache: dict[str, pd.DataFrame | None] = {}


def _get_sample(country_name: str) -> pd.DataFrame | None:
    """Build and cache sample() per country.  Returns None on build failure."""
    if country_name not in _sample_cache:
        try:
            c = ll.Country(country_name)
            result = c.sample()
            if isinstance(result, pd.DataFrame) and not result.empty:
                _sample_cache[country_name] = result
            else:
                _sample_cache[country_name] = None
        except Exception:
            _sample_cache[country_name] = None
    return _sample_cache[country_name]


@pytest.mark.parametrize("country_name", SAMPLE_COUNTRIES)
class TestSample:

    @pytest.fixture()
    def sample_df(self, country_name):
        """Build sample() once per country (cached across tests)."""
        df = _get_sample(country_name)
        if df is None:
            pytest.skip(f"{country_name}.sample() could not be built (missing data or DVC error)")
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

    def test_v_is_string_dtype(self, country_name, sample_df):
        """v must be pd.StringDtype so that _join_v_from_sample produces a
        uniform string index level regardless of source wave encoding.
        Mixed int/str in the v index level causes pyarrow failures when
        the caller does df.to_parquet() — GH #142.
        """
        if "v" not in sample_df.columns:
            pytest.skip(f"{country_name} has no v column")
        actual = sample_df["v"].dtype
        assert isinstance(actual, pd.StringDtype), (
            f"{country_name} sample 'v' dtype is {actual!r}, expected pd.StringDtype(). "
            f"Fix the wave-level extraction script to use format_id() on idxvars."
        )

    def test_v_mostly_populated(self, country_name, sample_df):
        """v should be non-null for nearly all rows."""
        v_null_rate = sample_df["v"].isna().mean()
        assert v_null_rate < 0.05, (
            f"{country_name} sample has {v_null_rate:.1%} null v values"
        )
        if v_null_rate > 0.01:
            import warnings
            warnings.warn(
                f"{country_name} sample has {v_null_rate:.1%} null v values "
                f"({sample_df['v'].isna().sum()} rows) — investigate per-wave"
            )

    def test_has_weight_column(self, country_name, sample_df):
        """sample() must have a weight column."""
        if country_name in NO_WEIGHT_COUNTRIES:
            pytest.xfail(
                f"{country_name} source data has no sampling weights "
                f"(genuine data limitation, not a library bug)"
            )
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
        for col in ["weight", "panel_weight"]:
            if col not in sample_df.columns:
                continue
            weights = sample_df[col].dropna()
            if weights.empty:
                continue
            assert (weights >= 0).all(), (
                f"{country_name} has {(weights < 0).sum()} negative values in {col}"
            )

    def test_weighted_population_stable_across_waves(self, country_name, sample_df):
        """Weighted population (sum of cross-sectional weights) should not
        jump more than 5x between adjacent waves — catches miscoded weights
        or wrong variable assignments."""
        if "weight" not in sample_df.columns:
            pytest.skip("no weight column")
        pop = sample_df.groupby("t")["weight"].sum().sort_index()
        pop = pop[pop > 0]
        if len(pop) < 2:
            pytest.skip("fewer than 2 waves with positive weights")
        ratios = pop / pop.shift(1)
        ratios = ratios.dropna()
        for wave, r in ratios.items():
            assert 0.2 < r < 5.0, (
                f"{country_name}: weighted population ratio {wave} vs prior = {r:.2f} "
                f"(sum={pop[wave]:,.0f}). Possible weight variable error."
            )

    def test_cross_section_weight_positive_when_panel_null(self, country_name, sample_df):
        """Refreshment-sample households (panel_weight NaN but weight present)
        should have a positive cross-sectional weight — they were interviewed.
        Rows where BOTH weights are NaN are non-response, not refreshment."""
        if "panel_weight" not in sample_df.columns or "weight" not in sample_df.columns:
            pytest.skip("need both weight columns")
        # Refreshment = panel_weight NaN but cross-sectional weight exists
        refresh = sample_df[
            sample_df["panel_weight"].isna() & sample_df["weight"].notna()
        ]
        if refresh.empty:
            pytest.skip("no refreshment-sample households identified")
        bad = refresh["weight"] <= 0
        assert not bad.any(), (
            f"{country_name}: {bad.sum()} refreshment households have "
            f"non-positive cross-sectional weight"
        )
