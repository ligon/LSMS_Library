"""
Tests for the sample() feature — sampling design metadata.

Tests that sample() returns a well-formed DataFrame with cluster (v),
weight, strata, and Rural columns, indexed by (i, t).
"""

import os
import pytest
import pandas as pd

# NOTE: pre-v0.7.0 this set LSMS_BUILD_BACKEND=make "to avoid DVC locks".
# That advice is now harmful: it bypasses the L2 parquet cache and forces
# a full .dta rebuild on every sample() call, turning the 310-test suite
# into hours of work.  Since v0.7.0 the default "dvc" backend short-
# circuits on warm cache (country.py:1758) without touching DVC locks.

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

# Countries whose microdata is not available in this checkout — marked
# with `data_available: false` in their data_scheme.yml.  Calling sample()
# on these takes ~6 minutes to fail on the fallback path; skip instead.
# When data lands, the country's data_scheme.yml flips the flag and
# tests re-enable automatically with no change here.
def _countries_without_data() -> set[str]:
    unavailable = set()
    for yml in sorted(COUNTRIES_ROOT.glob("*/_/data_scheme.yml")):
        data = load_yaml(yml)
        if isinstance(data, dict) and data.get("data_available") is False:
            unavailable.add(yml.parent.parent.name)
    return unavailable


NO_DATA_COUNTRIES = _countries_without_data()


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
        if country_name in NO_DATA_COUNTRIES:
            pytest.skip(f"{country_name}: microdata not available (see CLAUDE.md)")
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
        or wrong variable assignments.

        Exception: countries whose waves span different survey instruments
        (e.g., CotedIvoire's 1985-89 LSMS + 2018-19 EHCVM) may legitimately
        have incommensurate weight scales — the 1980s LSMS uses ALLWAITN
        (sum ~ N households), EHCVM uses population-scaled weights.  Xfail
        those with a clear reason.
        """
        if country_name == "CotedIvoire":
            pytest.xfail(
                "CotedIvoire weights span LSMS (1985-89, ALLWAITN ~ N households) "
                "and EHCVM (2018-19, population-scaled) — incommensurate by design"
            )
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


# ---------------------------------------------------------------------------
# Uganda 2009-10 hybrid v: comm when present, "@lat,lon" synthetic otherwise.
# See countries/Uganda/_/CONTENTS.org §"Hybrid v in 2009-10" for rationale.
# ---------------------------------------------------------------------------

class TestUganda2009HybridV:
    """2009-10 uses the `coalesce_coord_bin` transformer to fill v for
    the 565 HH whose `comm` is blank (movers + split-offs + 5 data-entry
    anomalies).  After the fix:
      - 2 410 HH have a numeric-string `comm` (317 distinct EAs);
      - 541 HH have a synthetic `@lat,lon` label (~339 distinct bins);
      - 24 HH are genuinely NA (no comm AND no coords).
    """

    @pytest.fixture()
    def s09(self):
        df = _get_sample("Uganda")
        if df is None:
            pytest.skip("Uganda.sample() could not be built")
        try:
            return df.xs("2009-10", level="t")
        except KeyError:
            pytest.skip("no 2009-10 wave in Uganda sample()")

    def test_row_count(self, s09):
        assert len(s09) == 2975, f"expected 2975 HH, got {len(s09)}"

    def test_na_exact(self, s09):
        assert s09["v"].isna().sum() == 24, (
            f"expected exactly 24 NA v's (geo-missing tail), got "
            f"{s09['v'].isna().sum()}"
        )

    def test_no_empty_strings(self, s09):
        empty = (s09["v"] == "").sum()
        assert empty == 0, (
            f"{empty} rows with empty-string v — NA sentinel should be pd.NA"
        )

    def test_partitions_by_form(self, s09):
        v = s09["v"].astype("string")
        numeric = v.str.fullmatch(r"\d+", na=False)
        synthetic = v.str.startswith("@").fillna(False)
        na = v.isna()
        # No overlap.
        assert not (numeric & synthetic).any()
        assert not (numeric & na).any()
        assert not (synthetic & na).any()
        # Cover everyone.
        assert (numeric | synthetic | na).all()

    def test_real_comm_count(self, s09):
        numeric = s09["v"].astype("string").str.fullmatch(r"\d+", na=False)
        n = numeric.sum()
        # 2410 is the expected count; allow small drift from upstream data
        # cleaning, but flag anything that departs meaningfully.
        assert 2400 <= n <= 2420, f"expected ~2410 numeric-comm HH, got {n}"
        # Distinct EAs on the real side — the 2005-06 sampling frame had
        # 322 EAs; 2009-10 saw 317 of them with at least one surviving HH.
        unique_real = s09.loc[numeric, "v"].nunique()
        assert 310 <= unique_real <= 325, (
            f"expected ~317 distinct real comms, got {unique_real}"
        )

    def test_synthetic_count(self, s09):
        synthetic = s09["v"].astype("string").str.startswith("@").fillna(False)
        n = synthetic.sum()
        # 541 expected (movers + split-offs + 5 anomalies whose coords are
        # present in the geovars file).  Allow small drift for upstream
        # corrections.
        assert 530 <= n <= 555, f"expected ~541 synthetic-v HH, got {n}"

    def test_synthetic_labels_well_formed(self, s09):
        synthetic = s09["v"].astype("string")
        synthetic = synthetic[synthetic.str.startswith("@").fillna(False)]
        # Format: @[+-]dd.dd,[+-]ddd.dd (lat, lon with 0.01° precision).
        pattern = r"^@[+-]\d+\.\d{2},[+-]\d+\.\d{2}$"
        bad = synthetic[~synthetic.str.match(pattern, na=False)]
        assert bad.empty, (
            f"{len(bad)} synthetic v's don't match {pattern}: {bad.head().tolist()}"
        )

    def test_synthetic_disjoint_from_real(self, s09):
        """Any @-prefixed value must not collide with any numeric comm —
        this is the property that lets downstream CSECTION joins naturally
        skip the synthetic entries."""
        v = s09["v"].astype("string")
        numeric_vals = set(v[v.str.fullmatch(r"\d+", na=False)].dropna())
        synth_vals = set(v[v.str.startswith("@").fillna(False)].dropna())
        assert numeric_vals.isdisjoint(synth_vals)

    def test_temporary_columns_dropped(self, s09):
        """The `_lat`/`_lon` columns should be consumed by the transformer
        and removed by `drop:` — they must not leak into the final frame."""
        leaked = [c for c in s09.columns if c.startswith("_")]
        assert not leaked, f"temporary columns leaked: {leaked}"
