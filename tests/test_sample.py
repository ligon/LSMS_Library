"""
Tests for the sample() feature — sampling design metadata.

Tests that sample() returns a well-formed DataFrame with cluster (v),
weight, strata, and Rural columns, indexed by (i, t).
"""

import importlib.util
import os
import pytest
import pandas as pd

# NOTE: pre-v0.7.0 this set LSMS_BUILD_BACKEND=make "to avoid DVC locks".
# That advice is now harmful: it bypasses the L2-country parquet cache
# and forces a full .dta rebuild on every sample() call, turning the
# 310-test suite into hours of work.  Since v0.7.0 the default "dvc"
# backend short-circuits on warm cache (country.py:1758) without
# touching DVC locks.

import lsms_library as ll
from lsms_library.paths import countries_root, data_root
from lsms_library.yaml_utils import load_yaml

pytestmark = pytest.mark.requires_s3


def _countries_with_sample() -> list[str]:
    """Discover countries whose data_scheme.yml declares a sample table."""
    countries = []
    for yml in sorted(countries_root().glob("*/_/data_scheme.yml")):
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
# Peru 1994 REG01._weight_ is an SPSS export artefact (constant 1);
# the documented seven domain expansion factors are not in this extract.
# See Peru/_/CONTENTS.org, "sample carries NO weight".
NO_WEIGHT_COUNTRIES = {"China", "Kazakhstan", "Pakistan", "Peru"}

# Acquired rounds with no sample wiring yet: Peru's three, and new waves that
# are registered ahead of their tables (Malawi 2024-25, GH #1275; the four
# EHCVM-2 waves, GH #552).
# Do not infer exemptions from directory shape: multi-round countries use
# wave_folder_map for valid rounds without their own _/ directories.
PARTIALLY_WIRED_SAMPLE = {
    "Peru": {"1985", "1990", "1991"},
    "Malawi": {"2024-25"},
    "Togo": {"2021-22"},
    "Guinea-Bissau": {"2021-22"},
    "CotedIvoire": {"2021-22"},
    "Benin": {"2021-22"},
}

# ---------------------------------------------------------------------------
# Raw-layer weight-scale checks (the home of the scale check after the
# API-level one became meaningless).
#
# sample() rescales every wave's weights to mean 1, so the API deliberately
# ERASES scale.  A scale check therefore has to read the L2-country parquet,
# which still holds the raw values.  See `_read_raw_sample_weights` and the
# two tests that use it.
#
# The two classes, measured across the whole corpus on 2026-08-22
# (slurm_logs/weight_normalisation/corpus_before.csv):
#
#   self-weighting / already-normalised : 13 cells, raw mean 0.99999999 .. 1.00016665
#   expansion                           : 80 cells, raw mean 25.7197 .. 8658.9664
#
# Nothing lies between: the gap is 25.7x wide and EMPTY.  The two constants
# below bracket that empty band -- they are the observed gap, not tuned knobs,
# and `test_raw_weight_scale_classes_are_unambiguous` is what keeps them
# honest.  A weight accidentally wired to household size would land at mean
# ~5, i.e. squarely inside the band, and fail loudly.
_RAW_SELF_WEIGHTING_RTOL = 0.01   # |mean - 1| <= 1%  -> self-weighting scale
_RAW_EXPANSION_FLOOR = 10.0       # mean >= 10        -> expansion scale


def _read_raw_sample_weights(country_name: str):
    """Per-wave RAW weight stats from the L2-country parquet, or None.

    The parquet stores weights PRE-normalisation (CLAUDE.md §Cache Behavior:
    "cached parquets store pre-transformation data"), which is the only place
    the survey's own scale still exists once `_normalise_sample_weights` has
    run.  Returns a DataFrame indexed by wave with columns n / mean / sum.

    Returns None when there is no parquet to read -- a genuinely cold data
    root, `LSMS_BUILD_BACKEND=make`, or a country with no `weight` column.
    Callers skip; this is a check on data that exists, not a demand that it be
    materialized.
    """
    path = data_root(country_name) / "var" / "sample.parquet"
    try:  # the read is the check; a concurrent rebuild may delete it (GH #778)
        raw = pd.read_parquet(path)
    except FileNotFoundError:
        return None
    if "weight" not in raw.columns:
        return None
    if "t" in (raw.index.names or []):
        waves = raw.index.get_level_values("t")
    elif "t" in raw.columns:
        waves = raw["t"]
    else:
        return None
    w = pd.to_numeric(raw["weight"], errors="coerce")
    flat = pd.DataFrame({"t": pd.Index(waves).astype(str), "w": w.to_numpy()})
    out = (flat.dropna(subset=["w"])
               .groupby("t")["w"]
               .agg(n="size", mean="mean", sum="sum"))
    return out if len(out) else None

# Countries whose microdata is not available in this checkout — marked
# with `data_available: false` in their data_scheme.yml.  Calling sample()
# on these takes ~6 minutes to fail on the fallback path; skip instead.
# When data lands, the country's data_scheme.yml flips the flag and
# tests re-enable automatically with no change here.
def _countries_without_data() -> set[str]:
    unavailable = set()
    for yml in sorted(countries_root().glob("*/_/data_scheme.yml")):
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
        except Exception:  # broad catch intentional: skip country on any load failure
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
        expected_waves = set(c.waves) - PARTIALLY_WIRED_SAMPLE.get(country_name, set())
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

    def test_weights_normalised_to_within_wave_mean_one(self, country_name, sample_df):
        """Every wave's non-null weights have mean 1.

        `Country._finalize_result` divides each wave's `weight` /
        `panel_weight` by that wave's own non-null mean, so the two scales the
        corpus ships (13 already-normalised cells, 80 expansion cells summing
        to a national population) are indistinguishable at the API.  The raw
        values stay in the parquet.  See `_normalise_sample_weights`.
        """
        checked = 0
        for col in ("weight", "panel_weight"):
            if col not in sample_df.columns:
                continue
            for wave, g in sample_df.groupby("t", observed=True):
                w = pd.to_numeric(g[col], errors="coerce").dropna()
                if w.empty:
                    continue  # all-null wave: nothing to normalise
                if not (w.mean() > 0):
                    continue  # zero/negative mean: skipped by design, warns
                checked += 1
                assert abs(w.mean() - 1.0) < 1e-9, (
                    f"{country_name} {wave} {col}: mean {w.mean():.6g}, expected 1.0. "
                    f"Weights should be normalised at API time."
                )
        if not checked:
            pytest.skip("no non-null weights in any wave")

    def test_weighted_sum_equals_household_count(self, country_name, sample_df):
        """Corollary of mean-1: sum(weight) over a wave is its non-null count.

        This replaces the former `test_weighted_population_stable_across_waves`,
        which compared adjacent waves' weighted population totals to catch a
        miscoded weight variable.  That signal is no longer expressible at API
        level — the totals are sample sizes now, by construction — and the
        CotedIvoire xfail it carried ("LSMS ALLWAITN vs EHCVM population-scaled
        — incommensurate by design") described exactly the mixing this
        normalisation removes.  A miscoded weight variable must now be caught
        against the raw parquet or at the wave config.
        """
        if "weight" not in sample_df.columns:
            pytest.skip("no weight column")
        for wave, g in sample_df.groupby("t", observed=True):
            w = pd.to_numeric(g["weight"], errors="coerce").dropna()
            if w.empty or not (w.mean() > 0):
                continue
            assert abs(w.sum() - len(w)) < 1e-6 * max(len(w), 1), (
                f"{country_name} {wave}: sum(weight)={w.sum():.6g} but "
                f"{len(w)} non-null weights"
            )

    def test_weight_normalisation_is_idempotent(self, country_name, sample_df):
        """Re-applying the transform to an already-normalised frame is a no-op.

        Load-bearing: `_join_v_from_sample` re-enters `sample()` while
        finalising every other household-level table.
        """
        from lsms_library.country import _normalise_sample_weights
        again = _normalise_sample_weights(sample_df.copy(), country=country_name)
        for col in ("weight", "panel_weight"):
            if col not in sample_df.columns:
                continue
            before = pd.to_numeric(sample_df[col], errors="coerce")
            after = pd.to_numeric(again[col], errors="coerce")
            pd.testing.assert_series_equal(before, after, check_names=False,
                                           rtol=1e-12, atol=0)

    def test_weighted_ratios_unchanged_by_normalisation(self, country_name, sample_df):
        """A weighted share is invariant under within-wave rescaling.

        This is the mathematical guarantee that makes the change safe:
        sum(w*x)/sum(w) is unchanged when every w in a wave is multiplied by
        the same positive constant.  Checked here against a deliberately
        de-normalised copy (each wave scaled by a distinct factor), using
        `Rural` as x where present.
        """
        if "weight" not in sample_df.columns or "Rural" not in sample_df.columns:
            pytest.skip("need weight and Rural")
        from lsms_library.country import _normalise_sample_weights
        # `Rural` is a categorical label ('Rural' / 'Urban' / 'Semi-urban'),
        # not a number — turn it into a 0/1 indicator.  Coercing it with
        # pd.to_numeric would silently produce an all-NaN x and make this test
        # skip everywhere while appearing to pass.
        x = (sample_df["Rural"].astype("string").str.strip().str.lower()
             .map({"rural": 1.0, "true": 1.0,
                   "urban": 0.0, "semi-urban": 0.0, "false": 0.0}))
        if x.notna().sum() == 0:
            x = pd.to_numeric(sample_df["Rural"], errors="coerce")
        if x.notna().sum() == 0:
            pytest.skip("Rural carries no usable indicator")
        waves = list(dict.fromkeys(sample_df.index.get_level_values("t")))
        factors = {w: 10.0 ** (i + 1) for i, w in enumerate(waves)}
        scrambled = sample_df.copy()
        scrambled["weight"] = pd.to_numeric(scrambled["weight"], errors="coerce") * \
            pd.Series(scrambled.index.get_level_values("t"),
                      index=scrambled.index).map(factors).astype("float64")
        restored = _normalise_sample_weights(scrambled, country=country_name)
        w_all = pd.to_numeric(sample_df["weight"], errors="coerce")
        w_new = pd.to_numeric(restored["weight"], errors="coerce")
        tvals = sample_df.index.get_level_values("t")
        compared = 0
        for wave in waves:
            m = (tvals == wave)
            w0, w1, xv = w_all[m], w_new[m], x[m]
            ok = (w0.notna() & xv.notna()).to_numpy()
            if not ok.any() or w0[ok].sum() <= 0:
                continue
            compared += 1
            share0 = (w0[ok] * xv[ok]).sum() / w0[ok].sum()
            share1 = (w1[ok] * xv[ok]).sum() / w1[ok].sum()
            assert abs(share0 - share1) < 1e-9, (
                f"{country_name} {wave}: weighted Rural share moved "
                f"{share0:.12g} -> {share1:.12g} under rescaling"
            )
        if not compared:
            pytest.skip("no wave with both positive weights and a Rural indicator")

    # -- raw-layer scale checks -------------------------------------------
    # `sample_df` is requested (not used) purely to force the build, so the
    # L2-country parquet these read is present.

    def test_raw_weight_scale_classes_are_unambiguous(self, country_name, sample_df):
        """Each wave's RAW weight mean is either ~1 or >= 10, never between.

        Half of the restored miscoded-weight check.  The corpus has exactly
        two legitimate scales -- self-weighting/normalised (mean 1.0000) and
        expansion (mean >= 25.72) -- separated by an empty 25.7x band.  A
        weight variable wired to the wrong column lands in that band: household
        size gives mean ~5, a per-capita or per-adult-equivalent figure
        similar.  Anything landing there is a wiring bug, not a third scale.

        Deliberately NOT a "weights must be big" check: a raw mean of 1 is
        correct for 13 cells and must pass.  CotedIvoire is the case that
        proves it -- four waves at raw mean 1.0000 (1980s LSMS `ALLWAITN`) and
        one at 443.42 (2018-19 EHCVM), all legitimate.
        """
        raw = _read_raw_sample_weights(country_name)
        if raw is None:
            pytest.skip("no raw sample parquet to read (cold data root?)")
        bad = raw[~(
            ((raw["mean"] - 1.0).abs() <= _RAW_SELF_WEIGHTING_RTOL)
            | (raw["mean"] >= _RAW_EXPANSION_FLOOR)
        )]
        assert bad.empty, (
            f"{country_name}: raw weight mean falls in the empty band between "
            f"the self-weighting (~1) and expansion (>={_RAW_EXPANSION_FLOOR:g}) "
            f"scales, which no legitimate weight in the corpus does:\n"
            f"{bad[['n', 'mean', 'sum']].to_string()}\n"
            f"Check the wave's data_info.yml -- a mean near household size is "
            f"the signature of a weight wired to the wrong column. Read the "
            f"parquet at {data_root(country_name) / 'var' / 'sample.parquet'}."
        )

    def test_raw_expansion_totals_stable_across_waves(self, country_name, sample_df):
        """Adjacent expansion waves' RAW weighted totals stay within 5x.

        The other half, and the direct successor to the deleted
        `test_weighted_population_stable_across_waves`: same intent (a
        miscoded weight variable moves the implied population), same (0.2,
        5.0) bounds, moved to the layer where scale still exists.  At the API
        every total is now the wave's household count, so the old form could
        only ever pass.

        Restricted to the EXPANSION class, because comparing an expansion
        total against a self-weighting one is comparing a population estimate
        against a sample size -- which is the very mixing this PR removed, not
        a defect. CotedIvoire consequently has one comparable wave and skips.

        Measured headroom (2026-08-22): the widest real ratios are Malawi 2.51
        and Tanzania 2.07/0.48; every country passes.
        """
        raw = _read_raw_sample_weights(country_name)
        if raw is None:
            pytest.skip("no raw sample parquet to read (cold data root?)")
        exp = raw[raw["mean"] >= _RAW_EXPANSION_FLOOR].sort_index()
        if len(exp) < 2:
            pytest.skip(
                f"{country_name} has {len(exp)} expansion-scale wave(s); "
                f"nothing to compare"
            )
        totals = exp["sum"]
        ratios = (totals / totals.shift(1)).dropna()
        for wave, r in ratios.items():
            assert 0.2 < r < 5.0, (
                f"{country_name}: RAW weighted total ratio {wave} vs prior "
                f"= {r:.2f} (total {totals[wave]:,.0f}). A jump this size in "
                f"the implied population is the signature of a miscoded "
                f"weight variable. Totals:\n{totals.to_string()}"
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

def _uganda_2009_sources():
    """(GSEC1 ids, ids only in AGSEC3B/GSEC12, GSEC1 ids with no comm and no
    coordinates), read from the raw 2009-10 files, not through the builder."""
    import pyreadstat
    from lsms_library.data_access import get_data_file
    from lsms_library.local_tools import format_id

    def read(name):
        df, _ = pyreadstat.read_dta(str(get_data_file(f"Uganda/2009-10/Data/{name}")),
                                    apply_value_formats=False)
        return df.assign(i=df["HHID"].map(format_id))

    cover, geo = read("GSEC1.dta"), read("UNPS_Geovars_0910.dta")
    frame = set(read("AGSEC3B.dta").i) | set(read("GSEC12.dta").i)
    blank = cover[cover["comm"].astype("string").fillna("").str.strip() == ""]
    located = geo[geo["lat_mod"].notna() & geo["lon_mod"].notna()]
    return set(cover.i), frame - set(cover.i), set(blank.i) - set(located.i)


class TestUganda2009HybridV:
    """2009-10 uses the `coalesce_coord_bin` transformer to fill v for
    the 565 HH whose `comm` is blank (movers + split-offs + 5 data-entry
    anomalies).  After the fix:
      - 2 410 HH have a numeric-string `comm` (317 distinct EAs);
      - 541 HH have a synthetic `@lat,lon` label (~339 distinct bins);
      - 28 HH have the `unlocated-{hhid}` identity (GH #1120 D2, #1358): the
        24 with no comm AND no coords, and 4 known only from AGSEC3B/GSEC12.
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
        """Own-wave membership: 2,975 GSEC1 households + 4 (#1358, was 2,975)."""
        cover, frame_only, _ = _uganda_2009_sources()
        assert len(s09) == len(cover | frame_only) == 2979, (
            f"expected the GSEC1 + AGSEC3B + GSEC12 households, got {len(s09)}")

    def test_na_exact(self, s09):
        """No NA left: #1358 gave the 24 that were NA a singleton identity."""
        assert s09["v"].isna().sum() == 0
        _, frame_only, unlocatable = _uganda_2009_sources()
        singletons = s09["v"].astype("string").str.startswith("unlocated-").fillna(False)
        assert (len(unlocatable), len(frame_only)) == (24, 4)
        assert singletons.sum() == len(unlocatable | frame_only)

    def test_no_empty_strings(self, s09):
        empty = (s09["v"] == "").sum()
        assert empty == 0, (
            f"{empty} rows with empty-string v — NA sentinel should be pd.NA"
        )

    def test_partitions_by_form(self, s09):
        v = s09["v"].astype("string")
        numeric = v.str.fullmatch(r"\d+", na=False)
        synthetic = v.str.startswith("@").fillna(False)
        singleton = v.str.startswith("unlocated-").fillna(False)  # #1358
        na = v.isna()
        # No overlap.
        assert not (numeric & synthetic).any()
        assert not (numeric & na).any()
        assert not (synthetic & na).any()
        assert not (singleton & (numeric | synthetic | na)).any()
        # Cover everyone.
        assert (numeric | synthetic | singleton | na).all()

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


# ---------------------------------------------------------------------------
# Uganda: HH-level Region exposed on sample() (not just cluster-level).
# ---------------------------------------------------------------------------

UGANDA_WAVES = ["2005-06", "2009-10", "2010-11", "2011-12",
                "2013-14", "2015-16", "2018-19", "2019-20"]


class TestUgandaRegionOnSample:
    """`sample()[['Region']]` should be populated per-HH for every Uganda
    wave, mapping to the canonical 4-region set used by cluster_features.
    Kampala is folded into Central (2009-10, 2010-11); 2005-06's `'0'`
    sentinel (same encoding quirk as Rural) is also folded into Central.
    """

    @pytest.fixture()
    def uga_sample(self):
        df = _get_sample("Uganda")
        if df is None:
            pytest.skip("Uganda.sample() could not be built")
        if "Region" not in df.columns:
            pytest.fail("Uganda.sample() missing 'Region' column")
        return df

    def test_region_present(self, uga_sample):
        assert "Region" in uga_sample.columns

    def test_region_canonical_labels(self, uga_sample):
        """Every non-NA Region value must be one of the four canonical
        labels — no '0', 'None', 'Kampala', or numeric residue."""
        canonical = {"Central", "Eastern", "Northern", "Western"}
        bad = set(uga_sample["Region"].dropna().astype(str).unique()) - canonical
        assert not bad, (
            f"Uganda sample().Region contains non-canonical labels: {sorted(bad)}"
        )

    def test_region_coverage_per_wave(self, uga_sample):
        """Every wave's LOCATED households should have ≥99% populated Region.
        2019-20 has a handful of NA in the refreshment tail.

        `unlocated-` identities (#1358, GH #1120 D2) are excluded because they
        have no cluster key: 128 of the 153 have no Region either, and 25 do
        (24 in 2009-10, 1 in 2013-14).  Counted, all households would read
        2,850 of 2,907 in 2011-12, 3,176 of 3,242 in 2018-19.  Judged on located
        households alone this test cannot see a household that loses both `v`
        and Region, so the singleton test below pins who the excluded ones are."""
        v = uga_sample["v"].astype("string")
        located = uga_sample[~v.str.startswith("unlocated-").fillna(False)]
        for wave, sub in located.groupby("t"):
            non_null = sub["Region"].notna().sum()
            frac = non_null / len(sub)
            assert frac >= 0.995, (
                f"{wave}: only {frac:.1%} of HH have Region populated "
                f"({non_null}/{len(sub)})"
            )

    @pytest.mark.parametrize("wave, n", [("2011-12", 57), ("2018-19", 66)])
    def test_agsec1_only_households_have_no_region_in_their_own_source(
            self, uga_sample, wave, n):
        """Their missing Region is the source's, not a lost join: AGSEC1, which
        gives them membership, has no `region` (2011-12) or leaves it blank
        (2018-19).  They are also listed in other files (21 in 2011-12, 53 in
        2018-19), none with a region or district for them (red-team sweep by
        column name, 2026-11-04; this test reads AGSEC1 only)."""
        import pyreadstat
        from lsms_library.data_access import get_data_file
        from lsms_library.local_tools import format_id

        hhid = "HHID" if wave == "2011-12" else "hhid"
        ag, _ = pyreadstat.read_dta(str(get_data_file(f"Uganda/{wave}/Data/AGSEC1.dta")),
                                    apply_value_formats=False)
        cover, _ = pyreadstat.read_dta(str(get_data_file(f"Uganda/{wave}/Data/GSEC1.dta")),
                                       usecols=[hhid], apply_value_formats=False)
        only = ag[~ag[hhid].map(format_id).isin(set(cover[hhid].map(format_id)))]
        assert len(only) == n
        assert "region" not in ag.columns or only["region"].isna().all()
        sub = uga_sample.xs(wave, level="t")
        singletons = sub[sub["v"].astype("string").str.startswith("unlocated-").fillna(False)]
        assert len(singletons) == n and singletons["Region"].isna().all()

    @pytest.mark.parametrize("wave", UGANDA_WAVES)
    def test_singletons_are_the_households_with_no_location_in_their_sources(
            self, uga_sample, wave):
        """A located household must not become `unlocated-` (#1120 D2).

        The rule is read from each wave's locating sources, through the input
        function registered for the identity.  Region coverage cannot catch this:
        a household losing `v` and Region together leaves its denominator.
        """
        spec = importlib.util.spec_from_file_location(
            "uganda_singletons", countries_root() / "Uganda" / "_" / "uganda.py")
        ug = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ug)
        v = uga_sample.xs(wave, level="t")["v"].astype("string")
        served = set(v[v.str.startswith("unlocated-").fillna(False)].str.removeprefix("unlocated-"))
        rule = set(ug.inputs_unlocated_identity(wave).index.get_level_values("i"))
        # Counts, so a failure prints no household ids.
        assert (len(served - rule), len(rule - served)) == (0, 0), (
            f"{wave}: {len(served)} served, {len(rule)} with no location in source")

    def test_every_wave_gets_the_singleton_check(self, uga_sample):
        assert sorted(uga_sample.index.get_level_values("t").unique()) == UGANDA_WAVES


# ---------------------------------------------------------------------------
# _add_market_index: HH-level fallback for synthetic-v rows
# ---------------------------------------------------------------------------

def _labelled_households(df, wave):
    """Distinct households in *wave* whose market label ``m`` is not NA."""
    sub = df.xs(wave, level="t")
    labelled = sub.index.get_level_values("m").notna()
    return sub.index.get_level_values("i")[labelled].nunique()


class TestUganda2009MarketFallback:
    """When `cluster_features.Region` has no row for a HH's v (e.g.
    synthetic `@lat,lon` in 2009-10), `_add_market_index` should fall back
    to `sample()['Region']` at HH level rather than silently drop the HH.
    Without the fallback, movers + split-offs were being filtered out of
    `food_expenditures(market='Region')` — defeating the hybrid-v recovery.
    """

    @pytest.fixture(scope="class")
    def fe(self):
        """Uganda food_expenditures(market='Region'), or skip if the
        underlying microdata cannot be built in this environment
        (e.g. CI without DVC S3 credentials).

        Uses ``basis='total'`` (#575): this test asserts the market-fallback
        v-retention coverage (≥2929 HH), which is orthogonal to acquisition
        source.  The purchased-only default (the new #575 default) legitimately
        drops the 29 Uganda HH that consumed only own-production / in-kind food
        (zero cash expenditure); ``basis='total'`` keeps the full HH set the
        coverage invariant is about."""
        try:
            return (
                ll.Country("Uganda")
                .food_expenditures(market="Region", basis="total")
                .squeeze()
            )
        except Exception as exc:  # broad catch intentional: skip on any build failure
            pytest.skip(f"Uganda food_expenditures unavailable: {exc}")

    @pytest.fixture(scope="class")
    def hc(self):
        """Uganda household_characteristics(market='Region'), or skip if
        the underlying microdata cannot be built in this environment
        (e.g. CI without DVC S3 credentials)."""
        try:
            return ll.Country("Uganda").household_characteristics(market="Region")
        except Exception as exc:  # broad catch intentional: skip on any build failure
            pytest.skip(
                f"Uganda household_characteristics unavailable: {exc}"
            )

    def test_food_expenditures_retains_hybrid_v_HH(self, fe):
        """food_expenditures(market='Region') should retain the 2 929 HH
        in Uganda 2009-10, not the 2 240 that survive cluster-only Region."""
        if "t" not in fe.index.names or "m" not in fe.index.names:
            pytest.fail(
                f"food_expenditures(market='Region') index {fe.index.names} "
                "missing required t/m levels"
            )
        if "2009-10" not in fe.index.get_level_values("t").unique():
            pytest.skip("no 2009-10 wave")
        # Count households that GOT a market label.  _add_market_index now
        # keeps an unlabelled household with m NA instead of dropping it, so
        # a bare nunique would count them too and stop testing the fallback.
        hh09 = _labelled_households(fe, "2009-10")
        # Sample-level HH count in 2009-10 is 2 975 (full roster).  22 HH have
        # no food-expenditure records and drop out for that reason, not because
        # of a NaN Region.  Another 24 are dropped by the MonthsSpent filter
        # in roster_to_characteristics (departed-only HHs).  The remaining
        # 2 929 is what the HH-level _add_market_index fallback delivers,
        # confirmed by Slurm-rebuild on 2026-05-09 with both warm and cold
        # caches.  The pre-fallback coverage was ~2 240.
        assert hh09 >= 2929, (
            f"food_expenditures(market='Region') retained only {hh09} HH in "
            f"2009-10 — expected ≥2929 after _add_market_index HH-level "
            f"fallback (sample 2975 − 22 no-food − 24 departed-only)"
        )

    def test_household_characteristics_retains_hybrid_v_HH(self, hc):
        """household_characteristics(market='Region') should cover every
        household in the 2009-10 GSEC1 cover: 2,975.  `sample()` has 2,979 since
        #1358, but the 4 it added (known only from AGSEC3B/GSEC12) are not in
        this table.

        Pin history:

        - 2026-05-09: pinned to 2951 (sample 2975 − 24 mover HHs whose
          NaN ``v`` was being silently dropped by the household
          groupby, originally attributed in the comment to the
          MonthsSpent filter).
        - 2026-05-11 (GH #268): re-pinned to 2975.  The 24 mover HHs
          previously dropped by the groupby now survive as
          ``v == 'Mover'`` under the new ``mover_sentinel`` default in
          ``roster_to_characteristics``.  Sample carried a valid
          Region for those HHs all along, so they're correctly
          assignable to a market.
        - 2026-10-30 (GH #1120 D2): the pin stands, the mechanism does not.
          Those 24 now carry their own ``v == 'unlocated-{hhid}'``, so no Uganda
          household reaches the ``'Mover'`` sentinel.  ``sample`` itself holds
          2,979: the 4 it gained are on no cover and have no roster.

        Any further drift in either direction is meaningful: investigate
        before re-pinning.
        """
        if "2009-10" not in hc.index.get_level_values("t").unique():
            pytest.skip("no 2009-10 wave")
        # Households that GOT a market label (see the fe test above for why).
        hh09 = _labelled_households(hc, "2009-10")
        assert hh09 == 2975, (
            f"household_characteristics(market='Region') retained {hh09} HH "
            f"in 2009-10 — expected exactly 2975 (every roster household, including "
            f"the 24 with neither comm nor GPS, whose v is 'unlocated-{{hhid}}'; "
            f"GH #1120 D2).  Drift in either "
            f"direction is meaningful: investigate before re-pinning."
        )

    def test_nan_m_after_fallback_is_the_measured_residual(self, fe):
        """After the fallback, Uganda 2009-10 households with a NaN m are
        only the residual neither source can label.

        This used to assert ``nan_count == 0`` across all waves, which was
        vacuous: ``_add_market_index`` DELETED every unlabelled row, so no
        NaN m could ever reach the test.  It now keeps them (m NA, with a
        ``NullIndexKeyWarning``), so the assertion is scoped to the wave the
        fallback is about, and pinned to the census of 2026-11-03: 4 of
        2,979 sample households in 2009-10 have no Region in sample() and
        none in cluster_features for their (t, v).  A broken HH-level
        fallback would show ~700 (the pre-fallback coverage was ~2,240)."""
        if "2009-10" not in fe.index.get_level_values("t").unique():
            pytest.skip("no 2009-10 wave")
        sub = fe.xs("2009-10", level="t")
        m_vals = sub.index.get_level_values("m")
        assert not m_vals.astype("string").isin(["nan", "<NA>", "None"]).any(), (
            "a missing market label must be NA, not a stringified NA")
        nan_hh = sub.index.get_level_values("i")[m_vals.isna()].nunique()
        assert nan_hh <= 4, (
            f"{nan_hh} Uganda 2009-10 households have NaN m after the "
            f"HH-level fallback; the measured residual is 4.")
