"""GH #602 -- a declared vocabulary must actually be ENFORCED.

``lsms_library/data_info.yml`` declares accepted values via ``spellings``
blocks, but ``_enforce_canonical_spellings()`` only ever *mapped known
variants*; it never *rejected unknown ones*, and no sanity check read the
``spellings`` declarations at all.  A value the schema never declared therefore
flowed straight through to the user -- Uganda's ``sample.Rural`` was the literal
string ``'0'`` for 2,263 households (72% of the 2005-06 wave), so the idiomatic
filter ``df[df['Rural'] == 'Rural']`` silently returned ZERO rows, and
``is_this_feature_sane(...).ok`` was ``True``.

These tests pin:
  1. the new ``_check_declared_spellings`` check (fires on unknown values, in
     columns AND index levels; silent on known variants and clean frames; does
     NOT fire on the ``housing.Tenure`` false-positive trap);
  2. ``sample`` being declared under ``Columns:`` at all (it was not, which made
     ``_enforce_canonical_spellings`` a total no-op on ``sample()``);
  3. the per-country data fixes, each of which is a distinct root cause.
"""

import pandas as pd
import pytest

from lsms_library.country import _enforce_canonical_spellings
from lsms_library.diagnostics import (
    _check_declared_spellings,
    _DECLARED_VOCABULARIES,
    is_this_feature_sane,
)


# ---------------------------------------------------------------------------
# Known-unharmonized (table, column) pairs -- an ENUMERATED list that is meant
# to be burned down, NOT a permanent exemption.
#
# These five countries ship raw, unharmonized survey labels in plot_features
# `Tenure` / `TenureSystem` (Cambodia's ALL-CAPS questionnaire text; Kosovo's
# 'Owned'/'Rented'; Timor-Leste's 'Part owner'; Albania's and Tajikistan's
# post-socialist documentary-title vocabulary -- 'privatised', 'deed',
# 'usufruct', 'CERTIFICATE', 'ACT (SEALED DOCUMENT)').  They are REAL defects
# and the check correctly FAILS them: `df[df.Tenure == 'owned']` returns zero
# rows for Kosovo despite 6,419 owned plots.
#
# They are xfailed rather than "fixed" because several values are genuinely
# ambiguous without the questionnaire codebooks (Cambodia's "GIVEN BY THE
# GOVERNMENT OR LOCAL AUTHORITY"; Timor-Leste's "Part owner", 81% of its plots),
# and guessing a mapping would replace a VISIBLY wrong value with an INVISIBLY
# wrong one -- exactly the class-1 failure GH #602 is about.  The check keeps
# them loud; it does not paper over them.
KNOWN_UNHARMONIZED: frozenset[tuple[str, str]] = frozenset({
    ("Albania", "plot_features"),
    ("Cambodia", "plot_features"),
    ("Kosovo", "plot_features"),
    ("Tajikistan", "plot_features"),
    ("Timor-Leste", "plot_features"),
})


def _frame(col, values, feature_index=False):
    n = len(values)
    df = pd.DataFrame({col: values, "x": range(n)})
    df["i"] = [f"h{k}" for k in range(n)]
    df["t"] = "2020"
    if feature_index:
        return df.set_index(["i", "t", col])
    return df.set_index(["i", "t"])


# ---------------------------------------------------------------------------
# 1. The check itself
# ---------------------------------------------------------------------------

class TestCheckDeclaredSpellings:
    def test_rejects_unknown_column_value(self):
        """The exact Uganda shape: a literal '0' where 'Rural' was declared."""
        chk = _check_declared_spellings(
            _frame("Rural", ["0", "Urban", "0"]), "cluster_features")
        assert chk.status == "fail"
        assert "'0'" in chk.message and "(2)" in chk.message

    def test_rejects_unknown_index_level_value(self):
        """_enforce_canonical_spellings handles index levels; so must the check."""
        chk = _check_declared_spellings(
            _frame("Rural", ["0", "Urban"], feature_index=True), "cluster_features")
        assert chk.status == "fail"

    def test_passes_on_canonical_values(self):
        chk = _check_declared_spellings(
            _frame("Rural", ["Rural", "Urban"]), "cluster_features")
        assert chk.status == "pass"

    def test_passes_on_declared_variants(self):
        """Membership is tested AFTER the variant map, so a cached parquet
        holding 'rural'/'RURAL' (Malawi) must not be flagged."""
        chk = _check_declared_spellings(
            _frame("Rural", ["rural", "RURAL", "urban", "URBAN"]), "cluster_features")
        assert chk.status == "pass"

    def test_all_null_column_does_not_fire(self):
        chk = _check_declared_spellings(
            _frame("Rural", [None, None]), "cluster_features")
        assert chk.status == "pass"

    def test_housing_tenure_is_not_flagged(self):
        """FALSE-POSITIVE TRAP: `housing.Tenure` is a legitimately different
        vocabulary (dwelling tenure) and `housing` declares none.  A check keyed
        on column NAME rather than (table, column) fires on ~12 countries here
        and discredits itself."""
        chk = _check_declared_spellings(
            _frame("Tenure", ["Owned", "Rent-free", "Perching"]), "housing")
        assert chk.status == "pass"

    def test_empty_variant_vocabularies_are_still_checked(self):
        """Tenure/TenureSystem/Affinity declare canonical keys with EMPTY variant
        lists.  country._load_canonical_spellings() DROPS those tables (its
        `if variant_map:` guard), so the check must read `Columns` itself or they
        go unchecked entirely."""
        assert "Tenure" in _DECLARED_VOCABULARIES["plot_features"]
        assert "Affinity" in _DECLARED_VOCABULARIES["household_roster"]
        chk = _check_declared_spellings(
            _frame("Tenure", ["Owned", "owned"]), "plot_features")
        assert chk.status == "fail"

    def test_wired_into_is_this_feature_sane(self):
        """The check must actually be part of the battery -- Uganda's '0' graded
        sane.ok == True for three months precisely because nothing ran it."""
        df = _frame("Rural", ["0", "Urban"])
        report = is_this_feature_sane(df, "Uganda", "cluster_features")
        assert not report.ok
        assert any(c.name == "declared_spellings" for c in report.errors)


# ---------------------------------------------------------------------------
# 2. `sample` must be declared, or enforcement is a no-op on it
# ---------------------------------------------------------------------------

class TestSampleIsDeclared:
    def test_sample_rural_has_a_vocabulary(self):
        assert "sample" in _DECLARED_VOCABULARIES, (
            "`sample` absent from data_info.yml Columns -- "
            "_enforce_canonical_spellings is a total no-op on sample()")
        assert "Rural" in _DECLARED_VOCABULARIES["sample"]

    def test_sample_rural_variants_are_now_mapped(self):
        """This is the Tajikistan bug: 9,020 rows of lowercase 'rural'/'urban'
        sat next to canonical values because `sample` was undeclared, so not even
        the KNOWN variants were mapped."""
        df = pd.DataFrame({"Rural": ["rural", "urban", "Rural"]})
        out = _enforce_canonical_spellings(df, "sample")
        assert out["Rural"].tolist() == ["Rural", "Urban", "Rural"]

    def test_sample_rural_is_not_required(self):
        """Many countries' sample has no urban/rural column at all; declaring
        Rural `required: true` would fail every one of them."""
        from importlib.resources import files
        import yaml
        info = yaml.safe_load(
            (files("lsms_library") / "data_info.yml").read_text(encoding="utf-8"))
        assert not info["Columns"]["sample"]["Rural"].get("required", False)


# ---------------------------------------------------------------------------
# 3. Per-country regressions (each a distinct root cause)
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestCountryRegressions:
    def test_uganda_2005_06_rural_is_canonical(self):
        """Root cause: YAML key TYPE mismatch.  Raw `urban` is object-dtype but
        MIXED -- python int 0 (2263 rows) + str 'urban' (860).  The declared key
        was the STRING '0', which never matched, so the value passed through and
        was stringified to '0'.  (Commit 9959b9f3 introduced that string key to
        'fix' GH #163 and was silently dead for three months.)"""
        import lsms_library as ll
        s = ll.Country("Uganda").sample()
        r = s.xs("2005-06", level="t")["Rural"]
        assert "0" not in set(r.dropna().unique())
        assert (r == "Rural").sum() == 2263
        assert (r == "Urban").sum() == 860

    def test_malawi_cluster_features_rural_not_sign_flipped(self):
        """Root cause: `mapping:` stanzas converted good labels into 0/1 and got
        the DIRECTION wrong.  The Cross_Sectional and Panel files disagree on
        case ('rural' vs 'RURAL'), and 2016-17/2019-20 mapped BOTH into one dict,
        so rural households landed on both codes -- inverting the indicator
        between waves.  cluster_features.Rural must now agree with the (correct)
        sample.Rural label."""
        import lsms_library as ll
        cf = ll.Country("Malawi").cluster_features().reset_index()[["t", "v", "Rural"]]
        assert set(cf["Rural"].dropna()) <= {"Rural", "Urban"}
        sm = (ll.Country("Malawi").sample().reset_index()[["t", "v", "Rural"]]
              .rename(columns={"Rural": "sample_label"}))
        j = cf.rename(columns={"Rural": "cf_label"}).merge(sm, on=["t", "v"]).dropna()
        for t, g in j.groupby("t"):
            agree = (g["cf_label"] == g["sample_label"]).mean()
            assert agree > 0.80, f"Malawi {t}: cluster_features.Rural agrees with sample only {agree:.1%}"

    def test_india_sample_has_no_fabricated_rural(self):
        """Root cause: `Rural` was derived from `stratum` via a FLOAT-keyed map
        (1.0..4.0) against a STRING column, so it never fired and the raw stratum
        label ('UP-other', ' B-qual', ...) reached the user as `Rural` for 100% of
        households.  `stratum` is a state x phase stratum (perfectly collinear
        with `state`) and carries NO urban/rural information, so Rural is dropped
        rather than invented."""
        import lsms_library as ll
        s = ll.Country("India").sample()
        if "Rural" in s.columns:
            assert set(s["Rural"].dropna().unique()) <= {"Rural", "Urban", "Informal"}
        assert not set(s["strata"].dropna().unique()) & {"UP-other", " B-other"}

    def test_ghanalss_1991_92_rural_code_map_says_urban(self):
        """LATENT defect (not currently reaching sample() in a clean build -- the
        'Semi-urban' values I first saw came from a STALE shared-cache parquet).
        The `rural` code->label table in GhanaLSS/1991-92/_/categorical_mapping.org
        asserted 1 = 'Semi-urban'.  That is provably wrong: loc2=1 nests EXACTLY
        onto loc3 in {1,2} == loc5 in {1,2} == Accra (459) + Other Urban (1119) =
        1578, i.e. it is the union of the two URBAN strata.  mapping.Rural() reads
        that table, so the wrong label is one YAML edit away from going live."""
        import importlib.util
        import sys

        from lsms_library.paths import countries_root
        p = countries_root() / "GhanaLSS" / "1991-92" / "_" / "mapping.py"
        spec = importlib.util.spec_from_file_location("_ghanalss_1991_92_mapping", p)
        m = importlib.util.module_from_spec(spec)
        sys.modules["_ghanalss_1991_92_mapping"] = m
        spec.loader.exec_module(m)
        assert m.rural_dict[1] == "Urban", m.rural_dict
        assert m.Rural(1.0) == "Urban"
        assert m.Rural(2.0) == "Rural"

    def test_ghanalss_2016_17_cluster_rural_has_no_trailing_space(self):
        """Root cause: g7sec8h's loc2 carries a TRAILING SPACE ('Urban '), which
        looks right in a value_counts but compares false against 'Urban'."""
        import lsms_library as ll
        cf = ll.Country("GhanaLSS").cluster_features()
        vals = set(cf.xs("2016-17", level="t")["Rural"].dropna().unique())
        assert "Urban " not in vals
        assert vals <= {"Rural", "Urban"}

    @pytest.mark.parametrize("country", ["Mali", "Guinea-Bissau"])
    def test_ehcvm_cluster_rural_is_canonical(self, country):
        """French / Portuguese survey labels ('Urbain', 'Autre urbain',
        'Urbain (District de Bamako)', 'Urbano') reached the user verbatim, so
        `== 'Urban'` returned zero rows across every wave."""
        import lsms_library as ll
        cf = ll.Country(country).cluster_features()
        assert set(cf["Rural"].dropna().unique()) <= {"Rural", "Urban"}
        assert (cf["Rural"] == "Urban").sum() > 0
