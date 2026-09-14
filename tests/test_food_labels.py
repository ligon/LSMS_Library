"""Tests for the ``labels=`` kwarg on derived food tables.

The first block unit-tests ``Country._relabel_j`` with a synthetic DataFrame
and an inline fake ``categorical_mapping``; it runs without any data cache.

The second block exercises ``Country('Uganda').food_expenditures(labels=...)``
end-to-end and is skipped when the Uganda food cache is cold.
"""
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from lsms_library import Country
from lsms_library.country import Country as _CountryCls
from lsms_library.paths import data_root


# ---------------------------------------------------------------------------
# Cache-independent unit tests for _relabel_j
# ---------------------------------------------------------------------------


def _fake_country(cat_maps):
    """Minimal stand-in exposing the attributes ``_relabel_j`` reads."""
    fake = SimpleNamespace(
        name="TestLand",
        categorical_mapping=cat_maps,
    )
    # Bind the real method; it only touches self.name / self.categorical_mapping.
    fake._relabel_j = _CountryCls._relabel_j.__get__(fake)
    return fake


def _sample_expenditure_df():
    idx = pd.MultiIndex.from_tuples(
        [
            ("T1", "V1", "H1", "Beans (fresh)"),
            ("T1", "V1", "H1", "Beans (dry)"),
            ("T1", "V1", "H1", "Matoke (bunch)"),
            ("T1", "V1", "H2", "Matoke (cluster)"),
        ],
        names=["t", "v", "i", "j"],
    )
    return pd.DataFrame({"Expenditure": [10.0, 5.0, 2.0, 3.0]}, index=idx)


def _food_items_table():
    return pd.DataFrame(
        {
            "Code": [1, 2, 3, 4],
            "Preferred Label": [
                "Beans (fresh)",
                "Beans (dry)",
                "Matoke (bunch)",
                "Matoke (cluster)",
            ],
            "Aggregate Label": ["Beans", "Beans", "Matoke", "Matoke"],
        }
    )


def test_relabel_j_preferred_is_noop():
    fake = _fake_country({"food_items": _food_items_table()})
    df = _sample_expenditure_df()
    out = fake._relabel_j(df, "Preferred", reaggregate=True)
    assert out is df  # short-circuits before any work


def test_relabel_j_none_is_noop():
    fake = _fake_country({"food_items": _food_items_table()})
    df = _sample_expenditure_df()
    out = fake._relabel_j(df, None, reaggregate=True)
    assert out is df


def test_relabel_j_aggregate_collapses_and_sums():
    fake = _fake_country({"food_items": _food_items_table()})
    df = _sample_expenditure_df()
    out = fake._relabel_j(df, "Aggregate", reaggregate=True)

    j = set(out.index.get_level_values("j"))
    assert j == {"Beans", "Matoke"}
    # Beans (fresh) 10 + Beans (dry) 5 = 15 for (T1, V1, H1, Beans)
    assert out.loc[("T1", "V1", "H1", "Beans"), "Expenditure"] == 15.0
    # Totals conserved
    assert out["Expenditure"].sum() == pytest.approx(df["Expenditure"].sum())


def test_relabel_j_no_reaggregate_keeps_duplicates():
    fake = _fake_country({"food_items": _food_items_table()})
    df = _sample_expenditure_df()
    out = fake._relabel_j(df, "Aggregate", reaggregate=False)
    # Two "Beans" rows remain (not summed) — price-table semantics
    assert out.index.duplicated().any()


def test_relabel_j_preserves_rows_with_na_index():
    """Regression: groupby().sum() must not drop groups with NA index keys.

    Household-level tables routinely have NA in the ``v`` level
    (households not matched by _join_v_from_sample). Without
    ``dropna=False`` on the re-aggregation groupby, those rows were
    silently dropped — ~1.7% of Uganda expenditure (fixed in commit
    12459a22).
    """
    idx = pd.MultiIndex.from_tuples(
        [
            ("T1", "V1", "H1", "Beans (fresh)"),
            ("T1", "V1", "H1", "Beans (dry)"),
            ("T1", pd.NA, "H2", "Matoke (bunch)"),   # v is NA
            ("T1", pd.NA, "H2", "Matoke (cluster)"), # v is NA
        ],
        names=["t", "v", "i", "j"],
    )
    df = pd.DataFrame({"Expenditure": [10.0, 5.0, 7.0, 3.0]}, index=idx)
    fake = _fake_country({"food_items": _food_items_table()})
    out = fake._relabel_j(df, "Aggregate", reaggregate=True)

    # All four rows' expenditure must survive the re-aggregation
    assert out["Expenditure"].sum() == pytest.approx(df["Expenditure"].sum())
    # The NA-v group is preserved, collapsed to 'Matoke'
    j_with_na_v = {
        j for (_, v, _, j) in out.index if pd.isna(v)
    }
    assert "Matoke" in j_with_na_v


def test_relabel_j_missing_column_raises():
    fake = _fake_country({"food_items": _food_items_table()})
    df = _sample_expenditure_df()
    with pytest.raises(KeyError, match="French"):
        fake._relabel_j(df, "French", reaggregate=True)


def test_relabel_j_missing_table_raises():
    fake = _fake_country({})
    df = _sample_expenditure_df()
    with pytest.raises(KeyError, match="food label table"):
        fake._relabel_j(df, "Aggregate", reaggregate=True)


def test_relabel_j_uses_harmonize_food_fallback():
    table = _food_items_table().rename(columns={"Aggregate Label": "Aggregate"})
    fake = _fake_country({"harmonize_food": table})
    df = _sample_expenditure_df()
    # Column is named 'Aggregate' (no ' Label' suffix); the helper should find it.
    out = fake._relabel_j(df, "Aggregate", reaggregate=True)
    assert set(out.index.get_level_values("j")) == {"Beans", "Matoke"}


# ---------------------------------------------------------------------------
# An UNLABELLED food keeps its Preferred Label -- blank is absent, on both
# sides, whether it parses as NaN or as an empty string.
#
# `all_dfs_from_orgfile` returns EMPTY STRINGS for blank org cells, so the
# historical `.dropna()` guarded only the tables whose blanks happened to parse
# as NaN.  Where they parsed as '' every unlabelled food was renamed to '' and,
# under reaggregate=True, summed into ONE unnamed bucket: GhanaLSS
# labels='1987-88' merged 146 distinct foods over 55,314 rows.  17 (country,
# column) pairs corpus-wide.
# ---------------------------------------------------------------------------


def _partially_labelled_table(blank):
    """Food table whose ``Wave1`` column labels only two of four foods."""
    return pd.DataFrame(
        {
            "Preferred Label": ["Beans (fresh)", "Beans (dry)",
                                "Matoke (bunch)", "Matoke (cluster)"],
            "Wave1": ["Haricot", blank, "Igname", blank],
        }
    )


@pytest.mark.parametrize("blank", ["", "   ", None])
def test_relabel_j_unlabelled_food_keeps_its_preferred_label(blank):
    """Blank target cell -> not renamed.  Never renamed to nothing."""
    fake = _fake_country({"food_items": _partially_labelled_table(blank)})
    df = _sample_expenditure_df()
    out = fake._relabel_j(df, "Wave1", reaggregate=True)

    j = set(map(str, out.index.get_level_values("j")))
    assert "" not in j, f"a food was renamed to the empty string: {j}"
    # The two labelled foods are renamed; the two unlabelled keep their own.
    assert j == {"Haricot", "Igname", "Beans (dry)", "Matoke (cluster)"}
    assert out["Expenditure"].sum() == pytest.approx(df["Expenditure"].sum())


@pytest.mark.parametrize("blank", ["", "   ", None])
def test_relabel_j_unlabelled_foods_are_not_merged_into_one_bucket(blank):
    """The reaggregate=True consequence: distinct foods must stay distinct.

    Expenditure is conserved either way -- that is exactly why the defect was
    silent -- so conservation alone does NOT witness it.  Row identity does.
    """
    fake = _fake_country({"food_items": _partially_labelled_table(blank)})
    df = _sample_expenditure_df()
    out = fake._relabel_j(df, "Wave1", reaggregate=True)

    # 'Beans (dry)' (5.0) and 'Matoke (cluster)' (3.0) are both unlabelled and
    # sit in DIFFERENT households, so a merge would be invisible in the total.
    assert len(out) == len(df)
    assert out.loc[("T1", "V1", "H1", "Beans (dry)"), "Expenditure"] == 5.0
    assert out.loc[("T1", "V1", "H2", "Matoke (cluster)"), "Expenditure"] == 3.0


def test_relabel_j_blank_preferred_label_is_not_a_rename_key():
    """A blank key is no more a food to rename FROM than a blank value is one
    to rename TO.  Two shipped countries carry blank Preferred Labels
    (GhanaSPS 4, Mali 2)."""
    table = pd.DataFrame(
        {
            "Preferred Label": ["Beans (fresh)", ""],
            "Wave1": ["Haricot", "Phantom"],
        }
    )
    fake = _fake_country({"food_items": table})
    idx = pd.MultiIndex.from_tuples(
        [("T1", "V1", "H1", "Beans (fresh)"), ("T1", "V1", "H1", "")],
        names=["t", "v", "i", "j"],
    )
    df = pd.DataFrame({"Expenditure": [10.0, 5.0]}, index=idx)
    out = fake._relabel_j(df, "Wave1", reaggregate=True)

    j = set(map(str, out.index.get_level_values("j")))
    assert "Phantom" not in j, "a blank j was given a label it was never assigned"
    assert j == {"Haricot", ""}


# ---------------------------------------------------------------------------
# Uganda end-to-end tests (require food_acquired cache)
# ---------------------------------------------------------------------------


def _uganda_food_cache_exists() -> bool:
    root = data_root("Uganda")
    return (root / "var" / "food_acquired.parquet").exists()


_SKIP_NO_CACHE = pytest.mark.skipif(
    not _uganda_food_cache_exists(),
    reason="Uganda food_acquired parquet not cached; requires data build",
)


@pytest.fixture(scope="module")
def uga():
    return Country("Uganda")


@_SKIP_NO_CACHE
def test_default_labels_matches_preferred(uga):
    """``labels='Preferred'`` is the explicit form of the default."""
    pref = uga.food_expenditures()
    explicit = uga.food_expenditures(labels="Preferred")
    pd.testing.assert_frame_equal(
        pref.sort_index(), explicit.sort_index(), check_like=True
    )


@_SKIP_NO_CACHE
def test_aggregate_collapses_variants(uga):
    """``labels='Aggregate'`` replaces Preferred labels with coarser Aggregate ones."""
    pref = uga.food_expenditures()
    agg = uga.food_expenditures(labels="Aggregate")

    j_pref = set(pref.index.get_level_values("j"))
    j_agg = set(agg.index.get_level_values("j"))

    # Variants like "Matoke (bunch)" collapse to "Matoke"
    assert any(lbl.startswith("Matoke (") for lbl in j_pref), (
        f"Expected 'Matoke (...)' variants in Preferred labels; got sample "
        f"{sorted(list(j_pref))[:10]}"
    )
    assert "Matoke" in j_agg
    assert not any(lbl.startswith("Matoke (") for lbl in j_agg), (
        f"Aggregate labels should not contain 'Matoke (...)' variants; got "
        f"{[l for l in j_agg if l.startswith('Matoke')]}"
    )

    # Aggregate index must have fewer or equal distinct items
    assert len(j_agg) <= len(j_pref)


@_SKIP_NO_CACHE
def test_aggregate_preserves_total_expenditure(uga):
    """Summing collapses groups; the grand total must be preserved."""
    pref = uga.food_expenditures()
    agg = uga.food_expenditures(labels="Aggregate")
    # NaN-tolerant comparison on the single Expenditure column
    pref_total = pref["Expenditure"].sum(skipna=True)
    agg_total = agg["Expenditure"].sum(skipna=True)
    assert agg_total == pytest.approx(pref_total, rel=1e-9)


@_SKIP_NO_CACHE
def test_unknown_labels_raise_keyerror(uga):
    """A non-existent label column raises ``KeyError`` listing what's available."""
    with pytest.raises(KeyError, match=r"(?i)food label table|French|not in"):
        uga.food_expenditures(labels="French")


# ---------------------------------------------------------------------------
# GH #783 -- the food vocabulary must live where the API can SEE it.
#
# Config-only (no microdata, no cache): these read `_/categorical_mapping.org`
# and nothing else.  They exist because the failure mode they pin was silent
# for a long time -- a country curated hundreds of labels in a standalone
# `_/food_items.org`, `Country.categorical_mapping` never opened that file
# under ANY table name, and every `labels=` call raised LabelUnavailableError
# while the vocabulary sat on disk two directories away.
# ---------------------------------------------------------------------------

#: Countries migrated by GH #783, with the column set each one must keep.
#: The columns are load-bearing, not decoration: a `Preferred Label`-only
#: table would move a country from "no table" to "table with nothing to
#: select", which is the consolidation closing nothing.
_GH783_MIGRATED = {
    "GhanaLSS": {"Preferred Label", "1987-88", "1988-89", "1991-92",
                 "1998-99", "2005-06", "2012-13", "2016-17", "FCT Code"},
    "GhanaSPS": {"2009-10", "2013-14", "2017-18", "Preferred Label",
                 "Food Codes", "FCT Label"},
    "Cambodia": {"Preferred Label", "2019-20", "Code", "FCT ID"},
    "Serbia": {"Preferred Label", "proizvod", "Serbian Label", "FCT ID"},
    "Guatemala": {"Preferred Label", "2000", "FCT code"},
    "Panama": {"Preferred Label", "2008", "2003", "1997", "FCT ID"},
}


@pytest.mark.parametrize("country,expected_cols", sorted(_GH783_MIGRATED.items()))
def test_gh783_food_vocabulary_is_reachable(country, expected_cols):
    """`_relabel_j` must find a food-label table, with all its columns intact."""
    cat = Country(country).categorical_mapping or {}
    table = cat.get("food_items") or cat.get("harmonize_food")
    assert table is not None, (
        f"{country} has no 'food_items'/'harmonize_food' in "
        f"Country.categorical_mapping, so every labels= call raises "
        f"LabelUnavailableError. Did the vocabulary get moved back out of "
        f"_/categorical_mapping.org? (GH #783)"
    )
    assert "Preferred Label" in table.columns
    missing = expected_cols - set(table.columns)
    assert not missing, (
        f"{country}/harmonize_food lost column(s) {sorted(missing)}; the whole "
        f"point of GH #783 was to carry every column across. Got "
        f"{list(table.columns)}"
    )


@pytest.mark.parametrize("country", sorted(_GH783_MIGRATED))
def test_gh783_standalone_food_items_org_stays_retired(country):
    """A re-introduced `_/food_items.org` would be invisible to the API again."""
    from lsms_library.paths import countries_root
    stranded = countries_root() / country / "_" / "food_items.org"
    assert not stranded.exists(), (
        f"{stranded} is back. Country.categorical_mapping never opens this "
        f"file under any table name, so anything curated here cannot reach "
        f"labels=. Put it in _/categorical_mapping.org as `harmonize_food` "
        f"(GH #783; cf. Tanzania and Ethiopia 'Unit #0')."
    )
