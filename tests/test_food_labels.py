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
