"""GH #461: df_from_orgfile / get_categorical_mapping must not silently fall
back to the FIRST org table when a *named* table is absent.

Before the fix, requesting a table that did not exist returned the first table
in the file (e.g. an absent ``harmonize_seed_crop`` silently returned the ``u``
units map -> every crop labelled with a unit).  The fix raises a clear
``KeyError`` for a missing *named* table while preserving the documented
``name=None`` "first table in the file" behaviour.

These tests parse small temp org files only -- no microdata, CI-safe.
"""
from __future__ import annotations

import textwrap

import pytest

from lsms_library.local_tools import df_from_orgfile, get_categorical_mapping


# Two named tables, realistic spacing (blank lines between tables).
ORG = textwrap.dedent(
    """\
    #+title: fixture

    #+name: u
    | Original Label | Preferred Label |
    |----------------+-----------------|
    | kg             | Kg              |
    | litre          | Litre           |

    #+name: harmonize_food
    | Original Label | Preferred Label |
    |----------------+-----------------|
    | maize          | Maize           |
    | rice           | Rice            |
    """
)

# A bare pipe-table file with no #+name header (e.g. Ethiopia nonfood_items.org,
# Panama food_items.org) -- the only legitimate name=None use.
BARE_ORG = textwrap.dedent(
    """\
    | Original Label | Preferred Label |
    |----------------+-----------------|
    | maize          | Maize           |
    | rice           | Rice            |
    """
)


@pytest.fixture()
def orgfile(tmp_path):
    p = tmp_path / "categorical_mapping.org"
    p.write_text(ORG, encoding="utf-8")
    return p


@pytest.fixture()
def barefile(tmp_path):
    p = tmp_path / "food_items.org"
    p.write_text(BARE_ORG, encoding="utf-8")
    return p


def test_named_table_returns_that_table(orgfile):
    # Sanity: the SECOND table is selected by name, not the first.
    df = df_from_orgfile(orgfile, name="harmonize_food")
    assert list(df.columns) == ["Original Label", "Preferred Label"]
    assert set(df["Preferred Label"]) == {"Maize", "Rice"}


def test_missing_named_table_raises(orgfile):
    # The regression: an absent named table must NOT silently return the
    # first table (``u``); it must raise a clear KeyError naming the table.
    with pytest.raises(KeyError, match="harmonize_seed_crop"):
        df_from_orgfile(orgfile, name="harmonize_seed_crop")


def test_name_none_still_reads_first_table(barefile):
    # Documented default preserved: name=None -> first table in a bare file.
    df = df_from_orgfile(barefile, name=None)
    assert list(df.columns) == ["Original Label", "Preferred Label"]
    assert set(df["Preferred Label"]) == {"Maize", "Rice"}


def test_get_categorical_mapping_missing_table_raises(orgfile, monkeypatch):
    # End-to-end through get_categorical_mapping: a missing tablename must
    # surface a clear error, not the first table's contents.
    monkeypatch.chdir(orgfile.parent)
    with pytest.raises((KeyError, FileNotFoundError)):
        get_categorical_mapping(
            "categorical_mapping.org",
            tablename="harmonize_seed_crop",
            idxvars="Original Label",
            dirs=["./"],
            **{"Preferred Label": "Preferred Label"},
        )


def test_get_categorical_mapping_present_table_ok(orgfile, monkeypatch):
    # And a present table still resolves to its own contents (mirrors the
    # real crop_production.py call shape).
    monkeypatch.chdir(orgfile.parent)
    d = get_categorical_mapping(
        "categorical_mapping.org",
        tablename="harmonize_food",
        idxvars="Original Label",
        dirs=["./"],
        **{"Preferred Label": "Preferred Label"},
    )
    assert d == {"maize": "Maize", "rice": "Rice"}
