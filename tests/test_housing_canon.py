"""GH #727 / #170: every country's housing ``Roof`` / ``Floor`` table targets
the canonical vocabulary in
``lsms_library/categorical_mapping/canonical_housing_labels.org``.

Data-free: org parsing only (no microdata, no cache), so CI-safe and fast.

Why a *subset* assertion and not a distribution check: ``Country._apply_
categorical_mappings`` applies ``Series.replace()``, which passes an
unmatched or mistyped label through UNCHANGED -- it neither nulls nor
raises.  A country table whose Preferred Label is not in the canon
therefore ships a non-canonical value silently.  Nothing else pins the
canon (there was no test reading ``canonical_housing_labels.org`` before
this file).

The canon's two label tables are deliberately NOT ``#+name:``-tagged: the
org parser is not block-aware (GH #693), so a named table anywhere in a
global ``.org`` becomes a LIVE mapping for every country.  They are
parsed here by section heading instead, and ``test_canon_label_tables_
are_not_live`` guards that they stay documentation.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root

# Column name (as dispatched by _apply_categorical_mappings, which matches
# table names case-insensitively) -> the canon section that governs it.
_HEADINGS = {
    "Roof": "* Canonical Roof Preferred Labels",
    "Floor": "* Canonical Floor Preferred Labels",
}

# GH #727: a raw label that is EXACTLY one of these (case-insensitive) must
# target the new canonical label.  Compound labels (``Reed/Bamboo``,
# ``Grass/Leaves/Bamboo``, ``Bamboo strips``) are deliberately NOT here.
_EXACT_REPOINTS = {
    "bamboo": "Bamboo",
    "terrazzo": "Terrazzo",
    "terrazo": "Terrazzo",   # GhanaSPS 2009-10 / GhanaLSS 2005-06 source spelling
    "terazo": "Terrazzo",    # Nigeria 2023-24 source spelling
}

# Countries whose Roof/Floor tables violate the canon on the tree this test
# was written against.  Empty: none found on origin/development (9c075214)
# or on the #727 branch.  Populate ONLY with a reason and an issue number.
_KNOWN_VIOLATORS: dict[str, str] = {}


def _canon_path() -> Path:
    # Resolved via countries_root() so LSMS_COUNTRIES_ROOT redirects the
    # canon together with the country tables (same pattern as
    # tests/test_global_u_org.py).
    return countries_root().parent / "categorical_mapping" / "canonical_housing_labels.org"


def parse_canon(path: Path) -> dict[str, set[str]]:
    """First column of each heading-scoped canonical label table."""
    by_heading = {v: k for k, v in _HEADINGS.items()}
    out: dict[str, set[str]] = {k: set() for k in _HEADINGS}
    section = None
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s.startswith("* "):
            section = by_heading.get(s)
            continue
        if section and s.startswith("|") and not set(s) <= set("|-+ "):
            first = s.split("|")[1].strip()
            if first != "Preferred Label":
                out[section].add(first)
    return out


def _housing_tables() -> dict[str, dict[str, pd.DataFrame]]:
    """{country: {'Roof': table, 'Floor': table}} over every country org."""
    found: dict[str, dict[str, pd.DataFrame]] = {}
    for org in sorted(countries_root().glob("*/_/categorical_mapping.org")):
        country = org.parent.parent.name
        for name, table in all_dfs_from_orgfile(org).items():
            col = {k.lower(): k for k in _HEADINGS}.get(name.lower())
            if col is not None:
                found.setdefault(country, {})[col] = table
    return found


_CANON = parse_canon(_canon_path())
_TABLES = _housing_tables()


def _preferred(table: pd.DataFrame) -> pd.Series:
    return table["Preferred Label"].dropna().astype(str).str.strip()


def _key_column(table: pd.DataFrame) -> str:
    # Same rule as country._categorical_key_column: first non-Preferred column.
    return [c for c in table.columns if c != "Preferred Label"][0]


def test_canon_parses_and_carries_727_labels():
    """The canon has both lists, and the #727 additions are in them."""
    assert _CANON["Roof"] and _CANON["Floor"], _CANON
    assert "Bamboo" in _CANON["Roof"]
    assert {"Bamboo", "Terrazzo"} <= _CANON["Floor"]
    # Ruled unchanged 2026-09-02: no combined label, Cement and Concrete stay distinct.
    assert "Cement/Concrete" not in _CANON["Roof"] | _CANON["Floor"]
    assert {"Cement", "Concrete"} <= _CANON["Floor"]


def test_canon_label_tables_are_not_live():
    """GH #693: a ``#+name:`` on either canonical label table would make it a
    live global Roof/Floor mapping (keyed on the canonical label, valued with
    its prose note).  Only the documented ``#+begin_example`` placeholders
    may parse out of this file."""
    live = all_dfs_from_orgfile(_canon_path())
    for name, table in live.items():
        keys = set(table[_key_column(table)].dropna().astype(str))
        assert not (keys & (_CANON["Roof"] | _CANON["Floor"])), (
            f"canon table {name!r} is live and keyed on canonical labels: "
            f"{sorted(keys)[:5]}")


def test_at_least_the_known_countries_declare_tables():
    for c in ("GhanaSPS", "GhanaLSS", "Timor-Leste", "Nigeria", "Uganda", "Malawi"):
        assert set(_TABLES.get(c, {})) == {"Roof", "Floor"}, (c, _TABLES.get(c))


@pytest.mark.parametrize("country", sorted(_TABLES))
def test_country_preferred_labels_in_canon(country):
    """Every Preferred Label in a country's Roof / Floor table is canonical."""
    if country in _KNOWN_VIOLATORS:
        pytest.skip(f"{country}: known violator -- {_KNOWN_VIOLATORS[country]}")
    for col, table in _TABLES[country].items():
        extra = set(_preferred(table)) - _CANON[col]
        assert not extra, (
            f"{country} {col}: Preferred Labels not in the canon: {sorted(extra)}")


@pytest.mark.parametrize("country", sorted(_TABLES))
def test_exact_bamboo_terrazzo_rows_repointed(country):
    """GH #727: an EXACT ``Bamboo`` / ``Terrazzo`` (or misspelt) raw label
    targets the new canonical label; compounds are untouched by this test."""
    for col, table in _TABLES[country].items():
        key = _key_column(table)
        for raw, pref in zip(table[key].astype(str), table["Preferred Label"].astype(str)):
            want = _EXACT_REPOINTS.get(raw.strip().lower())
            if want is not None:
                assert pref.strip() == want, (
                    f"{country} {col}: exact row {raw!r} -> {pref!r}, expected {want!r}")
