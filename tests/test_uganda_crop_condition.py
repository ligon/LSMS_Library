"""Regression tests for the ``condition`` index level on Uganda ``crop_production``.

GH #323 / #637.  The UNPS post-harvest module asks quantity, unit AND
condition/state as one compound question, so the same plot-crop-season is
reported once per condition.  ``crop_production`` was keyed
``(t, i, plot, j, u, season)`` with no level for the condition, so those
records collided and the de-duplication block at the end of
``uganda.crop_production_for_wave`` SUMMED them — adding dry weight to
fresh weight.

These tests are written to FAIL on the pre-fix tree:

* ``test_condition_is_an_index_level``          — the level did not exist.
* ``test_declared_index_carries_condition``     — ``data_scheme.yml`` did not
  declare it, and ``_normalize_dataframe_index`` drops undeclared levels.
* ``test_fresh_and_dry_no_longer_collide``      — the 2011-12 coffee record was
  one 340 kg row instead of 240 kg dry + 100 kg fresh.
* ``test_harvest_conditions_table_exists``      — the mapping table did not exist.

Schema rules are READ from ``lsms_library/data_info.yml`` and from Uganda's
``_/data_scheme.yml`` / ``_/categorical_mapping.org``, never hardcoded here
(CLAUDE.md, "Canonical Schema").
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest
import yaml
from importlib.resources import files

from lsms_library.paths import countries_root

TABLE = "crop_production"
LEVEL = "condition"


# ---------------------------------------------------------------------------
# config fixtures (cheap — no data build)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def canonical_vocabulary() -> set[str]:
    """Canonical `condition` values, from data_info.yml Columns.

    `spellings` is an inverse dict; "the canonical values are simply
    spellings.keys()" (data_info.yml, Columns preamble).
    """
    with open(files("lsms_library") / "data_info.yml", encoding="utf-8") as f:
        info = yaml.safe_load(f)
    col = info.get("Columns", {}).get(TABLE, {}).get(LEVEL)
    assert isinstance(col, dict), (
        f"data_info.yml declares no Columns.{TABLE}.{LEVEL} entry; the canonical "
        f"{LEVEL} vocabulary has no home"
    )
    spellings = col.get("spellings") or {}
    assert spellings, f"Columns.{TABLE}.{LEVEL} declares an empty spellings dict"
    return set(spellings)


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the !make tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


@pytest.fixture(scope="module")
def uganda_scheme() -> dict:
    path = countries_root() / "Uganda" / "_" / "data_scheme.yml"
    with open(path, encoding="utf-8") as f:
        return yaml.load(f, Loader=_SchemeLoader)["Data Scheme"][TABLE]


@pytest.fixture(scope="module")
def harvest_conditions_table() -> pd.DataFrame:
    """The `harvest_conditions` Code -> Preferred Label table."""
    from lsms_library.local_tools import df_from_orgfile

    path = countries_root() / "Uganda" / "_" / "categorical_mapping.org"
    return df_from_orgfile(str(path), name="harvest_conditions")


# ---------------------------------------------------------------------------
# built-table fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def crop_production() -> pd.DataFrame:
    try:
        import lsms_library as ll

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country("Uganda", preload_panel_ids=False,
                            verbose=False).crop_production()
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Uganda crop_production unavailable: {exc!r}")
    if df is None or df.empty:
        pytest.skip("Uganda crop_production built empty")
    return df


# ---------------------------------------------------------------------------
# config-level tests (run without any data)
# ---------------------------------------------------------------------------

def test_declared_index_carries_condition(uganda_scheme):
    """`_normalize_dataframe_index` DROPS index levels the scheme does not
    declare, so an undeclared `condition` would be a silent no-op that still
    sums fresh onto dry."""
    declared = uganda_scheme["index"]
    assert LEVEL in [tok.strip() for tok in declared.strip("() ").split(",")], (
        f"Uganda data_scheme.yml declares {TABLE} index {declared!r} without "
        f"{LEVEL!r}; country.py::_normalize_dataframe_index would drop the level"
    )


def test_harvest_conditions_table_exists(harvest_conditions_table):
    t = harvest_conditions_table
    assert "Preferred Label" in t.columns, t.columns.tolist()
    assert len(t) >= 20, f"harvest_conditions has only {len(t)} rows"
    assert t["Preferred Label"].is_unique, "duplicate Preferred Labels"


def test_harvest_conditions_labels_are_canonical(harvest_conditions_table,
                                                 canonical_vocabulary):
    """Every label Uganda emits must be a declared canonical value, so the
    cross-country vocabulary in data_info.yml is not silently bypassed."""
    labels = {str(v).strip() for v in harvest_conditions_table["Preferred Label"]
              if pd.notna(v) and str(v).strip() not in ("", "---")}
    unknown = labels - canonical_vocabulary
    assert not unknown, (
        f"harvest_conditions emits labels absent from data_info.yml "
        f"Columns.{TABLE}.{LEVEL}.spellings: {sorted(unknown)}"
    )


def test_every_wave_sources_a_condition_column():
    """Each wave/season condition slot must name a condition column; a `None`
    would silently sentinel the whole wave."""
    import sys

    sys.path.insert(0, str(countries_root() / "Uganda" / "_"))
    try:
        from uganda import CROP_COLMAPS
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"cannot import uganda module: {exc!r}")

    missing = []
    for wave, seasons in CROP_COLMAPS.items():
        for season in ("A", "B"):
            cm = seasons.get(season)
            if not cm:
                continue
            for n, cond in enumerate(cm["conditions"]):
                if not cond.get("condition"):
                    missing.append(f"{wave}/{season}[{n}]")
    assert not missing, f"condition column unwired for: {missing}"


# ---------------------------------------------------------------------------
# built-table tests
# ---------------------------------------------------------------------------

def test_condition_is_an_index_level(crop_production):
    assert LEVEL in list(crop_production.index.names), (
        f"crop_production index is {list(crop_production.index.names)}; harvest "
        f"records differing only by condition collide and are summed (GH #323)"
    )


def test_condition_level_has_no_nulls(crop_production):
    """A null index key is SILENTLY DROPPED by the duplicate collapse's
    `groupby(level=...)` (pandas default dropna=True), so the level must carry
    a sentinel string instead."""
    values = crop_production.index.get_level_values(LEVEL)
    assert int(pd.isna(values).sum()) == 0


def test_condition_values_are_canonical(crop_production, canonical_vocabulary):
    observed = set(crop_production.index.get_level_values(LEVEL).unique())
    unknown = {v for v in observed if pd.notna(v)} - canonical_vocabulary
    assert not unknown, (
        f"crop_production emits {LEVEL} values not declared in data_info.yml: "
        f"{sorted(unknown)}"
    )


def test_index_is_unique(crop_production):
    assert crop_production.index.is_unique


def test_fresh_and_dry_no_longer_collide(crop_production):
    """The measured GH #323 example: one household's coffee off one plot,
    reported twice in the same month and unit — 240 kg dry and 100 kg fresh.
    Pre-fix these summed to a single 340 kg row, which is not a quantity of
    anything."""
    f = crop_production.reset_index()
    sel = f[(f["t"] == "2011-12") & (f["i"] == "1033000506")
            & (f["plot"] == "1033000506-1-6") & (f["j"] == "Coffee")]
    if sel.empty:  # pragma: no cover - data subset without this household
        pytest.skip("2011-12 HH 1033000506 not present in this build")

    qty = sorted(float(x) for x in sel["Quantity"])
    assert qty == [100.0, 240.0], (
        f"expected the dry (240) and fresh (100) coffee records to stay "
        f"separate, got quantities {qty}"
    )
    assert sel[LEVEL].nunique() == 2, sel[LEVEL].tolist()
    assert {"dried_in_pods", "fresh_in_pods"} <= set(sel[LEVEL])


def test_condition_actually_separates_records(crop_production):
    """Sanity: the level does real work — thousands of plot-crop-unit-season
    groups carry more than one condition.  A level that never varies would
    pass every test above while fixing nothing."""
    f = crop_production.reset_index()
    keys = [k for k in ["t", "i", "plot", "j", "u", "season"] if k in f.columns]
    n_split = int((f.groupby(keys, dropna=False)[LEVEL].nunique() > 1).sum())
    assert n_split > 1000, (
        f"only {n_split} plot-crop-unit-season groups carry >1 condition; "
        f"expected ~3200 (2026-07-21 measurement)"
    )
