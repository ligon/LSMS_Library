"""Pin ``currency.py``'s livestock registry against the canonical YAML.

WHY THIS FILE EXISTS
--------------------
``currency._DEFAULT_MONETARY`` is a hand-maintained *registry of which columns
hold money*.  It is a second copy of a fact whose first copy is the
``monetary:`` flag in ``lsms_library/data_info.yml``'s ``Columns`` section --
and a second copy of a fact is a fact that can go stale.

Renaming a monetary column and forgetting the registry is silent in the worst
way this codebase knows: right shape, absent content.  Two mechanisms fail
differently, and BOTH are load-bearing:

1.  ``attach_currency`` gates on ``_monetary_columns(table, country)`` being
    *non-empty* -- a TABLE-level test.  A stale registry entry (a column name
    nothing declares any more) keeps that set non-empty, so the currency label
    still attaches and the bug shows no symptom at all.
2.  ``conversion.convert`` scales exactly ``_all_monetary_columns()`` -- a
    COLUMN-level test.  A renamed column missing from the registry is
    therefore **silently never converted**: the FX/PPP layer walks straight
    past it and returns nominal local-currency numbers labelled as converted.

Mechanism 1 is why the failure is invisible; mechanism 2 is why it matters.
Neither is caught by the coverage matrix -- ``sane`` is a cold build and (per
`docs/guide/coverage.md`) the grader does not check currency labels at all.

WHAT THE PIN ASSERTS
--------------------
Set EQUALITY between the two declarations, so it fails if EITHER side changes
alone -- adding a `monetary:` column to the YAML without the registry, or vice
versa.  Plus a reachability check that every value column any country actually
declares is covered, which is the direction a rename breaks.
"""
from importlib.resources import files

import pytest
import yaml

from lsms_library.catalog import _country_dirs
from lsms_library.currency import (
    _DEFAULT_MONETARY,
    _all_monetary_columns,
    _load_country_scheme,
    _monetary_columns,
)

TABLE = "livestock"

# Structural keys of a `data_scheme.yml` table entry that are not columns.
_NON_COLUMN_KEYS = {"index", "materialize", "backend", "aggregation", "optional"}


def _canonical_columns() -> dict:
    """The ``Columns: livestock:`` block of the canonical data_info.yml."""
    path = files("lsms_library") / "data_info.yml"
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    cols = (data.get("Columns") or {}).get(TABLE)
    assert isinstance(cols, dict), (
        "data_info.yml has no `Columns: livestock:` block.  It is the "
        "canonical declaration of the livestock schema; do not remove it."
    )
    return cols


def _yaml_monetary() -> set:
    return {c for c, meta in _canonical_columns().items()
            if isinstance(meta, dict) and meta.get("monetary") is True}


# ---------------------------------------------------------------------------
# The pin itself
# ---------------------------------------------------------------------------

def test_registry_matches_canonical_yaml():
    """``_DEFAULT_MONETARY['livestock']`` == the YAML's ``monetary: true`` set.

    EQUALITY, deliberately -- not a subset either way.  A subset test would
    pass while one side silently grew or shrank, which is exactly the drift
    this file exists to stop.
    """
    seed = set(_DEFAULT_MONETARY[TABLE])
    declared = _yaml_monetary()
    assert seed == declared, (
        "currency._DEFAULT_MONETARY['livestock'] has drifted from the "
        "`monetary: true` columns in data_info.yml's `Columns: livestock:`.\n"
        f"  only in currency.py : {sorted(seed - declared)}\n"
        f"  only in data_info.yml: {sorted(declared - seed)}\n"
        "Both must be updated together.  A column missing from the registry "
        "is silently skipped by conversion.convert()."
    )


def test_canonical_value_columns_are_the_expected_three():
    """The additivity split is a definition; pin the vocabulary itself.

    ValuePerAnimal (a unit price, NOT additive), HerdValue (a stock total,
    additive) and SalesValue (a transaction-flow total, additive) are three
    genuinely different quantities that were all once spelled `Value`.
    Adding a fourth is a definitional act and should be deliberate.
    """
    assert _yaml_monetary() == {"ValuePerAnimal", "HerdValue", "SalesValue"}


def test_every_declared_livestock_value_column_is_monetary():
    """No country may declare a livestock value column the registry misses.

    This is the direction a rename breaks: the country YAML moves, the
    registry does not, and `conversion.convert` stops scaling the column
    without raising anything.
    """
    known = _all_monetary_columns()
    non_monetary = {c for c, meta in _canonical_columns().items()
                    if not (isinstance(meta, dict) and meta.get("monetary"))}

    offenders = {}
    for country in _country_dirs():
        entry = _load_country_scheme(country).get(TABLE)
        if not isinstance(entry, dict):
            continue
        for col in entry:
            if not isinstance(col, str) or col in _NON_COLUMN_KEYS:
                continue
            if col in non_monetary or col in known:
                continue
            # A column name that reads like money but is in neither list.
            if "value" in col.lower() or "price" in col.lower():
                offenders.setdefault(country, []).append(col)

    assert not offenders, (
        "livestock value column(s) declared by a country but absent from the "
        f"monetary registry: {offenders}.  Add them to "
        "currency._DEFAULT_MONETARY['livestock'] AND to data_info.yml's "
        "`Columns: livestock:` with `monetary: true`."
    )


def test_bare_Value_is_no_longer_a_livestock_column():
    """`Value` was ambiguous across three meanings; it must not come back.

    It stays legitimate in `assets`, which is why this check is scoped to
    livestock rather than being a global Rejected Spelling.
    """
    offenders = [c for c in _country_dirs()
                 if isinstance(_load_country_scheme(c).get(TABLE), dict)
                 and "Value" in _load_country_scheme(c)[TABLE]]
    assert not offenders, (
        f"{offenders} still declare a bare `Value` in livestock.  Use "
        "ValuePerAnimal (unit price), HerdValue (stock total) or SalesValue "
        "(sales flow) -- see `Columns: livestock:` in data_info.yml."
    )
    assert "Value" not in _DEFAULT_MONETARY[TABLE]
    # ... but assets must be untouched by that removal.
    assert "Value" in _DEFAULT_MONETARY["assets"]


def test_purchase_price_not_claimed_for_livestock():
    """`Purchase Price` is an assets column; no livestock table declares it.

    It sat in the livestock seed as a copy-paste from the `assets` line.
    Harmless in isolation, but a registry that lists columns nothing declares
    is precisely what keeps `attach_currency`'s non-empty gate open after a
    real column has been renamed away -- i.e. it hides the failure above.
    """
    assert "Purchase Price" not in _DEFAULT_MONETARY[TABLE]
    assert "Purchase Price" in _DEFAULT_MONETARY["assets"]

    for country in _country_dirs():
        entry = _load_country_scheme(country).get(TABLE)
        if isinstance(entry, dict):
            assert "Purchase Price" not in entry, (
                f"{country} declares `Purchase Price` in livestock; the "
                "registry no longer covers it."
            )


@pytest.mark.parametrize("column", ["ValuePerAnimal", "HerdValue", "SalesValue"])
def test_conversion_layer_sees_each_value_column(column):
    """`conversion.convert` scales exactly `_all_monetary_columns()`.

    A canonical livestock value column missing from that union is silently
    left in nominal local currency by every conversion call.
    """
    assert column in _all_monetary_columns()


@pytest.mark.parametrize(
    "country,column",
    [("Uganda", "ValuePerAnimal"), ("Nigeria", "ValuePerAnimal"),
     ("Malawi", "ValuePerAnimal"), ("Ethiopia", "SalesValue"),
     ("Mali", "SalesValue"), ("EthiopiaRHS", "HerdValue")],
)
def test_country_value_column_resolves_as_monetary(country, column):
    """End-to-end through the real resolver, per renamed country.

    Belt-and-braces over the set equality above: this is the call
    `Country._finalize_result` actually makes.
    """
    entry = _load_country_scheme(country).get(TABLE)
    assert isinstance(entry, dict), f"{country} no longer declares livestock"
    assert column in entry, (
        f"{country}'s livestock declares {sorted(entry)}, not {column!r}"
    )
    assert column in _monetary_columns(TABLE, country)
