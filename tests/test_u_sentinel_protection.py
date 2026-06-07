"""GH #361: a country's #+name:u categorical table must not remap the
reserved 'kg'/'Value' conversion sentinels on derived food tables.

These exercise Country._apply_categorical_mappings directly with a stub
``self`` carrying a categorical_mapping, so no data access / Country.__init__
is needed.
"""
from __future__ import annotations

import pandas as pd

from lsms_library.country import Country, _RESERVED_U_SENTINELS


class _Stub:
    """Minimal stand-in exposing only what _apply_categorical_mappings needs."""

    def __init__(self, cat_maps):
        self.categorical_mapping = cat_maps


def _u_table():
    # Mirrors Burkina: unifies raw 'kg'/'Kg' -> 'Kg', identity on a container.
    return {
        "u": pd.DataFrame(
            {
                "Original Label": ["kg", "Kg", "Tas"],
                "Preferred Label": ["Kg", "Kg", "Tas"],
            }
        )
    }


def _frame():
    idx = pd.MultiIndex.from_tuples(
        [("A", "kg"), ("A", "Tas")], names=["i", "u"]
    )
    return pd.DataFrame({"Quantity": [1.0, 2.0]}, index=idx)


def test_kg_in_reserved_sentinels():
    assert "kg" in _RESERVED_U_SENTINELS
    assert "Value" in _RESERVED_U_SENTINELS


def test_protected_keeps_kg_sentinel():
    """Derived food tables: the lowercase 'kg' conversion tag survives."""
    out = Country._apply_categorical_mappings(
        _Stub(_u_table()), _frame(), protect_u_sentinels=True
    )
    u = list(out.index.get_level_values("u"))
    assert u == ["kg", "Tas"], u  # 'kg' untouched; non-sentinel still mapped


def test_unprotected_remaps_kg():
    """food_acquired (unprotected): raw 'kg' unifies to 'Kg' as before."""
    out = Country._apply_categorical_mappings(
        _Stub(_u_table()), _frame(), protect_u_sentinels=False
    )
    u = list(out.index.get_level_values("u"))
    assert u == ["Kg", "Tas"], u  # raw-spelling unification preserved


def test_protection_only_touches_u_level():
    """A non-'u' level with a matching table is unaffected by the flag."""
    cat = {
        "j": pd.DataFrame(
            {"Original Label": ["kg"], "Preferred Label": ["MAPPED"]}
        )
    }
    idx = pd.MultiIndex.from_tuples([("A", "kg")], names=["i", "j"])
    df = pd.DataFrame({"Quantity": [1.0]}, index=idx)
    out = Country._apply_categorical_mappings(
        _Stub(cat), df, protect_u_sentinels=True
    )
    # 'kg' on the 'j' level is a normal label, not a u-sentinel -> still mapped.
    assert list(out.index.get_level_values("j")) == ["MAPPED"]
