"""A unit whose size is REGIONAL is declared in config, not in code (GH #919).

``transformations.KNOWN_METRIC`` is a Python dict keyed on the raw, lower-cased
``u`` label, and ``_seeded_kg_factors`` does ``dict(KNOWN_METRIC)`` and only
infers for units *not* already in it. So an entry there beats every
country-level factor unconditionally and cannot be overridden. That is fine for
a kilogram and wrong for a **quintal**: the metric centner (100 kg) in Ethiopia,
100 *pounds* (45.36 kg) in Central America, where the pound is itself regional.

`f7c1eb380` seeded `quintal` there. It did not change a served kilogram --
`KNOWN_METRIC` and Ethiopia's shipped WB table both say 100.0 -- but it moved
13,977 rows off the `shipped` rung onto `metric`, and those rows were
`Ethiopia/_/CONTENTS.org`'s *"real external check"*, the one non-tautological
corroboration the shipped-factor work had. `43,384 - 13,924 = 29,460`, the
observed failure, to the row.

The replacement is the `u_kg` table in `categorical_mapping/u.org`:

* keyed on the **canonical** unit (``Unit``, whose values are ``Preferred
  Label``s), so one row serves every spelling -- ``KNOWN_METRIC`` has to
  enumerate ``kg``/``kilogram``/``kilogramme``/``kgs``/... because it is keyed
  on the variant, and the ``u`` table already collapses those;
* in ``_ADDITIVE_CATEGORICAL_TABLES``, so the global row is a DEFAULT and a
  country overrides it by redeclaring the row;
* composed into a raw-label-keyed dict by ``Country._unit_kg_factors`` and
  passed down, because the kg factor is computed BEFORE the categorical mapping
  is applied (``conversion_to_kgs`` sees the raw ``u``; GH #770 rejected a fix
  that assumed otherwise).

The key column is deliberately **not** named ``Preferred Label``:
``_categorical_key_column`` takes the first non-``Preferred Label`` column as a
table's key, so such a table would be keyed on ``Kg``.
"""
from __future__ import annotations

import pandas as pd
import pytest

from lsms_library.country import (
    Country, _ADDITIVE_CATEGORICAL_TABLES, _categorical_key_column,
    _merge_categorical_tables, _row_union_categorical,
)
from lsms_library.transformations import (
    KNOWN_METRIC, _parse_explicit_metric, _seeded_kg_factors,
)

GLOBAL = pd.DataFrame({"Unit": ["Quintal"], "Kg": [100.0]})
SPANISH = pd.DataFrame({"Unit": ["Quintal"], "Kg": [45.359237]})


# --------------------------------------------------------------------------
# Nothing country-qualified may live in KNOWN_METRIC
# --------------------------------------------------------------------------

def test_quintal_is_not_in_known_metric():
    assert "quintal" not in KNOWN_METRIC and "quintals" not in KNOWN_METRIC, (
        "quintal is country-qualified (100 kg in Ethiopia, 100 lb in Central "
        "America); a KNOWN_METRIC entry cannot be overridden by a country"
    )


def test_the_explicit_metric_parser_also_stops_claiming_quintals():
    """The label parser is a second, independent seed of the same factor."""
    assert _parse_explicit_metric("2 quintals") is None
    assert _parse_explicit_metric("50 kgs") == 50.0       # control: still works


def test_universal_metric_spellings_stay_in_known_metric():
    """Only the REGIONAL entry moved; a centilitre is a centilitre everywhere."""
    for u in ("kg", "gram", "tonne", "litre", "centilitre", "milligramme"):
        assert u in KNOWN_METRIC, u


# --------------------------------------------------------------------------
# The declared table outranks KNOWN_METRIC and is overridable
# --------------------------------------------------------------------------

def test_a_declared_factor_outranks_known_metric():
    df = pd.DataFrame({"Quantity": [1.0]},
                      index=pd.MultiIndex.from_tuples([("a", "Kg")],
                                                      names=["i", "u"]))
    assert _seeded_kg_factors(df)["kg"] == 1
    assert _seeded_kg_factors(df, unit_kg={"kg": 0.5})["kg"] == 0.5, (
        "a declared country factor must beat the framework dict"
    )


def test_u_kg_composes_additively_so_a_country_row_wins():
    assert "u_kg" in _ADDITIVE_CATEGORICAL_TABLES
    merged = _row_union_categorical(GLOBAL, SPANISH)
    assert len(merged) == 1
    assert float(merged["Kg"].iloc[0]) == pytest.approx(45.359237)


def test_the_key_column_is_unit_not_preferred_label():
    """`Preferred Label` as the key would make `_categorical_key_column` pick `Kg`."""
    assert _categorical_key_column(GLOBAL) == "Unit"
    bad = pd.DataFrame({"Preferred Label": ["Quintal"], "Kg": [100.0]})
    assert _categorical_key_column(bad) == "Kg"   # why the name matters


def test_the_table_is_inert_to_the_label_auto_apply():
    """No `Preferred Label` column => it can never be applied as a label map."""
    assert "Preferred Label" not in GLOBAL.columns


# --------------------------------------------------------------------------
# Country composition: raw label -> canonical -> kg
# --------------------------------------------------------------------------

def _country_with(monkeypatch, name, global_maps, country_maps):
    c = Country(name)
    _ = c.categorical_mapping                       # warm, then replace
    c.__dict__["_categorical_mapping_cache"] = _merge_categorical_tables(
        global_maps, country_maps)
    return c


def test_the_canonical_name_resolves_without_a_u_row(monkeypatch):
    """Ethiopia serves the canonical `Quintal` directly; no `u` row needed."""
    c = _country_with(monkeypatch, "Ethiopia", {"u_kg": GLOBAL}, {})
    assert c._unit_kg_factors() == {"quintal": 100.0}


@pytest.mark.parametrize("key_column", ["Original Label", "Code"])
def test_a_raw_label_composes_through_u_however_the_country_keys_it(monkeypatch, key_column):
    """Malawi/Mali/Uganda key `u` on `Code`; the rest on `Original Label`.

    Both carry `Preferred Label`, which is why the MASS is keyed on the
    canonical name -- it is the one column every country's table has.
    """
    u = pd.DataFrame({key_column: ["centner"], "Preferred Label": ["Quintal"]})
    c = _country_with(monkeypatch, "Panama", {"u_kg": GLOBAL}, {"u": u})
    factors = c._unit_kg_factors()
    assert factors["centner"] == 100.0
    assert factors["quintal"] == 100.0


def test_a_country_override_reaches_the_raw_label(monkeypatch):
    """The whole design, end to end: Panama's `centner` must weigh 45.36 kg."""
    u = pd.DataFrame({"Original Label": ["centner"], "Preferred Label": ["Quintal"]})
    c = _country_with(monkeypatch, "Panama",
                      {"u_kg": GLOBAL}, {"u": u, "u_kg": SPANISH})
    factors = c._unit_kg_factors()
    assert factors["centner"] == pytest.approx(45.359237)
    assert factors["quintal"] == pytest.approx(45.359237)


def test_a_malformed_table_is_ignored_loudly(monkeypatch):
    c = _country_with(monkeypatch, "Ethiopia",
                      {"u_kg": pd.DataFrame({"Unit": ["Quintal"]})}, {})
    with pytest.warns(UserWarning, match="Unit.*Kg"):
        assert c._unit_kg_factors() == {}


# --------------------------------------------------------------------------
# Delivered
# --------------------------------------------------------------------------

def test_every_country_inherits_the_global_default():
    """Including the `Code`-keyed ones, which compose via the canonical name."""
    for name in ("Ethiopia", "Panama", "Malawi", "Uganda", "Mali"):
        assert Country(name)._unit_kg_factors().get("quintal") == 100.0, name


@pytest.mark.requires_s3
def test_ethiopia_quintal_rows_still_weigh_100kg():
    """Removing the KNOWN_METRIC entry ALONE would have been a ~100x regression.

    Measured: without the declared table those rows fall to the price-ratio
    inference at 0.682 / 1.024 kg.
    """
    from lsms_library.transformations import food_kg_factors

    c = Country("Ethiopia")
    fa = c.food_acquired()
    u = pd.Series(fa.index.get_level_values("u")).astype(str)
    mask = (u == "Quintal").to_numpy()
    if not mask.any():
        pytest.skip("no Quintal rows in this cache")

    kgf = food_kg_factors(fa, unit_kg=c._unit_kg_factors())
    served = kgf[mask]
    assert set(served["kg_per_unit"].dropna().round(6)) == {100.0}
    assert set(served["KgFactorSource"]) == {"metric"}
