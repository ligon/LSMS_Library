"""GH #323: collapsing a non-unique DECLARED index must not silently destroy data.

`_normalize_dataframe_index` reduces duplicate index tuples.  Historically it did
so with `groupby().first()`, which DISCARDS the dropped rows -- for an additive
measure (a quantity, an expenditure) that is silent data loss.  Togo's
`plot_inputs` lost 11,045 of 938,713 units of Quantity that way: three distinct
EHCVM seed types are harmonized onto the SAME (Seed, Autre crop) pair, so two
real input applications collided on one canonical index tuple and one was thrown
away.  The raw source has ZERO duplicates -- the collision is manufactured by the
taxonomy, and the rows are additive.

The reducer is now DECLARED, per column, in the `Aggregation:` block of
lsms_library/data_info.yml (overridable per country in data_scheme.yml).  These
tests pin three things:

  1. the reducer semantics (sum / any / derive / first + loud warning);
  2. that every DECLARED policy is well-formed and names real columns
     ("prose is not enforcement" -- a typo'd column must fail, not degrade);
  3. a RATCHET over the tables that still collapse with `.first()`.  Those are
     class-2 (silently MISSING) rather than class-1 (silently WRONG) because the
     framework warns -- but the set must never GROW without a deliberate
     decision.  A new additive table that starts colliding fails here.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest
import yaml
from importlib.resources import files

from lsms_library.country import _normalize_dataframe_index
from lsms_library.feature import (
    _VALID_REDUCERS,
    collapse_with_policy,
    resolve_aggregation,
)
from lsms_library.paths import countries_root


def _data_info() -> dict:
    with open(files("lsms_library") / "data_info.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)


# --------------------------------------------------------------------------
# 1. Reducer semantics
# --------------------------------------------------------------------------

def _plot_inputs_frame():
    """Two rows colliding on (t, i, input, crop, u) -- Togo's real shape.

    Both are 'Seed'/'Autre crop' because harmonize_input + harmonize_seed_crop
    send "Autres semences" and "Plants/boutures de tubercules" to the same pair.
    """
    idx = pd.MultiIndex.from_tuples(
        [("2018", "h1", "Seed", "Autre crop", "Kg"),
         ("2018", "h1", "Seed", "Autre crop", "Kg"),
         ("2018", "h2", "NPK", "(not crop-specific)", "Kg")],
        names=["t", "i", "input", "crop", "u"],
    )
    return pd.DataFrame(
        {"Quantity": [10.0, 25.0, 4.0],
         "Purchased": [False, True, True],
         "Quantity_purchased": [0.0, 5.0, 4.0]},
        index=idx,
    )


PLOT_INPUTS_SCHEME = {
    "index": "(t, i, input, crop, u)",
    "Quantity": "float",
    "Purchased": "bool",
    "Quantity_purchased": "float",
}


def test_plot_inputs_collision_sums_quantity_not_first():
    """The GH #323 regression: `first` kept 10.0 and threw away 25.0."""
    df = _plot_inputs_frame()
    assert df["Quantity"].sum() == 39.0

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _normalize_dataframe_index(df, PLOT_INPUTS_SCHEME, None, "plot_inputs")

    assert len(out) == 2, "the two colliding rows collapse to one group"
    assert out["Quantity"].sum() == 39.0, "Quantity is ADDITIVE and must be conserved"
    assert out.loc[("2018", "h1", "Seed", "Autre crop", "Kg"), "Quantity"] == 35.0
    assert out["Quantity_purchased"].sum() == 9.0
    # A declared policy conserves the data, so no data-loss warning is due.
    assert not [w for w in caught if "GH #323" in str(w.message)]


def test_plot_inputs_purchased_uses_any_not_first():
    """`first` on (False, True) reports the household did not purchase. It did."""
    out, applied = collapse_with_policy(
        _plot_inputs_frame(),
        ["t", "i", "input", "crop", "u"],
        "plot_inputs",
    )
    assert applied
    assert bool(out.loc[("2018", "h1", "Seed", "Autre crop", "Kg"), "Purchased"]) is True


def test_food_acquired_sums_and_rederives_price():
    """Price is PER UNIT: it must be re-derived from the summed totals, never summed."""
    idx = pd.MultiIndex.from_tuples(
        [("2018", "v1", "h1", "Rice", "Kg", "purchased"),
         ("2018", "v1", "h1", "Rice", "Kg", "purchased")],
        names=["t", "v", "i", "j", "u", "s"],
    )
    df = pd.DataFrame(
        {"Quantity": [2.0, 3.0], "Expenditure": [20.0, 45.0], "Price": [10.0, 15.0]},
        index=idx,
    )
    out, applied = collapse_with_policy(df, list(idx.names), "food_acquired")
    assert applied
    assert len(out) == 1
    row = out.iloc[0]
    assert row["Quantity"] == 5.0
    assert row["Expenditure"] == 65.0
    assert row["Price"] == pytest.approx(13.0), "65/5, NOT 10+15"


def test_undeclared_table_still_collapses_but_warns_loudly():
    """No policy => historical .first(), but the loss must be AUDIBLE (class-2)."""
    idx = pd.MultiIndex.from_tuples(
        [("2018", "h1", "Radio"), ("2018", "h1", "Radio")], names=["t", "i", "j"])
    df = pd.DataFrame({"Quantity": [1.0, 2.0]}, index=idx)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _normalize_dataframe_index(df, {"index": "(t, i, j)"}, None, "assets")

    assert len(out) == 1
    assert [w for w in caught if "GH #323" in str(w.message)], (
        "an undeclared collapse must warn -- silence is how #323 hid"
    )


def test_country_override_beats_canonical_policy():
    scheme = dict(PLOT_INPUTS_SCHEME, aggregation={"Quantity": "max"})
    assert resolve_aggregation("plot_inputs", scheme)["Quantity"] == "max"
    # ... and the canonical default still applies where not overridden.
    assert resolve_aggregation("plot_inputs", scheme)["Quantity_purchased"] == "sum"


def test_index_level_keys_are_inert_not_column_reducers():
    """Ten countries declare interview_date `aggregation: {visit: first}`.

    `visit` is an index LEVEL, not a column: it is reserved for the future
    grain-collapse feature and must not be mistaken for a column reducer.
    """
    scheme = {"index": "(t, i, visit)", "Interview start": "datetime",
              "aggregation": {"visit": "first"}}
    assert resolve_aggregation("interview_date", scheme) == {}


def test_unknown_reducer_raises():
    """A typo must FAIL, not silently degrade to .first()."""
    scheme = dict(PLOT_INPUTS_SCHEME, aggregation={"Quantity": "summ"})
    with pytest.raises(ValueError, match="Unknown aggregation reducer"):
        resolve_aggregation("plot_inputs", scheme)


# --------------------------------------------------------------------------
# 2. The declared policy is well-formed  ("prose is not enforcement")
# --------------------------------------------------------------------------

def test_canonical_policy_reducers_are_valid():
    for table, policy in (_data_info().get("Aggregation") or {}).items():
        for col, reducer in policy.items():
            assert reducer in _VALID_REDUCERS, (
                f"data_info.yml Aggregation: {table}.{col} = {reducer!r} "
                f"is not a known reducer"
            )


def test_canonical_policy_columns_exist_in_some_country():
    """Every column named in the canonical policy must be a real declared column.

    Guards the silent-no-op failure mode: a policy for `Quantity_Purchased`
    (wrong case) would look right in YAML and reduce nothing.
    """
    declared: dict[str, set[str]] = {}
    for scheme_path in sorted(countries_root().glob("*/_/data_scheme.yml")):
        with open(scheme_path, encoding="utf-8") as f:
            try:
                doc = yaml.safe_load(f) or {}
            except yaml.YAMLError:
                continue  # custom !make tags in some schemes; skip
        for table, entry in (doc.get("Data Scheme") or {}).items():
            if isinstance(entry, dict):
                declared.setdefault(table, set()).update(
                    k for k in entry
                    if k not in {"index", "materialize", "backend", "aggregation"}
                )

    for table, policy in (_data_info().get("Aggregation") or {}).items():
        if table not in declared:
            continue  # table declared by no country in this checkout
        for col in policy:
            assert col in declared[table], (
                f"data_info.yml Aggregation: {table}.{col} names a column no "
                f"country declares (typo?). Declared: {sorted(declared[table])}"
            )


# --------------------------------------------------------------------------
# 3. The ratchet: tables that still collapse with .first()
# --------------------------------------------------------------------------
#
# These tables have a non-unique declared index in at least one wave and NO
# aggregation policy, so they reduce with .first() and emit the loud GH #323
# warning.  Each is either non-additive (a roster row, a housing attribute --
# `first` is a defensible tie-break) or genuinely AMBIGUOUS.
#
#   assets -- Quantity is additive, but Age / Value / Purchase Price across two
#     rows for the same (t, i, j) are not obviously sum/mean/max.  Guessing would
#     ship silently-WRONG numbers; we keep the loud drop instead.  Resolving it
#     needs a survey-doc decision, not a code decision.  (Niger, Nigeria)
#
# ADDING A TABLE HERE IS A DECISION, NOT A FORMALITY: if it carries an additive
# measure, declare a reducer in data_info.yml instead.
KNOWN_FIRST_COLLAPSE_TABLES = {
    "assets",
    "cluster_features",
    "household_roster",
    "individual_education",
    "housing",
    "sample",
    "shocks",
    "interview_date",
    "food_security",
    "panel_ids",
    "employment",
}

#: Tables carrying an ADDITIVE measure.  Every one of these MUST have a declared
#: reducer -- that is the invariant that stops the next plot_inputs.
ADDITIVE_TABLES = {
    "food_acquired", "plot_inputs", "crop_production", "livestock", "plot_labor",
}


def test_every_additive_table_declares_a_reducer():
    """The core enforcement: an additive table may not fall through to .first()."""
    policy = _data_info().get("Aggregation") or {}
    for table in sorted(ADDITIVE_TABLES):
        assert table in policy, (
            f"{table!r} carries an additive measure but declares no reducer in "
            f"data_info.yml `Aggregation:` -- it would collapse with "
            f"groupby().first() and silently destroy quantities (GH #323)."
        )
        assert any(r == "sum" for r in policy[table].values()), (
            f"{table!r} declares a policy with no `sum` column; an additive "
            f"table must sum at least one measure."
        )


def test_additive_and_first_collapse_sets_are_disjoint():
    """A table cannot be both 'additive' and 'we accept .first() loss'."""
    overlap = ADDITIVE_TABLES & KNOWN_FIRST_COLLAPSE_TABLES
    assert not overlap, (
        f"{sorted(overlap)} is additive AND on the .first() ratchet -- one of the "
        f"two lists is wrong. An additive table must declare a reducer."
    )
