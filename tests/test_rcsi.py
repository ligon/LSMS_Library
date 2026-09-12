"""``rcsi`` (reduced Coping Strategies Index) and ``rcsi_phase`` (GH #863).

Pattern follows ``tests/test_crop_kg_factor.py``: hand-built minimal frames
for the pure-function behaviour, plus a data-gated test against a warm
country build at the bottom of the file.

``rcsi`` is a MECHANICAL reduction over the ``food_coping`` item table
``(t, i, Strategy)`` -> ``Days``.  The two things GH #863 says must not be
copied from EPAR: (1) the five-term WFP formula is not universal -- a
``Strategy`` label the caller's ``weights`` doesn't recognise must raise,
not silently fall back to the five-term score; (2) the phase cutoffs must
PARTITION the score line (EPAR's own cutoffs leave ``rcsi == 19`` in no
phase).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import _RCSI_WFP_WEIGHTS, rcsi, rcsi_phase


def _food_coping(rows):
    """Build a minimal food_coping-shaped frame: rows of (t, i, Strategy, Days)."""
    df = pd.DataFrame(rows, columns=["t", "i", "Strategy", "Days"])
    return df.set_index(["t", "i", "Strategy"])[["Days"]]


# ---------------------------------------------------------------------------
# The score itself
# ---------------------------------------------------------------------------

def test_default_weights_reproduce_the_wfp_five_term_formula():
    """Hand-computed: rcsi = LessPreferred + LimitPortion + ReduceMeals
    + 3*RestrictAdults + 2*BorrowFood, matching EPAR's own Malawi coefficients
    once its strategy1..5 renaming is unwound (see the docstring / ledger)."""
    rows = [
        ("2010-11", "hh1", "LessPreferred", 2),
        ("2010-11", "hh1", "LimitPortion", 1),
        ("2010-11", "hh1", "ReduceMeals", 3),
        ("2010-11", "hh1", "RestrictAdults", 1),
        ("2010-11", "hh1", "BorrowFood", 4),
    ]
    fc = _food_coping(rows)
    out = rcsi(fc)
    assert list(out.columns) == ["rCSI"]
    # 2 + 1 + 3 + 3*1 + 2*4 = 17
    expected = 2 + 1 + 3 + 3 * 1 + 2 * 4
    assert out.loc[("2010-11", "hh1"), "rCSI"] == expected


def test_multiple_households_and_waves():
    rows = [
        ("2010-11", "hh1", "LessPreferred", 0),
        ("2010-11", "hh1", "LimitPortion", 0),
        ("2010-11", "hh1", "ReduceMeals", 0),
        ("2010-11", "hh1", "RestrictAdults", 0),
        ("2010-11", "hh1", "BorrowFood", 0),
        ("2010-11", "hh2", "LessPreferred", 7),
        ("2010-11", "hh2", "LimitPortion", 7),
        ("2010-11", "hh2", "ReduceMeals", 7),
        ("2010-11", "hh2", "RestrictAdults", 7),
        ("2010-11", "hh2", "BorrowFood", 7),
        ("2013-14", "hh1", "LessPreferred", 1),
        ("2013-14", "hh1", "LimitPortion", 1),
        ("2013-14", "hh1", "ReduceMeals", 1),
        ("2013-14", "hh1", "RestrictAdults", 1),
        ("2013-14", "hh1", "BorrowFood", 1),
    ]
    fc = _food_coping(rows)
    out = rcsi(fc)
    assert out.loc[("2010-11", "hh1"), "rCSI"] == 0
    # 7*(1+1+1+3+2) = 7*8 = 56 -- the WFP rCSI maximum for one week.
    assert out.loc[("2010-11", "hh2"), "rCSI"] == 56
    assert out.loc[("2013-14", "hh1"), "rCSI"] == 8
    assert len(out) == 3


def test_weights_override():
    """A caller-supplied weights dict is used instead of the WFP default."""
    rows = [
        ("2010-11", "hh1", "LessPreferred", 1),
        ("2010-11", "hh1", "LimitPortion", 1),
        ("2010-11", "hh1", "ReduceMeals", 1),
        ("2010-11", "hh1", "RestrictAdults", 1),
        ("2010-11", "hh1", "BorrowFood", 1),
    ]
    fc = _food_coping(rows)
    custom = {"LessPreferred": 10, "LimitPortion": 0, "ReduceMeals": 0,
              "RestrictAdults": 0, "BorrowFood": 0}
    out = rcsi(fc, weights=custom)
    assert out.loc[("2010-11", "hh1"), "rCSI"] == 10


def test_weights_override_can_cover_a_wider_battery():
    """A country with extra strategies (Tanzania/Ethiopia/Nigeria-shaped) works
    once the caller supplies weights naming every label present."""
    rows = [
        ("t1", "hh1", "LessPreferred", 1),
        ("t1", "hh1", "LimitPortion", 1),
        ("t1", "hh1", "ReduceMeals", 1),
        ("t1", "hh1", "RestrictAdults", 1),
        ("t1", "hh1", "BorrowFood", 1),
        ("t1", "hh1", "LimitVariety", 1),
        ("t1", "hh1", "NoFood", 1),
        ("t1", "hh1", "WholeDay", 1),
    ]
    fc = _food_coping(rows)
    eight_term = dict(_RCSI_WFP_WEIGHTS, LimitVariety=1, NoFood=4, WholeDay=4)
    out = rcsi(fc, weights=eight_term)
    assert out.loc[("t1", "hh1"), "rCSI"] == sum(eight_term.values())


def test_unrecognised_strategy_raises_by_default():
    """A Strategy label outside the WFP five must not be silently scored on
    just the five it recognises -- GH #863's central 'must not do'."""
    rows = [
        ("t1", "hh1", "LessPreferred", 1),
        ("t1", "hh1", "LimitPortion", 1),
        ("t1", "hh1", "ReduceMeals", 1),
        ("t1", "hh1", "RestrictAdults", 1),
        ("t1", "hh1", "BorrowFood", 1),
        ("t1", "hh1", "LimitVariety", 5),
    ]
    fc = _food_coping(rows)
    with pytest.raises(ValueError, match="LimitVariety"):
        rcsi(fc)


def test_weights_naming_a_strategy_the_table_never_carries_raises():
    """Symmetric to the unrecognised-label check: a country fielding only
    4 of the 5 standard strategies must not silently get a 4-term score
    under the 5-term default -- the strategy set is a property of the
    questionnaire in both directions."""
    rows = [
        ("t1", "hh1", "LessPreferred", 1),
        ("t1", "hh1", "LimitPortion", 1),
        ("t1", "hh1", "ReduceMeals", 1),
        ("t1", "hh1", "BorrowFood", 1),
        # RestrictAdults never appears anywhere in this table.
    ]
    fc = _food_coping(rows)
    with pytest.raises(ValueError, match="RestrictAdults"):
        rcsi(fc)


def test_missing_strategy_row_is_not_zero_filled():
    """A household missing one of the weighted strategies had it UNANSWERED
    (see the docstring's three citations), not zero -- and must be excluded
    from the score entirely, not scored on a truncated sum."""
    rows = [
        # hh1: all five present -> scored.
        ("2010-11", "hh1", "LessPreferred", 1),
        ("2010-11", "hh1", "LimitPortion", 1),
        ("2010-11", "hh1", "ReduceMeals", 1),
        ("2010-11", "hh1", "RestrictAdults", 1),
        ("2010-11", "hh1", "BorrowFood", 1),
        # hh2: missing RestrictAdults entirely -> must be dropped, not scored
        # as if RestrictAdults were 0.
        ("2010-11", "hh2", "LessPreferred", 7),
        ("2010-11", "hh2", "LimitPortion", 7),
        ("2010-11", "hh2", "ReduceMeals", 7),
        ("2010-11", "hh2", "BorrowFood", 7),
    ]
    fc = _food_coping(rows)
    out = rcsi(fc)
    assert list(out.index) == [("2010-11", "hh1")]
    assert ("2010-11", "hh2") not in out.index


def test_missing_days_index_raises():
    fc = _food_coping([("t1", "hh1", "LessPreferred", 1)]).rename(
        columns={"Days": "NotDays"})
    with pytest.raises(ValueError, match="Days"):
        rcsi(fc)


def test_missing_strategy_level_raises():
    df = pd.DataFrame({"Days": [1]}, index=pd.MultiIndex.from_tuples(
        [("t1", "hh1")], names=["t", "i"]))
    with pytest.raises(ValueError, match="Strategy"):
        rcsi(df)


def test_extra_v_index_level_is_collapsed_before_scoring():
    """Some countries' food_coping arrives with a joined 'v' cluster level
    in the index (the sample()-join, CLAUDE.md 'sample() and Cluster
    Identity') -- (t, i) must still be the grouping grain."""
    rows = [
        ("2010-11", "hh1", "v1", "LessPreferred", 1),
        ("2010-11", "hh1", "v1", "LimitPortion", 1),
        ("2010-11", "hh1", "v1", "ReduceMeals", 1),
        ("2010-11", "hh1", "v1", "RestrictAdults", 1),
        ("2010-11", "hh1", "v1", "BorrowFood", 1),
    ]
    df = pd.DataFrame(rows, columns=["t", "i", "v", "Strategy", "Days"])
    df = df.set_index(["i", "t", "v", "Strategy"])[["Days"]]
    out = rcsi(df)
    assert list(out.index.names) == ["t", "i"]
    assert out.loc[("2010-11", "hh1"), "rCSI"] == 1 + 1 + 1 + 3 + 2


# ---------------------------------------------------------------------------
# rcsi_phase: partitioning intervals
# ---------------------------------------------------------------------------

def test_every_integer_0_to_60_lands_in_exactly_one_phase():
    scores = pd.Series(range(0, 61))
    phases = rcsi_phase(scores)
    assert phases.isna().sum() == 0
    assert len(phases) == len(scores)


def test_rcsi_19_lands_in_phase_3_not_no_phase():
    """The exact defect GH #863 names: EPAR's own cutoffs
    (Malawi IHS Wave 1 .do:4530-4531) leave rcsi == 19 in no phase at all.
    Ours must partition, so 19 lands somewhere -- specifically phase 3,
    matching the LABEL EPAR intended ("19-42")."""
    out = rcsi_phase(pd.Series([19]))
    assert out.iloc[0] == "Phase 3"


def test_boundary_values_at_default_cutoffs():
    s = pd.Series([0, 3, 4, 18, 19, 42, 43])
    out = rcsi_phase(s)
    assert list(out) == [
        "Phase 1", "Phase 1", "Phase 2", "Phase 2",
        "Phase 3", "Phase 3", "Phase 4",
    ]


def test_rcsi_phase_is_ordered_categorical():
    out = rcsi_phase(pd.Series([0, 50]))
    assert isinstance(out.dtype, pd.CategoricalDtype)
    assert out.dtype.ordered
    assert list(out.dtype.categories) == [
        "Phase 1", "Phase 2", "Phase 3", "Phase 4"]


def test_rcsi_phase_accepts_a_dataframe():
    scores = pd.DataFrame({"rCSI": [0, 19, 50]})
    out = rcsi_phase(scores)
    assert list(out) == ["Phase 1", "Phase 3", "Phase 4"]


def test_custom_cutoffs_still_partition():
    scores = pd.Series(range(0, 21))
    phases = rcsi_phase(scores, cutoffs=(2, 5, 10))
    assert phases.isna().sum() == 0
    # And the boundary itself lands with the upper (closed) side.
    assert rcsi_phase(pd.Series([5]), cutoffs=(2, 5, 10)).iloc[0] == "Phase 2"
    assert rcsi_phase(pd.Series([6]), cutoffs=(2, 5, 10)).iloc[0] == "Phase 3"


# ---------------------------------------------------------------------------
# Data-gated: warm Malawi food_coping (GH #863's natural first country)
# ---------------------------------------------------------------------------

def test_malawi_warm_food_coping_scores_cleanly():
    """Malawi fields exactly the standard five strategies (data_scheme.yml
    comment), so the default weights should apply with zero ValueErrors, and
    every score should land in a phase.

    Skips cleanly (rather than erroring) if no warm cache / credentials are
    available -- this must never attempt to BUILD Malawi's food_coping.
    """
    try:
        import lsms_library as ll
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"lsms_library unavailable: {exc}")

    try:
        fc = ll.Country("Malawi").food_coping()
    except Exception as exc:
        pytest.skip(f"Malawi food_coping not available in this environment: {exc}")

    if fc is None or len(fc) == 0:
        pytest.skip("Malawi food_coping returned empty -- no warm cache here")

    strategies = set(fc.index.get_level_values("Strategy").astype(str))
    assert strategies == set(_RCSI_WFP_WEIGHTS), (
        "Malawi's Strategy vocabulary changed -- re-check the default-weights "
        "assumption in the rcsi docstring before trusting this test"
    )

    out = rcsi(fc)
    assert len(out) > 0
    assert (out["rCSI"] >= 0).all()
    assert (out["rCSI"] <= 7 * sum(_RCSI_WFP_WEIGHTS.values())).all()

    phases = rcsi_phase(out)
    assert phases.isna().sum() == 0
