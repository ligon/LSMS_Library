"""GH #537: the `s` (acquisition source) canonical-value guard must actually run.

`validate_acquisition_source` had ZERO call sites from the day it was written,
while its docstring asserted it was "Called from Country._finalize_result".  A
guarantee that does not execute is worse than no guarantee, because it is
trusted.

Two things are tested here:

1. **It is wired in.**  `Country._finalize_result` -- the universal read path --
   must reject a non-canonical `s`.  Pre-fix these raise nothing at all.

2. **It can see NaN.**  The original check called `.dropna()` before comparing,
   which made it structurally blind to a missing `s` -- the *dominant*
   real-world non-conformity (an unmapped source code becomes NaN, not a bad
   string; EthiopiaRHS/1989 has 677 such rows upstream).  A check that cannot
   see the failure mode that actually occurs is not a guarantee.

Plus a regression test for the definition drift: EthiopiaRHS carried its own
narrower `CANON_S` that silently omitted 'other'.

These exercise `Country._finalize_result` with a stub `self` (cf.
test_u_sentinel_protection.py), so no data access is needed.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.transformations import S_VALUES, validate_acquisition_source


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
class _Stub:
    """Minimal stand-in exposing only what _finalize_result touches.

    `data_scheme` has no 'sample', so the v-join is skipped; `_updated_ids_cache`
    is None, so id_walk is skipped.  Neither is under test here.
    """

    name = "Testland"
    data_scheme: dict = {}
    categorical_mapping: dict = {}
    _updated_ids_cache = None

    def _augment_index_from_related_tables(self, df, scheme_entry, _):
        return df

    def _apply_categorical_mappings(self, df, protect_u_sentinels=False):
        return df


def _frame(s_values):
    """A food_acquired-shaped frame whose `s` level is exactly `s_values`."""
    idx = pd.MultiIndex.from_arrays(
        [
            ["hh1"] * len(s_values),
            ["2019-20"] * len(s_values),
            ["Maize"] * len(s_values),
            ["Kg"] * len(s_values),
            list(s_values),
        ],
        names=["i", "t", "j", "u", "s"],
    )
    return pd.DataFrame(
        {"Quantity": np.arange(1.0, len(s_values) + 1),
         "Expenditure": np.arange(10.0, 10.0 + len(s_values))},
        index=idx,
    )


def _finalize(df, method_name="food_acquired"):
    """Drive the real _finalize_result over a stub self."""
    return Country._finalize_result(_Stub(), df, {}, method_name)


# --------------------------------------------------------------------------
# 1. the guard is wired into the universal read path
# --------------------------------------------------------------------------
def test_finalize_result_accepts_canonical_s():
    """Guard against overreach: every canonical value must survive untouched."""
    out = _finalize(_frame(S_VALUES))
    assert len(out) == len(S_VALUES)
    assert set(out.index.get_level_values("s")) == set(S_VALUES)


@pytest.mark.parametrize("bad", ["Purchased", "purchase", "own-production", "gift"])
def test_finalize_result_rejects_noncanonical_s(bad):
    """A typo'd literal in a wave script must be LOUD, not silently a new category.

    19 of the 20 countries that set `s` set it as a bare string literal, so this
    is the realistic regression.  Pre-fix: _finalize_result never called the
    validator, so this returned a frame with a phantom `s` category and no
    complaint.
    """
    with pytest.raises(ValueError, match=r"[Ss]'? \(acquisition source\)|non-canonical"):
        _finalize(_frame(["purchased", bad]))


def test_finalize_result_rejects_nan_s():
    """NaN `s` must raise.

    This is the failure mode that actually occurs in the wild (an unmapped raw
    source code maps to NaN), and the original `.dropna()`-based check was
    structurally blind to it -- so this test fails BOTH on the unwired code and
    on the old check naively wired in.
    """
    with pytest.raises(ValueError, match="missing"):
        _finalize(_frame(["purchased", np.nan]))


def test_finalize_result_noop_without_s_level():
    """Tables with no `s` axis (i.e. all the non-food ones) are untouched."""
    idx = pd.MultiIndex.from_arrays(
        [["hh1", "hh2"], ["2019-20", "2019-20"]], names=["i", "t"]
    )
    df = pd.DataFrame({"Age": [30, 40]}, index=idx)
    out = Country._finalize_result(_Stub(), df, {}, "household_roster")
    assert len(out) == 2


# --------------------------------------------------------------------------
# 2. the validator itself: NaN visibility + the levels-based fast path
# --------------------------------------------------------------------------
def test_validator_sees_nan_directly():
    """Unit-level: the .dropna() blindness is gone."""
    with pytest.raises(ValueError, match="missing"):
        validate_acquisition_source(_frame(["purchased", np.nan]))


def test_validator_reports_the_offending_values():
    """The error must name what is wrong -- an actionable crash, not a bare raise."""
    with pytest.raises(ValueError) as exc:
        validate_acquisition_source(_frame(["purchased", "Purchased"]))
    assert "Purchased" in str(exc.value)


def test_validator_no_false_positive_on_stale_levels():
    """A MultiIndex retains filtered-out entries in `.levels`.

    The fast path reads unique values off `.levels` (O(#unique), not O(#rows) --
    it runs on every read).  Without remove_unused_levels() a row filtered out
    upstream would still sit in `.levels` and raise a FALSE positive.
    """
    df = _frame(["purchased", "bogus"])
    df = df[df.index.get_level_values("s") != "bogus"]
    assert "bogus" in df.index.levels[df.index.names.index("s")]  # stale, as designed
    validate_acquisition_source(df)  # must NOT raise


# --------------------------------------------------------------------------
# 3. definition drift: EthiopiaRHS must not re-spell the canonical set
# --------------------------------------------------------------------------
_ERHS = (Path(__file__).resolve().parents[1]
         / "lsms_library" / "countries" / "EthiopiaRHS" / "_" / "ethiopiarhs.py")


def _load_erhs():
    spec = importlib.util.spec_from_file_location("ethiopiarhs_for_test", _ERHS)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:  # pragma: no cover - environment-dependent imports
        pytest.skip(f"ethiopiarhs.py not importable here: {exc}")
    return mod


def test_ethiopiarhs_does_not_narrow_the_canonical_set():
    """ERHS's pass-through filter must admit every canonical value, incl. 'other'.

    It used to hardcode CANON_S = ('purchased','produced','inkind') -- a second,
    narrower definition of "canonical s" -- so any ERHS row with s='other' would
    have been silently deleted by the country's own hook.  Behaviour-neutral on
    today's data (no ERHS wave emits 'other'); this pins the definition.
    """
    mod = _load_erhs()
    idx = pd.MultiIndex.from_arrays(
        [["hh1"] * 4, ["Maize"] * 4, ["Kg"] * 4, list(S_VALUES)],
        names=["i", "j", "u", "s"],
    )
    df = pd.DataFrame({"Quantity": [1.0, 2.0, 3.0, 4.0],
                       "Expenditure": [5.0, 6.0, 7.0, 8.0]}, index=idx)

    out = mod.food_acquired(df)  # pass-through path: no 'q_purch' column

    survived = set(out.reset_index()["s"])
    assert "other" in survived, (
        "EthiopiaRHS dropped s='other' -- it is re-spelling the canonical set "
        "instead of importing transformations.S_VALUES"
    )
    assert survived == set(S_VALUES)
