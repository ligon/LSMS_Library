"""``id_walk``: a single-pass rename, applied EXACTLY ONCE (GH #1245).

History
-------
Commit ``4db41a27`` (2026-04-10) patched a symptom: ``id_walk`` was a legacy
single-pass ``rename``, a map with a chain (``A -> B -> C``) is not idempotent
under it, and losing ``df.attrs['id_converted']`` let a second application
manufacture duplicates (Burkina Faso 2021-22: 392 duplicate tuples).  The
structural fix was ``_close_id_map``: resolve each wave's map to its fixed
point, so ``{A: B, B: C}`` became ``{A: C, B: C}`` and a second application was
a no-op.

GH #1245 measured that the closure is wrong for a chain that is not an alias
chain.  Within a wave the keys are that wave's RAW ids and the values are
CANONICAL (earlier-wave) ids -- two namespaces sharing one string space.  When
a survey renumbers households, household ``X`` maps to ``T`` (its true earlier
id) while a DIFFERENT household, whose raw id happens to be ``T``, maps to
``U``.  The closure sent ``X`` on to ``U``.  Burkina's ``panel_ids.py`` dropped
all 80 such links rather than let that happen.

The ruling (@ligon, 2026-10-23): a single-pass rename, every id to its own
target.  That reintroduces the original hazard -- a second application moves
``T`` on to ``U`` -- and frames ARE offered to ``id_walk`` twice: every table
derived from a served one (``food_quantities``, Uganda ``nutrition``) reaches
``_finalize_result`` without the flag.  So exactly-once is now decided by
``id_walk`` itself, from each wave's own ids (``_walk_partition``).

These tests pin, in order: the single-pass semantics; exactly-once (including
the counterfactual that a bare single-pass rename is NOT idempotent, so the
detection is load-bearing); the two refusals and the stated limit of the mixed
check; ``state='raw'`` for callers that know a frame is unwalked; an
index-only frame being walked (the panel diagnostic depends on it); and the
flag surviving a parquet round trip, which is what keeps warm reads of a table
whose own build walked it off the walk entirely.
"""

from __future__ import annotations

import pandas as pd
import pytest

from lsms_library.local_tools import (
    IdWalkError, _walk_partition, get_dataframe, id_walk, to_parquet,
)


def _frame(ids, wave="w1", extra=None):
    rows = [(i, wave) for i in ids] + list(extra or [])
    return pd.DataFrame(
        {"val": range(len(rows))},
        index=pd.MultiIndex.from_tuples(rows, names=["i", "t"]),
    )


def _ids(df, wave="w1"):
    return sorted(df.xs(wave, level="t").index.get_level_values("i"))


# A Burkina-shaped renumbering.  Raw ids of wave w1 and canonical ids share one
# string space: household X was T in the earlier wave, and the household whose
# w1 raw id is T was U.  P -> Q is an ordinary link, S keeps its id.
RENUMBERED = {"w1": {"X": "T", "T": "U", "P": "Q", "S": "S"}}


# ---------------------------------------------------------------------------
# _walk_partition
# ---------------------------------------------------------------------------


def test_walk_partition_classes():
    raw_only, walked_only, chained = _walk_partition(RENUMBERED["w1"])
    assert raw_only == {"X", "P"}      # move, and nobody's target
    assert walked_only == {"U", "Q"}   # targets that do not move
    assert chained == {"T"}            # a moving key AND a target
    assert "S" not in raw_only | walked_only | chained   # self-map: inert


def test_walk_partition_empty():
    assert _walk_partition({}) == (frozenset(), frozenset(), frozenset())


# ---------------------------------------------------------------------------
# Single pass: every id to its OWN target
# ---------------------------------------------------------------------------


def test_single_pass_does_not_follow_a_chain():
    """X goes to T -- its own target -- not on to U.  The closure sent X to U,
    the id of a different household; that is GH #1245."""
    walked = id_walk(_frame(["X", "T", "P", "S"]), RENUMBERED)
    assert _ids(walked) == ["Q", "S", "T", "U"]
    assert not walked.index.duplicated().any()


def test_a_swap_is_two_correct_renames():
    """Two households that exchanged ids between waves.  A closure called this
    a cycle and raised; it is two ordinary renames, which a whole wave (with
    other moving households in it) decides unambiguously."""
    ui = {"w1": {"A": "B", "B": "A", "P": "Q"}}
    walked = id_walk(_frame(["A", "B", "P"]), ui)
    assert _ids(walked) == ["A", "B", "Q"]
    assert walked.xs("w1", level="t").loc["B", "val"] == 0   # row of raw A
    assert walked.xs("w1", level="t").loc["A", "val"] == 1   # row of raw B


def test_waves_are_renamed_independently():
    ui = {"w1": {"A": "B"}, "w2": {"B": "C"}}
    df = _frame(["A"], wave="w1", extra=[("B", "w2"), ("Z", "w3")])
    walked = id_walk(df, ui)
    assert set(walked.index) == {("B", "w1"), ("C", "w2"), ("Z", "w3")}


def test_self_mapping_wave_is_a_no_op():
    ui = {"w1": {"X": "X", "Y": "Y"}}
    walked = id_walk(_frame(["X", "Y"]), ui)
    assert _ids(walked) == ["X", "Y"]


def test_empty_frame_and_frame_without_t():
    empty = _frame([]).iloc[:0]
    assert id_walk(empty, RENUMBERED).attrs["id_converted"] is True
    no_t = pd.DataFrame({"val": [1]}, index=pd.Index(["X"], name="i"))
    assert list(id_walk(no_t, RENUMBERED).index) == ["X"]


def test_row_order_and_values_are_preserved():
    df = _frame(["P", "S", "X", "T"])
    walked = id_walk(df.copy(), RENUMBERED)
    assert list(walked["val"]) == list(df["val"])
    assert list(walked.index.get_level_values("i")) == ["Q", "S", "T", "U"]


# ---------------------------------------------------------------------------
# Exactly once
# ---------------------------------------------------------------------------


def test_a_bare_single_pass_rename_is_not_idempotent():
    """The counterfactual that makes the detection load-bearing.  Applying the
    single-pass map twice moves T (X's correct id) on to U, where it merges
    with the raw-T household: one duplicate and one household lost."""
    df = _frame(["X", "T", "P", "S"])
    m = RENUMBERED["w1"]
    once = df.rename(index=m, level="i")
    twice = once.rename(index=m, level="i")
    assert not once.index.duplicated().any()
    assert twice.index.duplicated().sum() == 1
    assert "T" not in _ids(twice)


def test_id_walk_applied_twice_equals_once_without_the_flag():
    """The exactly-once contract.  The flag is cleared between passes, which
    is what a derived table, a merge or a concat of disagreeing frames does;
    the second pass must recognise the walked wave and leave it alone."""
    once = id_walk(_frame(["X", "T", "P", "S"]), RENUMBERED)
    again = once.copy()
    again.attrs = {}
    twice = id_walk(again, RENUMBERED)
    pd.testing.assert_frame_equal(twice, once)


def test_a_walked_frame_rebuilt_from_scratch_is_not_rewalked():
    """Derived-table shape: a NEW frame built from walked ids (no attrs at all,
    as `food_quantities` is built from the served `food_acquired`)."""
    once = id_walk(_frame(["X", "T", "P", "S"]), RENUMBERED)
    flat = once.reset_index()
    derived = pd.DataFrame(
        {"val": flat["val"].to_numpy()},
        index=pd.MultiIndex.from_arrays([flat["i"].to_numpy(), flat["t"].to_numpy()],
                                        names=["i", "t"]))
    assert derived.attrs == {}
    assert _ids(id_walk(derived, RENUMBERED)) == _ids(once)


def test_walked_detection_is_per_wave():
    """One wave already walked, another raw: each is handled on its own."""
    ui = {"w1": {"X": "T", "T": "U", "P": "Q"}, "w2": {"A": "B"}}
    w1_walked = id_walk(_frame(["X", "T", "P"]), ui)
    w1_walked.attrs = {}
    df = pd.concat([w1_walked, _frame(["A"], wave="w2")])
    out = id_walk(df, ui)
    assert _ids(out, "w1") == ["Q", "T", "U"]
    assert _ids(out, "w2") == ["B"]


def test_raises_on_a_mixed_wave_when_both_sides_show_their_class():
    """A concat of a walked and an unwalked frame of the SAME wave, where the
    walked side holds a walked-only id (U) and the raw side a raw-only id (P):
    renaming would walk the walked rows twice, not renaming would serve raw
    ids.  When one side shows no id of its own class the mix is NOT caught --
    see the next test."""
    walked = id_walk(_frame(["X", "T"]), RENUMBERED)
    walked.attrs = {}
    mixed = pd.concat([walked, _frame(["P"])])
    with pytest.raises(IdWalkError, match="mixed"):
        id_walk(mixed, RENUMBERED)


def test_raises_on_a_target_held_by_a_household_that_keeps_its_id():
    """A raw household already holding a non-moving target: a crosswalk
    collision, which a rename would turn into a duplicate."""
    ui = {"w1": {"A": "B", "P": "Q"}}
    with pytest.raises(IdWalkError, match="mixed"):
        id_walk(_frame(["A", "B"]), ui)   # B is raw here, and A's target


def test_raises_when_only_chained_ids_are_present():
    """Ids that are both a moving key and a target, and nothing else: raw T
    must become U, walked T must stay -- the ids cannot say which this is."""
    with pytest.raises(IdWalkError, match="cannot be told"):
        id_walk(_frame(["T"]), RENUMBERED)


def test_a_mixed_wave_escapes_when_one_side_holds_only_chained_ids():
    """The stated LIMIT of detection (GH #1245 review), pinned so a change to
    it is seen.  No code path builds such a frame today (0 mixed waves in
    1,250 measured reads), but these are the two ways it goes wrong."""
    # walked {P -> Q} beside raw {T}: no raw-only id, so the wave reads as
    # walked and is left alone -- raw T is served unwalked (should be U).
    walked = id_walk(_frame(["P"]), RENUMBERED)
    walked.attrs = {}
    out = id_walk(pd.concat([walked, _frame(["T"])]), RENUMBERED)
    assert _ids(out) == ["Q", "T"]
    # walked {X -> T} beside raw {P}: no walked-only id, so the wave reads as
    # raw and is renamed -- the walked T moves on to U (walked twice).
    walked = id_walk(_frame(["X"]), RENUMBERED)
    walked.attrs = {}
    out = id_walk(pd.concat([walked, _frame(["P"])]), RENUMBERED)
    assert _ids(out) == ["Q", "U"]


def test_state_raw_renames_an_undecidable_wave():
    """A caller that KNOWS the frame is unwalked (stage 2 of load_from_waves,
    derivation_inputs) says so, and a small raw frame of only chained ids is
    renamed instead of refused: raw T is the household that moves to U."""
    out = id_walk(_frame(["T"]), RENUMBERED, state="raw")
    assert _ids(out) == ["U"]
    out = id_walk(_frame(["T", "S"]), RENUMBERED, state="raw")
    assert _ids(out) == ["S", "U"]


def test_state_raw_still_leaves_a_detectably_walked_wave_alone():
    """state='raw' resolves only the undecidable case.  A frame that is walked
    by its own ids (here: reaching stage 2 unflagged, e.g. through the
    country-parquet fall-through) is still not walked twice, and a mixed wave
    is still refused."""
    once = id_walk(_frame(["X", "T", "P", "S"]), RENUMBERED)
    again = once.copy()
    again.attrs = {}
    pd.testing.assert_frame_equal(id_walk(again, RENUMBERED, state="raw"), once)
    mixed = pd.concat([again, _frame(["P"])])
    with pytest.raises(IdWalkError, match="mixed"):
        id_walk(mixed, RENUMBERED, state="raw")


def test_state_rejects_an_unknown_value():
    with pytest.raises(ValueError, match="state"):
        id_walk(_frame(["X"]), RENUMBERED, state="walked")


def test_an_index_only_frame_is_walked():
    """A frame with rows but no columns is `df.empty` to pandas.  The old guard
    returned it untouched and flagged -- which is how the panel diagnostics'
    spine (index only) never reached the walk (GH #1245 review, R1)."""
    df = _frame(["X", "T", "P", "S"])[[]]
    assert df.empty and len(df) == 4
    walked = id_walk(df, RENUMBERED)
    assert _ids(walked) == ["Q", "S", "T", "U"]


def _bare_single_pass(df, updated_ids, hh_index="i", state="unknown"):
    """id_walk with the exactly-once detection removed (rows guard kept)."""
    if len(df.index) == 0 or "t" not in (df.index.names or []):
        return df
    parts = []
    for w in df.index.get_level_values("t").unique():
        d = df[df.index.get_level_values("t") == w]
        m = updated_ids.get(w) or {}
        parts.append(d.rename(index=m, level=hh_index) if m else d)
    return pd.concat(parts)


def test_the_panel_diagnostic_exercises_the_walk(monkeypatch):
    """`diagnostics._check_id_walk_idempotent` re-walks the served spine (an
    index-only frame).  It passes with the real id_walk and FAILS with the
    detection removed -- so the check is not vacuous."""
    from types import SimpleNamespace
    from lsms_library import diagnostics
    import lsms_library.local_tools as lt

    spine = id_walk(_frame(["X", "T", "P", "S"])[[]], RENUMBERED)
    spine.attrs = {}

    def country():
        return SimpleNamespace(name="Fixtureland", updated_ids=RENUMBERED,
                               _diagnostics_spine_cache=(spine.copy(), "fixture"))

    assert diagnostics._check_id_walk_idempotent(country()).status == "pass"
    monkeypatch.setattr(lt, "id_walk", _bare_single_pass)
    check = diagnostics._check_id_walk_idempotent(country())
    assert check.status == "fail", check


def test_id_walk_error_is_not_swallowed_by_the_fallback_catches():
    """country.py's derived-table and legacy paths catch KeyError, ValueError
    and RuntimeError and retry another build; this must not be one of them."""
    assert not issubclass(IdWalkError, (KeyError, ValueError, RuntimeError))


# ---------------------------------------------------------------------------
# The flag: still set, and it survives the cache
# ---------------------------------------------------------------------------


def test_id_walk_sets_id_converted_attr():
    assert id_walk(_frame(["X"]), RENUMBERED).attrs.get("id_converted") is True


@pytest.mark.parametrize("stamped", [False, True], ids=["plain", "stamped"])
def test_the_flag_survives_a_parquet_round_trip(tmp_path, stamped):
    """Both `to_parquet` routes (a wave script's plain write and the L2-country
    write that embeds a cache hash) carry `attrs` in the parquet's pandas
    metadata, and `get_dataframe` restores them -- which is why a warm read of
    a table whose own build walked it does not reach `id_walk` at all
    (measured on d353b7ea7 across the 11 panel countries).  Pinned so a pandas
    or pyarrow upgrade that stops doing this is seen; correctness would then
    rest on the detection above, at the cost of a classification per read."""
    walked = id_walk(_frame(["X", "T", "P", "S"]), RENUMBERED)
    path = tmp_path / "t.parquet"
    to_parquet(walked, path, absolute_path=True,
               cache_hash="h" if stamped else None)
    back = get_dataframe(path)
    assert back.attrs.get("id_converted") is True
