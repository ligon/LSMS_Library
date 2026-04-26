"""Regression tests for ``id_walk`` idempotence and chain handling.

History
-------
Commit ``4db41a27`` ("Fix _join_v_from_sample: preserve df.attrs across
the v-join", 2026-04-10) patched a symptom of a deeper bug: ``id_walk``
itself is not idempotent under the legacy single-pass ``rename`` if
``updated_ids`` contains transitive chains (``A -> B -> C``).  The
``df.attrs['id_converted']`` flag was protecting callers from a second
application — and any helper that dropped ``attrs`` re-exposed the
hazard.  Burkina Faso 2021-22 produced 392 duplicate ``(i, t, v, pid)``
tuples; Niger and others had similar but smaller counts.

The structural fix lives in :func:`lsms_library.local_tools._close_id_map`:
each wave's mapping is closure-resolved before ``rename`` is applied,
so the result is in the post-walk space and a second application is a
no-op — ``attrs`` becomes a perf hint rather than a correctness gate.

These tests pin that contract.  They use synthetic data because the
shipped ``updated_ids`` dictionaries have all been pre-closed (the
panel-id generation pipeline computes the closure manually); the bug
mechanism still has to be defended against future contributions.
"""

from __future__ import annotations

import pandas as pd
import pytest

from lsms_library.local_tools import _close_id_map, id_walk


# ---------------------------------------------------------------------------
# _close_id_map
# ---------------------------------------------------------------------------


def test_close_id_map_resolves_two_step_chain():
    """``{A: B, B: C}`` collapses to ``{A: C, B: C}``."""
    closed = _close_id_map({"A": "B", "B": "C"})
    assert closed == {"A": "C", "B": "C"}


def test_close_id_map_resolves_long_chain():
    """A four-link chain still terminates at the canonical id."""
    closed = _close_id_map({"A": "B", "B": "C", "C": "D", "D": "E"})
    assert closed == {"A": "E", "B": "E", "C": "E", "D": "E"}


def test_close_id_map_preserves_self_mappings():
    """``{X: X}`` is harmless; closure must not strip or rewrite it."""
    closed = _close_id_map({"X": "X", "Y": "Z"})
    assert closed == {"X": "X", "Y": "Z"}


def test_close_id_map_preserves_unrelated_entries():
    """Entries with no chain neighbour pass through unchanged."""
    mapping = {"A": "B", "B": "C", "P": "Q", "M": "M"}
    assert _close_id_map(mapping) == {"A": "C", "B": "C", "P": "Q", "M": "M"}


def test_close_id_map_no_value_is_a_non_self_key():
    """Post-closure invariant: every value is either equal to its key
    (self-map) or absent from the key set.  This is the property that
    makes ``rename`` idempotent."""
    mapping = {"A": "B", "B": "C", "C": "D", "P": "Q", "M": "M"}
    closed = _close_id_map(mapping)
    keys = set(closed)
    for k, v in closed.items():
        if k != v:
            assert v not in keys, (
                f"closure leaked: {k!r} -> {v!r} but {v!r} is still a key"
            )


def test_close_id_map_detects_cycle():
    """Cycles indicate corrupt panel-id data; raise rather than spin."""
    with pytest.raises(ValueError, match="Cycle"):
        _close_id_map({"A": "B", "B": "A"}, wave="2099-99")


def test_close_id_map_detects_three_node_cycle():
    with pytest.raises(ValueError, match="Cycle"):
        _close_id_map({"A": "B", "B": "C", "C": "A"})


def test_close_id_map_empty():
    assert _close_id_map({}) == {}


# ---------------------------------------------------------------------------
# id_walk — the regression case the closure exists to fix
# ---------------------------------------------------------------------------


def _spine_with_chain():
    """Synthetic panel spine that exercises the chain-collision bug.

    Wave ``w1`` has rows for ids ``A`` and ``C``; ``updated_ids['w1']``
    declares the chain ``A -> B -> C``.

    Under the *legacy* single-pass rename, the first application yields
    ``[B, C]`` (A advanced to B; original C unchanged).  A *second*
    application would advance B -> C and collide with the surviving C —
    exactly the BF 2021-22 mechanism.  Under closure-resolved
    ``id_walk`` both A and B map directly to C in one pass, and a
    second pass is a no-op.
    """
    df = pd.DataFrame(
        {"val": [10, 30]},
        index=pd.MultiIndex.from_tuples(
            [("A", "w1"), ("C", "w1")], names=["i", "t"]
        ),
    )
    updated_ids = {"w1": {"A": "B", "B": "C"}}
    return df, updated_ids


def test_id_walk_idempotent_on_two_step_chain():
    """Running ``id_walk`` twice must produce the same result.

    This is the contract whose violation produced 392 duplicate tuples
    on Burkina Faso 2021-22 before commit ``4db41a27``.  The mechanism:
    a non-closed mapping ``{A: B, B: C}`` advances entries one chain
    link per pass, so a second application carries previously-rewritten
    rows further along the chain into the canonical id space, where
    they collide with rows that started canonical.

    We clear ``attrs`` between passes to remove the fast-path
    short-circuit in ``_finalize_result`` — the closure-based fix in
    :func:`_close_id_map` makes ``id_walk`` correct *without* relying
    on ``attrs`` for safety.
    """
    df, ui = _spine_with_chain()

    pass1 = id_walk(df.copy(), ui)
    pass1.attrs = {}  # simulate set_index/merge dropping the flag
    pass2 = id_walk(pass1, ui)

    # Pass 2 must be a no-op on the (i, t) index.  Whether or not pass
    # 1 produced duplicates is data-dependent (and a valid feature when
    # raw data legitimately contains alias rows for the same canonical
    # household); the *invariant* is that pass 2 doesn't change anything.
    assert sorted(pass1.index.tolist()) == sorted(pass2.index.tolist()), (
        f"id_walk is not idempotent: pass 1 -> {sorted(pass1.index.tolist())}, "
        f"pass 2 -> {sorted(pass2.index.tolist())}"
    )


def test_legacy_single_pass_rename_is_not_idempotent():
    """Documents the bug closure fixes — and why the closure is needed.

    ``pandas.DataFrame.rename(index=mapping)`` performs a single
    substitution per call, not a transitive walk.  On data ``[A, C]``
    with mapping ``{A: B, B: C}``:

      * Pass 1 advances A to B (one chain link), leaves C untouched.
      * Pass 2 then advances the surviving B to C, where it now
        collides with the original C — a duplicate manufactured purely
        by repeating the rename.

    Closure-resolving the mapping eliminates this: every value becomes
    terminal, so a second pass has nothing left to advance.
    """
    df = pd.DataFrame(
        {"v": [1, 2]},
        index=pd.MultiIndex.from_tuples(
            [("A", "w1"), ("C", "w1")], names=["i", "t"]
        ),
    )
    legacy_map = {"A": "B", "B": "C"}

    # Legacy single-pass behaviour: pass 2 introduces a duplicate that
    # pass 1 did not have.
    legacy_pass1 = df.rename(index=legacy_map, level="i")
    legacy_pass2 = legacy_pass1.rename(index=legacy_map, level="i")
    assert legacy_pass1.index.duplicated().sum() == 0
    assert legacy_pass2.index.duplicated().sum() == 1, (
        "Expected the documented BF mechanism: pass 2 introduces a "
        "duplicate via single-step rename advancing one more chain link"
    )

    # Closure-resolved mapping: stable from pass 1 onward.
    closed = _close_id_map(legacy_map)
    closed_pass1 = df.rename(index=closed, level="i")
    closed_pass2 = closed_pass1.rename(index=closed, level="i")
    assert sorted(closed_pass1.index.tolist()) == sorted(closed_pass2.index.tolist())


def test_id_walk_resolves_chain_in_one_pass():
    """A row whose id is the head of a chain lands on the terminal id,
    not on an intermediate link."""
    df, ui = _spine_with_chain()
    walked = id_walk(df.copy(), ui)
    ids = sorted(walked.index.get_level_values("i").tolist())
    # A and C both end at C; no row should remain at the intermediate B.
    assert ids == ["C", "C"], f"expected both rows at terminal id 'C', got {ids}"


def test_id_walk_passes_through_when_no_mapping_for_wave():
    """Waves absent from ``updated_ids`` are returned untouched."""
    df = pd.DataFrame(
        {"val": [1, 2]},
        index=pd.MultiIndex.from_tuples(
            [("A", "w1"), ("Z", "w2")], names=["i", "t"]
        ),
    )
    ui = {"w1": {"A": "B"}}  # no entry for w2
    walked = id_walk(df, ui)
    assert ("Z", "w2") in walked.index
    assert ("B", "w1") in walked.index


def test_id_walk_sets_id_converted_attr():
    """The fast-path flag is still set — losing it just isn't dangerous
    anymore."""
    df, ui = _spine_with_chain()
    walked = id_walk(df, ui)
    assert walked.attrs.get("id_converted") is True


def test_id_walk_handles_self_mapping_wave():
    """Wave with only self-mappings (the canonical post-fix shape of
    Burkina Faso 2021-22) is a no-op on the index."""
    df = pd.DataFrame(
        {"val": [1, 2]},
        index=pd.MultiIndex.from_tuples(
            [("X", "w1"), ("Y", "w1")], names=["i", "t"]
        ),
    )
    ui = {"w1": {"X": "X", "Y": "Y"}}
    walked = id_walk(df, ui)
    assert sorted(walked.index.get_level_values("i").tolist()) == ["X", "Y"]
    assert walked.index.duplicated().sum() == 0


def test_id_walk_idempotent_under_attrs_loss_simulation():
    """Belt-and-suspenders: even if a downstream helper drops ``attrs``
    (the failure mode that motivated commit ``4db41a27`` —
    ``set_index`` and ``merge`` shed it in pandas 2.x; behaviour is
    pandas-version-dependent so we force the loss explicitly here),
    re-running ``id_walk`` must produce the same result."""
    df, ui = _spine_with_chain()
    walked = id_walk(df.copy(), ui)

    # Round-trip through set_index (an attrs-dropping op in some pandas
    # versions) and then *force* attrs empty to simulate the hazard
    # regardless of the local pandas version.
    flat = walked.reset_index()
    walked2 = flat.set_index(["i", "t"])
    walked2.attrs = {}

    walked3 = id_walk(walked2, ui)
    assert sorted(walked.index.tolist()) == sorted(walked3.index.tolist())
