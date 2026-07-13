"""GH #323 -- a non-unique declared index must never be collapsed SILENTLY.

The historical `_normalize_dataframe_index` reduced a duplicated canonical index
with ``groupby(level=...).first()``, discarding every row after the first in each
group.  Measured across the cached wave parquets that destroyed 7,244,929 rows,
of which 461,176 carried genuinely CONFLICTING payloads -- e.g. 30,800 distinct
people in Mali's 2014-15 household_roster, silently vaporized.  The collapse was
then baked into the L2-country parquet, so the (cold-build-only) warning never
fired again: the bug hid behind the cache it had poisoned.

The contract now (see SkunkWorks/grain_aggregation_policy.org, which requires the
access path to NEVER reduce grain):

  1. additive tables (food_acquired)  -> declared SUM reducer   (unchanged)
  2. byte-identical duplicate rows    -> dropped (LOSSLESS; provably identical
                                         to .first(), which ignores multiplicity)
  3. conflicting-payload duplicates   -> DuplicateIndexError    (loud, not silent)

Guinea-Bissau's cluster_features is the CONTROL for (2): a household-grained
cover page projected onto the cluster grain (t, v), so all 4,960 duplicate rows
are byte-identical repeats of the cluster's own attributes -> 450 clusters, no
loss, no warning.  Uganda's cluster_features is the counterexample: same shape,
but Latitude/Longitude genuinely differ across households in the same cluster.

NB every test drives the real entry point ``_normalize_dataframe_index`` and
imports the new symbols LAZILY, so this module still COLLECTS against the
pre-fix code -- the conflict tests must fail there by observing rows silently
disappear, not by an ImportError.
"""
import numpy as np
import pandas as pd
import pytest

from lsms_library.country import _normalize_dataframe_index

SCHEME = {"index": "(t, v)"}
FOOD_SCHEME = {"index": "(t, v, i, j, u, s)"}


def _frame(rows):
    return pd.DataFrame(rows).set_index(["t", "v"])


def _duplicate_index_error():
    from lsms_library.country import DuplicateIndexError
    return DuplicateIndexError


class _no_323_warning:
    """Assert no GH #323 data-loss warning is emitted (the control must be silent)."""

    def __enter__(self):
        import warnings
        self._ctx = warnings.catch_warnings(record=True)
        self._rec = self._ctx.__enter__()
        warnings.simplefilter("always")
        return self

    def __exit__(self, *exc):
        leaked = [str(r.message) for r in self._rec if "#323" in str(r.message)]
        self._ctx.__exit__(*exc)
        assert not leaked, f"control case must not warn: {leaked}"
        return False


# --------------------------------------------------------------------------
# (3) the bug itself: conflicting payloads must never vanish quietly
# --------------------------------------------------------------------------
def test_conflicting_rows_are_never_silently_collapsed():
    """THE #323 REGRESSION. Pre-fix this silently returns 1 row of 2.

    Stated without reference to any new symbol, so it runs -- and fails -- on
    the pre-fix code rather than erroring at import.
    """
    bad = _frame([
        {"t": "2014-15", "v": "A", "Region": "Kayes"},
        {"t": "2014-15", "v": "A", "Region": "Segou"},   # a DISTINCT observation
    ])

    try:
        out = _normalize_dataframe_index(bad, SCHEME, "2014-15", "cluster_features")
    except Exception as exc:                       # noqa: BLE001 -- see assert
        assert type(exc).__name__ == "DuplicateIndexError", exc
        assert "#323" in str(exc)
        return

    pytest.fail(
        f"GH #323: 2 conflicting rows were silently collapsed to {len(out)} "
        f"row(s) -- {2 - len(out)} distinct observation(s) destroyed with no error."
    )


def test_conflict_is_not_masked_by_byte_identical_repeats():
    """A conflict hidden among redundant repeats must still be caught."""
    bad = _frame([
        {"t": "2014-15", "v": "A", "Region": "Kayes"},
        {"t": "2014-15", "v": "A", "Region": "Kayes"},   # redundant repeat
        {"t": "2014-15", "v": "A", "Region": "Kayes"},   # redundant repeat
        {"t": "2014-15", "v": "A", "Region": "Segou"},   # the real conflict
    ])
    with pytest.raises(_duplicate_index_error()):
        _normalize_dataframe_index(bad, SCHEME, "2014-15", "cluster_features")


def test_error_message_names_the_table_and_the_disagreeing_column():
    bad = _frame([
        {"t": "2014-15", "v": "A", "Region": "Kayes", "Rural": "Rural"},
        {"t": "2014-15", "v": "A", "Region": "Segou", "Rural": "Rural"},
    ])
    with pytest.raises(_duplicate_index_error()) as e:
        _normalize_dataframe_index(bad, SCHEME, "2014-15", "cluster_features")

    msg = str(e.value)
    assert "cluster_features" in msg
    assert "2014-15" in msg
    assert "Region" in msg          # the column that actually disagrees
    assert "Rural" not in msg.split("disagreeing column(s):")[1].split("]")[0]
    assert "1 row(s)" in msg        # must not overstate the loss


# --------------------------------------------------------------------------
# (2) the Guinea-Bissau control: byte-identical repeats collapse losslessly
# --------------------------------------------------------------------------
def test_byte_identical_duplicates_are_dropped_losslessly_and_silently():
    df = _frame([
        {"t": "2018-19", "v": "A", "Region": "Bafata", "Rural": "Rural"},
        {"t": "2018-19", "v": "A", "Region": "Bafata", "Rural": "Rural"},   # repeat
        {"t": "2018-19", "v": "A", "Region": "Bafata", "Rural": "Rural"},   # repeat
        {"t": "2018-19", "v": "B", "Region": "Oio", "Rural": "Urban"},
    ])
    with _no_323_warning():
        out = _normalize_dataframe_index(df, SCHEME, "2018-19", "cluster_features")

    assert len(out) == 2
    assert out.index.is_unique
    assert out.loc[("2018-19", "A"), "Region"] == "Bafata"
    assert out.loc[("2018-19", "B"), "Rural"] == "Urban"


# --------------------------------------------------------------------------
# (1) additive tables: the halving trap
# --------------------------------------------------------------------------
def test_additive_table_sums_and_is_NOT_deduplicated():
    """THE TRAP: two byte-identical food_acquired rows are two REAL transactions.

    De-duplicating them before the sum would silently HALVE the household's
    quantity and expenditure.  The additive path must never dedup.
    """
    df = pd.DataFrame([
        {"t": "2018-19", "v": "A", "i": "1", "j": "Rice", "u": "kg", "s": "purchased",
         "Quantity": 2.0, "Expenditure": 10.0},
        {"t": "2018-19", "v": "A", "i": "1", "j": "Rice", "u": "kg", "s": "purchased",
         "Quantity": 2.0, "Expenditure": 10.0},
    ]).set_index(["t", "v", "i", "j", "u", "s"])

    out = _normalize_dataframe_index(df, FOOD_SCHEME, "2018-19", "food_acquired")

    assert len(out) == 1
    assert out.iloc[0]["Quantity"] == 4.0        # summed, NOT 2.0
    assert out.iloc[0]["Expenditure"] == 20.0


# --------------------------------------------------------------------------
# invariants the fix must preserve
# --------------------------------------------------------------------------
def test_legacy_lossy_collapse_available_only_via_explicit_opt_in(monkeypatch):
    df = _frame([
        {"t": "2014-15", "v": "A", "Region": "Kayes"},
        {"t": "2014-15", "v": "A", "Region": "Segou"},
    ])
    monkeypatch.setenv("LSMS_INDEX_COLLAPSE", "warn")

    with pytest.warns(RuntimeWarning, match="#323"):
        out = _normalize_dataframe_index(df, SCHEME, "2014-15", "cluster_features")

    assert len(out) == 1                      # lossy, as of old
    assert out.iloc[0]["Region"] == "Kayes"


def test_nan_keyed_rows_are_dropped_as_before():
    """Legacy groupby(dropna=True) dropped NaN-key groups; keep that no-op.

    Those rows are a separate silent-loss class -- not #323 -- and resurfacing
    them here would inject NaN into the index and change countries this fix does
    not own (GhanaLSS 2016-17 food_security: 110 NaN-keyed rows).
    """
    df = _frame([
        {"t": "2018-19", "v": "A", "Region": "Bafata"},
        {"t": "2018-19", "v": "A", "Region": "Bafata"},
        {"t": "2018-19", "v": np.nan, "Region": "???"},
        {"t": "2018-19", "v": np.nan, "Region": "???"},
    ])
    out = _normalize_dataframe_index(df, SCHEME, "2018-19", "cluster_features")

    assert len(out) == 1
    assert out.index.get_level_values("v").tolist() == ["A"]


def test_attrs_survive_the_dedup():
    """_finalize_result's id_converted flag must not be dropped (pandas 2.x)."""
    df = _frame([
        {"t": "2018-19", "v": "A", "Region": "Bafata"},
        {"t": "2018-19", "v": "A", "Region": "Bafata"},
    ])
    df.attrs["id_converted"] = True

    out = _normalize_dataframe_index(df, SCHEME, "2018-19", "cluster_features")

    assert out.attrs.get("id_converted") is True
