"""GH #819: the cluster join must preserve a string ``v`` and see the walked ``i``.

Two defects in ``Country._finalize_result``'s cluster join left two thirds of
Ethiopia ESS 2013-14 / 2015-16 household rows with ``v = NaN``, and made the
surviving third the urban refreshment cohort:

1. ``_join_v_from_sample`` normalised ``v`` with ``str(int(float(x)))``, which
   rewrote a string id that was already canonical -- Ethiopia's zero-padded
   15-digit EA id ``'010101088801601'`` came back ``'10101088801601'`` and
   compared unequal to ``cluster_features.v``.  The coercion is now
   :func:`lsms_library.local_tools.format_id`, the same rule ``df_data_grabber``
   applies to every ``idxvars`` entry (and hence to ``cluster_features.v``).
2. the ``v`` join ran BEFORE ``id_walk`` re-keyed ``i``, while ``sample()`` --
   finalised through the same method -- was already walked.  Only households
   ``updated_ids`` never re-keyed could match.  ``id_walk`` now runs first.

Both are pinned with synthetic frames, so the tests cost no microdata.  The
fakes follow ``tests/test_finalize_order_797.py``: the REAL methods are bound
onto a ``SimpleNamespace`` and only what these tests do not exercise is stubbed.
"""
from __future__ import annotations

import warnings
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from lsms_library.country import Country as _CountryCls


ETHIOPIA_EA = "010101088801601"     # Ethiopia/_/CONTENTS.org, "Cluster (PSU) identifier"


def _fake_country(sample_v, name="Fixtureland", updated=None):
    """Minimal stand-in carrying the real ``_join_v_from_sample`` machinery.

    *sample_v* is ``{(i, t): raw_v}`` -- the ``sample()[['v']]`` frame the join
    caches on the instance, with ``v`` left at whatever dtype the survey gave.
    """
    idx = pd.MultiIndex.from_tuples(list(sample_v), names=["i", "t"])
    fake = SimpleNamespace(name=name)
    fake._sample_v_cache = pd.DataFrame({"v": list(sample_v.values())}, index=idx)
    fake.categorical_mapping = {}
    fake.data_scheme = ["sample"]
    fake._updated_ids_cache = updated
    fake.updated_ids = updated
    fake._augment_index_from_related_tables = lambda df, scheme_entry, wave: df
    fake._join_v_from_sample = _CountryCls._join_v_from_sample.__get__(fake)
    fake._apply_categorical_mappings = _CountryCls._apply_categorical_mappings.__get__(fake)
    fake._finalize_result = _CountryCls._finalize_result.__get__(fake)
    return fake


def _table(keys, wave="2015-16"):
    """A household-level frame indexed ``(i, t)``, one row per key."""
    return pd.DataFrame(
        {"Age": [30] * len(keys)},
        index=pd.MultiIndex.from_tuples([(i, wave) for i in keys], names=["i", "t"]),
    )


# --------------------------------------------------------------------------
# Defect 1 -- the coercion.  Every case named in GH #819 is pinned.
# --------------------------------------------------------------------------
CASES = {
    "h_str_zeropadded": (ETHIOPIA_EA, ETHIOPIA_EA),   # THE regression: zeros survive
    "h_float":          (1013.0,      "1013"),        # why the coercion exists at all
    "h_str_float":      ("1013.0",    "1013"),        # stringified float, same rule
    "h_str_plain":      ("1013",      "1013"),        # already canonical, untouched
    "h_int":            (1013,        "1013"),        # int64 v from a numeric survey
    "h_nan":            (np.nan,      pd.NA),
    "h_empty":          ("",          pd.NA),
}


def test_v_coercion_pins_every_case_in_gh819():
    fake = _fake_country({(k, "2015-16"): raw for k, (raw, _) in CASES.items()})
    out = fake._join_v_from_sample(_table(CASES))
    got = dict(zip(out.index.get_level_values("i"), out.index.get_level_values("v")))
    for key, (_, expected) in CASES.items():
        if expected is pd.NA:
            assert pd.isna(got[key]), f"{key}: expected NA, got {got[key]!r}"
        else:
            assert got[key] == expected, f"{key}: expected {expected!r}, got {got[key]!r}"


def test_leading_zeros_are_what_the_old_coercion_destroyed():
    """The counterfactual, run explicitly: ``str(int(float(x)))`` on the same id.

    Pins *why* `format_id` was reached for, rather than a bespoke predicate --
    the old expression is correct on the float it was written for and wrong on
    every zero-padded string id in the corpus.
    """
    assert str(int(float(ETHIOPIA_EA))) == "10101088801601" != ETHIOPIA_EA
    fake = _fake_country({("h1", "2015-16"): ETHIOPIA_EA})
    out = fake._join_v_from_sample(_table(["h1"]))
    assert out.index.get_level_values("v")[0] == ETHIOPIA_EA


def test_v_dtype_is_still_canonical_string():
    """GH #142's invariant is unchanged: mixed source dtypes -> one StringDtype."""
    fake = _fake_country({("h1", "2015-16"): 1013.0, ("h2", "2015-16"): ETHIOPIA_EA})
    out = fake._join_v_from_sample(_table(["h1", "h2"]))
    assert isinstance(out.index.get_level_values("v").dtype, pd.StringDtype)


def test_a_household_absent_from_sample_gets_NA_not_an_error():
    fake = _fake_country({("h1", "2015-16"): ETHIOPIA_EA})
    out = fake._join_v_from_sample(_table(["h1", "h2"]))
    v = dict(zip(out.index.get_level_values("i"), out.index.get_level_values("v")))
    assert v["h1"] == ETHIOPIA_EA and pd.isna(v["h2"])


# --------------------------------------------------------------------------
# Defect 2 -- the order.  `sample()` is walked; the table must be too, first.
# --------------------------------------------------------------------------
UPDATED = {"2013-14": {"old": "new"}}      # the panel cohort's re-key
WALKED_SAMPLE = {("new", "2013-14"): ETHIOPIA_EA,     # sample() is already walked
                 ("refresh", "2013-14"): "020202099902602"}   # never re-keyed


def test_id_walk_runs_before_the_v_join():
    fake = _fake_country(WALKED_SAMPLE, updated=UPDATED)
    out = fake._finalize_result(_table(["old"], wave="2013-14"), {}, "household_roster")
    assert list(out.index.get_level_values("i")) == ["new"], "i must be walked"
    assert list(out.index.get_level_values("v")) == [ETHIOPIA_EA], (
        "a re-keyed household must still find its cluster")


def test_the_old_order_matched_only_the_never_rekeyed_cohort():
    """Counterfactual: join first, and only the refreshment household matches.

    This is the shape of the Ethiopia failure -- 68.5% / 66.6% NaN in W2/W3,
    with the survivors urban -- reproduced on two rows.
    """
    fake = _fake_country(WALKED_SAMPLE, updated=UPDATED)
    unwalked = _table(["old", "refresh"], wave="2013-14")
    old_order = fake._join_v_from_sample(unwalked)
    v = dict(zip(old_order.index.get_level_values("i"),
                 old_order.index.get_level_values("v")))
    assert pd.isna(v["old"]), "the panel household used to lose its cluster"
    assert v["refresh"] == "020202099902602", "and the refreshment cohort survived"


def test_id_converted_is_set_once_and_survives_the_merge():
    """The `attrs` discipline (CLAUDE.md, "Panel ID Transitive Chains").

    `id_walk` sets the flag after its per-wave concat; `_join_v_from_sample`'s
    merge DISAGREES on attrs (test_population.py::TestVJoinIsADisagreeingMerge)
    and carries it over only via the explicit `result.attrs = dict(df.attrs)`.
    """
    fake = _fake_country(WALKED_SAMPLE, updated=UPDATED)
    out = fake._finalize_result(_table(["old"], wave="2013-14"), {}, "household_roster")
    assert out.attrs["id_converted"] is True


def test_an_already_walked_frame_is_not_walked_twice():
    """The flag still short-circuits: a pre-walked table joins on its own `i`."""
    fake = _fake_country(WALKED_SAMPLE, updated=UPDATED)
    pre = _table(["new"], wave="2013-14")
    pre.attrs["id_converted"] = True
    out = fake._finalize_result(pre, {}, "household_roster")
    assert list(out.index.get_level_values("i")) == ["new"]
    assert list(out.index.get_level_values("v")) == [ETHIOPIA_EA]


def test_the_silent_skip_warning_still_fires_on_a_fully_unmatched_wave():
    """GH #256's guard is untouched by the reorder: 100% NaN v still warns."""
    fake = _fake_country({("somebody-else", "2015-16"): ETHIOPIA_EA})
    with pytest.warns(UserWarning, match="100% NaN v after"):
        fake._join_v_from_sample(_table(["h1"]))
