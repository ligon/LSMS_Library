"""Panel-id rename collisions: two households must never share one canonical id.

GH #548.  ``id_walk`` applies a wave's ``updated_ids`` map with a single
``DataFrame.rename``.  If the map is **many-to-one** — two live current-wave
households told to rename onto the same id — the rename silently merges them,
and ``Country._normalize_dataframe_index``'s ``groupby(...).first()`` /
additive ``sum()`` then collapses the duplicate ``(i, t)`` tuples.  That
happens *inside* ``load_from_waves``, before the L2-country parquet is
written, so the loss is baked into the cache and every warm read is silent.

The library's guard is :func:`lsms_library.local_tools.update_id`, which mints
the ``base_N`` split suffix (``101332`` / ``101332_1``) when several current
households claim one previous household — the Malawi IHPS convention.  A
country whose bespoke ``_/panel_ids.py`` writes ``updated_ids.json`` *directly*
bypasses that guard; GhanaLSS did, and paid for it:

- ``food_acquired`` for (i=101332, t=1988-89) reported **15,850** — the sum of
  two different households' food expenditure (5,570 + 10,280);
- ``household_roster`` reported household 101332 as a 6-person household whose
  members were entirely household 204922's — household 204932 (an 83-year-old
  head and three others) was **erased from the data**.

The invariant test below is the cross-country instrument: it reads every
shipped ``updated_ids.json`` and fails on any many-to-one map.  Before the fix
it flags GhanaLSS 1988-89 (2 targets); every other country passes, which is
what makes a negative result meaningful.
"""

from __future__ import annotations

import json
import os
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.diagnostics import _is_split_of
from lsms_library.local_tools import id_walk, update_id
from lsms_library.paths import countries_root


# ---------------------------------------------------------------------------
# The invariant, over every shipped updated_ids.json  (hermetic: no data)
# ---------------------------------------------------------------------------


def _countries_with_shipped_updated_ids() -> list[str]:
    root = countries_root()
    return sorted(
        p.parent.parent.name
        for p in root.glob("*/_/updated_ids.json")
    )


def _many_to_one(mapping: dict[str, str]) -> dict[str, list[str]]:
    """Targets claimed by more than one current-wave household."""
    inv: dict[str, list[str]] = {}
    for cur, new in mapping.items():
        inv.setdefault(new, []).append(cur)
    return {new: curs for new, curs in inv.items() if len(curs) > 1}


@pytest.mark.parametrize("country", _countries_with_shipped_updated_ids())
def test_shipped_updated_ids_are_injective(country):
    """No two households may be renamed onto the same canonical id.

    A many-to-one entry is *silently wrong*, not loudly broken: id_walk
    merges the two households and groupby().first() keeps one of each
    colliding pair, so the survivor carries a mixture of two households'
    rows and the other household disappears.  Splits are legitimate — they
    just have to be spelled with ``update_id``'s ``base_N`` suffix so the
    two lineages keep distinct ids.
    """
    path = countries_root() / country / "_" / "updated_ids.json"
    with open(path) as f:
        updated_ids = json.load(f)

    offenders = {
        wave: _many_to_one(mapping)
        for wave, mapping in updated_ids.items()
        if mapping and _many_to_one(mapping)
    }
    assert not offenders, (
        f"{country}: {sum(len(v) for v in offenders.values())} rename target(s) "
        f"claimed by more than one household -- id_walk will merge them and "
        f"groupby().first() will silently drop the loser. Route the linkage "
        f"through local_tools.update_id (GH #548). Offenders: {offenders}"
    )


# ---------------------------------------------------------------------------
# The machinery contract that the bespoke scripts must not bypass
# ---------------------------------------------------------------------------


def test_update_id_mints_split_suffix_for_many_to_one():
    """Two current households claiming one previous id get ``X`` and ``X_1``."""
    raw = {"204932": "101332", "204922": "101332", "204933": "101334"}
    updated, _ = update_id(dict(raw), {})
    assert updated["204932"] == "101332"
    assert updated["204922"] == "101332_1"
    assert updated["204933"] == "101334"  # untouched single-source entry
    assert len(set(updated.values())) == len(updated), "map must stay injective"


def test_id_walk_merges_households_under_a_raw_many_to_one_map():
    """The hazard itself: a raw many-to-one map produces duplicate (i, t).

    This is what the bespoke GhanaLSS script handed to ``id_walk``.  The
    framework is not at fault -- it faithfully applies the map it is given.
    """
    df = pd.DataFrame(
        {"Expenditure": [5570.0, 10280.0]},
        index=pd.MultiIndex.from_tuples(
            [("1988-89", "204932"), ("1988-89", "204922")], names=["t", "i"]
        ),
    )
    raw = {"204932": "101332", "204922": "101332"}
    walked = id_walk(df.copy(), {"1988-89": raw})
    assert walked.index.duplicated().sum() == 1, (
        "raw many-to-one map should collide -- if this ever stops being true, "
        "the premise of GH #548 changed"
    )
    # ... and the collapse the framework then applies loses a household.
    collapsed = walked.groupby(level=["t", "i"]).first()
    assert len(collapsed) == 1
    assert collapsed["Expenditure"].iloc[0] in (5570.0, 10280.0)


def test_id_walk_keeps_both_households_when_map_is_update_id_processed():
    """The fix: route the same linkage through ``update_id`` and nothing is lost."""
    df = pd.DataFrame(
        {"Expenditure": [5570.0, 10280.0]},
        index=pd.MultiIndex.from_tuples(
            [("1988-89", "204932"), ("1988-89", "204922")], names=["t", "i"]
        ),
    )
    guarded, _ = update_id({"204932": "101332", "204922": "101332"}, {})
    walked = id_walk(df.copy(), {"1988-89": guarded})
    assert walked.index.duplicated().sum() == 0
    assert set(walked.index.get_level_values("i")) == {"101332", "101332_1"}
    assert walked.xs("101332", level="i")["Expenditure"].sum() == 5570.0
    assert walked.xs("101332_1", level="i")["Expenditure"].sum() == 10280.0


# ---------------------------------------------------------------------------
# Diagnostics: a ``base_N`` split is the convention, not an inconsistency
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cur,prev,expected",
    [
        ("101332_1", "101332", True),
        ("101332_12", "101332", True),
        ("101332", "101332", False),   # identity: equal, not a split
        ("101332_1", "101333", False),  # different base
        ("1013321", "101332", False),   # no underscore
        ("101332_x", "101332", False),  # suffix must be numeric
    ],
)
def test_is_split_of(cur, prev, expected):
    assert _is_split_of(cur, prev) is expected


# ---------------------------------------------------------------------------
# GhanaLSS end-to-end: the two split households survive the API
# ---------------------------------------------------------------------------


def _aws_creds_available() -> bool:
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (
        Path(__file__).parent.parent
        / "lsms_library" / "countries" / ".dvc" / "s3_creds"
    )
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


needs_data = pytest.mark.skipif(
    not _aws_creds_available(),
    reason="needs the GhanaLSS build (S3 / warm cache); unit-tests CI job skips",
)


@needs_data
def test_ghanalss_split_household_food_not_summed_into_one():
    """(101332, 1988-89) must be 5,570 -- not 5,570 + 10,280."""
    import lsms_library as ll

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fa = ll.Country("GhanaLSS").food_acquired()

    def spend(i):
        try:
            return float(
                fa.xs(i, level="i").xs("1988-89", level="t")["Expenditure"].sum()
            )
        except KeyError:
            return float("nan")

    assert spend("101332") == pytest.approx(5570.0), (
        "household 101332's 1988-89 food expenditure is the sum of two "
        "different households (GH #548)"
    )
    assert spend("101332_1") == pytest.approx(10280.0), (
        "the split-off household 101332_1 is missing from food_acquired"
    )


@needs_data
def test_ghanalss_split_household_roster_not_collapsed():
    """The split households keep their own members (4 and 6, not one set of 6)."""
    import lsms_library as ll

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        roster = ll.Country("GhanaLSS").household_roster()

    def members(i):
        try:
            return roster.xs(i, level="i").xs("1988-89", level="t")
        except KeyError:
            return roster.iloc[0:0]

    base, split = members("101332"), members("101332_1")
    assert len(base) == 4, "household 204932 (-> 101332) has 4 members"
    assert len(split) == 6, "household 204922 (-> 101332_1) has 6 members"
    # The 83-year-old head of 204932 was erased by the collapse.
    assert base["Age"].max() == 83

    for i, n in [("114008", 10), ("114008_1", 6)]:
        assert len(members(i)) == n, f"household {i} should have {n} members"
