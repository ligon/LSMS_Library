"""GH #833: Uganda's ``Aggregate`` keeps processing states apart.

The ruling (@ligon, 2026-11-04) splits ``Aggregate Label`` in ``harmonize_food``
where the state differs: Maize, Millet, Cassava, Sweet Potatoes, Ground Nuts and
Sim Sim.  Variety still folds.  ``harmonize_crop`` reuses the bare names so that
``crop_production.j`` joins ``food_acquired(labels='Aggregate').j``, so the bare name
stays, on the harvest-state bucket.  Config tests read the table and need no data.
"""
from __future__ import annotations

import re
import warnings

import pandas as pd
import pytest

from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root

ORG = countries_root() / "Uganda" / "_" / "categorical_mapping.org"
FAMILIES = ("Maize", "Millet", "Cassava", "Sweet Potatoes", "Ground Nuts", "Sim Sim")
#: The words of a Preferred Label that name a state, not a variety.
STATE = re.compile(r"\b(grains|cobs|flour|dry|fresh|in shell|shelled|pounded|paste)\b", re.I)
#: The state a family's bare Aggregate holds.  A label with no state word (``Millet``,
#: ``Sim Sim``) is the raw commodity, which is what ``None`` stands for here.
HARVEST_STATE = {"Maize": "grains", "Millet": None, "Cassava": "fresh",
                 "Sweet Potatoes": "fresh", "Ground Nuts": "in shell", "Sim Sim": None}


def _state(label):
    """The state a label names, with ``Cassava (dry/flour)`` as the one that is both."""
    found = {m.lower() for m in STATE.findall(label)}
    if found == {"dry", "flour"}:
        return "dry/flour"
    assert len(found) <= 1, label
    return found.pop() if found else None


@pytest.fixture(scope="module")
def food():
    t = all_dfs_from_orgfile(ORG)["harmonize_food"]
    return t[["Preferred Label", "Aggregate Label", "Nutrition Label"]].astype(str)


@pytest.fixture(scope="module")
def staples(food):
    pref = food["Preferred Label"]
    keep = pref.map(lambda p: any(p.startswith(f) for f in FAMILIES))
    return food[keep]


class TestTheColumn:

    def test_no_bucket_holds_two_states(self, staples):
        """``Cassava (dry/flour)`` is the instrument's one line for both, and shares
        ``Cassava (flour)`` as it always has; no other state meets another."""
        for agg, rows in staples.groupby("Aggregate Label"):
            states = set(rows["Preferred Label"].map(_state)) - {"dry/flour"}
            assert len(states) == 1, (agg, states)

    @pytest.mark.parametrize("family", FAMILIES)
    def test_each_family_still_has_its_bare_name_on_the_harvest_state(self, staples, family):
        bare = staples[staples["Aggregate Label"] == family]
        assert len(bare), family
        assert set(bare["Preferred Label"].map(_state)) == {HARVEST_STATE[family]}

    def test_variety_still_folds(self, food):
        agg = dict(zip(food["Preferred Label"], food["Aggregate Label"]))
        assert agg["Maize (white;flour)"] == agg["Maize (yellow;flour)"] == agg["Maize (flour)"]
        assert agg["Sweet Potatoes (white, fresh)"] == agg["Sweet Potatoes (orange, fresh)"] \
            == agg["Sweet Potatoes (fresh)"]

    def test_every_preferred_label_has_one_aggregate(self, food):
        """``_relabel_j`` is last-row-wins, so a second Aggregate would be silent.
        ``Cassava (flour)`` had two until this ruling (codes 182 and 1082)."""
        assert food.groupby("Preferred Label")["Aggregate Label"].nunique().max() == 1

    def test_a_state_bucket_is_named_for_its_family(self, staples):
        for agg in staples["Aggregate Label"].unique():
            assert any(agg == f or agg.startswith(f"{f} (") for f in FAMILIES), agg

    def test_the_nutrition_labels_did_not_move(self, staples):
        """They drive the FCT; the ruling is about ``Aggregate`` only."""
        flour = staples.set_index("Preferred Label")["Nutrition Label"]
        assert flour["Maize (flour)"] == "Maize (flour)"
        assert flour["Millet Flour"] == "Millet Flour"
        assert flour["Ground Nuts (paste)"] == "Ground Nuts (paste)"


class TestTheCropJoin:
    """``harmonize_crop``'s reuse of the bare names is a documented contract."""

    @pytest.mark.parametrize("family", FAMILIES)
    def test_the_crop_label_is_on_the_food_aggregate_axis(self, food, family):
        crops = set(all_dfs_from_orgfile(ORG)["harmonize_crop"]["Preferred Label"].astype(str))
        assert family in crops
        assert family in set(food["Aggregate Label"])


@pytest.mark.requires_s3
class TestServed:

    @pytest.fixture(scope="class")
    def uga(self):
        import lsms_library as ll
        return ll.Country("Uganda")

    @pytest.mark.parametrize("table, column", [("food_expenditures", "Expenditure"),
                                               ("food_quantities", "Quantity")])
    def test_the_relabel_conserves_the_measure(self, uga, table, column):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fine = getattr(uga, table)()
            coarse = getattr(uga, table)(labels="Aggregate")
        assert coarse[column].sum() == pytest.approx(fine[column].sum(), rel=1e-12)

    def test_food_acquired_only_renames(self, uga):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fine = uga.food_acquired()
            coarse = uga.food_acquired(labels="Aggregate")
        assert len(coarse) == len(fine)
        assert "labels_unmatched" not in coarse.attrs

    def test_flour_and_grain_no_longer_share_a_bucket(self, uga):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fine = uga.food_expenditures()["Expenditure"].groupby(level="j").sum()
            coarse = uga.food_expenditures(labels="Aggregate")["Expenditure"].groupby(level="j").sum()
        assert coarse["Maize"] == pytest.approx(fine["Maize (grains)"])
        assert coarse["Maize (flour)"] == pytest.approx(fine["Maize (flour)"])

    def test_the_crop_join_still_holds_on_the_served_axis(self, uga):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            crops = set(uga.crop_production().index.get_level_values("j"))
            foods = set(uga.food_acquired(labels="Aggregate").index.get_level_values("j"))
        assert set(FAMILIES) <= crops & foods
