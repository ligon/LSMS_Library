"""GH #1705: Benin is a panel country, EHCVM 2018-19 -> 2021-22, from PanelHH.

Ruling (@ligon, 2026-11-12, "Make Benin a panel country"): a committed design-A
crosswalk built from ``PanelHH``.  The rule (``_/panel_ids.py``): a 2021-22
household is linked to the 2018-19 household with the same ``(grappe, menage)``
when the 2021-22 cover flags it ``PanelHH = Panel`` and that key is a 2018-19
household.  The 2021-22 files carry no 2018-19 identifier of their own.

``TestCommittedCrosswalk`` reads only the committed JSON and needs no microdata.
It pins what makes a single-pass ``id_walk`` safe here: every key carries the
2021-22 prefix and no target does, so there is no chain and no rename onto an
occupied id (CLAUDE.md, "Panel ID Transitive Chains").

``TestAgainstTheFiles`` (``requires_s3``) re-derives the crosswalk from the raw
covers with its own rule, measures the link against the rosters, and reads the
served tables.
"""
from __future__ import annotations

import json
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import _walk_partition, get_dataframe, id_walk
from lsms_library.paths import countries_root

C_DIR = countries_root() / "Benin" / "_"
OLD, NEW = "2018-19", "2021-22"
PREFIX = "E2_"
LINKED = 6878


def _committed():
    with open(C_DIR / "updated_ids.json", encoding="utf-8") as fh:
        updated = json.load(fh)
    with open(C_DIR / "panel_ids.json", encoding="utf-8") as fh:
        panel = json.load(fh)
    return updated, panel


class TestCommittedCrosswalk:

    def test_the_shape_of_the_two_files(self):
        updated, panel = _committed()
        assert set(updated) == {OLD, NEW}
        assert updated[OLD] == {}
        assert len(updated[NEW]) == LINKED
        assert panel == {f"{NEW},{k}": f"{OLD},{v}" for k, v in updated[NEW].items()}

    def test_every_link_is_the_same_household_key_and_no_two_share_a_target(self):
        links = _committed()[0][NEW]
        assert all(k == PREFIX + v for k, v in links.items())
        assert len(set(links.values())) == len(links)
        assert not any(v.startswith(PREFIX) for v in links.values())

    def test_a_single_pass_walk_has_no_chain_and_no_occupied_target(self):
        """The classes of local_tools._walk_partition: every key is raw-only, every
        target walked-only, and none is both, so a walked frame cannot be renamed again."""
        links = _committed()[0][NEW]
        raw_only, walked_only, chained = _walk_partition(links)
        assert chained == frozenset()
        assert raw_only == frozenset(links)
        assert walked_only == frozenset(links.values())

    def test_the_walk_is_exactly_once(self):
        updated = _committed()[0]
        linked_raw = sorted(updated[NEW])[:3]
        unlinked = PREFIX + "999999"           # no such household: a raw id with no link
        assert unlinked not in updated[NEW]
        idx = pd.MultiIndex.from_tuples(
            [(NEW, i) for i in linked_raw + [unlinked]] + [(OLD, updated[NEW][linked_raw[0]])],
            names=["t", "i"])
        df = pd.DataFrame({"x": range(len(idx))}, index=idx)
        once = id_walk(df, updated)
        assert list(once.index.get_level_values("i")) == (
            [updated[NEW][i] for i in linked_raw] + [unlinked, updated[NEW][linked_raw[0]]])
        twice = id_walk(once.copy(), updated)
        pd.testing.assert_frame_equal(twice, once)
        fresh = once.copy()
        fresh.attrs = {}                       # a derived table has lost the flag
        pd.testing.assert_frame_equal(id_walk(fresh, updated), once)

    def test_the_scheme_declares_it_and_the_prefix_is_still_the_waves(self):
        text = (C_DIR / "data_scheme.yml").read_text(encoding="utf-8")
        assert any(line.strip().startswith("panel_ids:") for line in text.splitlines())
        mapping = (countries_root() / "Benin" / NEW / "_" / "mapping.py").read_text(encoding="utf-8")
        assert f"ID_PREFIX = '{PREFIX}'" in mapping

    def test_the_generator_asserts_both_hazards(self):
        source = (C_DIR / "panel_ids.py").read_text(encoding="utf-8")
        assert "chained" in source and "occupied" in source
        assert "PanelHH" in source


def _key(g, m):
    return f"{int(g)}{int(m):03d}"


@pytest.mark.requires_s3
class TestAgainstTheFiles:

    @pytest.fixture(scope="class")
    def covers(self):
        new = get_dataframe(countries_root() / "Benin" / NEW / "Data" / "s00_me_ben2021.dta",
                            convert_categoricals=True)
        old = get_dataframe(countries_root() / "Benin" / OLD / "Data" / "s00_me_ben2018.dta")
        new = new.assign(key=[_key(g, m) for g, m in zip(new["grappe"], new["menage"])])
        old = old.assign(key=[_key(g, m) for g, m in zip(old["grappe"], old["menage"])])
        return new, old

    def test_the_committed_crosswalk_is_the_rule_on_the_files(self, covers):
        new, old = covers
        panel = new["PanelHH"].astype(str).str.strip()
        assert set(panel) == {"Panel", "Non panel"}
        recurs = new["key"].isin(set(old["key"]))
        expected = {PREFIX + k: k for k in new.loc[(panel == "Panel") & recurs, "key"]}
        assert _committed()[0][NEW] == expected
        # The counts the PR reports.
        assert int((panel == "Panel").sum()) == 6889
        assert int(((panel == "Panel") & ~recurs).sum()) == 11
        assert int(((panel != "Panel") & recurs).sum()) == 116
        assert int(((panel != "Panel") & ~recurs).sum()) == 1027

    def test_the_2021_cover_records_no_2018_id(self, covers):
        """The link has to be the key itself: nothing else on the cover points back."""
        new, _ = covers
        assert not [c for c in new.columns
                    if c.startswith("s00q07f") or "ehcvm1" in c.lower() or "2018" in c]

    def test_the_link_agrees_with_the_2018_roster(self, covers):
        """Linked heads against another 2018-19 household of the same grappe."""
        new, old = covers
        s18 = get_dataframe(countries_root() / "Benin" / OLD / "Data" / "s01_me_ben2018.dta")
        s21 = get_dataframe(countries_root() / "Benin" / NEW / "Data" / "s01_me_ben2021.dta")
        s21 = s21[s21["s01q00aa"] != 2]          # those who have left
        h18 = s18[s18["s01q02"] == 1].assign(key=lambda d: [_key(g, m) for g, m in zip(d["grappe"], d["menage"])])
        h21 = s21[s21["s01q02"] == 1].assign(key=lambda d: [_key(g, m) for g, m in zip(d["grappe"], d["menage"])])
        sex18 = h18.set_index("key")["s01q01"]
        sex21 = h21.set_index("key")["s01q01"]
        linked = [v for v in _committed()[0][NEW].values()]
        agree = (sex21.reindex(linked).to_numpy() == sex18.reindex(linked).to_numpy()).mean()
        by_grappe = old.groupby("grappe")["key"].apply(sorted).to_dict()
        other = []
        for g, k in zip(new["grappe"], new["key"]):
            rest = [x for x in by_grappe.get(g, []) if x != k]
            if rest:
                other.append((k, rest[len(k) % len(rest)]))
        control = (sex21.reindex([a for a, _ in other]).to_numpy()
                   == sex18.reindex([b for _, b in other]).to_numpy()).mean()
        assert agree > 0.94 and control < 0.75, (agree, control)

    def test_served_ids_walk_once_and_keep_their_cluster(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c = ll.Country("Benin")
            s = c.sample().reset_index()
            hc = c.household_characteristics()
        s["t"] = s["t"].astype(str)
        assert not s.duplicated(["i", "t"]).any()
        a = s[s["t"] == OLD].set_index("i")["v"].astype(str)
        b = s[s["t"] == NEW].set_index("i")["v"].astype(str)
        both = a.index.intersection(b.index)
        assert len(both) == LINKED
        assert (a.loc[both] == b.loc[both]).all()
        assert not (hc.index.get_level_values("v") == "Mover").any()

    def test_check_panel_consistency(self):
        from lsms_library.diagnostics import check_panel_consistency
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            report = check_panel_consistency(ll.Country("Benin"))
        assert report.ok, report
