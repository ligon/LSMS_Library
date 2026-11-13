"""GH #1705: Benin is a panel country, EHCVM 2018-19 -> 2021-22, from PanelHH.

Rulings (@ligon): 2026-11-12, "Make Benin a panel country": a committed design-A
crosswalk built from ``PanelHH``; 2026-11-13, "Unlink the 63": one registered
exception.  The rule (``_/panel_ids.py``): a 2021-22 household is linked to the
2018-19 household with the same ``(grappe, menage)`` when the 2021-22 cover flags
it ``PanelHH = Panel``, that key is a 2018-19 household, and its 2021-22 roster
does not record its whole pre-loaded 2018-19 roster as gone
(``Benin::panel_ids::all-preloaded-left-not-linked``).  The 2021-22 files carry no
2018-19 identifier of their own.  2021-22 ``panel_weight`` is ``poids`` on the
linked households and NA on the others (ruling 2026-11-13, "NA for unlinked").

``TestCommittedCrosswalk`` reads only the committed JSON and needs no microdata.
It pins what makes a single-pass ``id_walk`` safe here: every key carries the
2021-22 prefix and no target does, so there is no chain and no rename onto an
occupied id (CLAUDE.md, "Panel ID Transitive Chains").

``TestAgainstTheFiles`` (``requires_s3``) re-derives the crosswalk from the raw
files with its own rule, measures the link against the rosters, and reads the
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
LINKED = 6815            # 6,878 PanelHH = Panel with a 2018-19 key, less the 63 excepted
EXCEPTED = 63


def _committed():
    with open(C_DIR / "updated_ids.json", encoding="utf-8") as fh:
        updated = json.load(fh)
    with open(C_DIR / "panel_ids.json", encoding="utf-8") as fh:
        panel = json.load(fh)
    return updated, panel


def _generator():
    """``_/panel_ids.py`` as a module.  Its data reads and writes sit under
    ``if __name__ == '__main__'``, so importing it reads and writes nothing."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("benin_panel_ids_for_test", C_DIR / "panel_ids.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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

    def test_the_generator_passes_the_committed_crosswalk(self):
        """panel_ids.check_single_pass, executed on what is committed."""
        gen = _generator()
        links = _committed()[0][NEW]
        targets = set(links.values())
        gen.check_single_pass(links, baseline=targets | {"999001"},
                              current=set(links) | {PREFIX + "999001"})

    @pytest.mark.parametrize("hazard,message", [
        ("chain", "chained"),
        ("occupied", "held by a 2021-22 household"),
        ("shared target", "claim one 2018-19 household"),
        ("not a 2018-19 household", "not a 2018-19 household"),
        ("not the same key", "not the same"),
    ])
    def test_the_generator_refuses_each_hazard(self, hazard, message):
        """Each guard fires, by its own message, on a map built to break it.  A chain
        cannot arise from real covers under the prefix, so it is made by hand: a key
        that is also another key's target, each key still its target's prefixed form."""
        gen = _generator()
        links = {PREFIX + "1001": "1001", PREFIX + "1002": "1002"}
        baseline = {"1001", "1002"}
        current = set(links)
        if hazard == "chain":
            links = {PREFIX + PREFIX + "1001": PREFIX + "1001", PREFIX + "1001": "1001"}
            baseline = {PREFIX + "1001", "1001"}
            current = set(links)
        elif hazard == "occupied":
            current = current | {"1001"}          # a 2021-22 household holding a target
        elif hazard == "shared target":
            links = {**links, PREFIX + "1003": "1001"}
        elif hazard == "not a 2018-19 household":
            baseline = {"1001"}
        elif hazard == "not the same key":
            links = {PREFIX + "1001": "1002"}
        with pytest.raises(AssertionError, match=message):
            gen.check_single_pass(links, baseline, current)

    def test_the_generator_links_by_panelhh_and_the_2018_key(self):
        """panel_ids.link on invented files: Panel and recurring links; Panel without a
        2018-19 key, Non panel recurring, Non panel new, and a Panel household whose
        whole pre-loaded roster is gone (the registered exception) keep their own id."""
        gen = _generator()
        old = pd.DataFrame({"grappe": [1, 1, 2, 3], "menage": [1, 2, 1, 1]})
        new = pd.DataFrame({"grappe": [1, 1, 2, 2, 3], "menage": [1, 2, 1, 9, 1],
                            "PanelHH": ["Panel", "Non panel", "Panel", "Non panel", "Panel"]})
        # s01q00aa: 1 still a member, 2 gone, NaN not pre-loaded.  (3, 1): every
        # pre-loaded member gone, plus one new member -> excepted.  (2, 1): one of two
        # gone -> linked.  (1, 1): no pre-load at all -> linked (PanelHH stays the rule).
        roster = pd.DataFrame({"grappe": [1, 2, 2, 3, 3, 3], "menage": [1, 1, 1, 1, 1, 1],
                               "s01q00aa": [float("nan"), 1, 2, 2, 2, float("nan")]})
        links, baseline, current, counts = gen.link(old, new, roster)
        assert links == {PREFIX + "1001": "1001", PREFIX + "2001": "2001"}
        assert counts == {"panel_no_target": 0, "recurring_non_panel": 1,
                          "all_preloaded_left": 1, "linked": 2, "own_id": 3}
        assert gen.all_preloaded_left(roster) == {(3, 1)}
        gen.check_single_pass(links, baseline, current)
        new.loc[3, "PanelHH"] = "Panel"
        assert gen.link(old, new, roster)[3]["panel_no_target"] == 1
        bad = roster.assign(s01q00aa=[float("nan"), 1, 2, 3, 2, float("nan")])
        with pytest.raises(AssertionError, match="s01q00aa codes"):
            gen.link(old, new, bad)
        new.loc[3, "PanelHH"] = "Oui"           # a vocabulary the rule was not written for
        with pytest.raises(AssertionError, match="PanelHH vocabulary"):
            gen.link(old, new, roster)

    def test_the_exception_is_registered(self):
        from lsms_library.derivations import derivation_records, resolve_callable
        rec = derivation_records("Benin")["Benin::panel_ids::all-preloaded-left-not-linked"]
        assert rec.rows == 0 and list(rec.waves) == [NEW] and rec.table == "panel_ids"
        assert resolve_callable(rec.function).__name__ == "all_preloaded_left"
        assert callable(resolve_callable(rec.inputs))
        assert _generator().EXPECTED["all_preloaded_left"] == EXCEPTED


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

    @pytest.fixture(scope="class")
    def roster(self):
        s21 = get_dataframe(countries_root() / "Benin" / NEW / "Data" / "s01_me_ben2021.dta",
                            convert_categoricals=False)
        return s21.assign(key=[_key(g, m) for g, m in zip(s21["grappe"], s21["menage"])])

    def test_the_committed_crosswalk_is_the_rule_on_the_files(self, covers, roster):
        new, old = covers
        panel = new["PanelHH"].astype(str).str.strip()
        assert set(panel) == {"Panel", "Non panel"}
        recurs = new["key"].isin(set(old["key"]))
        pre = roster[roster["s01q00aa"].notna()]
        gone = set(pre.groupby("key")["s01q00aa"].apply(lambda x: bool((x == 2).all()))
                   .loc[lambda s: s].index)
        excepted = (panel == "Panel") & recurs & new["key"].isin(gone)
        expected = {PREFIX + k: k for k in new.loc[(panel == "Panel") & recurs & ~excepted, "key"]}
        assert _committed()[0][NEW] == expected
        # The counts the PR reports.
        assert int((panel == "Panel").sum()) == 6889
        assert int(((panel == "Panel") & recurs).sum()) == 6878
        assert int(excepted.sum()) == EXCEPTED
        assert len(expected) == LINKED
        assert int(((panel == "Panel") & ~recurs).sum()) == 11
        assert int(((panel != "Panel") & recurs).sum()) == 116
        assert int(((panel != "Panel") & ~recurs).sum()) == 1027

    def test_the_generator_on_the_files_is_the_committed_crosswalk(self, covers, roster):
        """panel_ids.link and check_single_pass, executed on the real files (the work
        `make panel-ids` does, without the write)."""
        new, old = covers
        gen = _generator()
        links, baseline, current, counts = gen.link(old.drop(columns="key"), new.drop(columns="key"),
                                                    roster.drop(columns="key"))
        gen.check_single_pass(links, baseline, current)
        assert links == _committed()[0][NEW]
        assert counts == gen.EXPECTED == {"panel_no_target": 11, "recurring_non_panel": 116,
                                          "all_preloaded_left": EXCEPTED, "linked": LINKED,
                                          "own_id": 1217}

    def test_the_excepted_inputs_are_the_63_households(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = ll.Country("Benin").derivation_inputs(
                "Benin::panel_ids::all-preloaded-left-not-linked", wave=NEW)
        ids = set(inputs.index.get_level_values("i"))
        assert len(ids) == EXCEPTED and all(i.startswith(PREFIX) for i in ids)
        assert not ids & set(_committed()[0][NEW])
        pre = inputs[inputs["s01q00aa"].notna()]
        assert (pre["s01q00aa"] == 2).all()
        assert (inputs["PanelHH"].astype(str).str.strip() == "Panel").all()

    def test_the_2021_cover_records_no_2018_id(self, covers):
        """The link has to be the key itself: nothing else on the cover points back."""
        new, _ = covers
        assert not [c for c in new.columns
                    if c.startswith("s00q07f") or "ehcvm1" in c.lower() or "2018" in c]

    def test_the_link_agrees_with_the_2018_roster(self, covers):
        """Linked heads against another 2018-19 household of the same grappe."""
        new, old = covers
        s18 = get_dataframe(countries_root() / "Benin" / OLD / "Data" / "s01_me_ben2018.dta",
                            convert_categoricals=False)
        s21 = get_dataframe(countries_root() / "Benin" / NEW / "Data" / "s01_me_ben2021.dta",
                            convert_categoricals=False)
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
        # Measured about 0.95 on the link against 0.68 for the control (0.836 on the 116
        # recurring Non-panel keys).  The floor is set at 0.90, not at the measurement:
        # what this guards against is a crosswalk onto the WRONG households, which falls
        # to control level, not a drift of a few households in a regeneration.
        assert agree > 0.90 and control < 0.75 and agree - control > 0.2, (agree, control)

    def test_served_ids_walk_once_keep_their_cluster_and_weigh_only_the_linked(self):
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
        # Ruling 2026-11-13, "NA for unlinked": 2021-22 panel_weight on the linked only.
        new = s[s["t"] == NEW].set_index("i")
        linked = new.index.isin(set(_committed()[0][NEW].values()))
        assert int(linked.sum()) == LINKED
        assert new.loc[linked, "panel_weight"].notna().all()
        assert new.loc[~linked, "panel_weight"].isna().all() and int((~linked).sum()) == 8032 - LINKED
        assert new["weight"].notna().all()
        assert s.loc[s["t"] == OLD, "panel_weight"].notna().all()

    def test_check_panel_consistency(self):
        from lsms_library.diagnostics import check_panel_consistency
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            report = check_panel_consistency(ll.Country("Benin"))
        assert report.ok, report
