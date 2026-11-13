"""GH #1459: one design-PSU label across Uganda's eight waves.

``sample.ea`` starts in 2013-14, and in 2005-06 to 2011-12 ``v`` is already the
EA, under the 8-digit frame code ``comm``.  The ruling (@ligon, 2026-11-04) fills
those waves through the 212-to-212 ``comm`` to ``ea`` link, registered; an EA that
rotated out before any household could link it gets its own ``comm``, which cannot
collide with 1-430; a ``v`` that is not an 8-digit frame code stays NA.

The table is the reviewed ``comm_ea`` in ``Uganda/_/categorical_mapping.org``, so
``sample`` reads no further cover in those waves.  The ``requires_s3`` oracle
re-derives it from the raw covers and imports nothing from ``Uganda/_/``.  The rank
check reads no household at all.  Config tests need no data.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.local_tools import CATEGORICAL_KEY_ATTR, all_dfs_from_orgfile, format_id, get_dataframe
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

UGANDA = countries_root() / "Uganda"
ORG = UGANDA / "_" / "categorical_mapping.org"
LINKED = "Uganda::sample::ea-comm-link"
OWN = "Uganda::sample::ea-own-comm"
SAME = "Uganda::sample::ea-same-household"
EARLY = ("2005-06", "2009-10", "2010-11", "2011-12")
#: (linked rows, linked EAs, own-comm rows, own-comm EAs, NA), measured on the covers.
#: 2009-10's NA was 569 until GH #1536 filled 514 of them from the same household's
#: label in another wave (key SAME, below); the comm counts do not move.
SERVED = {"2005-06": (2066, 212, 1057, 110, 0), "2009-10": (1612, 210, 798, 107, 55),
          "2010-11": (1802, 212, 913, 109, 1), "2011-12": (1917, 212, 933, 109, 57)}


def _cover(wave, name="GSEC1.dta"):
    return get_dataframe(str(UGANDA / wave / "Data" / name), convert_categoricals=False)


@pytest.fixture(scope="module")
def table():
    t = all_dfs_from_orgfile(ORG)["comm_ea"].copy()
    return t.assign(comm8=t["comm8"].map(format_id), ea=t["ea"].map(format_id))


@pytest.fixture(scope="module")
def uga():
    import lsms_library as ll
    return ll.Country("Uganda", preload_panel_ids=False)


@pytest.fixture(scope="module")
def comm_ea(uga):
    return uga["2005-06"].formatting_functions["comm_ea"]


class TestTable:

    def test_key_is_declared(self):
        assert all_dfs_from_orgfile(ORG)["comm_ea"].attrs.get(CATEGORICAL_KEY_ATTR) == "comm8"

    def test_one_to_one_and_in_the_panel_range(self, table):
        assert len(table) == 212
        assert table["comm8"].is_unique and table["ea"].is_unique
        assert table["comm8"].str.fullmatch(r"\d{8}").all()
        assert table["ea"].astype(int).between(1, 322).all()

    def test_the_linked_households_are_counted(self, table):
        assert table["hh linked"].astype(int).sum() == 1717

    def test_the_label_rises_with_the_frame_code(self, table):
        """The no-data half of the rank check below."""
        assert table.sort_values("comm8")["ea"].astype(int).is_monotonic_increasing


class TestTheFunction:

    def test_a_linked_frame_code_takes_its_number(self, table, comm_ea):
        row = table.iloc[0]
        assert comm_ea(row["comm8"]) == row["ea"]

    def test_the_int32_rendering_is_the_same_code(self, table, comm_ea):
        row = table.iloc[0]
        assert comm_ea(int(row["comm8"])) == row["ea"]

    def test_a_seven_character_comm_goes_through_the_frame_table_first(self, table, comm_ea):
        frame = all_dfs_from_orgfile(ORG)["comm_frame_ea"]
        pairs = dict(zip(frame["comm8"].map(format_id), frame["comm7"].map(format_id)))
        row = table.loc[table["comm8"].isin(pairs)].iloc[0]
        assert comm_ea(pairs[row["comm8"]]) == row["ea"]

    def test_an_unlinked_frame_code_is_its_own_label(self, table, comm_ea):
        frame = all_dfs_from_orgfile(ORG)["comm_frame_ea"]["comm8"].map(format_id)
        unlinked = sorted(set(frame) - set(table["comm8"]))[0]
        assert comm_ea(unlinked) == unlinked

    @pytest.mark.parametrize("value", [None, "", "1530040", "@0.35,+32.60", "unlocated-x"])
    def test_anything_else_is_na(self, comm_ea, value):
        assert comm_ea(value) is None

    def test_it_takes_the_one_column_row_the_mappings_form_hands_it(self, table, comm_ea):
        row = table.iloc[0]
        assert comm_ea(pd.Series({"comm": row["comm8"]})) == row["ea"]


class TestRegistry:

    @pytest.mark.parametrize("key", [LINKED, OWN])
    def test_each_branch_is_registered_for_the_early_waves(self, key):
        from lsms_library.derivations import records_for
        rec = records_for("Uganda", "sample")[key]
        assert tuple(rec.waves) == EARLY
        assert tuple(rec.columns) == ("ea",)

    def test_nothing_later_reads_comm_for_ea(self):
        for wave in ("2013-14", "2015-16", "2018-19", "2019-20"):
            info = load_yaml(UGANDA / wave / "_" / "data_info.yml")
            assert "comm_ea" not in repr(info["sample"])


# ------------------------------------------------------------------ the oracle --

@pytest.fixture(scope="module")
def link():
    """``{comm: ea}``, from the two covers: the rule, not the table."""
    new, old = _cover("2013-14"), _cover("2011-12")
    linked = new.assign(key=new["HHID_old"].map(format_id)).dropna(subset=["key"]) \
        .merge(old.assign(key=old["HHID"].map(format_id))[["key", "comm"]], on="key")
    pairs = linked[["comm", "ea"]].drop_duplicates()
    return {str(int(c)): str(int(e)) for c, e in zip(pairs["comm"], pairs["ea"])}


@pytest.fixture(scope="module")
def sample():
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country("Uganda").sample()


@pytest.mark.requires_s3
class TestAgainstTheSourceFiles:

    def test_the_table_is_the_link(self, table, link):
        assert dict(zip(table["comm8"], table["ea"])) == link

    def test_ea_is_the_rank_of_comm_among_the_frame_eas(self, table):
        """Reads no household's link: only the sorted frame codes of 2005-06."""
        frame = sorted({str(int(c)) for c in _cover("2005-06")["comm"]})
        assert len(frame) == 322
        rank = {c: str(n + 1) for n, c in enumerate(frame)}
        assert dict(zip(table["comm8"], table["ea"])) == {c: rank[c] for c in table["comm8"]}

    def test_every_later_panel_label_has_a_comm(self, table):
        """So the 110 unlinked EAs never appear late, and have nothing to bridge."""
        late = set(_cover("2013-14")["ea"].astype(int).astype(str))
        late |= set(_cover("2015-16", "gsec1.dta")["ea"].astype(int).astype(str))
        panel = {e for e in late if int(e) <= 322}
        assert panel == set(table["ea"])

    @pytest.mark.parametrize("wave", EARLY)
    def test_counts_and_stamps(self, sample, wave):
        frame = sample.xs(wave, level="t")
        der = frame["Derivation"].astype("string")
        linked, own = frame[der.str.contains(LINKED, regex=False).fillna(False)], \
            frame[der.str.contains(OWN, regex=False).fillna(False)]
        assert (len(linked), linked["ea"].nunique(), len(own), own["ea"].nunique(),
                int(frame["ea"].isna().sum())) == SERVED[wave]
        assert linked["ea"].str.fullmatch(r"\d{1,3}").all()
        assert own["ea"].str.fullmatch(r"\d{8}").all()
        assert (own["ea"] == own["v"]).all()

    @pytest.mark.parametrize("wave", EARLY)
    def test_a_v_that_is_not_a_frame_code_stays_na(self, sample, wave):
        """Unless GH #1536 filled it from the same household in another wave."""
        frame = sample.xs(wave, level="t")
        frame_code = frame["v"].astype("string").str.fullmatch(r"\d{8}").fillna(False)
        same = frame["Derivation"].astype("string").str.contains(SAME, regex=False).fillna(False)
        assert frame.loc[~frame_code & ~same, "ea"].isna().all()
        assert frame.loc[frame_code, "ea"].notna().all()
        assert not (frame_code & same).any()
        assert int(same.sum()) == (514 if wave == "2009-10" else 0)

    def test_the_two_kinds_of_label_never_meet(self, sample):
        labels = sample["ea"].dropna().reset_index()
        early = set(labels.loc[labels["t"].isin(EARLY), "ea"])
        late = set(labels.loc[~labels["t"].isin(EARLY), "ea"])
        eight = {e for e in early if len(e) == 8}
        assert len(eight) == 110 and not eight & late
        assert all(1 <= int(e) <= 430 for e in late)
        assert len(early & late) == 212
        assert len(early | late) == 427

    @pytest.mark.parametrize("before, after", [
        ("2005-06", "2009-10"), ("2009-10", "2010-11"), ("2010-11", "2011-12"),
        ("2011-12", "2013-14"), ("2013-14", "2015-16"), ("2015-16", "2018-19"),
        ("2018-19", "2019-20")])
    def test_a_linked_household_keeps_its_label_across_every_boundary(self, sample, before, after):
        one = sample.xs(before, level="t")["ea"].dropna()
        two = sample.xs(after, level="t")["ea"].dropna()
        both = one.index.intersection(two.index)
        assert len(both) > 1000
        assert (one.loc[both] == two.loc[both]).all()

    @pytest.mark.parametrize("key", [LINKED, OWN])
    @pytest.mark.parametrize("wave", EARLY)
    def test_inputs_join_and_the_function_reproduces_the_value(self, sample, wave, key):
        import lsms_library as ll
        c = ll.Country("Uganda")
        inputs = c.derivation_inputs(key, wave=wave)
        frame = sample.xs(wave, level="t")
        stamped = frame.index[frame["Derivation"].astype("string")
                              .str.contains(key, regex=False).fillna(False)]
        assert set(inputs.index.get_level_values("i")) == set(stamped)
        derived = c.derivations()[key].resolve()(inputs).droplevel("t")
        assert derived.to_dict() == frame.loc[derived.index, "ea"].to_dict()

    def test_v_is_the_only_thing_the_join_carries(self):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            roster = ll.Country("Uganda").household_roster(waves=["2005-06"])
        assert "ea" not in roster.columns
        if "Derivation" in roster.columns:
            assert not roster["Derivation"].astype("string").str.contains(
                "ea-", regex=False).fillna(False).any()
