"""GH #1133: Uganda's early-wave cluster id ``v`` is ONE rendering of ``comm``.

UNPS GSEC1 writes the same enumeration area two ways: an int32 8-digit code in
2005-06 and 2011-12, a 7-character string in 2009-10 and 2010-11.  Served
un-harmonised, one EA carried two ids and ``v`` continuity across 2005-06 ->
2009-10 and 2010-11 -> 2011-12 measured 0%.  The 7-character waves are now
served the 8-digit frame EA through the reviewed ``comm_frame_ea`` table in
``Uganda/_/categorical_mapping.org``, applied by ``uganda.comm_frame_ea`` on
``sample`` and ``cluster_features``.

The table is a table and not a digit rule: 50 of its 370 pairs re-code the
district field (2010-11 GSEC1 writes 2010/11 district definitions for those
EAs), so "drop the 4th digit" is wrong for them.  Two 8-digit EAs take two
7-character codes -- one EA under two district definitions, which the survey's
own community files pair on one row -- so both are mapped.  One 2010-11 code
(``1530040``) is on no linkage and in no community file: it is left as-is.

Config tests (no data) pin the table, the function and the YAML wiring; the
``requires_s3`` tests re-derive the table from the source files, check that
``sample`` and ``cluster_features`` produce the same ``v`` for every
household, and check the served frame.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.local_tools import all_dfs_from_orgfile, format_id
from lsms_library.paths import countries_root

UGANDA = countries_root() / "Uganda"
ORG = UGANDA / "_" / "categorical_mapping.org"
SEVEN_CHAR_WAVES = ("2009-10", "2010-11")
EIGHT_DIGIT_WAVES = ("2005-06", "2011-12")
#: One EA under two district definitions (2005/06 form, 2010/11 form).
TWO_DEFINITIONS = {"1070027": "10730027", "1150027": "10730027",
                   "3060002": "30630002", "3180002": "30630002"}
#: On no household linkage and in no community file: served as-is.
UNMAPPED = "1530040"
LINKAGE_COLUMNS = {("2005-06", "2009-10"): "hh 05-06/09-10",
                   ("2005-06", "2010-11"): "hh 05-06/10-11",
                   ("2009-10", "2011-12"): "hh 09-10/11-12",
                   ("2010-11", "2011-12"): "hh 10-11/11-12"}


def _drop4(code: str) -> str:
    """The 8-digit code without its 4th digit (the 2005/06-definitions form)."""
    return code[:3] + code[4:]


@pytest.fixture(scope="module")
def table() -> pd.DataFrame:
    t = all_dfs_from_orgfile(ORG)["comm_frame_ea"].copy()
    for col in ("comm7", "comm8"):
        t[col] = t[col].map(format_id)
    return t


@pytest.fixture(scope="module")
def uga():
    import lsms_library as ll
    return ll.Country("Uganda")


@pytest.fixture(scope="module")
def comm_frame_ea(uga):
    return uga["2010-11"].formatting_functions["comm_frame_ea"]


# --------------------------------------------------------------- the table --

class TestTable:

    def test_key_is_declared(self, table):
        from lsms_library.local_tools import CATEGORICAL_KEY_ATTR
        raw = all_dfs_from_orgfile(ORG)["comm_frame_ea"]
        assert raw.attrs.get(CATEGORICAL_KEY_ATTR) == "comm7"

    def test_shape_and_forms(self, table):
        assert len(table) == 370
        assert table.comm7.is_unique, "a 7-character code maps to ONE frame EA"
        assert table.comm7.str.fullmatch(r"\d{7}").all()
        assert table.comm8.str.fullmatch(r"\d{8}").all()
        assert table.Form.value_counts().to_dict() == {
            "drop 4th digit": 320, "district recode": 50}

    def test_form_column_is_honest(self, table):
        is_drop4 = table.comm8.map(_drop4) == table.comm7
        assert (is_drop4 == (table.Form == "drop 4th digit")).all()

    def test_region_digit_and_ea_serial_survive_every_pair(self, table):
        assert (table.comm7.str[0] == table.comm8.str[0]).all()
        assert (table.comm7.str[-4:] == table.comm8.str[-4:]).all()

    def test_a_digit_rule_would_be_wrong(self, table):
        """The point of a table: dropping/inserting a digit misses 50 EAs."""
        recode = table[table.Form == "district recode"]
        assert len(recode) == 50
        assert (recode.comm8.map(_drop4) != recode.comm7).all()
        row = table.set_index("comm7").loc["1140003"]
        assert row.comm8 == "11030003" and row.Form == "district recode"

    def test_one_ea_two_district_definitions(self, table):
        """Every re-coded EA has one code per district definition; within
        2010-11 itself only two EAs appear in both (the "non-bijective" pairs
        of the 2010-11 -> 2011-12 boundary), and both codes are mapped."""
        m = dict(zip(table.comm7, table.comm8))
        assert {k: m.get(k) for k in TWO_DEFINITIONS} == TWO_DEFINITIONS
        per_ea = table.groupby("comm8").Form.agg(sorted)
        assert per_ea.map(len).max() == 2
        two = per_ea[per_ea.map(len) == 2]
        assert len(two) == 49
        assert (two.map(tuple) == ("district recode", "drop 4th digit")).all()
        # The 50th re-coded EA has no 2009-10 household with a `comm`, so only
        # its 2010/11 form is ever written.
        lone = per_ea[per_ea.map(tuple) == ("district recode",)]
        assert list(lone.index) == ["30430016"]
        in_1011 = table[table["hh 10-11/11-12"] > 0].groupby("comm8").comm7.nunique()
        assert sorted(in_1011[in_1011 > 1].index) == ["10730027", "30630002"]

    def test_unwitnessed_code_is_not_in_the_table(self, table):
        assert UNMAPPED not in set(table.comm7)

    def test_every_row_is_witnessed(self, table):
        counts = table[list(LINKAGE_COLUMNS.values())]
        assert (counts.sum(axis=1) > 0).all()
        assert (table["Community files"] != "-").all()


# ------------------------------------------------------------ the function --

class TestFunction:

    def test_maps_scalar_and_one_column_row(self, comm_frame_ea):
        assert comm_frame_ea("1010002") == "10130002"
        assert comm_frame_ea(pd.Series({"comm": "1010002"})) == "10130002"
        assert comm_frame_ea(" 1140003 ") == "11030003"

    def test_both_definitions_reach_one_ea(self, comm_frame_ea):
        assert {k: comm_frame_ea(k) for k in TWO_DEFINITIONS} == TWO_DEFINITIONS

    def test_unknown_code_is_served_as_is(self, comm_frame_ea):
        assert comm_frame_ea(UNMAPPED) == UNMAPPED
        assert comm_frame_ea("10130002") == "10130002"   # already 8-digit

    def test_blank_stays_none_for_the_coordinate_bin(self, comm_frame_ea):
        for blank in ("", None, float("nan")):   # format_id's blanks
            assert comm_frame_ea(blank) is None
            assert comm_frame_ea(pd.Series({"comm": blank})) is None

    def test_refuses_more_than_one_source_column(self, comm_frame_ea):
        with pytest.raises(ValueError, match="ONE source column"):
            comm_frame_ea(pd.Series({"comm": "1010002", "h1aq1": "101"}))


# ------------------------------------------------------- the YAML wiring --

def _v_mapping(uga, wave, table):
    w = uga[wave]
    info = w.resources[table]
    info = info.get("df_main", info)
    cm = w.column_mapping(table, info)
    (file_map,) = [m for k, m in cm.items() if isinstance(m, dict) and "myvars" in m]
    return file_map["idxvars"].get("v") or file_map["myvars"].get("v")


class TestWiring:

    @pytest.mark.parametrize("wave", SEVEN_CHAR_WAVES)
    @pytest.mark.parametrize("table", ["sample", "cluster_features"])
    def test_seven_char_waves_route_v_through_the_table(self, uga, wave, table):
        source, fn = _v_mapping(uga, wave, table)
        assert source == ["comm"]
        assert getattr(fn, "__name__", None) == "comm_frame_ea"

    @pytest.mark.parametrize("wave", EIGHT_DIGIT_WAVES)
    @pytest.mark.parametrize("table", ["sample", "cluster_features"])
    def test_eight_digit_waves_are_untouched(self, uga, wave, table):
        source, fn = _v_mapping(uga, wave, table)
        assert source == "comm"
        assert getattr(fn, "__name__", None) != "comm_frame_ea"

    def test_boundary_is_declared_stable(self):
        from lsms_library.sampling import sampling_records
        recs = sampling_records("Uganda")
        assert recs["2011-12"].cluster_id_continuity == "stable"
        assert recs["2009-10"].cluster_id_continuity == "mixed"   # GPS bins
        assert recs["2010-11"].cluster_id_continuity == "mixed"   # GPS bins


# ------------------------------------------- data: re-derive the table --

def _raw(wave, basename, cols):
    from lsms_library.local_tools import get_dataframe
    df = get_dataframe(str(UGANDA / wave / "Data" / basename),
                       convert_categoricals=False)
    return df[cols].map(format_id)


@pytest.mark.requires_s3
def test_table_is_what_household_linkage_derives(table):
    """Re-derive every (comm7, comm8) pair and its witness counts from GSEC1.

    HHID is unchanged across 2005-06 .. 2011-12 (``Waves`` declares no id
    change; ``updated_ids.json`` has no early-wave keys), so a household
    present in two waves IS the linkage.  A hand-edit of the table fails here.
    """
    gsec1 = {w: _raw(w, "GSEC1.dta", ["HHID", "comm"]).dropna().set_index("HHID").comm
             for w in ("2005-06", "2009-10", "2010-11", "2011-12")}
    pairs = []
    for (a, b), col in LINKAGE_COLUMNS.items():
        j = pd.concat([gsec1[a].rename("a"), gsec1[b].rename("b")], axis=1,
                      join="inner").dropna()
        j.columns = ["comm8", "comm7"] if a in EIGHT_DIGIT_WAVES else ["comm7", "comm8"]
        g = j.groupby(["comm7", "comm8"]).size().rename(col)
        pairs.append(g)
    derived = pd.concat(pairs, axis=1).fillna(0).astype(int).reset_index()
    assert derived.comm7.is_unique
    c09 = _raw("2009-10", "CSECTION1.dta", ["comm", "comcod"])
    c10 = _raw("2010-11", "CSECTION1.dta", ["comm", "comcod"])
    c11 = _raw("2011-12", "CSEC1.dta", ["comm", "comcod"])

    def corroborated(s7, e8):
        hit = [y for y, c in (("09", c09), ("10", c10))
               if (((c.comm == s7) | (c.comcod == s7)) & (c.comm == _drop4(e8))).any()]
        if ((c11.comm == e8) & (c11.comcod == s7)).any():
            hit.append("11")
        return " ".join(hit) or "-"

    derived["Community files"] = [corroborated(s, e) for s, e in
                                  zip(derived.comm7, derived.comm8)]
    derived["Form"] = ["drop 4th digit" if _drop4(e) == s else "district recode"
                       for s, e in zip(derived.comm7, derived.comm8)]
    cols = ["comm7", "comm8", "Form", *LINKAGE_COLUMNS.values(), "Community files"]
    got = table[cols].sort_values("comm7").reset_index(drop=True)
    want = derived[cols].sort_values("comm7").reset_index(drop=True)
    got[list(LINKAGE_COLUMNS.values())] = got[list(LINKAGE_COLUMNS.values())].astype(int)
    pd.testing.assert_frame_equal(got, want)

    # Coverage: every real 7-character code either wave serves is mapped,
    # except the one unwitnessed code.
    served7 = set(gsec1["2009-10"]) | set(gsec1["2010-11"])
    assert served7 - set(table.comm7) == {UNMAPPED}


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", SEVEN_CHAR_WAVES)
def test_sample_and_cluster_features_give_every_household_one_v(uga, wave):
    """The same function on both routes: per household, identical ``v``.

    ``_join_v_from_sample`` hands ``sample``'s ``v`` to every other table, so
    a ``cluster_features`` key built any other way would match nothing.
    """
    from lsms_library.local_tools import df_data_grabber
    path = str(UGANDA / wave / "Data" / "GSEC1.dta")
    out = {}
    for table in ("sample", "cluster_features"):
        mapping = _v_mapping(uga, wave, table)
        df = df_data_grabber(path, {"i": ("HHID", format_id)}, v=mapping)
        out[table] = df.v
    a, b = out["sample"], out["cluster_features"]
    assert len(a) == len(b) and a.index.equals(b.index)
    assert (a.fillna("<NA>") == b.fillna("<NA>")).all()
    real = a.dropna()
    assert (real.str.len() == 8).sum() == len(real) - (real == UNMAPPED).sum()


# --------------------------------------------------- data: the served frame --

@pytest.fixture(scope="module")
def served(uga):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        s = uga.sample()
    s = s.reset_index()
    s["v"] = s["v"].astype("string")
    return s


def _keep(s, a, b):
    A = s[s.t == a].set_index("i").v
    B = s[s.t == b].set_index("i").v
    j = pd.concat([A.rename("a"), B.rename("b")], axis=1, join="inner")
    return int((j.a == j.b).fillna(False).sum()), len(j)


@pytest.mark.requires_s3
class TestServed:

    def test_seven_char_waves_serve_the_frame_ea(self, served):
        for wave in SEVEN_CHAR_WAVES:
            v = served[served.t == wave].v.dropna()
            # GH #1120 D2: a household no file locates is its own cluster,
            # `moved-{hhid}` -- not an EA code, so not this test's subject.
            real = v[~v.str.startswith("@") & ~v.str.startswith("moved-")]
            odd = set(real[real.str.len() != 8])
            n_odd = len(odd - {UNMAPPED})
            assert n_odd == 0, f"{wave}: {n_odd} non-8-digit EA code(s)"

    def test_2009_10_bins_and_gaps_untouched(self, served):
        v = served[served.t == "2009-10"].v
        bins = v[v.str.startswith("@").fillna(False)]
        # GH #1120 D2 (2026-10-23): the 24 "no comm, no GPS" households used
        # to be served v = NA; each is now its own `moved-{hhid}` cluster, and
        # so are the 4 households only AGSEC3B / GSEC12 record -- 28 in all.
        single = v[v.str.startswith("moved-").fillna(False)]
        got = (len(bins), bins.nunique(), int(v.isna().sum()), len(single),
               single.nunique())
        assert got == (541, 339, 0, 28, 28), got

    def test_2010_11_to_2011_12_keeps_every_linked_household(self, served):
        # GH #1120 D2: 2011-12's frame now holds the 57 households only its
        # agriculture cover (AGSEC1) records; 30 of them are 2010-11
        # households.  No 2011-12 file locates them, so each is its own
        # `moved-{hhid}` cluster and cannot keep its 2010-11 EA.  Every
        # household 2011-12's GSEC1 locates still keeps its v.
        located = served[~served.v.astype(str).str.startswith("moved-")]
        kept, linked = _keep(located, "2010-11", "2011-12")
        assert kept == linked == 2510, (kept, linked)
        _, linked_all = _keep(served, "2010-11", "2011-12")
        n_single = linked_all - linked
        assert n_single == 30, n_single

    def test_2005_06_to_2009_10_keeps_every_real_ea(self, served):
        kept, linked = _keep(served, "2005-06", "2009-10")
        v09 = served[served.t == "2009-10"].set_index("i").v
        # A bin, a GH #1120 D2 singleton (it was NA until 2026-10-23) and NA
        # are not real EAs.
        real = v09[~(v09.str.startswith("@") | v09.str.startswith("moved-")).fillna(True)]
        assert kept == len(real.index.intersection(
            served[served.t == "2005-06"].set_index("i").index))
        assert (kept, linked) == (2410, 2607)

    def test_the_harmonised_boundary_is_silent(self, uga):
        from lsms_library import sampling as S
        S._clear_sampling_reports()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            uga.sample(waves=["2010-11", "2011-12"])
        names = [type(w.message).__name__ for w in caught]
        assert "ClusterIdentityWarning" not in names
