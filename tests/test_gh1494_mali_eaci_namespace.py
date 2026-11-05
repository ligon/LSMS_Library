"""GH #1494: Mali's EAC-I households carry a per-wave prefix (rulings 2026-11-04).

2014-15 and 2017-18 are EAC-I; 2018-19 and 2021-22 are EHCVM, a separate programme
with no household link.  Both number clusters and households from 1, so the two
spelled the same ``i`` for unrelated households, up to 3,613 of them.  The prefix
is Burkina Faso's ``EMC_`` precedent (GH #1244).  It is per wave because the two EAC-I
waves are not linked either, and shared 3,560 ids with no household behind them.

The first two classes need no microdata.  ``TestServed`` builds real tables, so it
carries ``requires_s3``, and its oracle spells the household id itself from the raw
covers, importing nothing from ``Mali/_/``.
"""
from __future__ import annotations

import ast
import json

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

ROOT = countries_root() / "Mali"
EACI = ("2014-15", "2017-18")
EHCVM = ("2018-19", "2021-22")
PREFIX = {"2014-15": "EACI14_", "2017-18": "EACI17_"}


def _digits(cluster, household):
    return f"{int(cluster)}0{int(household):02d}"


class TestFormatters:
    @pytest.mark.parametrize("wave", EACI)
    def test_an_eaci_wave_prefixes_every_yaml_table(self, wave):
        i = ll.Country("Mali")[wave].formatting_functions["i"]
        assert i(pd.Series([12, 3])) == PREFIX[wave] + "12003"

    @pytest.mark.parametrize("wave", EHCVM)
    def test_an_ehcvm_wave_keeps_the_bare_panel_id(self, wave):
        i = ll.Country("Mali")[wave].formatting_functions["i"]
        assert i(pd.Series([12, 3])) == "12003"

    @pytest.mark.parametrize("wave", EACI)
    def test_the_person_id_is_not_namespaced(self, wave):
        pid = ll.Country("Mali")[wave].formatting_functions["pid"]
        assert pid(pd.Series([12, 3, 4])) == "120030" + "04"


class TestEveryConsumerSharesTheNamespace:
    """A script on the bare ``i`` would orphan its table from ``sample``."""

    @pytest.mark.parametrize("wave", EACI)
    def test_no_eaci_wave_script_imports_the_bare_i(self, wave):
        scripts = 0
        for path in sorted((ROOT / wave / "_").glob("*.py")):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.ImportFrom) and node.module == "mali":
                    names = {a.name for a in node.names}
                    assert "i" not in names, path.name
                    if {"eaci14_i", "eaci17_i"} & names:
                        scripts += 1
        assert scripts >= 5

    @pytest.mark.parametrize("wave", EHCVM)
    def test_no_ehcvm_wave_script_imports_the_prefix(self, wave):
        for path in sorted((ROOT / wave / "_").glob("*.py")):
            text = path.read_text(encoding="utf-8")
            assert "eaci14_i" not in text and "eaci17_i" not in text, path.name

    def test_the_crosswalk_names_no_eaci_household(self):
        """Empty maps for the EAC-I waves: id_walk leaves their ids alone."""
        updated = json.loads((ROOT / "_" / "updated_ids.json").read_text())
        assert updated["2014-15"] == {} and updated["2017-18"] == {}
        panel = json.loads((ROOT / "_" / "panel_ids.json").read_text())
        assert not [k for k in panel if k.split(",")[0] in EACI or "EACI" in k]


class TestSamplingRecord:
    def test_the_programme_switch_is_declared_and_warns(self):
        from lsms_library.sampling import INCOMPARABLE_CLUSTER_IDS, sampling_records
        recs = sampling_records("Mali")
        assert set(recs) == {"2017-18", "2018-19", "2021-22"}
        switch = recs["2018-19"]
        assert switch.household_continuity == "independent"
        assert switch.cluster_id_continuity in INCOMPARABLE_CLUSTER_IDS
        for wave in ("2017-18", "2021-22"):
            assert recs[wave].cluster_id_continuity not in INCOMPARABLE_CLUSTER_IDS
        assert recs["2021-22"].household_continuity == "rotating-panel"


@pytest.mark.requires_s3
class TestServed:
    @pytest.fixture(scope="class")
    def sample(self):
        return ll.Country("Mali").sample().reset_index()

    @pytest.mark.parametrize("wave,fn,household", [
        ("2014-15", "EACICONTROLE_p1.dta", "menage"),
        ("2017-18", "eaci17_s00p1.dta", "exploitation"),
    ])
    def test_the_ids_are_the_covers_own_with_the_prefix(self, sample, wave, fn, household):
        cover = get_dataframe(str(ROOT / wave / "Data" / fn))
        expected = {PREFIX[wave] + _digits(g, h)
                    for g, h in zip(cover["grappe"], cover[household])}
        assert set(sample.loc[sample.t == wave, "i"].astype(str)) == expected

    def test_no_id_is_shared_across_the_programmes(self, sample):
        eaci = set(sample.loc[sample.t.isin(EACI), "i"].astype(str))
        ehcvm = set(sample.loc[sample.t.isin(EHCVM), "i"].astype(str))
        assert eaci and ehcvm and not (eaci & ehcvm)
        assert not any(x.startswith("EACI") for x in ehcvm)

    def test_no_id_is_shared_between_the_eaci_waves(self, sample):
        first = set(sample.loc[sample.t == "2014-15", "i"].astype(str))
        second = set(sample.loc[sample.t == "2017-18", "i"].astype(str))
        assert first and second and not (first & second)
        assert all(x.startswith(PREFIX["2014-15"]) for x in first)
        assert all(x.startswith(PREFIX["2017-18"]) for x in second)

    @pytest.mark.parametrize("table", ["household_roster", "food_coping", "plot_inputs",
                                       "plot_labor", "livestock", "people_last7days",
                                       "crop_production", "housing", "shocks"])
    def test_every_table_joins_the_sample(self, sample, table):
        frame = getattr(ll.Country("Mali"), table)().reset_index()
        for wave in EACI:
            ids = set(frame.loc[frame.t == wave, "i"].astype(str))
            if ids:
                assert all(x.startswith(PREFIX[wave]) for x in ids), (table, wave)
                assert ids & set(sample.loc[sample.t == wave, "i"].astype(str)), (table, wave)

    @pytest.mark.parametrize("wave", EACI)
    def test_the_tree_inputs_still_join_the_served_rows(self, wave):
        country = ll.Country("Mali")
        inputs = country.derivation_inputs(
            "Mali::crop_production::tree-twelve-month-total", wave=wave)
        served = country.crop_production().reset_index()
        # GH #1457: the tree key; a 2017-18 field row can carry the sale key instead.
        served = served[(served.t == wave) & served.Derivation.astype("string").str.contains(
            "tree-twelve-month-total", regex=False).fillna(False)]
        assert set(inputs.index.get_level_values("i")) >= set(served.i.astype(str))

    def test_the_ehcvm_panel_is_untouched(self):
        assert len(ll.Country("Mali").panel_ids) == 5011
