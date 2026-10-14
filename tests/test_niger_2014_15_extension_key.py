"""Niger 2014-15's household key is ``(GRAPPE, MENAGE, EXTENSION)`` -- GH #679.

ECVMA2 re-interviewed the 2011 households and records what became of each
in ``EXTENSION``: 0 = found in place, 1 = the whole household found after
moving, 2 = a SPLIT-OFF interviewed as a household of its own.  3,617
households share 3,558 ``(GRAPPE, MENAGE)`` pairs, so a two-column ``i``
merged 59 households into their parents and every within-household key
(member line, asset code, plot, species) collided -- 210 ``(t, i, pid)``
groups in the roster, 165 of them two POPULATED people.

Two families, split as in ``tests/test_niger_u_sentinel.py``:

* ``TestKeyIsDeclared`` needs NO microdata: it pins the config (every
  ``i:`` in the 2014-15 ``data_info.yml`` names EXTENSION, every wave script
  passes it, ``niger.i`` renders it) and the committed crosswalk (only
  extensions 0 and 1 link to 2011-12; no transitive chain; 3,529 links).
* ``TestServedKey`` builds the real tables and is skipped when Niger is
  unavailable.

The DECISION the crosswalk encodes -- split-offs are NEW households and do
not link to the 2011-12 parent -- is pinned here on purpose: a future edit
that links extension 2 would re-merge parent and split-off through
``id_walk`` and bring the collision back under another name.  See
``Niger/_/CONTENTS.org``, "the household key is (GRAPPE, MENAGE, EXTENSION)".
"""
from __future__ import annotations

import ast
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

COUNTRY = "Niger"
WAVE = "2014-15"
KEY = ["GRAPPE", "MENAGE", "EXTENSION"]

#: The five script-path tables of 2014-15 that build ``i`` themselves.
WAVE_SCRIPTS = ("crop_production.py", "livestock.py", "people_last7days.py",
                "plot_inputs.py", "plot_labor.py")

#: Cover-sheet facts (ECVMA2_MS00P1.dta, measured 2026-09-24).
HOUSEHOLDS = 3617
HOUSEHOLDS_BY_EXTENSION = {0: 3478, 1: 77, 2: 62}

# The ONE served household id that is legitimately outside sample()'s
# universe, named so the universe test stays exact (narrowed, not
# weakened): 2011-12 hid 27010 has two lines in the harvest module
# ecvmaas2e_p2.dta and none in the welfare, roster, plot or passage-2
# welfare files -- the 2-row NullIndexKeyWarning on crop_production's v.
# Recorded in Niger/_/CONTENTS.org ("2011-12 has one household in the
# harvest module that is in no other file", GH #679).  If the cell is
# fixed (or 27010 adjudicated as a mis-keyed 2701) the test fails until
# this entry is removed.
KNOWN_STRAY = {("crop_production", "2011-12"): {"27010"}}
#: 3,457 ext-0 + 77 ext-1 with a 2011-12 hid, minus 5 skipped by the
#: namespace guard in panel_ids.py.  3,251 under the old two-column key.
LINKED_2014_15 = 3529


def _root() -> Path:
    return countries_root() / COUNTRY


def _aws_creds_available() -> bool:
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (Path(__file__).parent.parent
                  / "lsms_library" / "countries" / ".dvc" / "s3_creds")
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


needs_data = pytest.mark.skipif(
    not (countries_root() / COUNTRY / "_" / "data_scheme.yml").exists()
    or os.environ.get("LSMS_SKIP_AUTH")
    or not _aws_creds_available(),
    reason=f"{COUNTRY} microdata unavailable (no config or no S3 credentials)",
)


# ---------------------------------------------------------------------------
# config-only
# ---------------------------------------------------------------------------

class TestKeyIsDeclared:

    @pytest.fixture(scope="class")
    def data_info(self) -> dict:
        with open(_root() / WAVE / "_" / "data_info.yml", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_every_i_in_the_wave_yaml_carries_all_three_columns(self, data_info):
        blocks = {k: v for k, v in data_info.items()
                  if isinstance(v, dict) and "idxvars" in v}
        assert blocks, "no idxvars blocks found in 2014-15 data_info.yml"
        seen = 0
        for table, spec in blocks.items():
            i = spec["idxvars"].get("i")
            if i is None:
                continue
            if isinstance(i, str):
                # cluster_features declares `i: MENAGE` only to license a
                # household-grain frame it then projects onto (t, v); that
                # is not a household id and is out of scope here.
                assert table == "cluster_features", (
                    f"{table}: `i` is the single column {i!r}; every "
                    f"household table must key on {KEY}")
                continue
            assert list(i) == KEY, f"{table}: i is {i}, expected {KEY}"
            seen += 1
        assert seen == 7, f"expected 7 household tables keyed on {KEY}, found {seen}"

    @pytest.mark.parametrize("script", WAVE_SCRIPTS)
    def test_every_wave_script_passes_extension_to_niger_i(self, script):
        src = (_root() / WAVE / "_" / script).read_text(encoding="utf-8")
        assert "r['EXTENSION']" in src, f"{script} builds i without EXTENSION"
        assert "index=['GRAPPE', 'MENAGE'])" not in src, (
            f"{script} still builds a two-column key somewhere")

    def test_niger_i_appends_the_extension_as_one_digit(self):
        sys.path.insert(0, str(_root() / "_"))
        try:
            from niger import i as niger_i
        finally:
            sys.path.pop(0)
        assert niger_i(pd.Series([2, 1, 2], index=KEY)) == "20012"
        assert niger_i(pd.Series([1, 1, 0], index=KEY)) == "10010"
        # The EHCVM shape is untouched.
        assert niger_i(pd.Series([2, 1], index=["grappe", "menage"])) == "E_2001"

    def test_crosswalk_links_only_extensions_0_and_1(self):
        with open(_root() / "_" / "updated_ids.json", encoding="utf-8") as f:
            updated = json.load(f)
        m = updated[WAVE]
        assert len(m) == LINKED_2014_15, (
            f"{len(m)} households linked 2014-15 -> 2011-12; measured "
            f"{LINKED_2014_15} (3,251 under the old two-column key)")
        assert all(len(k) >= 5 and k[-1] in "01" for k in m), (
            "a 2014-15 id whose last digit is not 0/1 is linked -- the "
            "split-off decision (extension 2 = new household) was reversed")
        assert not (set(m) & set(m.values())), (
            "a linked value is itself a key: transitive chain")
        # Baseline waves carry no mapping (id_walk leaves them alone).
        assert updated["2011-12"] == {} and updated["2018-19"] == {}

    def test_generator_names_the_decision(self):
        src = (_root() / "_" / "panel_ids.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        linked = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "LINKED_EXTENSIONS"
                    for t in node.targets):
                linked = ast.literal_eval(node.value)
        assert linked == {0, 1}, f"LINKED_EXTENSIONS is {linked!r}"
        assert re.search(r"DECISION.*GH #679", src), (
            "panel_ids.py must name the split-off decision")


# ---------------------------------------------------------------------------
# built tables
# ---------------------------------------------------------------------------

@needs_data
class TestServedKey:

    @pytest.fixture(scope="class")
    def country(self):
        import lsms_library as ll
        return ll.Country(COUNTRY)

    @pytest.fixture(scope="class")
    def sample_ids(self, country) -> dict[str, set]:
        s = country.sample()
        return {t: set(s.xs(t, level="t").index.get_level_values("i"))
                for t in country.waves}

    def test_2014_15_serves_every_household(self, sample_ids):
        assert len(sample_ids[WAVE]) == HOUSEHOLDS, (
            f"{len(sample_ids[WAVE])} households in sample 2014-15; the cover "
            f"sheet has {HOUSEHOLDS} (3,558 under the two-column key)")

    @pytest.mark.parametrize("table", ["household_roster", "individual_education",
                                       "people_last7days", "assets", "livestock",
                                       "crop_production", "plot_inputs", "plot_labor"])
    def test_every_wave_keys_into_the_sample_universe(self, country, sample_ids, table):
        """PER WAVE, never in aggregate -- a broken key produces zero
        duplicates and looks clean pooled (GH #679)."""
        df = getattr(country, table)()
        for t in country.waves:
            if t not in df.index.get_level_values("t"):
                continue
            ids = set(df.xs(t, level="t").index.get_level_values("i"))
            stray = ids - sample_ids[t]
            known = KNOWN_STRAY.get((table, t), set())
            assert stray == known, (
                f"{table} {t}: household ids not in sample() are {sorted(stray)[:5]} "
                f"({len(stray)}), expected exactly {sorted(known)} -- a NEW stray is a "
                f"broken key; a stray that has GONE means the cell was fixed and its "
                f"KNOWN_STRAY entry must be removed.  See Niger/_/CONTENTS.org, "
                f"'2011-12 has one household in the harvest module that is in no "
                f"other file' (GH #679).")

    def test_person_index_is_unique_in_every_wave(self, country):
        for table in ("household_roster", "people_last7days"):
            df = getattr(country, table)()
            dup = df.index.duplicated().sum()
            assert dup == 0, f"{table}: {dup} duplicated (t, i, pid) tuples"

    def test_split_offs_are_served_as_their_own_households(self, country):
        """Every extension-2 household of the cover sheet is served under its
        own (unlinked, hence un-walked) id -- including '20012', the one id
        that also spells a 2011-12 hid (CONTENTS.org, GH #679)."""
        from lsms_library.local_tools import format_id, get_dataframe
        cover = get_dataframe(
            str(_root() / WAVE / "Data" / "NER_2014_ECVMA-II_v02_M_STATA8"
                / "ECVMA2_MS00P1.dta"), convert_categoricals=False)
        split = cover.loc[cover["EXTENSION"] == 2, ["GRAPPE", "MENAGE"]]
        expected = {format_id(int(g)) + "0" + format_id(int(m), zeropadding=2) + "2"
                    for g, m in zip(split["GRAPPE"], split["MENAGE"])}
        assert len(expected) == HOUSEHOLDS_BY_EXTENSION[2]
        assert "20012" in expected
        roster = country.household_roster().xs(WAVE, level="t")
        served = set(roster.index.get_level_values("i"))
        missing = expected - served
        assert not missing, f"{len(missing)} split-off households not served: {sorted(missing)[:5]}"
