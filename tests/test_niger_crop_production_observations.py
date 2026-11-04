"""Niger ``crop_production`` carries the native observation axis -- GH #1360.

``(t, i, plot_id, crop, u)`` is not a harvest line in Niger: one (plot, crop,
unit) can carry several reported lines in 2011-12 (27 key groups, 57 rows)
and 2021-22 (846 groups, 1,856 rows).  The framework used to collapse each
group to one observed row (916 rows destroyed, GH #323), and because only a
non-unique index is collapsed AND key-sorted, the default route (per-wave
collapse) and the Make route (one collapse over the concatenated waves)
served the same 46,376 rows in different orders (22,516 positions differ,
measured at 3a33f2869).  The canonical ``production_obs`` axis
(``lsms_library/data_info.yml``, GH #1198) is the missing level:

* 2011-12 -- ``reported:<as02eq0>``, the harvest module's own line number;
* 2021-22 -- ``row:<source_fingerprint>:<n>``, CONSTRUCTED (s16c records no
  line number): the zero-based row of the held release, before any filter;
* 2014-15, 2018-19 -- ``unresolved_observation`` (unique without it).

With the level in place nothing collapses, and the two routes are
strictly equal (measured; see ``Niger/_/CONTENTS.org``, GH #1360 entry).

A second consequence is pinned here too: the crop-level sale of 2014-15 /
2021-22 is attributed only to an (i, crop) with ONE harvest line, so it can
never sit on two served rows.  Measured on the cold rebuild: 2021-22 sale
pairs 1,409 -> 1,265 (144 multi-line pairs now NaN, 16 of which the old
single-PLOT rule served twice); 2014-15 unchanged (564).

Two tiers: config/synthetic (no microdata) and ``TestServed`` (builds the
real table; skipped when Niger is unavailable).
"""
from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

COUNTRY = "Niger"
TABLE = "crop_production"
WAVES = ("2011-12", "2014-15", "2018-19", "2021-22")
# `condition` added by GH #1436 (tests/test_niger_crop_production_condition.py).
INDEX = ["t", "i", "plot_id", "crop", "u", "condition", "production_obs"]

#: Served rows per wave, cold rebuild after GH #1360 (before: 11,937 / 8,799
#: / 13,717 / 11,923 = 46,376).
ROWS = {"2011-12": 11967, "2014-15": 8799, "2018-19": 13717, "2021-22": 12933}
#: production_obs token kind served in each wave.
KIND = {"2011-12": "reported", "2014-15": "unresolved_observation",
        "2018-19": "unresolved_observation", "2021-22": "row"}
#: 2021-22 crop-level sale after the one-harvest-line rule.
W2021_SALE_PAIRS = 1265
W2021_VALUE_SOLD = 68_629_070


def _root() -> Path:
    return countries_root() / COUNTRY


def _script(wave: str) -> str:
    return (_root() / wave / "_" / f"{TABLE}.py").read_text(encoding="utf-8")


def _aws_creds_available() -> bool:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_shared_conftest", Path(__file__).with_name("conftest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


needs_data = pytest.mark.skipif(
    not (countries_root() / COUNTRY / "_" / "data_scheme.yml").exists()
    or os.environ.get("LSMS_SKIP_AUTH")
    or not _aws_creds_available(),
    reason=f"{COUNTRY} microdata unavailable (no config or no S3 credentials)",
)


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the ``!make`` tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


@pytest.fixture(scope="module")
def niger_module():
    """``Niger/_/niger.py``, imported the way the wave scripts import it."""
    sys.path.insert(0, str(_root() / "_"))
    try:
        import niger
    finally:
        sys.path.pop(0)
    return niger


# ---------------------------------------------------------------------------
# config-only
# ---------------------------------------------------------------------------

class TestDeclared:

    def test_data_scheme_declares_the_observation_axis(self):
        with open(_root() / "_" / "data_scheme.yml", encoding="utf-8") as f:
            scheme = yaml.load(f, Loader=_SchemeLoader)
        index = scheme["Data Scheme"][TABLE]["index"]
        assert index == "(" + ", ".join(INDEX) + ")", index

    def test_canonical_axis_and_sentinel_exist(self, niger_module):
        with open(Path(__file__).resolve().parents[1] / "lsms_library" / "data_info.yml",
                  encoding="utf-8") as f:
            info = yaml.safe_load(f)
        assert "production_obs" in info["Index Info"]["index_info"][TABLE]
        sentinel = info["Index Info"]["missing_level_sentinels"][TABLE]["production_obs"]
        assert sentinel == niger_module.CROP_OBS_NA

    def test_compiler_no_longer_claims_a_non_unique_index(self):
        src = (_root() / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert "intentionally NON-unique:" not in src

    @pytest.mark.parametrize("wave", WAVES)
    def test_every_wave_script_asserts_a_unique_index(self, wave):
        assert "assert df.index.is_unique" in _script(wave), wave

    def test_2011_12_uses_the_recorded_line_number(self):
        assert "_crop_reported_observations(hh, srcn['as02eq0'])" in _script("2011-12")

    def test_2021_22_tokens_are_taken_before_any_filter(self):
        src = _script("2021-22")
        assert "_crop_row_observations(\n        len(srcn), '../Data/s16c_me_ner2021.dta'" in src
        # Built on the raw frame: before _finish_crop_production drops no-crop rows.
        assert src.index("_crop_row_observations(") < src.index("_finish_crop_production(df")

    @pytest.mark.parametrize("wave", ("2014-15", "2021-22"))
    def test_crop_level_sale_needs_one_harvest_line(self, wave):
        src = _script(wave)
        assert "lines_per = df.dropna(subset=['i', 'crop']).groupby(['i', 'crop']).size()" in src
        assert "single = lines_per[lines_per == 1].index" in src
        assert "['plot_id'].nunique()" not in src, f"{wave}: the single-PLOT rule is back"


# ---------------------------------------------------------------------------
# synthetic: the helpers and the shared tail
# ---------------------------------------------------------------------------

class TestHelpers:

    def test_reported_tokens(self, niger_module):
        out = niger_module._crop_reported_observations(
            pd.Series(["1", "1", "2"]), pd.Series([1.0, 2.0, 1.0]))
        assert out.tolist() == ["reported:1", "reported:2", "reported:1"]
        assert str(out.dtype) == "string"

    @pytest.mark.parametrize("households, lines", [
        (["1", "1"], [3.0, 3.0]),          # repeated (household, line)
        (["1", "1"], [3.0, None]),         # missing line number
        (["1", "1"], [3.0, 2.5]),          # not an integer
        (["1", None], [3.0, 4.0]),         # missing household
    ])
    def test_reported_refusals(self, niger_module, households, lines):
        with pytest.raises(ValueError):
            niger_module._crop_reported_observations(pd.Series(households), pd.Series(lines))

    def test_row_tokens_are_bound_to_the_sidecar(self, niger_module, tmp_path):
        script = tmp_path / "_" / "crop_production.py"
        script.parent.mkdir()
        script.write_text("")
        data = tmp_path / "Data"
        data.mkdir()
        (data / "s16c.dta").write_bytes(b"not read")
        (data / "s16c.dta.dvc").write_text("outs:\n- md5: 0\n")
        out = niger_module._crop_row_observations(3, "../Data/s16c.dta", script_path=script)
        assert len(out) == 3 and str(out.dtype) == "string"
        assert all(t.startswith("row:dvc:s16c.dta:") for t in out)
        assert [t.rsplit(":", 1)[1] for t in out] == ["0", "1", "2"]

    def test_row_tokens_refuse_an_undeclared_source(self, niger_module, tmp_path):
        script = tmp_path / "_" / "crop_production.py"
        script.parent.mkdir()
        script.write_text("")
        (tmp_path / "Data").mkdir()
        (tmp_path / "Data" / "s16c.dta").write_bytes(b"no sidecar")
        with pytest.raises(ValueError):
            niger_module._crop_row_observations(2, "../Data/s16c.dta", script_path=script)

    def _frame(self, obs=None):
        df = pd.DataFrame({
            "i": ["1", "1"], "plot_id": ["1_1", "1_1"], "crop": ["Mil", "Mil"],
            "u": ["Kg", "Kg"], "Quantity": [10.0, 3.0],
            "Quantity_sold": [None, None], "Value_sold": [None, None]})
        if obs is not None:
            df["production_obs"] = obs
        return df

    def test_tail_serves_distinct_lines_as_distinct_rows(self, niger_module):
        out = niger_module._finish_crop_production(
            self._frame(["reported:1", "reported:2"]), "2099-00")
        assert list(out.index.names) == INDEX
        assert out.index.is_unique and len(out) == 2
        assert out["Quantity"].tolist() == [10.0, 3.0]

    def test_tail_fills_the_sentinel_when_no_coordinate_is_passed(self, niger_module):
        out = niger_module._finish_crop_production(self._frame(), "2099-00")
        assert list(out.index.names) == INDEX
        assert set(out.index.get_level_values("production_obs")) == {niger_module.CROP_OBS_NA}


# ---------------------------------------------------------------------------
# the built table
# ---------------------------------------------------------------------------

@needs_data
class TestServed:

    @pytest.fixture(scope="class")
    def built(self):
        import lsms_library as ll
        from lsms_library.country import grain_reports

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country(COUNTRY).crop_production()
        return df, grain_reports(COUNTRY, TABLE)

    def test_index_is_unique_and_declared(self, built):
        df, _ = built
        assert df.index.is_unique
        # The framework orders the served levels (i before t) and adds v.
        assert sorted(df.index.names) == sorted(INDEX + ["v"])

    def test_nothing_is_collapsed(self, built):
        _, reports = built
        assert [r for r in reports if r.get("destroyed") or r.get("dropped")] == []

    @pytest.mark.parametrize("wave", WAVES)
    def test_rows_and_token_kind_per_wave(self, built, wave):
        df, _ = built
        g = df.xs(wave, level="t")
        assert len(g) == ROWS[wave], f"{wave}: {len(g)} rows"
        kinds = set(g.index.get_level_values("production_obs").str.split(":").str[0])
        assert kinds == {KIND[wave]}, kinds

    @pytest.mark.parametrize("wave", ("2014-15", "2021-22"))
    def test_crop_level_sale_sits_on_one_row(self, built, wave):
        df, _ = built
        g = df.xs(wave, level="t").reset_index()
        g = g[g["Value_sold"].notna()]
        per_pair = g.groupby(["i", "crop"]).size()
        assert int((per_pair > 1).sum()) == 0, f"{wave}: a sale served on two rows"

    def test_2021_22_sale_totals(self, built):
        df, _ = built
        g = df.xs("2021-22", level="t").reset_index()
        g = g[g["Value_sold"].notna()]
        assert g.groupby(["i", "crop"]).ngroups == W2021_SALE_PAIRS
        assert float(g["Value_sold"].sum()) == W2021_VALUE_SOLD
