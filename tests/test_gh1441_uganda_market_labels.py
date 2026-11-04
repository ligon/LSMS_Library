"""GH #1441, Uganda: a household's own recorded district labels its market.

``market='District'`` reads ``sample()`` first and falls back to
``cluster_features()``.  ``sample`` carried no ``District``, so the only source was
the cluster table, whose ``reduce_to_agreed`` blanks a cluster whose households
disagree.  ``comm`` is the 2005-06 EA of origin, so a mover or a carved-out district
blanked the label for every household on that EA, though each had recorded its own.
``sample`` now carries the column ``cluster_features`` reads, and 2011-12's mixed-case
``h1aq1`` no longer counts as a disagreement.

The oracle reads the covers directly and imports nothing from ``Uganda/_/``.  Config
tests need no data; the ``requires_s3`` tests read covers and the build.
"""
from __future__ import annotations

import importlib.util
import json
import warnings

import pandas as pd
import pytest

from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

UGANDA = countries_root() / "Uganda"
#: wave: (cover under Data/, household id, region column, district column)
COVERS = {
    "2005-06": ("GSEC1.dta", "HHID", "region", "h1aq1"),
    "2009-10": ("GSEC1.dta", "HHID", "region", "h1aq1"),
    "2010-11": ("GSEC1.dta", "HHID", "region", "h1aq1"),
    "2011-12": ("GSEC1.dta", "HHID", "region", "h1aq1"),
    "2013-14": ("GSEC1.dta", "HHID", "region", "h1aq1a"),
    "2015-16": ("gsec1.dta", "HHID", "region", "district"),
    "2018-19": ("GSEC1.dta", "hhid", "region", "distirct_name"),
    "2019-20": ("HH/gsec1.dta", "hhid", "region", "district"),
}
#: Raw 2019-20 districts ``mapping._V_ALIASES`` re-labels; see CONTENTS.org.
ALIASED_2019_20 = {"namtumba", "mubende", "gulu"}


def _info(wave):
    return load_yaml(UGANDA / wave / "_" / "data_info.yml")


def _label(x):
    """A comparable label: blank is None, a numeric code loses its ``.0``, case folds."""
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return str(int(x))
    x = " ".join(str(x).split()).casefold()
    if x.endswith(".0") and x[:-2].isdigit():
        x = x[:-2]
    return x or None


def _load_mapping(wave):
    spec = importlib.util.spec_from_file_location(
        f"uganda_{wave.replace('-', '_')}_mapping", UGANDA / wave / "_" / "mapping.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestDeclarations:
    """No data: what the config says."""

    @pytest.mark.parametrize("wave", COVERS)
    @pytest.mark.parametrize("column", ["District", "Region"])
    def test_sample_reads_what_cluster_features_reads(self, wave, column):
        info = _info(wave)
        own = info["sample"]["locating_cover"]["myvars"][column]
        cluster = info["cluster_features"]["locating_cover"]["myvars"][column]
        assert own == cluster

    def test_the_alternate_cover_supplies_district_on_both(self):
        info = _info("2015-16")
        assert (info["sample"]["locating_ag"]["myvars"]["_ag_District"]
                == info["cluster_features"]["locating_ag"]["myvars"]["_ag_District"])

    def test_sample_declares_the_column(self):
        scheme = load_yaml(UGANDA / "_" / "data_scheme.yml")["Data Scheme"]
        assert "District" in scheme["sample"]

    def test_the_2011_12_formatter_folds_case_and_nothing_else(self):
        district = _load_mapping("2011-12").District
        assert district("kampala") == district("Kampala") == district("KAMPALA") == "KAMPALA"
        assert district("KAMPALA") != district("WAKISO")
        assert pd.isna(district(""))
        assert pd.isna(district(None))


@pytest.fixture(scope="module")
def built():
    import lsms_library as ll

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        country = ll.Country("Uganda")
        out = {
            "sample": country.sample().reset_index(),
            "cluster": country.cluster_features().reset_index(),
            "District": country.sample(market="District").reset_index(),
            "Region": country.sample(market="Region").reset_index(),
        }
    for frame in out.values():
        frame["t"] = frame["t"].astype(str)
    return out


@pytest.fixture(scope="module")
def covers():
    """Each cover read directly: ``(t, i, raw_region, raw_district)``, ``i`` walked."""
    walk = json.loads((UGANDA / "_" / "updated_ids.json").read_text())
    frames = []
    for wave, (name, key, region, district) in COVERS.items():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw = get_dataframe(str(UGANDA / wave / "Data" / name))
        ids = raw[key].map(_label_id)
        frames.append(pd.DataFrame({
            "t": wave,
            "i": ids.map(lambda x, m=walk.get(wave, {}): m.get(x, x)),
            "raw_region": raw[region].map(_label),
            "raw_district": raw[district].map(_label),
        }))
    return pd.concat(frames, ignore_index=True)


def _label_id(x):
    if pd.isna(x):
        return None
    return str(x).strip()


@pytest.mark.requires_s3
class TestServed:
    def test_every_label_a_cover_records_is_served(self, built, covers):
        got = covers.merge(built["sample"][["t", "i", "District", "Region"]], on=["t", "i"],
                           how="left", validate="one_to_one")
        recorded = got[got["raw_district"].notna()]
        assert recorded["District"].notna().all()
        assert got.loc[got["raw_region"].notna(), "Region"].notna().all()

        differ = recorded[recorded["District"].map(_label) != recorded["raw_district"]]
        assert set(differ["t"]) <= {"2019-20"}
        assert set(differ["raw_district"]) <= ALIASED_2019_20

    def test_a_households_own_label_is_its_market(self, built):
        for column in ("District", "Region"):
            frame = built[column]
            own = frame[column]
            assert (frame.loc[own.notna(), "m"].astype(str) == own.dropna().astype(str)).all()

    def test_a_cluster_label_contradicts_no_household(self, built):
        for column in ("District", "Region"):
            joined = built["sample"][["t", "v", column]].merge(
                built["cluster"][["t", "v", column]], on=["t", "v"], suffixes=("", "_cluster"))
            both = joined[joined[column].notna() & joined[f"{column}_cluster"].notna()]
            assert len(both)
            assert (both[column].astype(str) == both[f"{column}_cluster"].astype(str)).all()

    def test_an_unlabelled_household_has_no_label_to_read(self, built, covers):
        for column, raw in (("District", "raw_district"), ("Region", "raw_region")):
            frame = built[column]
            cluster = built["cluster"][["t", "v", column]].rename(columns={column: "_cluster"})
            lost = frame.loc[frame["m"].isna(), ["t", "i"]].merge(
                built["sample"][["t", "i", "v"]], on=["t", "i"]).merge(
                cluster, on=["t", "v"], how="left").merge(
                covers[["t", "i", raw]], on=["t", "i"], how="left")
            assert lost[raw].isna().all()
            assert lost["_cluster"].isna().all()

    def test_a_case_only_difference_is_not_a_conflict(self, built):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw = get_dataframe(str(UGANDA / "2011-12" / "Data" / "GSEC1.dta"))
        labels = pd.DataFrame({"v": raw["comm"].map(_label), "d": raw["h1aq1"].map(_label)})
        distinct = labels.dropna().groupby("v")["d"].nunique()
        agreed = distinct[distinct == 1].index
        cluster = built["cluster"]
        cluster = cluster[cluster["t"] == "2011-12"].set_index("v")["District"]
        assert cluster.reindex(agreed).notna().all()
