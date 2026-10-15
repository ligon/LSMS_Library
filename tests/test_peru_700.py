"""Peru 1994 keys must join, without inventing missing survey weights (#700).

CONTENTS.org identifies only 1994 as wired and REG01._weight_ as an SPSS
export artefact. The three earlier acquired waves require their own mappings.
"""
import pandas as pd
import pytest

from lsms_library import Country
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

pytestmark = pytest.mark.requires_s3


@pytest.fixture(scope="module")
def peru():
    return Country("Peru")


def test_sample_cluster_and_roster_keys_join(peru):
    sample = peru.sample()
    clusters = peru.cluster_features()
    roster = peru.household_roster()
    assert set(sample.index.get_level_values("t")) == {"1994"}
    assert len(sample) == 3623
    prefix = sample.index.get_level_values("i").str.split("-").str[0]
    expected = set(prefix)
    assert len(expected) == 364
    assert set(sample.v) == set(clusters.index.get_level_values("v")) == expected
    assert sample.v.notna().all()
    assert sample.v.str.len().eq(3).all()
    joined = roster.reset_index().merge(
        sample.reset_index()[["t", "i", "v"]], on=["t", "i"],
        how="left", validate="many_to_one", suffixes=("_roster", "_sample"))
    assert len(joined) == len(roster) == 19285
    assert joined.v_roster.notna().all()
    assert joined.v_sample.notna().all()
    assert joined.v_roster.eq(joined.v_sample).all()


def test_export_weight_is_not_a_sampling_weight(peru):
    raw = get_dataframe(countries_root() / "Peru/1994/Data/REG01.DTA",
                        convert_categoricals=False)
    assert len(raw) == 3623
    assert pd.to_numeric(raw["_weight_"]).eq(1).all()
    sample = peru.sample()
    assert "weight" not in sample.columns
    assert "panel_weight" not in sample.columns
