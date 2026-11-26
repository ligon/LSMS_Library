"""One eligible person is still an iterable, one-row roster."""
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.transformations import dummies, roster_to_characteristics


def _roster(wave, resident_filter):
    keys = [(wave, "V1", "HH1", 1)]
    members = [("FEMALE", 25, 12)]
    if resident_filter:
        keys.append((wave, "V1", "HH2", 1))
        members.append(("MALE", 40, 0))
    idx = pd.MultiIndex.from_tuples(keys, names=["t", "v", "i", "pid"])
    return pd.DataFrame(members, columns=["Sex", "Age", "MonthsSpent"], index=idx)


def _expected(wave):
    idx = pd.MultiIndex.from_tuples([(wave, "V1", "HH1")],
                                   names=["t", "v", "i"])
    return pd.DataFrame({"FEMALE 19-30": [1], "log HSize": [0.0]},
                        index=idx).rename_axis("sex_age", axis=1)


def test_dummies_preserves_one_row_and_its_full_category():
    df = pd.DataFrame({"kind": ["entire category"]},
                      index=pd.Index(["h1"], name="household"))
    expected = pd.DataFrame([[1]], index=df.index,
                            columns=pd.MultiIndex.from_tuples(
                                [("entire category",)], names=["kind"]))
    pd.testing.assert_frame_equal(dummies(df, ["kind"]), expected)


@pytest.mark.parametrize("resident_filter", [False, True])
def test_transform_counts_the_one_resident(resident_filter):
    pd.testing.assert_frame_equal(
        roster_to_characteristics(_roster("2018-19", resident_filter)),
        _expected("2018-19"),
    )


@pytest.mark.parametrize("country", ["Niger", "Uganda"])
@pytest.mark.parametrize("route", ["country", "feature"])
def test_public_api_returns_one_household(monkeypatch, country, route):
    wave = "2018-19"
    source = _roster(wave, resident_filter=True)

    def aggregate(self, waves, name, **kwargs):
        assert self.name == country and name == "household_roster"
        assert waves is None or wave in waves
        return source.copy()

    monkeypatch.setattr(ll.Country, "_aggregate_wave_data", aggregate)
    expected = _expected(wave)
    if route == "country":
        result = ll.Country(country).household_characteristics(waves=[wave])
    else:
        result = ll.Feature("household_characteristics")([country])
        assert result.index.names == ["country", "t", "v", "i"]
        result = result.xs(country, level="country")
    pd.testing.assert_frame_equal(result, expected)
