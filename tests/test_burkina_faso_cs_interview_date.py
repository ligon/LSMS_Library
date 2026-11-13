"""Burkina Faso 2021-22 ``interview_date`` covers BOTH sample components (GH #1276).

The panel cover (WB 6224, ``Data/``) and the cross-section cover (WB 6277,
``Data/Cross_Section/``) ask the same s00q23-25 per-visit start/end
timestamps.  ``interview_date`` read only the panel cover, so the 7,176
cross-section households ``sample()`` serves had no interview date (measured on
the warm cache at development ``3a33f2869``: 2021-22 ``interview_date`` 9,681
rows over 3,227 households, none ``CS_``).  The cross-section entry takes the
``cs_i`` per-file override, like ``sample`` / ``household_roster``, so its
households carry the same ``CS_`` ids.

Config and invented-input tests run anywhere; the data test is S3/cache-gated.
"""
import importlib.util
import pathlib

import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library import local_tools
from lsms_library.paths import countries_root


def _tests_conftest():
    """``tests/conftest.py``, loaded by path (the root conftest shadows it)."""
    path = pathlib.Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("lsms_tests_conftest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


requires_s3 = _tests_conftest().requires_s3

COUNTRY = "Burkina_Faso"
PANEL_COVER = "s00_me_bfa2021.dta"
CS_COVER = "Cross_Section/s00_me_bfa2021.dta"
ROOT = countries_root() / COUNTRY
STAMPS = ("s00q23a", "s00q23b", "s00q24a", "s00q24b", "s00q25a", "s00q25b")


def test_2021_22_interview_spec_reads_both_covers_with_cs_ids():
    with open(ROOT / "2021-22" / "_" / "data_info.yml") as f:
        spec = yaml.safe_load(f)["interview_date"]
    files = spec["file"]
    assert files[0] == PANEL_COVER
    assert list(files[1]) == [CS_COVER]
    assert files[1][CS_COVER]["i"] == ["grappe", "menage", {"mapping": "cs_i"}]
    assert set(spec["myvars"].values()) == set(STAMPS)


def _cover(grappe, menage, day):
    stamps = {s: [f"2021-11-{day:02d}T0{k}:00:00"] * len(grappe)
              for k, s in enumerate(STAMPS, start=1)}
    return pd.DataFrame({"grappe": grappe, "menage": menage, **stamps})


@pytest.fixture
def raw_covers(monkeypatch):
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    frames = {
        PANEL_COVER: _cover([2, 2], [1, 126], 10),
        CS_COVER: _cover([600], [126], 20),
    }

    def read(path, **kwargs):
        key = str(path).split("/Data/")[-1]
        assert key in frames, key
        return frames[key].copy()

    monkeypatch.setattr(local_tools, "get_dataframe", read)


def test_cross_section_households_get_cs_interview_dates(raw_covers):
    df = ll.Country(COUNTRY)["2021-22"].grab_data("interview_date")
    flat = df.reset_index()
    assert set(flat["i"]) == {"2001", "2126", "CS_600126"}
    assert flat.groupby("i").size().to_dict() == {
        "2001": 3, "2126": 3, "CS_600126": 3}
    cs = flat[flat["i"] == "CS_600126"]
    assert pd.to_datetime(cs["Interview start"]).dt.day.eq(20).all()
    assert df.index.is_unique


@requires_s3
def test_every_2021_22_sample_household_has_an_interview_date():
    c = ll.Country(COUNTRY)
    s = c.sample(waves=["2021-22"])
    d = c.interview_date(waves=["2021-22"])
    dated = set(d.index.get_level_values("i"))
    assert set(s.index.get_level_values("i")) <= dated
    cs = pd.Series(sorted(dated)).str.startswith("CS_")
    assert cs.sum() == 7176 and (~cs).sum() == 3227
    assert d["Interview start"].notna().all()
