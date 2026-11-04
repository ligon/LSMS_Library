"""GH #1434: Uganda 2015-16 ``shocks`` keeps households only ``AGSEC1`` records.

``GSEC16``'s one id column is NAMED ``HHID`` but holds ``gsec1.hh`` values, so a
row reaches the sample's ``HHID`` only through a cover's ``hh -> HHID`` pair.  The
script used to inner-merge on ``gsec1.hh`` and delete the rows of households only
``AGSEC1`` records.  ``AGSEC1`` is now the fallback, on the #1290 housing rules.

The script runs for real with its reader replaced by invented frames, so the
rules need no data.  The ``requires_s3`` oracle reads the source files directly
and imports nothing from ``Uganda/_/``.
"""
from __future__ import annotations

import json
import runpy
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import local_tools
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

UGANDA = countries_root() / "Uganda"
SCRIPT = UGANDA / "2015-16" / "_" / "shocks.py"


def _shock_rows(keys, year=2015):
    n = len(keys)
    return pd.DataFrame({
        "HHID": list(keys),
        "h16q00": [f"Shock {k}" for k in keys],
        "h16q02a": ["March"] * n,
        "h16q2y": [year] * n,
        "h16q02b": [2.0] * n,
        "h16q3a": ["Yes"] * n,
        "h16q3b": ["No"] * n,
        "h16q3c": ["Yes"] * n,
        "h16q3d": ["No"] * n,
        "h16q4a": ["Coped"] * n,
        "h16q4b": ["Coped"] * n,
        "h16q4c": ["Coped"] * n,
    })


def _cover(hh, target, year=2016.0, month=1.0):
    return pd.DataFrame({"hh": list(hh), "HHID": list(target),
                         "year": [year] * len(hh), "month": [month] * len(hh)})


def _run(monkeypatch, shocks, cover, agriculture):
    """Run the shipped script on invented frames; return what it would write."""
    frames = {"GSEC16.dta": shocks, "gsec1.dta": cover, "AGSEC1.dta": agriculture}
    written = {}

    def reader(path, **kwargs):
        return frames[Path(path).name].copy(deep=True)

    def writer(df, name, **kwargs):
        written[name] = df

    monkeypatch.setattr(local_tools, "get_dataframe", reader)
    monkeypatch.setattr(local_tools, "to_parquet", writer)
    runpy.run_path(str(SCRIPT))
    return written["shocks.parquet"]


def _i(frame):
    return sorted(frame.index.get_level_values("i"))


def test_a_household_only_agsec1_records_keeps_its_row(monkeypatch):
    out = _run(monkeypatch, _shock_rows(["cov", "ag"]),
               _cover(["cov"], ["COV"]), _cover(["ag"], ["AG"]))
    assert _i(out) == ["AG", "COV"]


def test_the_fallback_row_takes_onset_from_agsec1s_own_interview_month(monkeypatch):
    out = _run(monkeypatch, _shock_rows(["ag"]), _cover(["other"], ["OTHER"]),
               _cover(["ag"], ["AG"], year=2015.0, month=9.0))
    assert out["Onset"].tolist() == [6]  # March to September


def test_the_household_cover_stays_primary(monkeypatch):
    out = _run(monkeypatch, _shock_rows(["both"]),
               _cover(["both"], ["SAME"], year=2015.0, month=6.0),
               _cover(["both"], ["SAME"], year=2015.0, month=9.0))
    assert _i(out) == ["SAME"]
    assert out["Onset"].tolist() == [3]  # the household cover's June


def test_conflicting_recorded_targets_refuse(monkeypatch):
    with pytest.raises(ValueError, match="conflicting"):
        _run(monkeypatch, _shock_rows(["both"]), _cover(["both"], ["ONE"]),
             _cover(["both"], ["TWO"]))


def test_one_hh_with_two_targets_refuses(monkeypatch):
    with pytest.raises(ValueError, match="ambiguous"):
        _run(monkeypatch, _shock_rows(["ag"]), _cover(["other"], ["OTHER"]),
             _cover(["ag", "ag"], ["ONE", "TWO"]))


def test_a_key_neither_cover_records_is_kept_loudly(monkeypatch):
    with pytest.warns(UserWarning, match="unresolved hh/HHID bridge for 1 of 2"):
        out = _run(monkeypatch, _shock_rows(["cov", "lost"]), _cover(["cov"], ["COV"]),
                   _cover(["other"], ["OTHER"]))
    assert _i(out) == ["COV", "lost"]
    assert out["Onset"].isna().sum() == 1


def test_a_row_with_no_shock_year_is_still_not_a_shock(monkeypatch):
    shocks = _shock_rows(["cov", "none"]).astype({"h16q2y": "float"})
    shocks.loc[shocks["HHID"] == "none", "h16q2y"] = float("nan")
    out = _run(monkeypatch, shocks, _cover(["cov", "none"], ["COV", "NONE"]),
               _cover(["other"], ["OTHER"]))
    assert _i(out) == ["COV"]


@pytest.mark.requires_s3
def test_every_valid_source_row_is_served_under_a_covers_id():
    """Oracle: the source files, read directly, against ``Country.shocks()``."""
    import lsms_library as ll

    data = UGANDA / "2015-16" / "Data"
    shocks = get_dataframe(str(data / "GSEC16.dta"), convert_categoricals=False)
    valid = shocks[shocks["h16q2y"].notna()]
    pairs = pd.concat(
        get_dataframe(str(data / name), convert_categoricals=False)[["hh", "HHID"]]
        for name in ("gsec1.dta", "AGSEC1.dta")).drop_duplicates()
    assert not pairs["hh"].duplicated().any()
    target = valid["HHID"].map(pairs.set_index("hh")["HHID"])
    assert target.notna().all(), "a valid shock row names a key no cover records"

    walk = json.loads((UGANDA / "_" / "updated_ids.json").read_text()).get("2015-16", {})
    expected = target.map(lambda x: walk.get(x, x)).value_counts().sort_index()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        country = ll.Country("Uganda")
        served = country.shocks(waves=["2015-16"]).reset_index()
        members = set(country.sample(waves=["2015-16"]).reset_index()["i"])
    got = served["i"].value_counts().sort_index()
    pd.testing.assert_series_equal(got, expected, check_names=False)
    assert set(served["i"]) <= members
