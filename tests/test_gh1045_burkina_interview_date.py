"""GH #1045: Burkina Faso ``interview_date`` is parsed by the build-time parser.

Both EHCVM waves (2018-19, and 2021-22's two covers) used to hand the six
per-visit stamps s00q23a/b, s00q24a/b, s00q25a/b to the ``interview_date`` hook
as strings, where ``melt_visit_intervals`` guessed them with
``pd.to_datetime(errors='coerce')``.  Each is now a declarative ``datetime:`` spec
in the wave's ``data_info.yml`` (the Ethiopia W4/W5 form of #1413), parsed in
``Wave.grab_data`` before the hook runs:

* ``format: '%Y-%m-%dT%H:%M:%S'`` -- every stamp is a Survey Solutions string;
* ``sentinels: ['##N/A##']`` -- the not-answered code, the only non-ISO value in
  either wave, is counted as a sentinel rather than as a parse failure;
* the default resolution keeps the time of day, as served before;
* no ``window:`` (see the comments in the two ``data_info.yml``).

2014 (EMC) declares no ``interview_date``.  Served values are unchanged; what is new
is the per-column report, filed in ``datetime_reports`` and replayed on warm reads.

Config and invented-input tests run anywhere; the source oracle is S3/cache-gated.
"""
from __future__ import annotations

import importlib.util
import pathlib
import warnings

import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library import local_tools
from lsms_library.datetime_parse import datetime_reports
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
ROOT = countries_root() / COUNTRY
ISO = "%Y-%m-%dT%H:%M:%S"
SENTINEL = "##N/A##"
# target column -> source stamp, in visit order (melt_visit_intervals' names).
TARGETS = {"int_start": "s00q23a", "int_end": "s00q23b",
           "int_start_v2": "s00q24a", "int_end_v2": "s00q24b",
           "int_start_v3": "s00q25a", "int_end_v3": "s00q25b"}
COVERS = {"2018-19": ["s00_me_bfa2018.dta"],
          "2021-22": ["s00_me_bfa2021.dta", "Cross_Section/s00_me_bfa2021.dta"]}

# Measured 2026-11-13, cold on base f138a3d61 (job 41271518): per wave and target
# column, (sentinel '##N/A##', missing = blank).  n is 7,010 (2018-19) and
# 3,227 + 7,176 = 10,403 (2021-22, panel + cross-section covers).
N = {"2018-19": 7_010, "2021-22": 10_403}
COUNTS = {
    "2018-19": {"int_start": (0, 0), "int_end": (3, 0), "int_start_v2": (1, 0),
                "int_end_v2": (11, 1), "int_start_v3": (1, 0), "int_end_v3": (11, 1)},
    "2021-22": {"int_start": (0, 0), "int_end": (0, 0), "int_start_v2": (0, 0),
                "int_end_v2": (0, 0), "int_start_v3": (0, 3), "int_end_v3": (18, 32)},
}
# Served rows by (t, visit) before and after: unchanged.
VISIT_ROWS = {("2018-19", 1): 7_010, ("2018-19", 2): 7_009, ("2018-19", 3): 7_009,
              ("2021-22", 1): 10_403, ("2021-22", 2): 10_403, ("2021-22", 3): 10_400}


def _myvars(wave):
    with open(ROOT / wave / "_" / "data_info.yml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)["interview_date"]["myvars"]


@pytest.mark.parametrize("wave", sorted(COVERS))
def test_every_stamp_is_a_declared_gregorian_iso_spec(wave):
    myvars = _myvars(wave)
    assert set(myvars) == set(TARGETS)
    for target, source in TARGETS.items():
        spec = myvars[target]
        assert isinstance(spec, dict), (wave, target)
        assert spec["datetime"] == source
        assert spec["calendar"] == "gregorian"
        assert spec["format"] == ISO
        assert spec["sentinels"] == [SENTINEL]
        # The time of day is served, as before: no `resolution: day`.
        assert spec.get("resolution", "us") == "us"
        assert "window" not in spec


def test_2014_declares_no_interview_date():
    with open(ROOT / "2014" / "_" / "data_info.yml", encoding="utf-8") as fh:
        assert "interview_date" not in yaml.safe_load(fh)


def _cover(grappe, menage, stamps):
    return pd.DataFrame({"grappe": grappe, "menage": menage,
                         **{s: list(v) for s, v in stamps.items()}})


@pytest.fixture
def invented(monkeypatch):
    """One 2018-19 cover through the REAL grab_data / spec / hook, no data."""
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    stamps = {
        "s00q23a": ["2018-09-03T08:15:42", "2018-09-04T09:00:00"],
        "s00q23b": ["2018-09-03T10:01:05", SENTINEL],
        "s00q24a": ["2018-09-05T08:00:00", SENTINEL],
        "s00q24b": ["2018-09-05T09:30:00", ""],
        "s00q25a": ["2018-09-06T08:00:00", "2018-09-06T11:00:00"],
        "s00q25b": ["2018-09-06T09:00:00", "2018-09-06T12:00:00"],
    }
    frame = _cover([7, 7], [1, 2], stamps)

    def read(path, **kwargs):
        assert str(path).endswith("s00_me_bfa2018.dta"), path
        return frame.copy()

    monkeypatch.setattr(local_tools, "get_dataframe", read)
    return frame


def test_invented_cover_parses_counts_and_melts(invented):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country(COUNTRY)["2018-19"].grab_data("interview_date")
    flat = df.reset_index()
    start = pd.to_datetime(flat["Interview start"])
    # The time of day survives (no flooring to midnight).
    assert (start.dt.hour > 0).all()
    assert str(df["Interview start"].dtype) == "datetime64[us]"
    # Household 2's visit 2 has a sentinel start and a blank end: dropped, never made.
    per_hh = flat.groupby("i")["visit"].apply(sorted).to_dict()
    assert sorted(per_hh.values(), key=len) == [[1, 3], [1, 2, 3]]
    # The sentinel end of household 2's visit 1 is NaT, not a string.
    assert flat["Interview end"].isna().sum() == 1
    reps = {r["column"]: r for r in datetime_reports(COUNTRY, "interview_date")
            if r.get("wave") == "2018-19" and r.get("n") == 2}
    assert reps["int_end"]["sentinel"] == 1 and reps["int_end"]["unparsed"] == 0
    assert reps["int_end_v2"]["missing"] == 1
    assert all(r["unparsed"] == 0 for r in reps.values())


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country(COUNTRY).interview_date()


def _melted_oracle(wave):
    """The pre-#1045 rule on the raw covers: pd.to_datetime(errors='coerce') per stamp,
    one row per made visit.  Ids are left out: the multiset of stamps is the check."""
    rows = []
    for fn in COVERS[wave]:
        raw = local_tools.get_dataframe(str(ROOT / wave / "Data" / fn))
        for visit, (a, b) in enumerate((("s00q23a", "s00q23b"), ("s00q24a", "s00q24b"),
                                        ("s00q25a", "s00q25b")), start=1):
            s = pd.to_datetime(raw[a], errors="coerce")
            e = pd.to_datetime(raw[b], errors="coerce")
            keep = s.notna() | e.notna()
            rows.append(pd.DataFrame({"visit": visit, "start": s[keep].to_numpy(),
                                      "end": e[keep].to_numpy()}))
    return pd.concat(rows, ignore_index=True)


@requires_s3
@pytest.mark.parametrize("wave", sorted(COVERS))
def test_served_stamps_equal_the_old_coerce_rule_on_the_source(served, wave):
    got = served.xs(wave, level="t").reset_index()
    want = _melted_oracle(wave)
    key = ["visit", "start", "end"]
    got = got.rename(columns={"Interview start": "start", "Interview end": "end"})[key]
    got["visit"] = got["visit"].astype("int64")
    pd.testing.assert_frame_equal(
        got.sort_values(key, na_position="first").reset_index(drop=True),
        want.sort_values(key, na_position="first").reset_index(drop=True)
            .astype({"start": got["start"].dtype, "end": got["end"].dtype}),
        check_dtype=False)


@requires_s3
def test_served_rows_by_visit_are_unchanged(served):
    flat = served.reset_index()
    got = flat.groupby([flat["t"].astype(str), flat["visit"].astype(int)]).size().to_dict()
    assert got == VISIT_ROWS
    assert str(served["Interview start"].dtype) == "datetime64[us]"


@requires_s3
@pytest.mark.parametrize("wave", sorted(COVERS))
def test_report_counts_sentinels_and_no_parse_failure(served, wave):
    reps = [r for r in datetime_reports(COUNTRY, "interview_date") if r.get("wave") == wave
            and r.get("n") == N[wave]]
    by_col = {r["column"]: r for r in reps}
    assert set(by_col) == set(TARGETS)
    for column, (sentinel, missing) in COUNTS[wave].items():
        r = by_col[column]
        assert (r["sentinel"], r["missing"]) == (sentinel, missing), (wave, column)
        assert r["unparsed"] == r["ambiguous"] == r["out_of_range"] == r["incomplete"] == 0
        assert r["parsed"] == N[wave] - sentinel - missing
