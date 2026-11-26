"""The W4 migration audits missing visit clocks without shifting any clock."""
import runpy

import pandas as pd

from lsms_library import local_tools
from lsms_library.datetime_parse import _clear_datetime_reports
from lsms_library.paths import countries_root


def test_w4_native_clocks_and_round_audit(monkeypatch, tmp_path):
    nigeria = countries_root() / "Nigeria"
    script = nigeria / "2018-19" / "_" / "interview_date.py"
    covers = {
        "../Data/secta_plantingw4.dta": pd.DataFrame({
            "hhid": ["001", "002", "003"],
            "InterviewStart": pd.array(["2018-11-04 23:59:59.987", None,
                                        "2018-11-05 00:01:04.012"], dtype="datetime64[ms]"),
            "interview_result": ["1. COMPLETE", "1. COMPLETE",
                                 "4. UNABLE TO IDENTIFY HOUSEHOLD"],
        }),
        "../Data/secta_harvestw4.dta": pd.DataFrame({
            "hhid": ["001", "004"],
            "InterviewStart": pd.array(["2019-04-16 04:05:06.789", None],
                                       dtype="datetime64[ms]"),
            "interview_result": ["7. REFUSED", "6. LONG TERM UNAVAILABLE"],
        }),
    }
    read = []

    def read_cover(path, **kwargs):
        read.append(path)
        return covers[path].copy()

    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(nigeria / "_"))
    monkeypatch.setattr(local_tools, "get_dataframe", read_cover)
    # This real script redirects writes by its source path, not by cwd.
    monkeypatch.setattr(local_tools, "data_root", lambda country: tmp_path / country)
    _clear_datetime_reports()
    try:
        runpy.run_path(str(script))
        output = tmp_path / "Nigeria" / "2018-19" / "_" / "interview_date.parquet"
        frame = pd.read_parquet(output)
        expected = pd.DataFrame(
            {
                "int_t": pd.array(["2018-11-04 23:59:59.987", "2018-11-05 00:01:04.012",
                                   "2019-04-16 04:05:06.789"], dtype="datetime64[ms]"),
                "InterviewResult": pd.array(["Completed", "Unable to identify household",
                                            "Refused"], dtype="str"),
            },
            index=pd.MultiIndex.from_tuples(
                [("001", "2018Q3"), ("003", "2018Q3"), ("001", "2019Q1")], names=["i", "t"]
            ),
        )
        pd.testing.assert_frame_equal(frame, expected, check_exact=True, check_index_type=False,
                                      check_dtype=False)
        assert str(frame["int_t"].dtype) == "datetime64[ms]"
        assert frame["int_t"].dt.tz is None
        assert read == list(covers)
        reports = local_tools.read_parquet_datetime_audit(output)
        assert {r["wave"]: (r["n"], r["missing"], r["wave_folder"]) for r in reports} == {
            "2018Q3": (3, 1, "2018-19"),
            "2019Q1": (2, 1, "2018-19"),
        }
        assert all(r["parsed"] == r["n"] - r["missing"] for r in reports)
    finally:
        _clear_datetime_reports()
