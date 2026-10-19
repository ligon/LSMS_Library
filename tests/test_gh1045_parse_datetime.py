"""GH #1045: one build-time parser for every calendar quantity.

Charter: ``.coder/charter-1045-date-parser.md`` §2.  Data-free except where a
test says otherwise: every input kind, sentinels, out-of-range components,
fieldwork windows, calendars, the strict lever, the partial-date derivation,
the read-time pass-through, the scoped cache tag, and the stamp/replay of the
parse report across a FRESH process for a YAML-path and a script-path table.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import warnings
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library.country as C
from lsms_library import _build_registry as R
from lsms_library import datetime_parse as DP
from lsms_library import local_tools as lt
from lsms_library.calendars import ethiopian_to_gregorian
from lsms_library.datetime_parse import (
    CALENDAR_TABLES, CALENDAR_TYPES, DERIVATION_NAMES, DatetimeCoercionWarning,
    DatetimeParseError, DatetimeParseWarning, DatetimeSpecError, DatetimeWarning,
    derivation_key,
    DatetimeStringAtReadWarning, assign_datetime, datetime_reports,
    parse_datetime, spec_source_vars)

REPO = Path(__file__).resolve().parents[1]
PARTITION = ("parsed", "missing", "sentinel", "out_of_range", "incomplete",
             "ambiguous", "unparsed")


@pytest.fixture(autouse=True)
def _clean_ledger(monkeypatch):
    monkeypatch.delenv("LSMS_DATE_STRICT", raising=False)
    DP._clear_datetime_reports()
    yield
    DP._clear_datetime_reports()


def _parse(data, **kw):
    kw.setdefault("calendar", "gregorian")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        res = parse_datetime(data, **kw)
    return res, [w for w in caught if issubclass(w.category, DatetimeWarning)]


def _one(res):
    (rep,) = res.reports
    assert sum(rep[k] for k in PARTITION) == rep["n"], rep    # a partition
    return rep


# ---------------------------------------------------------------------------
# 1. Input kinds
# ---------------------------------------------------------------------------

class TestInputKinds:
    def test_components_full(self):
        res, w = _parse(pd.DataFrame({"y": [2014, 2015], "m": [7, 1], "d": [18, 31]}))
        assert res.values.dtype == "datetime64[us]"
        assert res.values.tolist() == [pd.Timestamp("2014-07-18"), pd.Timestamp("2015-01-31")]
        assert _one(res)["parsed"] == 2 and not w
        assert res.derivation.isna().all()

    def test_components_with_time(self):
        res, _ = _parse(pd.DataFrame({"y": [2014], "m": [7], "d": [18], "H": [9], "M": [5], "S": [3]}))
        assert res.values.iloc[0] == pd.Timestamp("2014-07-18 09:05:03")

    def test_components_month_names_float_and_label_strings(self):
        # Tanzania 2008-15: month NAME + float day/year; Mali 2017-18 labels.
        res, _ = _parse(pd.DataFrame({"y": [2008.0, "Année 2017", "2014.0"],
                                      "m": ["DECEMBER", "Septembre", "août"],
                                      "d": [13.0, 6, "5"]}))
        assert res.values.tolist() == [pd.Timestamp("2008-12-13"), pd.Timestamp("2017-09-06"),
                                       pd.Timestamp("2014-08-05")]

    def test_partial_year_month_is_day_one_and_keyed(self):
        res, w = _parse(pd.DataFrame({"y": [2010, 2010], "m": [11, 11], "d": [np.nan, 4]}),
                        table="interview_date")
        assert res.values.tolist() == [pd.Timestamp("2010-11-01"), pd.Timestamp("2010-11-04")]
        assert res.derivation.tolist()[0] == derivation_key("imputed_day", "interview_date") \
            == "::interview_date::date-day-imputed"
        assert pd.isna(res.derivation.iloc[1])
        rep = _one(res)
        assert rep["imputed_day"] == 1 and rep["parsed"] == 2
        assert not w, "a registered derivation is disclosed by its key, not a warning"

    def test_year_only_is_january_first_and_keyed(self):
        res, _ = _parse(pd.DataFrame({"y": [2010], "m": [np.nan], "d": [np.nan]}),
                        table="interview_date")
        assert res.values.iloc[0] == pd.Timestamp("2010-01-01")
        assert res.derivation.iloc[0] == "::interview_date::date-month-day-imputed"
        assert _one(res)["imputed_month"] == 1

    def test_day_without_month_is_incomplete_not_padded(self):
        res, w = _parse(pd.DataFrame({"y": [2010], "m": [np.nan], "d": [5]}))
        assert pd.isna(res.values.iloc[0]) and _one(res)["incomplete"] == 1 and w

    def test_stata_td(self):
        res, _ = _parse(pd.Series([0, 21900, np.nan]), stata="td")
        assert res.values.tolist()[:2] == [pd.Timestamp("1960-01-01"), pd.Timestamp("2019-12-17")]
        assert _one(res)["missing"] == 1

    def test_stata_tc(self):
        res, _ = _parse(pd.Series([86_400_000.0 + 3_600_000.0]), stata="tc")
        assert res.values.iloc[0] == pd.Timestamp("1960-01-02 01:00:00")

    def test_spss_seconds_since_1582(self):
        secs = (pd.Timestamp("2019-12-17") - pd.Timestamp("1582-10-14")).total_seconds()
        res, _ = _parse(pd.Series([secs]), spss=True)
        assert res.values.iloc[0] == pd.Timestamp("2019-12-17")

    def test_string_with_format(self):
        res, w = _parse(pd.Series(["2018-11-14T06:07:12", "14/11/2018"]), format="%Y-%m-%dT%H:%M:%S")
        assert res.values.iloc[0] == pd.Timestamp("2018-11-14 06:07:12")
        assert pd.isna(res.values.iloc[1]) and _one(res)["unparsed"] == 1 and w

    def test_string_without_format_year_first_per_value(self):
        # The #798 failure: two spellings in one column; each value on its own.
        res, w = _parse(pd.Series(["2014-08-05 00:00:00", "2018-11-14", "2019-09-28T10:47:45"]))
        assert res.values.tolist() == [pd.Timestamp("2014-08-05"), pd.Timestamp("2018-11-14"),
                                       pd.Timestamp("2019-09-28 10:47:45")]
        assert not w

    def test_string_without_format_year_last_settled_by_evidence(self):
        idx = pd.MultiIndex.from_tuples([("a", "1"), ("a", "2"), ("b", "1"), ("b", "2")],
                                        names=["t", "i"])
        res, w = _parse(pd.Series(["13/01/2019", "05/01/2019", "01/13/2019", "01/05/2019"], index=idx))
        assert res.values.tolist() == [pd.Timestamp("2019-01-13"), pd.Timestamp("2019-01-05"),
                                       pd.Timestamp("2019-01-13"), pd.Timestamp("2019-01-05")]
        assert not w

    def test_string_without_format_year_last_ambiguous_is_nat_and_reported(self):
        res, w = _parse(pd.Series(["01/05/2019", "02/06/2019"]))
        assert res.values.isna().all() and _one(res)["ambiguous"] == 2
        assert w and isinstance(w[0].message, DatetimeParseWarning)

    def test_datetime64_input_passes_through(self):
        s = pd.Series(pd.to_datetime(["2014-07-18 10:00", None]))
        res, _ = _parse(s)
        assert res.values.dtype == "datetime64[us]"
        assert res.values.iloc[0] == pd.Timestamp("2014-07-18 10:00")
        rep = _one(res)
        assert rep["parsed"] == 1 and rep["missing"] == 1

    def test_resolution_day_floors(self):
        res, _ = _parse(pd.Series(["2018-11-14T06:07:12"]), format="%Y-%m-%dT%H:%M:%S",
                        resolution="day")
        assert res.values.iloc[0] == pd.Timestamp("2018-11-14")
        assert res.values.dtype == "datetime64[us]"


# ---------------------------------------------------------------------------
# 2. Sentinels, out-of-range, windows: counted, never coerced
# ---------------------------------------------------------------------------

class TestCounting:
    def test_sentinels_are_sentinels_not_failures(self):
        res, w = _parse(pd.Series(["##N/A##", "2014-08-05", ""]), sentinels=["##N/A##"])
        rep = _one(res)
        assert rep["sentinel"] == 1 and rep["unparsed"] == 0 and rep["missing"] == 1
        assert rep["examples"]["sentinel"] == ["##N/A##"]
        assert pd.isna(res.values.iloc[0])
        assert w   # a sentinel count is information: the report warns (and replays)

    def test_numeric_sentinel_in_a_component(self):
        res, _ = _parse(pd.DataFrame({"y": [9999, 2014], "m": [1, 1], "d": [1, 99]}),
                        sentinels=[9999, 99])
        rep = _one(res)
        assert rep["sentinel"] == 2 and res.values.isna().all()
        assert res.derivation.isna().all(), "a sentinel day is not a blank day: never padded"

    def test_out_of_range_components_counted_not_rolled(self):
        res, w = _parse(pd.DataFrame({"y": [2014, 2014, 2009, 1700, 2014],
                                      "m": [13, 1, 2, 1, 1],
                                      "d": [1, 32, 31, 1, 1.5]}))
        rep = _one(res)
        assert rep["out_of_range"] == 5 and res.values.isna().all() and w
        assert "2014/13/1" in rep["examples"]["out_of_range"][0]

    def test_window_counts_and_keeps(self):
        res, w = _parse(pd.Series(["2014-06-30", "2014-07-01", "2015-03-31 23:00", "2015-04-01"]),
                        window=["2014-07-01", "2015-03-31"])
        rep = _one(res)
        assert rep["outside_window"] == 2 and rep["parsed"] == 4
        assert res.values.notna().all(), "an out-of-window date is reported, never dropped"
        assert w

    def test_reports_are_per_wave_t(self):
        idx = pd.MultiIndex.from_tuples([("2008-09", "1"), ("2010-11", "1"), ("2010-11", "2")],
                                        names=["t", "i"])
        res, _ = _parse(pd.DataFrame({"y": [2009, 2010, 2010], "m": [2, 11, 11], "d": [31, 1, np.nan]},
                                     index=idx), country="X", table="interview_date",
                        wave_folder="2008-15")
        by = {r["wave"]: r for r in res.reports}
        assert by["2008-09"]["out_of_range"] == 1 and by["2010-11"]["imputed_day"] == 1
        assert all(r["wave_folder"] == "2008-15" for r in res.reports)
        assert len(datetime_reports("X", "interview_date")) == 2


# ---------------------------------------------------------------------------
# 3. Calendars, kinds, the spec
# ---------------------------------------------------------------------------

class TestCalendarsKindsSpec:
    def test_ethiopian_components_go_through_calendars(self):
        res, _ = _parse(pd.DataFrame({"y": [2011, 2012, 2011], "m": [1, 13, "Tir"], "d": [1, 5, 10]}),
                        calendar="ethiopian")
        assert res.values.tolist() == [pd.Timestamp(ethiopian_to_gregorian(2011, 1, 1)),
                                       pd.Timestamp(ethiopian_to_gregorian(2012, 13, 5)),
                                       pd.Timestamp(ethiopian_to_gregorian(2011, 5, 10))]
        assert _one(res)["parsed"] == 3

    def test_ethiopian_invalid_day_is_out_of_range(self):
        res, _ = _parse(pd.DataFrame({"y": [2011], "m": [13], "d": [7]}), calendar="ethiopian")
        assert pd.isna(res.values.iloc[0]) and _one(res)["out_of_range"] == 1

    def test_ethiopian_refuses_gregorian_encodings(self):
        with pytest.raises(DatetimeSpecError):
            parse_datetime(pd.Series(["2011-01-01"]), calendar="ethiopian")

    def test_no_calendar_refused(self):
        with pytest.raises(DatetimeSpecError, match="calendar"):
            parse_datetime(pd.Series(["2014-08-05"]))
        with pytest.raises(DatetimeSpecError, match="calendar"):
            spec_source_vars("Int_t", {"datetime": "s00q23a"})
        with pytest.raises(DatetimeSpecError, match="calendar"):
            parse_datetime(pd.Series(["2014-08-05"]), calendar="julian")

    def test_unknown_spec_key_refused(self):
        with pytest.raises(DatetimeSpecError, match="unknown"):
            spec_source_vars("Int_t", {"datetime": "x", "calendar": "gregorian", "fromat": "%Y"})

    def test_spec_source_vars_components_and_single(self):
        assert spec_source_vars("Int_t", {"datetime": {"y": "a", "m": "b"}, "calendar": "gregorian"}) \
            == {"Int_t\x1fy": "a", "Int_t\x1fm": "b"}
        assert spec_source_vars("Int_t", {"datetime": "s", "calendar": "gregorian"}) == {"Int_t\x1f": "s"}

    def test_month_of_year_kind(self):
        res, w = _parse(pd.Series(["March", 3, "13", "x", None, 12.0]), kind="month")
        assert str(res.values.dtype) == "Int64"
        assert res.values.tolist()[:2] == [3, 3] and res.values.iloc[5] == 12
        rep = _one(res)
        assert rep["out_of_range"] == 1 and rep["unparsed"] == 1 and rep["missing"] == 1 and w

    def test_year_kind(self):
        res, _ = _parse(pd.Series([2014.0, "2015", 99]), kind="year")
        assert str(res.values.dtype) == "Int64"
        assert res.values.tolist()[:2] == [2014, 2015] and pd.isna(res.values.iloc[2])
        assert _one(res)["out_of_range"] == 1


# ---------------------------------------------------------------------------
# 4. The strict lever and the warning family
# ---------------------------------------------------------------------------

class TestLever:
    def test_default_warns_with_the_family_class(self):
        _, w = _parse(pd.Series(["garbage"]))
        assert w and issubclass(w[0].category, DatetimeParseWarning)
        # Siblings under one base: the build-time report is not a read-time
        # coercion failure, and a filter on the latter (#798) must not see it.
        assert issubclass(DatetimeParseWarning, DatetimeWarning)
        assert issubclass(DatetimeCoercionWarning, DatetimeWarning)
        assert not issubclass(DatetimeParseWarning, DatetimeCoercionWarning)
        assert w[0].message.report["unparsed"] == 1

    def test_strict_raises(self, monkeypatch):
        monkeypatch.setenv("LSMS_DATE_STRICT", "1")
        with pytest.raises(DatetimeParseError):
            parse_datetime(pd.Series(["garbage"]), calendar="gregorian")

    @pytest.mark.parametrize("val", ["true", "yes", "TRUE"])
    def test_strict_spellings(self, monkeypatch, val):
        monkeypatch.setenv("LSMS_DATE_STRICT", val)
        with pytest.raises(DatetimeParseError):
            parse_datetime(pd.Series(["garbage"]), calendar="gregorian")

    def test_strict_is_its_own_lever(self, monkeypatch):
        monkeypatch.setenv("LSMS_READ_STRICT", "1")
        monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
        _parse(pd.Series(["garbage"]))     # warns, does not raise

    def test_a_clean_parse_is_silent_but_filed(self):
        res, w = _parse(pd.Series(["2014-08-05"]), country="X", table="interview_date")
        assert not w and datetime_reports("X", "interview_date") == res.reports

    def test_country_reexports_the_classes(self):
        assert C.DatetimeCoercionWarning is DatetimeCoercionWarning
        assert C.DatetimeAmbiguityWarning is DP.DatetimeAmbiguityWarning


# ---------------------------------------------------------------------------
# 5. The partial-date derivation
# ---------------------------------------------------------------------------

class TestPartialDateDerivation:
    def test_registered_for_every_calendar_table_and_loadable(self):
        from lsms_library.derivations import framework_records, records_for
        recs = framework_records()
        for table in CALENDAR_TABLES:
            for which in DERIVATION_NAMES:
                key = derivation_key(which, table)
                assert key in recs, key
                r = recs[key]
                assert r.country is None and r.table == table
                assert "modelling-choice" in r.bases
                assert callable(r.resolve("function")) and callable(r.resolve("inputs"))
                assert key in records_for("Tanzania", table)

    def test_not_attached_to_other_tables(self):
        """An empty table slot would list both keys, at rows 0, on every table."""
        from lsms_library.derivations import records_for
        assert not [k for k in records_for("Uganda", "food_acquired") if "date-" in k]

    def test_listed_by_ll_derivations(self):
        import lsms_library as ll
        listed = ll.derivations()
        for table in CALENDAR_TABLES:
            for which in DERIVATION_NAMES:
                assert derivation_key(which, table) in listed.index

    def test_every_padded_date_carries_the_key(self):
        df = pd.DataFrame(index=pd.RangeIndex(4))
        out = assign_datetime(df, "Int_t", pd.DataFrame({"y": [2010, 2010, 2010, 2010],
                                                         "m": [11, 11, np.nan, 5],
                                                         "d": [np.nan, 4, np.nan, np.nan]}),
                              calendar="gregorian", table="interview_date")
        padded = out["Int_t"].dt.day.eq(1) & out.index.isin([0, 2, 3])
        assert padded.sum() == 3
        assert out.loc[padded, "Derivation"].notna().all()
        assert pd.isna(out.loc[1, "Derivation"])

    def test_a_row_with_an_existing_key_carries_both(self):
        from lsms_library.derivations import MULTI_SEP, summary_for_frame
        df = pd.DataFrame({"Derivation": ["Mali::interview_date::visit-x", pd.NA]})
        out = assign_datetime(df, "Int_t", pd.DataFrame({"y": [2014, 2014], "m": [3, 4]}),
                              calendar="gregorian", table="interview_date")
        key = "::interview_date::date-day-imputed"
        assert out.loc[0, "Derivation"] == MULTI_SEP.join(
            sorted(["Mali::interview_date::visit-x", key]))
        assert out.loc[1, "Derivation"] == key
        summary = summary_for_frame(out, "Mali", "interview_date")
        assert summary[key]["rows"] == 2 and summary[key]["columns"] == ["Int_t"]
        assert summary["Mali::interview_date::visit-x"]["rows"] == 1

    def test_inputs_callable_lists_the_padding_reports(self):
        _parse(pd.DataFrame({"y": [2010], "m": [11], "d": [np.nan]}), country="X",
               table="interview_date", wave="2010-11")
        frame = DP.partial_date_inputs()
        assert frame[["country", "wave", "imputed_day"]].values.tolist() == [["X", "2010-11", 1]]


# ---------------------------------------------------------------------------
# 6. The read-time cast
# ---------------------------------------------------------------------------

class TestReadTimeCast:
    @pytest.mark.parametrize("unit", ["us", "ms", "ns"])
    def test_datetime64_is_a_pass_through(self, unit):
        s = pd.Series(pd.to_datetime(["2014-07-18 10:00:01", None])).astype(f"datetime64[{unit}]")
        out = C._coerce_datetime_column(s, country="X", table="interview_date", column="Int_t")
        assert out is s, "a build-time parsed column must be served exactly as stored"

    def test_string_switch_off_by_default(self):
        assert DP.READ_CAST_WARNS_ON_STRINGS is False
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            C._coerce_datetime_column(pd.Series(["2014-08-05"]), country="X",
                                      table="interview_date", column="Int_t")
        assert not [w for w in caught if issubclass(w.category, DatetimeStringAtReadWarning)]

    def test_string_switch_on_warns(self, monkeypatch):
        monkeypatch.setattr(DP, "READ_CAST_WARNS_ON_STRINGS", True)
        with pytest.warns(DatetimeStringAtReadWarning, match="1 string value"):
            out = C._coerce_datetime_column(pd.Series(["2014-08-05", None]), country="X",
                                            table="interview_date", column="Int_t")
        assert out.iloc[0] == pd.Timestamp("2014-08-05"), "the warning does not change the cast"

    def test_pr1025_contract_unchanged_for_unmigrated_strings(self):
        with pytest.warns(DP.DatetimeAmbiguityWarning):
            out = C._coerce_datetime_column(pd.Series(["01/05/2019"]), country="X",
                                            table="interview_date", column="Int_t")
        assert out.isna().all()


# ---------------------------------------------------------------------------
# 7. The scoped cache tag (decision 1)
# ---------------------------------------------------------------------------

_ENTRY_POINTS = ("parse_datetime", "assign_datetime", "spec_source_vars",
                 "apply_datetime_specs", "is_datetime_spec")


def _calendar_tables_in_data_info() -> set[str]:
    info = yaml.safe_load((REPO / "lsms_library" / "data_info.yml").read_text())
    return {t for t, cols in (info.get("Columns") or {}).items()
            for c, meta in (cols or {}).items()
            if isinstance(meta, dict) and meta.get("type") in CALENDAR_TYPES}


class TestScopedTag:
    def test_tables_list_equals_the_calendar_columns_in_data_info(self):
        assert set(CALENDAR_TABLES) == _calendar_tables_in_data_info()

    def test_entry_points_are_tagged_for_exactly_those_tables_and_excluded_from_recursion(self):
        for name in _ENTRY_POINTS:
            qn = f"lsms_library.datetime_parse.{name}"
            assert qn in R._BUILD_TRANSFORMS, qn
            assert set(R._BUILD_TRANSFORMS[qn][1]) == set(CALENDAR_TABLES), qn
            assert qn in R._EXCLUDED_CALLABLES, qn
        assert "_DATETIME_LEDGER" in R._EXCLUDED_CONSTANTS

    def test_every_spec_and_script_call_is_on_the_tables_list(self):
        """The exclusion is safe only if no table OFF the list uses the parser:
        such a table's parquet would depend on code its fingerprint omits."""
        roots = REPO / "lsms_library" / "countries"
        offenders = []
        for p in roots.glob("*/*/_/data_info.yml"):
            try:
                info = yaml.safe_load(p.read_text()) or {}
            except yaml.YAMLError:
                continue
            for table, block in info.items():
                if not isinstance(block, dict):
                    continue
                blocks = [block] + [b for b in (block.get("dfs") or {}).values()
                                    if isinstance(b, dict)] if isinstance(block.get("dfs"), dict) else [block]
                for b in blocks:
                    for col, v in (b.get("myvars") or {}).items():
                        if DP.is_datetime_spec(v) and table not in CALENDAR_TABLES:
                            offenders.append(f"{p}:{table}.{col}")
        for p in list(roots.glob("*/_/*.py")) + list(roots.glob("*/*/_/*.py")):
            src = p.read_text(errors="ignore")
            if ("parse_datetime(" in src or "assign_datetime(" in src) \
                    and p.stem not in CALENDAR_TABLES:
                offenders.append(str(p))
        assert not offenders, offenders

    def test_a_noop_parser_edit_moves_exactly_the_scoped_tables(self, monkeypatch):
        """Positive and negative control, measured with the real fingerprint."""
        tables = sorted({"interview_date", "household_roster", "food_acquired",
                         "cluster_features", "sample", "crop_production", "shocks"})
        R.clear_caches()
        before = {t: R.build_transforms_fingerprint(t) for t in tables}
        real_norm = R._norm

        def edited(fn):
            out = real_norm(fn)
            if getattr(fn, "__module__", "") == "lsms_library.datetime_parse":
                out += "#noop-edit"
            return out

        monkeypatch.setattr(R, "_norm", edited)
        R.build_transforms_fingerprint.cache_clear()
        after = {t: R.build_transforms_fingerprint(t) for t in tables}
        monkeypatch.undo()
        R.clear_caches()
        moved = {t for t in tables if before[t] != after[t]}
        assert moved == set(CALENDAR_TABLES), moved

    def test_grab_data_does_not_fold_the_parser(self):
        seen, parts = set(), []
        parts += R._closure_parts(C.Wave.grab_data, seen)
        assert not [p for p in parts if p.startswith("lsms_library.datetime_parse.")]


# ---------------------------------------------------------------------------
# 8. Stamp and replay across a FRESH process (YAML path and script path)
# ---------------------------------------------------------------------------

_YAML_SCHEME = """\
Waves: ['2020']
Data Scheme:
  interview_date:
    index: (t, i)
    Int_t: datetime
"""

_YAML_INFO = """\
interview_date:
  file: cover.csv
  idxvars:
    i: hid
  myvars:
    Int_t:
      datetime: when
      format: '%Y-%m-%d'
      sentinels: ['##N/A##']
      calendar: gregorian
"""

_SCRIPT_SCHEME = """\
Waves: ['2020']
Data Scheme:
  interview_date:
    index: (t, i)
    Int_t: datetime
    materialize: make
"""

_MAKEFILE = """\
../%/_/interview_date.parquet: ../%/_/interview_date.py
\tcd $(@D) && python interview_date.py

%.parquet: %.py
\tcd $(@D) && python $(<F)
"""

_SCRIPT = """\
import os
from pathlib import Path
import pandas as pd
from lsms_library.local_tools import to_parquet, assign_datetime, datetime_reports
raw = pd.DataFrame({"t": ["2020"] * 3, "i": ["h1", "h2", "h3"],
                    "y": [2020, 9999, 2020], "m": [3, 1, 4], "d": [2, 1, 9]}).set_index(["t", "i"])
df = assign_datetime(raw.drop(columns=["y", "m", "d"]), "Int_t", raw[["y", "m", "d"]],
                     calendar="gregorian", sentinels=[9999], country="{country}",
                     table="interview_date", wave_folder="2020")
# Relative, exactly as Tanzania 2008-15/_/interview_date.py writes: the
# redirect to data_root() is what the real script relies on (GH #803).
to_parquet(df, "interview_date.parquet",
           datetime_audit=datetime_reports("{country}", "interview_date"))
"""

_PROBE = """\
import json, warnings, sys
import lsms_library as ll
from lsms_library.datetime_parse import datetime_reports, DatetimeParseWarning
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    if len(sys.argv) > 2:      # wave level: the L2-wave / script parquet read
        df = ll.Country(sys.argv[1])[sys.argv[2]].grab_data("interview_date")
    else:
        df = ll.Country(sys.argv[1]).interview_date()
print(json.dumps({
    "rows": len(df), "dtype": str(df["Int_t"].dtype),
    "warned": [str(w.message) for w in caught if issubclass(w.category, DatetimeParseWarning)],
    "reports": datetime_reports(sys.argv[1], "interview_date")}, default=str))
"""


def _env(tmp_path):
    env = dict(os.environ)
    env.update(LSMS_COUNTRIES_ROOT=str(tmp_path / "countries"),
               LSMS_DATA_DIR=str(tmp_path / "data"),
               LSMS_ISSUES_LOG=str(tmp_path / "issues.log"),
               PYTHONPATH=str(REPO) + os.pathsep + env.get("PYTHONPATH", ""),
               LSMS_BUILD_WORKERS="1")
    env.pop("LSMS_NO_CACHE", None)
    env.pop("LSMS_DATE_STRICT", None)
    return env


def _probe(tmp_path, country, *extra):
    r = subprocess.run([sys.executable, "-c", _PROBE, country, *extra], env=_env(tmp_path),
                       cwd=str(tmp_path), capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stderr[-3000:]
    out = json.loads(r.stdout.strip().splitlines()[-1])
    out["_stderr"] = r.stderr
    return out


# The two L2-country write sites a cold build can take (r1 F1).  A real
# checkout has a DVC repo at countries_root(), so `Repo()` opens and the build
# takes the NO-STAGE write (`if not stage_infos:` in load_dataframe_with_dvc);
# a tree without one (pip install, a bare LSMS_COUNTRIES_ROOT) raises
# NotDvcRepoError and takes the DVC-FALLBACK write.  The fixtures used to take
# only the latter, so dropping a stamp at the production write left every
# synthetic test green.  The no-stage fixture carries the SHIPPED `.dvc/config`
# (its remote stanza is required: `local_tools._build_dvcfs` adds credentials to
# `ligonresearch_s3` whenever a `.dvc/config` exists, and DVC refuses a remote
# without a url) plus `no_scm`, since the tmp tree is not in git.  No dvc.yaml,
# so no stage resolves; every source is a local file, so nothing is fetched.
WRITES = ("no-stage", "dvc-fallback")
_SHIPPED_DVC_CONFIG = REPO / "lsms_library" / "countries" / ".dvc" / "config"
_FALLBACK_MARK = "DVC unavailable for interview_date"


def _assert_write(cold, write):
    """The cold build took the write route the test names."""
    assert (_FALLBACK_MARK in cold["_stderr"]) == (write == "dvc-fallback"), (
        write, cold["_stderr"][-2000:])


def _make_country(tmp_path, name, scheme, info=None, makefile=None, script=None,
                  write="dvc-fallback"):
    assert write in WRITES, write
    if write == "no-stage":
        shipped = _SHIPPED_DVC_CONFIG.read_text()
        assert shipped.startswith("[core]\n"), "shipped .dvc/config changed shape"
        dvc = tmp_path / "countries" / ".dvc"
        dvc.mkdir(parents=True, exist_ok=True)
        (dvc / "config").write_text(shipped.replace("[core]\n", "[core]\n    no_scm = true\n", 1))
    c = tmp_path / "countries" / name
    (c / "_").mkdir(parents=True)
    (c / "2020" / "_").mkdir(parents=True)
    (c / "2020" / "Data").mkdir(parents=True)
    (c / "_" / "data_scheme.yml").write_text(scheme)
    if info:
        (c / "2020" / "_" / "data_info.yml").write_text(info)
        pd.DataFrame({"hid": ["h1", "h2", "h3"],
                      "when": ["2020-03-02", "##N/A##", "2020-04-09"]}
                     ).to_csv(c / "2020" / "Data" / "cover.csv", index=False)
    if makefile:
        (c / "_" / "Makefile").write_text(makefile)
    if script:
        (c / "2020" / "_" / "interview_date.py").write_text(script.replace("{country}", name))
    return c


@pytest.mark.parametrize("write", WRITES)
@pytest.mark.parametrize("route", ["yaml", "script"])
def test_the_report_is_stamped_and_replayed_in_a_fresh_process(tmp_path, route, write):
    name = f"Dateland{route.capitalize()}"
    if route == "yaml":
        _make_country(tmp_path, name, _YAML_SCHEME, info=_YAML_INFO, write=write)
    else:
        _make_country(tmp_path, name, _SCRIPT_SCHEME, makefile=_MAKEFILE, script=_SCRIPT,
                      write=write)

    cold = _probe(tmp_path, name)                    # process 1: builds
    _assert_write(cold, write)
    assert cold["rows"] == 2 and cold["dtype"] == "datetime64[us]"
    assert cold["warned"], "the cold build must warn about the sentinel"
    (rep,) = [r for r in cold["reports"] if r["sentinel"]]
    assert rep["sentinel"] == 1 and rep["parsed"] == 2 and rep["wave"] == "2020"

    data = tmp_path / "data" / name
    for p in (data / "2020" / "_" / "interview_date.parquet", data / "var" / "interview_date.parquet"):
        stamped = lt.read_parquet_datetime_audit(p)
        assert stamped and stamped[0]["sentinel"] == 1, p
        import pyarrow.parquet as pq
        assert str(pq.read_schema(p).field("Int_t").type) == "timestamp[us]", p

    parquets = [data / "2020" / "_" / "interview_date.parquet", data / "var" / "interview_date.parquet"]
    stamp = {p: p.stat().st_mtime_ns for p in parquets}

    warm = _probe(tmp_path, name)                    # process 2: warm read only
    assert {p: p.stat().st_mtime_ns for p in parquets} == stamp, "the read was not warm"
    assert [m for m in warm["warned"] if "replayed" in m]
    (wrep,) = [r for r in warm["reports"] if r["sentinel"]]
    assert wrep["from_cache"] is True
    assert {k: wrep[k] for k in PARTITION} == {k: rep[k] for k in PARTITION}

    wave = _probe(tmp_path, name, "2020")            # process 3: the WAVE stamp
    assert {p: p.stat().st_mtime_ns for p in parquets} == stamp
    assert [m for m in wave["warned"] if "replayed" in m]
    (vrep,) = [r for r in wave["reports"] if r["sentinel"]]
    assert vrep["from_cache"] is True and vrep["sentinel"] == 1
    assert not list((tmp_path / "countries").rglob("*.parquet")), "GH #803: nothing in-tree"


def test_a_clear_ledger_then_a_warm_wave_hit_replays(tmp_path, monkeypatch):
    """In-process L2-wave hit (no country parquet): the wave stamp replays."""
    from lsms_library.paths import countries_root, data_root
    _make_country(tmp_path, "Dateland", _YAML_SCHEME, info=_YAML_INFO)
    for k, v in _env(tmp_path).items():
        if k.startswith("LSMS_"):
            monkeypatch.setenv(k, v)
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    countries_root.cache_clear(); data_root.cache_clear()
    try:
        wave = C.Country("Dateland")["2020"]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = wave.grab_data("interview_date")
        assert str(df["Int_t"].dtype) == "datetime64[us]" and df["Int_t"].isna().sum() == 1
        DP._clear_datetime_reports()
        with pytest.warns(DatetimeParseWarning, match="replayed"):
            wave.grab_data("interview_date")
        (rep,) = datetime_reports("Dateland", "interview_date")
        assert rep["from_cache"] is True and rep["sentinel"] == 1
    finally:
        countries_root.cache_clear(); data_root.cache_clear()


def test_assume_cache_fresh_also_replays(tmp_path):
    """The `assume_cache_fresh=True` short-circuit serves the country parquet
    without the hash gate; it must still replay the date-parse report."""
    _make_country(tmp_path, "DatelandFresh", _YAML_SCHEME, info=_YAML_INFO)
    _probe(tmp_path, "DatelandFresh")                                # build
    code = _PROBE.replace("ll.Country(sys.argv[1])",
                          "ll.Country(sys.argv[1], assume_cache_fresh=True)")
    r = subprocess.run([sys.executable, "-c", code, "DatelandFresh"], env=_env(tmp_path),
                       cwd=str(tmp_path), capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stderr[-3000:]
    out = json.loads(r.stdout.strip().splitlines()[-1])
    assert out["warned"] and [rep for rep in out["reports"] if rep["sentinel"] == 1]


# ---------------------------------------------------------------------------
# 9. Red-team / review round 2 (each a defect found and fixed)
# ---------------------------------------------------------------------------

class TestRound2:
    def test_empty_input_with_a_multiindex(self):
        idx = pd.MultiIndex.from_tuples([], names=["t", "i"])
        res = parse_datetime(pd.Series([], index=idx, dtype=object), calendar="gregorian")
        assert len(res.values) == 0 and res.values.dtype == "datetime64[us]" and res.reports == []

    def test_absurd_stata_values_are_out_of_range_not_a_crash(self):
        res, _ = _parse(pd.Series([1e8, -1e9, 21900.0]), stata="td")
        rep = _one(res)
        assert rep["out_of_range"] == 2 and res.values.iloc[2] == pd.Timestamp("2019-12-17")

    def test_year_bound_applies_to_strings_and_datetime64(self):
        res, _ = _parse(pd.Series(["9999-12-31", "1700-01-01", "2014-08-05"]))
        assert _one(res)["out_of_range"] == 2
        res, _ = _parse(pd.Series(pd.to_datetime(["1700-01-01", "2014-08-05"])))
        assert _one(res)["out_of_range"] == 1

    def test_two_digit_year_is_ambiguous_not_guessed(self):
        res, w = _parse(pd.Series(["13/01/19", "05/01/19"]))
        assert res.values.isna().all() and _one(res)["ambiguous"] == 2 and w

    @pytest.mark.parametrize("kind,cols", [("month", ["y"]), ("year", ["m"]), ("date", ["m", "d"])])
    def test_a_kind_without_its_component_is_refused(self, kind, cols):
        with pytest.raises(DatetimeSpecError, match="needs a"):
            parse_datetime(pd.DataFrame({c: [1] for c in cols}), calendar="gregorian", kind=kind)

    def test_assign_datetime_refuses_a_misaligned_index(self):
        df = pd.DataFrame(index=pd.Index(["a", "b"]))
        data = pd.DataFrame({"y": [2014, 2015], "m": [1, 1], "d": [1, 1]}, index=["b", "a"])
        with pytest.raises(ValueError, match="same order"):
            assign_datetime(df, "Int_t", data, calendar="gregorian")

    def test_assign_datetime_keeps_a_duplicated_index_row_for_row(self):
        idx = pd.Index(["h1", "h1", "h2"])
        data = pd.DataFrame({"y": [2014, 2014, 2015], "m": [7, 12, 1], "d": [2, np.nan, 3]}, index=idx)
        out = assign_datetime(pd.DataFrame(index=idx), "Int_t", data, calendar="gregorian",
                              table="interview_date")
        assert out["Int_t"].tolist() == [pd.Timestamp("2014-07-02"), pd.Timestamp("2014-12-01"),
                                         pd.Timestamp("2015-01-03")]
        assert out["Derivation"].isna().tolist() == [True, False, True]

    def test_replay_is_filtered_to_the_waves_served(self):
        reports = [{"wave": "2008-09", "n": 1, "parsed": 0, "out_of_range": 1},
                   {"wave": "2019-20", "n": 1, "parsed": 1}]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            DP.replay_datetime_audit(reports, "X", "interview_date", waves=["2019-20"])
        assert [r["wave"] for r in datetime_reports("X", "interview_date")] == ["2019-20"]
        assert not [w for w in caught if issubclass(w.category, DatetimeParseWarning)]

    def test_strict_reaches_through_the_currency_conversion(self, monkeypatch):
        """conversion._interview_dates swallowed every exception and fell back to
        wave-nominal dates; under LSMS_DATE_STRICT that must be fatal."""
        import lsms_library
        from lsms_library import conversion

        class Boom:
            def __init__(self, *a, **k):
                self.data_scheme = ["interview_date"]

            def interview_date(self):
                raise DatetimeParseError("strict")

        monkeypatch.setattr(lsms_library, "Country", Boom)
        with pytest.raises(DatetimeParseError):
            conversion._interview_dates("X")

    def test_column_mapping_adds_no_key_when_no_spec(self):
        """ehcvm's raw-input lookup unpacks the mapping as one file."""
        w = C.Country("Togo")["2018"]
        mapping = w.column_mapping("food_acquired", w.resources["food_acquired"])
        assert "datetime_specs" not in mapping

    def test_column_mapping_carries_the_spec_when_present(self):
        w = C.Country("Mali")["2018-19"]
        mapping = w.column_mapping("interview_date", w.resources["interview_date"])
        assert set(mapping["datetime_specs"]) == {"Int_t"}
        (fname,) = [k for k in mapping if k not in ("df_edit", "datetime_specs")]
        assert mapping[fname]["myvars"] == {"Int_t\x1f": "s00q23a"}

    def test_a_per_file_override_of_a_spec_is_refused(self):
        w = C.Country("Mali")["2014-15"]
        info = dict(w.resources["interview_date"])
        info["file"] = ["EACICONTROLE_p1.dta",
                        {"EACICONTROLE_p2.dta": {"Int_t": {"datetime": "x", "calendar": "gregorian"}}}]
        with pytest.raises(ValueError, match="per-file override"):
            w.column_mapping("interview_date", info)


# ---------------------------------------------------------------------------
# 10. Coexistence with GH #675 (lsms_build_drops) and GH #908 (the extracted
#     L2-country read gate), after the 2026-10-18 corpus merge.  Two stamps on
#     one parquet, each written where it was written before and each replayed
#     at every gate that replayed it before.
# ---------------------------------------------------------------------------

_DROP = {"wave": "2020", "sub": "df_geo", "file": "DatelandDrops/2020/Data/geo.csv",
         "kind": "unavailable", "error": "FileNotFoundError: simulated",
         "missing_required": [], "missing_optional": ["Latitude"]}

# A fresh process in which the dropped source still cannot be fetched (the
# GH #675 probe answers False), so a degraded parquet is SERVED and warned.
_PROBE_BOTH = """\
import json, warnings, sys
import lsms_library as ll
import lsms_library.country as C
from lsms_library.datetime_parse import datetime_reports, DatetimeParseWarning
C._probe_source = lambda rel: False
kw = {"assume_cache_fresh": True} if sys.argv[2] == "fresh" else {}
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    df = ll.Country(sys.argv[1], **kw).interview_date()
print(json.dumps({
    "rows": len(df),
    "dates": [str(w.message) for w in caught if issubclass(w.category, DatetimeParseWarning)],
    "degraded": [str(w.message) for w in caught if issubclass(w.category, C.DegradedCacheWarning)],
    "reports": datetime_reports(sys.argv[1], "interview_date"),
    "drops": C.build_drop_reports(sys.argv[1], "interview_date")}, default=str))
"""


def _probe_both(tmp_path, country, mode="ordinary"):
    r = subprocess.run([sys.executable, "-c", _PROBE_BOTH, country, mode], env=_env(tmp_path),
                       cwd=str(tmp_path), capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stderr[-3000:]
    return json.loads(r.stdout.strip().splitlines()[-1])


def _add_drop_stamp(path):
    """Add an lsms_build_drops stamp, keeping every other key byte for byte."""
    import pyarrow.parquet as pq
    table = pq.read_table(path)
    md = dict(table.schema.metadata or {})
    keep = {k: v for k, v in md.items() if k.startswith(b"lsms_")}
    md[b"lsms_build_drops"] = json.dumps([_DROP]).encode()
    pq.write_table(table.replace_schema_metadata(md), path)
    after = pq.read_schema(path).metadata
    assert all(after[k] == v for k, v in keep.items())


def _keys(path):
    import pyarrow.parquet as pq
    return {k.decode() for k in (pq.read_schema(path).metadata or {}) if k.startswith(b"lsms_")}


@pytest.mark.parametrize("write", WRITES)
def test_date_and_drop_stamps_coexist_at_every_gate(tmp_path, write):
    name = "DatelandDrops"
    _make_country(tmp_path, name, _YAML_SCHEME, info=_YAML_INFO, write=write)
    _assert_write(_probe(tmp_path, name), write)              # process 1: cold build
    data = tmp_path / "data" / name
    wave_pq = data / "2020" / "_" / "interview_date.parquet"
    var_pq = data / "var" / "interview_date.parquet"
    assert {"lsms_cache_hash", "lsms_datetime_audit"} <= _keys(wave_pq)
    assert {"lsms_cache_hash", "lsms_cache_waves", "lsms_datetime_audit"} <= _keys(var_pq)
    for p in (wave_pq, var_pq):
        _add_drop_stamp(p)

    # (a) the L2-country gate, Country._read_l2_country_cache (GH #908):
    # served warm, and BOTH reports replayed.
    stamp = {p: p.stat().st_mtime_ns for p in (wave_pq, var_pq)}
    out = _probe_both(tmp_path, name)
    assert {p: p.stat().st_mtime_ns for p in stamp} == stamp, "the read was not warm"
    assert out["rows"] == 2
    assert [m for m in out["dates"] if "replayed" in m]
    assert [r for r in out["reports"] if r["sentinel"] == 1 and r["from_cache"]]
    assert out["degraded"] and "optional column(s) ['Latitude']" in out["degraded"][0]

    # (b) the L2-wave gate in Wave.grab_data: with the country parquet gone,
    # the wave parquet is served (not rebuilt), both stamps replay, and the
    # country parquet rebuilt from it carries BOTH forward.
    var_pq.unlink()
    out = _probe_both(tmp_path, name)
    assert wave_pq.stat().st_mtime_ns == stamp[wave_pq], "the wave parquet was rebuilt"
    assert [m for m in out["dates"] if "replayed" in m]
    assert out["degraded"]
    assert [d for d in out["drops"] if d["file"] == _DROP["file"]]
    assert {"lsms_cache_hash", "lsms_cache_waves", "lsms_datetime_audit",
            "lsms_build_drops"} <= _keys(var_pq)
    (carried,) = lt.read_parquet_build_drops(var_pq)
    assert {k: carried[k] for k in _DROP} == _DROP
    (dated,) = [r for r in lt.read_parquet_datetime_audit(var_pq) if r["sentinel"]]
    assert dated["sentinel"] == 1 and dated["wave"] == "2020"

    # (c) the assume_cache_fresh serve replays the date report.  (It replays
    # neither the grain audit nor the drop record -- pre-existing gaps of
    # those mechanisms, not pinned here.)
    out = _probe_both(tmp_path, name, "fresh")
    assert [m for m in out["dates"] if "replayed" in m]
    assert not list((tmp_path / "countries").rglob("*.parquet")), "GH #803: nothing in-tree"


# ---------------------------------------------------------------------------
# 10b. GH #1066: the grain audit is stamped at BOTH L2-country writes.  The
#      DVC-fallback write (a tree with no DVC repo: pip install, a bare
#      LSMS_COUNTRIES_ROOT) passed every stamp but `grain_audit=`, so a grain
#      loss warned on the cold build and was silent on every warm read -- the
#      #323 "the signal survives the cache" invariant, broken on one route.
# ---------------------------------------------------------------------------

_GRAIN_SCHEME = """\
Waves: ['2020']
Data Scheme:
  housing:
    index: (t, i)
    Roof: str
"""

_GRAIN_INFO = """\
housing:
  file: cover.csv
  idxvars:
    i: hid
  myvars:
    Roof: roof
"""

# Household h1 appears twice with DISAGREEING roofs: a destructive collapse.
_GRAIN_ROWS = {"hid": ["h1", "h1", "h2"], "roof": ["Grass", "Iron", "Tile"]}

_GRAIN_PROBE = """\
import json, warnings, sys
import lsms_library as ll
import lsms_library.country as C
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    df = ll.Country(sys.argv[1]).housing()
print(json.dumps({
    "served": sorted(map(list, df.reset_index()[["t", "i", "Roof"]].astype(str).values)),
    "warned": [str(w.message) for w in caught if issubclass(w.category, C.GrainCollapseWarning)],
    "reports": C.grain_reports(sys.argv[1], "housing")}, default=str))
"""


def _grain_probe(tmp_path, country):
    r = subprocess.run([sys.executable, "-c", _GRAIN_PROBE, country], env=_env(tmp_path),
                       cwd=str(tmp_path), capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stderr[-3000:]
    out = json.loads(r.stdout.strip().splitlines()[-1])
    out["_stderr"] = r.stderr
    return out


@pytest.mark.parametrize("write", WRITES)
def test_grain_audit_is_stamped_and_replayed_at_both_l2_country_writes(tmp_path, write):
    name = "Grainland"
    _make_country(tmp_path, name, _GRAIN_SCHEME, info=_GRAIN_INFO, write=write)
    pd.DataFrame(_GRAIN_ROWS).to_csv(tmp_path / "countries" / name / "2020" / "Data" / "cover.csv",
                                     index=False)

    cold = _grain_probe(tmp_path, name)                       # process 1: cold build
    assert ("DVC unavailable for housing" in cold["_stderr"]) == (write == "dvc-fallback"), (
        write, cold["_stderr"][-2000:])
    (msg,) = cold["warned"]
    assert "Grainland/housing/2020" in msg and "DESTROYED 1 of 3" in msg, msg
    (rep,) = cold["reports"]
    assert (rep["destroyed"], rep["rows"], rep["wave"]) == (1, 3, "2020") and not rep.get("from_cache")

    # The direct pin on the write site: the country parquet carries the audit.
    var_pq = tmp_path / "data" / name / "var" / "housing.parquet"
    assert {"lsms_cache_hash", "lsms_cache_waves", "lsms_grain_audit"} <= _keys(var_pq), (
        write, sorted(_keys(var_pq)))
    (stamped,) = lt.read_parquet_grain_audit(var_pq)
    assert (stamped["destroyed"], stamped["rows"]) == (1, 3)

    stamp = var_pq.stat().st_mtime_ns
    warm = _grain_probe(tmp_path, name)                       # process 2: warm read only
    assert var_pq.stat().st_mtime_ns == stamp, "the read was not warm"
    assert "DVC unavailable for housing" not in warm["_stderr"]
    assert warm["served"] == cold["served"] and len(warm["served"]) == 2
    (wmsg,) = warm["warned"]
    assert "Grainland/housing/2020" in wmsg and "DESTROYED 1 of 3" in wmsg, wmsg
    (wrep,) = warm["reports"]
    assert wrep["from_cache"] is True
    assert {k: wrep[k] for k in ("rows", "dropped", "destroyed", "conflicting_groups", "wave")} \
        == {k: rep[k] for k in ("rows", "dropped", "destroyed", "conflicting_groups", "wave")}
    assert not list((tmp_path / "countries").rglob("*.parquet")), "GH #803: nothing in-tree"


def test_a_forked_wave_relays_all_three_ledgers():
    """_parallel_waves relays the grain, date-parse and drop ledgers together:
    each reaches the parent's L2-country stamp."""
    from lsms_library import _parallel_waves as PW
    key = ("X", "interview_date")
    grain = {"country": "X", "table": "interview_date", "wave": "w1", "site": "test"}
    date = {"country": "X", "table": "interview_date", "wave": "w1", "sentinel": 1}
    drop = {**_DROP, "wave": "w1", "file": "X/w1/Data/geo.csv"}

    def build_one(w):
        C._GRAIN_LEDGER.setdefault(key, []).append(grain)
        DP._file_datetime_report(date)
        C._file_build_drop(*key, drop)
        return True, None

    saved = {n: dict(getattr(m, n)) for m, n in ((C, "_GRAIN_LEDGER"), (DP, "_DATETIME_LEDGER"),
                                                  (C, "_BUILD_DROP_LEDGER"))}
    try:
        for m, n in ((C, "_GRAIN_LEDGER"), (DP, "_DATETIME_LEDGER"), (C, "_BUILD_DROP_LEDGER")):
            getattr(m, n).pop(key, None)
        payload = PW._build_captured(build_one, "w1")
        assert payload["grain"] and payload["datetime"] and payload["drops"]
        for m, n in ((C, "_GRAIN_LEDGER"), (DP, "_DATETIME_LEDGER"), (C, "_BUILD_DROP_LEDGER")):
            getattr(m, n).pop(key, None)            # the parent never saw the filings
        PW.relay(payload, country="X", table="interview_date")
        assert C._GRAIN_LEDGER.get(key) == [grain]
        assert datetime_reports("X", "interview_date") == [date]
        assert C.build_drop_reports("X", "interview_date") == [drop]
    finally:
        for m, n in ((C, "_GRAIN_LEDGER"), (DP, "_DATETIME_LEDGER"), (C, "_BUILD_DROP_LEDGER")):
            getattr(m, n).clear()
            getattr(m, n).update(saved[n])


# ---------------------------------------------------------------------------
# 11. Real data, cache-gated (r1 F1): the PRODUCTION L2-country write stamps
#     the report, and a fresh-process warm read replays exactly it.
# ---------------------------------------------------------------------------

# Every wave's report is stamped; Tanzania's 2008-09 report is the one that
# warns (a recorded 2009/FEBRUARY/31, `out_of_range`, served NaT -- see
# Tanzania/_/CONTENTS.org, 2026-10-18); every Mali report is clean.
REAL_WAVES = {
    "Mali": ["2014-15", "2017-18", "2018-19", "2021-22"],
    "Tanzania": ["2008-09", "2010-11", "2012-13", "2014-15", "2019-20", "2020-21"],
}
REAL_WARNS = {"Mali": [], "Tanzania": ["2008-09"]}

_REAL_PROBE = """\
import json, sys, warnings
import lsms_library as ll
from lsms_library.datetime_parse import DatetimeWarning, datetime_reports
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    df = ll.Country(sys.argv[1]).interview_date()
dated = [w.message for w in caught if issubclass(w.category, DatetimeWarning)]
print(json.dumps({
    "pkg": ll.__file__, "rows": len(df),
    "dated": [{"cls": type(m).__name__, "report": getattr(m, "report", None),
               "msg": str(m)} for m in dated],
    "reports": datetime_reports(sys.argv[1], "interview_date")}, default=str))
"""


def _real_probe(country):
    """One fresh process reading the REAL corpus (this checkout's code, the
    session's data root).  ``PYTHONSAFEPATH`` is dropped so a ``make``
    subprocess's wave script can import its sibling by bare name."""
    env = dict(os.environ)
    for k in ("LSMS_NO_CACHE", "LSMS_DATE_STRICT", "PYTHONSAFEPATH"):
        env.pop(k, None)
    env["PYTHONPATH"] = str(REPO) + os.pathsep + env.get("PYTHONPATH", "")
    r = subprocess.run([sys.executable, "-c", _REAL_PROBE, country], env=env, cwd=str(REPO),
                       capture_output=True, text=True, timeout=1800)
    assert r.returncode == 0, r.stderr[-3000:]
    out = json.loads(r.stdout.strip().splitlines()[-1])
    assert Path(out["pkg"]).resolve().is_relative_to(REPO), out["pkg"]
    return out


@pytest.mark.parametrize("country", sorted(REAL_WAVES))
def test_real_l2_country_stamp_is_written_and_replayed_warm(country):
    from lsms_library.paths import data_root

    var_pq = data_root() / country / "var" / "interview_date.parquet"
    if not var_pq.exists():
        pytest.skip(f"{country} interview_date not cached here")

    # Process 1: an ordinary read.  If the parquet predates this code it is
    # stale and is rebuilt -- through load_dataframe_with_dvc's no-stage
    # write, the one a checkout with a DVC repo takes.  (That write lives in
    # the fingerprinted _aggregate_wave_data, so dropping its stamp moves the
    # hash and forces this rebuild on any root, warm or not.)
    _real_probe(country)

    # The footer the production write left.
    stamped = lt.read_parquet_datetime_audit(var_pq)
    assert stamped, f"{var_pq} carries no lsms_datetime_audit"
    assert sorted(r["wave"] for r in stamped) == REAL_WAVES[country]
    assert sorted(r["wave"] for r in stamped if DP._warn_worthy(r)) == REAL_WARNS[country]
    for r in stamped:
        assert sum(int(r.get(k) or 0) for k in PARTITION) == r["n"], r
    if country == "Tanzania":
        (feb31,) = [r for r in stamped if r["wave"] == "2008-09"]
        assert feb31["out_of_range"] == 1

    # Process 2: a fresh-process WARM read replays exactly the stamp -- one
    # warning per warn-worthy wave, of the build-time sibling class, and no
    # read-time DatetimeCoercionWarning (the cast passes datetime64 through).
    warm = _real_probe(country)
    assert warm["reports"] and all(r["from_cache"] for r in warm["reports"]), \
        "the second read was not served from the cache"
    assert sorted(r["wave"] for r in warm["reports"]) == REAL_WAVES[country]
    assert [d["cls"] for d in warm["dated"]] == ["DatetimeParseWarning"] * len(REAL_WARNS[country])
    assert [d["report"]["wave"] for d in warm["dated"]] == REAL_WARNS[country]
    assert all("replayed" in d["msg"] for d in warm["dated"])
    if country == "Tanzania":
        assert warm["dated"][0]["report"]["out_of_range"] == 1


# ---------------------------------------------------------------------------
# 12. The audit harness does not flag the provenance column (r1 §3): the
#     feature-audit scan relays diagnostics.is_this_feature_sane, whose
#     no_constant_columns check graded Tanzania's interview_date `warn` for a
#     Derivation column holding one key on 1 of 22,428 rows.
# ---------------------------------------------------------------------------

def _constant_check(df):
    from lsms_library.diagnostics import is_this_feature_sane
    (chk,) = [c for c in is_this_feature_sane(df, "Tanzania", "interview_date").checks
              if c.name == "no_constant_columns"]
    return chk


def test_derivation_column_is_not_a_constant_column():
    idx = pd.MultiIndex.from_tuples([("2010-11", f"h{k}") for k in range(5)], names=["t", "i"])
    df = pd.DataFrame({
        "Int_t": pd.to_datetime(["2010-11-01", "2010-11-03", "2010-11-04",
                                 "2010-11-05", "2010-11-09"]).astype("datetime64[us]"),
        "Derivation": pd.array([derivation_key("imputed_day", "interview_date")]
                               + [pd.NA] * 4, dtype="string"),
    }, index=idx)
    assert _constant_check(df).status == "pass"

    # Exempt by NAME: a genuinely constant column beside it is still reported,
    # and the provenance column is not named in the report.
    chk = _constant_check(df.assign(Rural="Rural"))
    assert chk.status == "warn" and "'Rural'" in chk.message and "Derivation" not in chk.message
