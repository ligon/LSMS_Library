"""GH #1547: actual grab_data, strict invented receiver, no source or parquet IO.

The fixture replaces only field boundaries; it never captures grab_data or
reimplements its handler. Dispatch verification uses a separately reviewed
guard plugin; ordinary CI installs the same strict fakes with monkeypatch.
Policy anchors: .coder/ledger/1547-optional-source-credentials.md §2–§4.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import os
import sys
import warnings

from botocore.exceptions import (
    BotoCoreError, NoCredentialsError, PartialCredentialsError,
)
from dvc.exceptions import DvcException
import pandas as pd
import pytest

from lsms_library import country as C, local_tools as LT
from lsms_library.paths import countries_root, data_root

TABLE = "alpha_table"
YEAR = "w1"
FILES = ("primary.fake", "secondary.fake", "sibling.fake")


class InventedCountry:
    __slots__ = ("name", "resources")

    def __init__(self):
        self.name = "Alpha"
        self.resources = {"Data Scheme": {TABLE: {
            "index": "(t, v)", "Region": "str", "Other": "Int64",
            "Latitude": {"type": "float", "optional": True},
        }}}


class InventedWave:
    """Duck receiver: no Country/Wave constructor, descriptors or getters."""
    __slots__ = ("country", "year", "name", "file_path", "data_scheme",
                 "resources", "formatting_functions", "harness")

    def __init__(self, harness):
        self.country = InventedCountry()
        self.year = YEAR
        self.name = "Alpha/w1"
        # Invented path strings only. No file or sidecar is created/read here.
        self.file_path = countries_root() / "Alpha" / YEAR
        self.data_scheme = [TABLE]
        self.resources = {TABLE: {
            "dfs": ["df_primary", "df_secondary", "df_sibling"],
            "merge_on": ["v"], "final_index": ["t", "v"],
            "df_primary": {"file": FILES[0], "idxvars": {"v": "v"},
                           "myvars": {"Region": "Region"}},
            "df_secondary": {"file": FILES[1], "idxvars": {"v": "v"},
                             "myvars": {"Latitude": "Latitude"}},
            "df_sibling": {"file": FILES[2], "idxvars": {"v": "v"},
                           "myvars": {"Other": "Other"}},
        }}
        self.formatting_functions = {}
        self.harness = harness

    def column_mapping(self, request, info):
        h = self.harness
        h.mapping_calls.append(request)
        if request in h.mapping_errors:
            raise h.mapping_errors[request]
        allowed = self.resources[TABLE]
        if request == TABLE:
            assert info is allowed and "dfs" not in info
        else:
            assert request in allowed["dfs"] and info is allowed[request]
        assert info["file"] in FILES
        return {"df_edit": None, info["file"]: {
            "idxvars": dict(info["idxvars"]), "myvars": dict(info["myvars"])}}

    def _read_l2_wave_cache(self, request, path):
        assert request == TABLE and path == self.harness.output
        self.harness.cache_misses += 1
        return None

    def _input_hash(self, request):
        assert request == TABLE
        return "invented-1547-hash"

    def _required_scheme_columns(self, request):
        assert request == TABLE
        return C._required_scheme_columns(self.country.resources["Data Scheme"][TABLE])

    def _cartesian_keys(self, *args):
        return C.Wave._cartesian_keys(self, *args)

    def _merge_subframes(self, *args, **kwargs):
        return C.Wave._merge_subframes(self, *args, **kwargs)


class Harness:
    """Every provider path and writer argument is finite and asserted."""
    def __init__(self):
        self.wave = InventedWave(self)
        self.output = data_root("Alpha") / YEAR / "_" / f"{TABLE}.parquet"
        idx = pd.Index(["c1", "c2"], name="v")
        self.frames = {
            FILES[0]: pd.DataFrame({"Region": pd.Series(["N", None], dtype=object, index=idx)}, index=idx),
            FILES[1]: pd.DataFrame({"Latitude": [1.5, 2.5]}, index=idx),
            FILES[2]: pd.DataFrame({"Other": pd.array([7, 8], dtype="Int64")}, index=idx),
        }
        self.errors = {}
        self.mapping_errors = {}
        self.tier = "dvc"
        self.fast_result = False
        self.read_calls, self.write_calls, self.tier_calls = [], [], []
        self.fast_calls, self.mapping_calls = [], []
        self.cache_misses = 0
        self.paths = {self.wave.file_path / "Data" / f for f in FILES}
        self.candidates = {}
        for path in self.paths:
            for spelling in (os.path.relpath(path, Path.cwd()),
                             str(path.relative_to(countries_root())), str(path)):
                self.candidates[spelling] = path.name

    def read(self, candidate, idxvars, **kwargs):
        assert candidate in self.candidates, candidate
        file = self.candidates[candidate]
        root = self.wave.resources[TABLE]
        info = (next(v for v in root.values()
                     if isinstance(v, dict) and v.get("file") == file)
                if "dfs" in root else root)
        assert idxvars == info["idxvars"]
        assert kwargs == {**info["myvars"], "convert_categoricals": True,
                          "missing_ok": False}
        self.read_calls.append(file)
        if file in self.errors:
            raise self.errors[file]
        return self.frames[file].copy(deep=True)

    def write(self, frame, path, **kwargs):
        assert path == self.output and type(frame) is pd.DataFrame
        assert kwargs == {
            "absolute_path": True, "cache_hash": "invented-1547-hash",
            "grain_audit": None, "datetime_audit": None,
            "build_drops": C._wave_build_drops("Alpha", TABLE, YEAR),
        }
        self.write_calls.append((frame.copy(deep=True), deepcopy(kwargs)))
        return frame

    def source_tier(self, path):
        assert path in self.paths, path
        self.tier_calls.append(path)
        return self.tier

    def fast(self, path):
        assert path == self.wave.file_path / "Data" / FILES[1], path
        self.fast_calls.append(path)
        if isinstance(self.fast_result, Exception):
            raise self.fast_result
        return self.fast_result

    def call(self, *, origin=False):
        return C.Wave.grab_data(self.wave, TABLE, _with_origin=origin)

    def records(self):
        return C.build_drop_reports("Alpha", TABLE)

    def required(self):
        self.wave.country.resources["Data Scheme"][TABLE]["Latitude"] = "float"


@pytest.fixture
def invented(monkeypatch):
    # No source flags, constructor, raw fixture, real reader or writer.
    assert not os.environ.get("LSMS_NO_CACHE")
    assert not os.environ.get("LSMS_ALLOW_INTREE_SOURCES")
    for state in (C._BUILD_DROP_LEDGER, C._DEGRADED_ATTEMPTED, C._SOURCE_PROBES):
        state.clear()
    h = Harness()
    guard = sys.modules.get("gh1547_guard")
    if guard is not None:
        guard.activate(h)
    else:
        monkeypatch.setattr(C, "df_data_grabber", h.read)
        monkeypatch.setattr(C, "to_parquet", h.write)
        monkeypatch.setattr(C, "source_tier", h.source_tier)
        monkeypatch.setattr(LT, "source_fetchable_fast", h.fast)
    yield h
    if guard is not None:
        guard.deactivate(h)
    for state in (C._BUILD_DROP_LEDGER, C._DEGRADED_ATTEMPTED, C._SOURCE_PROBES):
        state.clear()


def drop_expected(exc, *, required=False, kind="unavailable", cause=None, lost=True):
    return {"wave": YEAR, "sub": "df_secondary", "file": "Alpha/w1/Data/secondary.fake",
            "kind": kind, "cause": cause, "error": f"{type(exc).__name__}: {exc}",
            "missing_required": ["Latitude"] if required and lost else [],
            "missing_optional": ["Latitude"] if not required and lost else []}


@pytest.mark.parametrize("required", [False, True])
@pytest.mark.parametrize("error_type", [NoCredentialsError, FileNotFoundError, DvcException])
def test_secondary_unavailable_uses_existing_drop_policy(invented, required, error_type):
    h = invented
    if required:
        h.required()
    exc = error_type() if error_type is NoCredentialsError else error_type("invented unavailable")
    h.errors[FILES[1]] = exc
    with pytest.warns(UserWarning, match="could not load sub-df 'df_secondary'") as caught:
        frame, origin = h.call(origin=True)
    assert len(caught) == 1 and str(exc) in str(caught[0].message)
    expected = h.frames[FILES[0]].join(h.frames[FILES[2]])
    expected["t"] = YEAR
    expected = expected.reset_index().set_index(["t", "v"])
    pd.testing.assert_frame_equal(frame, expected)
    assert origin == "materialized" and h.cache_misses == 1
    assert h.records() == [drop_expected(exc, required=required)]
    assert len(h.write_calls) == 1
    pd.testing.assert_frame_equal(h.write_calls[0][0], expected)
    assert h.write_calls[0][1]["build_drops"] == h.records()
    assert h.read_calls[0] == FILES[0] and h.read_calls[-1] == FILES[2]
    assert h.read_calls.count(FILES[1]) == (3 if error_type is FileNotFoundError else 1)
    # A just-written degraded key does not probe. New process while down does.
    assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE, YEAR)) == ("serve", None)
    assert h.fast_calls == []
    C._DEGRADED_ATTEMPTED.clear()
    assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE, YEAR)) == ("serve", None)
    assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE, YEAR)) == ("serve", None)
    assert len(h.fast_calls) == 1
    with pytest.warns(C.DegradedCacheWarning) as down:
        C._warn_degraded_serve("Alpha", TABLE, h.records(), h.output)
    assert down[0].message.missing == (("Latitude",) if required else ())
    assert down[0].message.missing_optional == (() if required else ("Latitude",))
    # Fresh process, source healed: once per wave key, independent country key.
    C._SOURCE_PROBES.clear()
    h.fast_result = True
    verdict, reason = C._degraded_cache_verdict(h.records(), ("Alpha", TABLE, YEAR))
    assert verdict == "rebuild" and "Latitude" in reason and FILES[1] in reason
    assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE, YEAR)) == ("serve", None)
    assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE))[0] == "rebuild"
    assert len(h.fast_calls) == 2


@pytest.mark.parametrize("single_source", [False, True])
def test_primary_and_single_source_preserve_error_identity(invented, single_source):
    h = invented
    if single_source:
        h.wave.resources[TABLE] = dict(h.wave.resources[TABLE]["df_primary"])
    exc = NoCredentialsError()
    h.errors[FILES[0]] = exc
    with warnings.catch_warnings(record=True) as caught:
        with pytest.raises(NoCredentialsError) as raised:
            h.call()
    assert raised.value is exc and raised.value.__cause__ is None
    assert h.read_calls == [FILES[0]]
    assert h.records() == [] and h.write_calls == [] and caught == []


@pytest.mark.parametrize("required", [False, True])
@pytest.mark.parametrize("no_source", [False, True])
def test_config_drops_retain_optional_soft_required_hard(invented, required, no_source):
    h = invented
    if required:
        h.required()
    exc = NoCredentialsError() if no_source else KeyError("invented missing column")
    h.errors[FILES[1]] = exc
    h.tier = None if no_source else "dvc"
    cause = "no-such-source" if no_source else "missing-column"
    message = "no such source.*check the file: path" if no_source else "could not load"
    with pytest.warns(UserWarning, match=message):
        if required:
            with pytest.raises(RuntimeError, match="required declared column") as raised:
                h.call()
            assert raised.value.__cause__ is exc
            assert h.records() == [] and h.write_calls == []
            return
        frame = h.call()
    assert "Latitude" not in frame and "Region" in frame and "Other" in frame
    assert h.records() == [drop_expected(exc, kind="config", cause=cause)]
    assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE)) == ("complete", None)
    with warnings.catch_warnings(record=True) as caught:
        C._warn_degraded_serve("Alpha", TABLE, h.records(), h.output)
    assert caught == [] and h.fast_calls == [] and len(h.write_calls) == 1


@pytest.mark.parametrize("supply", ["sibling", "derived", "hook", "drop"])
@pytest.mark.parametrize("error_type", [NoCredentialsError, KeyError])
def test_presence_is_judged_after_all_transforms(invented, supply, error_type):
    h = invented
    h.required()
    exc = error_type() if error_type is NoCredentialsError else error_type("invented missing")
    h.errors[FILES[1]] = exc
    if supply == "sibling":
        h.frames[FILES[2]]["Latitude"] = [3.0, 4.0]
        h.wave.resources[TABLE]["df_sibling"]["myvars"]["Latitude"] = "Latitude"
    elif supply == "derived":
        # The existing derived transformer fills an existing target; no new
        # transformer or registry entry is invented for this credentials test.
        h.frames[FILES[0]]["Latitude"] = pd.Series(
            [None, None], dtype=object, index=h.frames[FILES[0]].index)
        h.frames[FILES[0]]["_lat"] = [1.0, 2.0]
        h.frames[FILES[0]]["_lon"] = [3.0, 4.0]
        h.wave.resources[TABLE]["df_primary"]["myvars"].update(
            {"Latitude": "Latitude", "_lat": "_lat", "_lon": "_lon"})
        h.wave.resources[TABLE]["derived"] = {
            "Latitude": {"kind": "coalesce_coord_bin", "lat": "_lat", "lon": "_lon"}}
        h.wave.resources[TABLE]["drop"] = ["_lat", "_lon"]
    else:
        def hook(frame):
            return frame.assign(Latitude=3.0)
        h.wave.formatting_functions[TABLE] = hook
        if supply == "drop":
            h.frames[FILES[0]]["Latitude"] = [8.0, 9.0]
            h.wave.resources[TABLE]["df_primary"]["myvars"]["Latitude"] = "Latitude"
            h.wave.resources[TABLE]["drop"] = ["Latitude"]
            # No hook: a dropped column is absent at the final check.
            h.wave.formatting_functions.clear()
    with pytest.warns(UserWarning, match="could not load"):
        if supply == "drop" and error_type is KeyError:
            with pytest.raises(RuntimeError, match="required declared column") as raised:
                h.call()
            assert raised.value.__cause__ is exc
            assert h.records() == [] and h.write_calls == []
            return
        frame = h.call()
    assert ("Latitude" in frame) == (supply != "drop")
    if supply == "derived":
        assert frame["Latitude"].tolist() == ["@+1.00,+3.00", "@+2.00,+4.00"]
        assert "_lat" not in frame and "_lon" not in frame
    assert h.records() == [drop_expected(exc, required=True,
        kind="config" if error_type is KeyError else "unavailable",
        cause="missing-column" if error_type is KeyError else None, lost=supply == "drop")]
    assert len(h.write_calls) == 1
    if supply != "drop":
        assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE)) == ("complete", None)
        assert h.fast_calls == []


@pytest.mark.parametrize("exc", [PartialCredentialsError(provider="invented", cred_var="invented"),
    BotoCoreError(), PermissionError("invented"), OSError("invented"),
    ValueError("invented"), TypeError("invented")], ids=[
        "partial-credentials", "botocore", "permission", "oserror", "value", "type"])
def test_unrelated_secondary_errors_stay_loud(invented, exc):
    h = invented
    h.errors[FILES[1]] = exc
    with warnings.catch_warnings(record=True) as caught:
        with pytest.raises(type(exc)) as raised:
            h.call()
    assert raised.value is exc
    assert h.records() == [] and h.write_calls == [] and caught == []
    assert h.read_calls == [FILES[0], FILES[1]]


def test_mapping_error_outside_handler_stays_loud(invented):
    h = invented
    exc = KeyError("invented structural mapping")
    h.mapping_errors["df_secondary"] = exc
    with pytest.raises(KeyError) as raised:
        h.call()
    assert raised.value is exc
    assert h.records() == [] and h.write_calls == [] and h.read_calls == [FILES[0]]


def test_bad_final_index_stays_loud(invented):
    h = invented
    h.wave.resources[TABLE]["final_index"] = ["t", "invented_absent_key"]
    with pytest.raises(KeyError, match="invented_absent_key"):
        h.call()
    assert h.records() == [] and h.write_calls == []
    assert h.read_calls == list(FILES)


def test_bad_derived_schema_stays_loud(invented):
    h = invented
    h.wave.resources[TABLE]["derived"] = {"Latitude": {"kind": "invented_absent_kind"}}
    with pytest.raises(ValueError, match="unknown kind"):
        h.call()
    assert h.records() == [] and h.write_calls == []
    assert h.read_calls == list(FILES)


def test_available_control_preserves_values_schema_nulls_and_origin(invented):
    h = invented
    with warnings.catch_warnings(record=True) as caught:
        frame, origin = h.call(origin=True)
    expected = h.frames[FILES[0]].join(h.frames[FILES[1]]).join(h.frames[FILES[2]])
    expected["t"] = YEAR
    expected = expected.reset_index().set_index(["t", "v"])
    pd.testing.assert_frame_equal(frame, expected)
    assert origin == "materialized" and caught == [] and h.records() == []
    assert h.read_calls == list(FILES) and len(h.write_calls) == 1
    assert h.write_calls[0][1]["build_drops"] is None and h.tier_calls == []


def test_failed_fast_probe_is_memoized_without_full_reader(invented):
    h = invented
    h.fast_result = NoCredentialsError()
    rel = "Alpha/w1/Data/secondary.fake"
    assert C._probe_source(rel) is False
    assert C._probe_source(rel) is False
    assert len(h.fast_calls) == 1 and h.read_calls == [] and h.write_calls == []
