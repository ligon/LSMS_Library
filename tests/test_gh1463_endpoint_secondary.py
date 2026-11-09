"""GH1463: exact endpoint failures enter the existing GH675 secondary policy.

Reuse the strict GH1547 invented providers (ledger §2); no real country,
source, SDK request or native producer runs. Storage expectations follow the
declared Alpha schema independently of the writer's observed output.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import MethodType, SimpleNamespace
import warnings

from botocore.exceptions import (
    BotoCoreError, ConnectionClosedError, EndpointConnectionError,
    NoCredentialsError, PartialCredentialsError,
)
from dvc.exceptions import DvcException
import pandas as pd
import pyarrow.parquet as pq
import pytest

from lsms_library import country as C, local_tools as LT
from lsms_library.paths import countries_root
from test_gh1547_optional_credentials import (
    FILES, TABLE, YEAR, drop_expected, invented as invented,
)


def endpoint():
    exc = EndpointConnectionError(endpoint_url="https://invented.invalid")
    exc.__cause__ = OSError("invented endpoint cause")
    return exc


def call(h, *, origin=False):
    # Bind the actual current method; do not capture or extract a handler.
    return MethodType(C.Wave.grab_data, h.wave)(TABLE, _with_origin=origin)


def expected_frame(h, *, complete=False, stored=False):
    """Independent values/types: Alpha declares Region:str, Other:Int64,
    Latitude:float. Its provider deliberately supplies Region as object;
    the sanctioned writer stores that as nullable string[pyarrow]."""
    schema = h.wave.country.resources["Data Scheme"][TABLE]
    assert schema["Region"] == "str" and schema["Other"] == "Int64"
    lat = schema["Latitude"]
    assert (lat["type"] if isinstance(lat, dict) else lat) == "float"
    idx = pd.MultiIndex.from_tuples([(YEAR, "c1"), (YEAR, "c2")], names=["t", "v"])
    payload = {"Region": pd.array(["N", pd.NA], dtype="string[pyarrow]")
               if stored else pd.Series(["N", None], dtype=object).array}
    if complete:
        payload["Latitude"] = pd.array([1.5, 2.5], dtype="float64")
    payload["Other"] = pd.array([7, 8], dtype="Int64")
    return pd.DataFrame(payload, index=idx)


@pytest.mark.parametrize("required", [False, True])
@pytest.mark.parametrize("tier", ["dvc", "local", None], ids=["sidecar", "local", "no-source"])
def test_secondary_endpoint_preserves_source_classifier_and_loss_contract(invented, required, tier):
    h = invented
    if required:
        h.required()
    h.tier = tier
    exc = endpoint()
    h.errors[FILES[1]] = exc
    message = "no such source.*check the file: path" if tier is None else "could not load sub-df 'df_secondary'"
    with pytest.warns(UserWarning, match=message) as caught:
        if required and tier is None:
            with pytest.raises(RuntimeError, match="required declared column") as raised:
                call(h)
            assert raised.value.__cause__ is exc
            assert h.records() == [] and h.write_calls == []
            assert h.read_calls == list(FILES)
            return
        frame, origin = call(h, origin=True)
    assert len(caught) == 1 and str(exc) in str(caught[0].message)
    pd.testing.assert_frame_equal(frame, expected_frame(h), check_exact=True)
    assert origin == "materialized" and h.read_calls == list(FILES)
    assert h.records() == [drop_expected(exc, required=required,
        kind="config" if tier is None else "unavailable",
        cause="no-such-source" if tier is None else None)]
    assert len(h.write_calls) == 1
    pd.testing.assert_frame_equal(h.write_calls[0][0], expected_frame(h), check_exact=True)
    assert h.write_calls[0][1]["build_drops"] == h.records()
    assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE, YEAR)) == (
        "complete" if tier is None else "serve", None)
    assert h.fast_calls == []  # config never probes; just-written unavailable does not retry


@pytest.mark.parametrize("tier", ["local", "dvc", None])
def test_actual_source_evidence_is_used_without_reading_a_source(invented, monkeypatch, tmp_path, tier):
    h = invented
    # Only metadata/empty invented files; the strict provider supplies all rows.
    root = tmp_path / "invented-config"
    h.wave.file_path = root / "Alpha" / YEAR
    source = h.wave.file_path / "Data" / FILES[1]
    source.parent.mkdir(parents=True)
    if tier == "local":
        source.touch()
    elif tier == "dvc":
        source.with_name(source.name + ".dvc").write_text("outs:\n- md5: invented\n")
    h.paths = {h.wave.file_path / "Data" / f for f in FILES}
    h.candidates = {}
    import os
    for path in h.paths:
        for spelling in (os.path.relpath(path, Path.cwd()), str(path.relative_to(root)), str(path)):
            h.candidates[spelling] = path.name
    monkeypatch.setattr(C, "source_tier", LT.source_tier)
    assert LT.source_tier(source) == tier
    exc = endpoint()
    h.errors[FILES[1]] = exc
    with pytest.warns(UserWarning):
        call(h)
    rec = h.records()[0]
    assert rec["file"] == str(source)
    assert (rec["kind"], rec["cause"]) == (
        ("config", "no-such-source") if tier is None else ("unavailable", None))
    assert rec["missing_optional"] == ["Latitude"] and rec["missing_required"] == []
    assert h.read_calls == list(FILES) and len(h.write_calls) == 1


@pytest.mark.parametrize("single_source", [False, True])
def test_primary_endpoint_keeps_original_identity_cause_and_traceback(invented, single_source):
    h = invented
    if single_source:
        h.wave.resources[TABLE] = dict(h.wave.resources[TABLE]["df_primary"])
    exc = endpoint()
    cause = exc.__cause__
    h.errors[FILES[0]] = exc
    with warnings.catch_warnings(record=True) as caught:
        with pytest.raises(EndpointConnectionError) as raised:
            call(h)
    assert raised.value is exc and raised.value.__cause__ is cause
    trace = []
    tb = raised.value.__traceback__
    while tb is not None:
        trace.append(tb.tb_frame.f_code.co_name)
        tb = tb.tb_next
    assert trace[-1] == "read" and trace.count("grab_data") == 1 and "get_data" in trace
    assert h.read_calls == [FILES[0]]
    assert h.records() == [] and h.write_calls == [] and caught == []


@pytest.mark.parametrize("exc", [
    LT.InTreeSourceError("invented source refusal"), LT.ParquetFormatError("invented format"),
    ValueError("invented value"), PermissionError("invented permission"),
    BotoCoreError(), ConnectionClosedError(endpoint_url="https://invented.invalid"),
    PartialCredentialsError(provider="invented", cred_var="invented"),
], ids=["in-tree", "parquet", "value", "permission", "botocore", "other-connection", "partial-credentials"])
def test_unrelated_secondary_failures_remain_loud(invented, exc):
    h = invented
    h.errors[FILES[1]] = exc
    with warnings.catch_warnings(record=True) as caught:
        with pytest.raises(type(exc)) as raised:
            call(h)
    assert raised.value is exc
    assert h.read_calls == [FILES[0], FILES[1]]
    assert h.records() == [] and h.write_calls == [] and caught == []


@pytest.mark.parametrize("site", ["column-mapping", "formatter", "final-hook"])
def test_endpoint_retains_the_existing_formatter_and_hook_boundaries(invented, monkeypatch, site):
    h = invented
    exc = endpoint()
    if site == "column-mapping":
        h.mapping_errors["df_secondary"] = exc  # outside the secondary try
    elif site == "final-hook":
        def hook(frame):
            raise exc
        h.wave.formatting_functions[TABLE] = hook  # outside the secondary try
    else:
        # Execute real df_data_grabber, including its normal callable formatter.
        def formatter(value):
            raise exc
        h.wave.resources[TABLE]["df_secondary"]["myvars"]["Latitude"] = ("Latitude", formatter)
        def reader(candidate, **kwargs):
            assert candidate in h.candidates
            assert kwargs == {"convert_categoricals": True, "encoding": None}
            file = h.candidates[candidate]
            h.read_calls.append(file)
            return h.frames[file].reset_index()
        monkeypatch.setattr(C, "df_data_grabber", LT.df_data_grabber)
        monkeypatch.setattr(LT, "get_dataframe", reader)
        # Formatter errors inside get_data already share this source catch:
        # admitting endpoint preserves that boundary too, without moving it.
        with pytest.warns(UserWarning, match="could not load"):
            out = call(h)
        pd.testing.assert_frame_equal(out, expected_frame(h), check_exact=True)
        assert h.records() == [drop_expected(exc)]
        assert h.read_calls == list(FILES) and len(h.write_calls) == 1
        return
    with warnings.catch_warnings(record=True) as caught:
        with pytest.raises(EndpointConnectionError) as raised:
            call(h)
    assert raised.value is exc and h.records() == [] and h.write_calls == [] and caught == []
    assert h.read_calls == ([FILES[0]] if site == "column-mapping" else list(FILES))


@pytest.mark.parametrize("supply", ["sibling", "hook", "drop"])
def test_required_loss_is_judged_after_siblings_and_final_transforms(invented, supply):
    h = invented
    h.required()
    exc = endpoint()
    h.errors[FILES[1]] = exc
    if supply == "sibling":
        h.frames[FILES[2]]["Latitude"] = [3.0, 4.0]
        h.wave.resources[TABLE]["df_sibling"]["myvars"]["Latitude"] = "Latitude"
    elif supply == "hook":
        h.wave.formatting_functions[TABLE] = lambda frame: frame.assign(Latitude=3.0)
    else:
        h.frames[FILES[0]]["Latitude"] = [8.0, 9.0]
        h.wave.resources[TABLE]["df_primary"]["myvars"]["Latitude"] = "Latitude"
        h.wave.resources[TABLE]["drop"] = ["Latitude"]
    with pytest.warns(UserWarning):
        out = call(h)
    lost = supply == "drop"
    assert h.records() == [drop_expected(exc, required=True, lost=lost)]
    assert ("Latitude" not in out) == lost
    assert len(h.write_calls) == 1
    assert C._degraded_cache_verdict(h.records(), ("Alpha", TABLE)) == (
        "serve" if lost else "complete", None)
    if not lost:
        assert out["Latitude"].tolist() == ([3.0, 4.0] if supply == "sibling" else [3.0, 3.0])


def test_actual_generated_wave_dispatcher_reaches_the_repaired_handler(invented):
    h = invented
    class CountryReceiver:
        data_scheme = property(lambda self: [TABLE])
    class WaveReceiver:
        data_scheme = property(lambda self: [TABLE])
        __getattr__ = C.Wave.__getattr__
        grab_data = C.Wave.grab_data
    receiver = WaveReceiver()
    for name in type(h.wave).__slots__:
        if name != "data_scheme":
            setattr(receiver, name, getattr(h.wave, name))
    receiver.country = CountryReceiver()
    receiver.country.name = "Alpha"
    receiver.country.resources = h.wave.country.resources
    for name in ("column_mapping", "_input_hash", "_read_l2_wave_cache",
                 "_required_scheme_columns", "_cartesian_keys", "_merge_subframes"):
        setattr(receiver, name, getattr(h.wave, name))
    exc = endpoint()
    h.errors[FILES[1]] = exc
    with pytest.warns(UserWarning):
        out, origin = getattr(receiver, TABLE)(_with_origin=True)
    pd.testing.assert_frame_equal(out, expected_frame(h), check_exact=True)
    assert origin == "materialized" and h.records() == [drop_expected(exc)]
    assert "_in_getattr" not in receiver.__dict__ and len(h.write_calls) == 1


@pytest.mark.parametrize("required", [False, True])
@pytest.mark.parametrize("tier", ["wave", "country"])
def test_endpoint_drop_footer_fresh_warm_serve_and_healing(invented, monkeypatch, tmp_path, required, tier):
    h = invented
    if required:
        h.required()
    h.output = tmp_path / "private-data" / "Alpha" / YEAR / "_" / f"{TABLE}.parquet"
    exc = endpoint()
    h.errors[FILES[1]] = exc
    def writer(frame, path, **kwargs):
        assert path == h.output
        assert kwargs["cache_hash"] == "invented-1547-hash"
        assert kwargs["build_drops"] == [drop_expected(exc, required=required)]
        h.write_calls.append((frame.copy(deep=True), deepcopy(kwargs)))
        return LT.to_parquet(frame, path, **kwargs)
    monkeypatch.setattr(C, "to_parquet", writer)
    with pytest.warns(UserWarning):
        built, origin = call(h, origin=True)
    assert origin == "materialized"
    # Compare storage to independently DECLARED types, never astype(actual).
    expected = expected_frame(h, stored=True)
    pd.testing.assert_frame_equal(built, expected, check_exact=True)
    assert h.write_calls[0][0]["Region"].dtype == object
    assert built["Region"].dtype == pd.StringDtype(storage="pyarrow")
    assert LT.read_parquet_build_drops(h.output) == [drop_expected(exc, required=required)]
    assert LT.read_parquet_cache_hash(h.output) == "invented-1547-hash"
    assert b"lsms_build_drops" in pq.read_schema(h.output).metadata
    pd.testing.assert_frame_equal(LT.get_dataframe(h.output), expected, check_exact=True)

    if tier == "wave":
        gate = MethodType(C.Wave._read_l2_wave_cache, h.wave)
        def read_gate():
            return gate(TABLE, h.output), None
    else:
        # Actual country read gate on an invented receiver; no Country API or constructor.
        receiver = SimpleNamespace(name="Alpha", waves=[YEAR], wave_folder_map={})
        def table_hash(request, waves):
            assert request == TABLE and waves == [YEAR]
            return "invented-1547-hash"
        receiver._table_cache_hash = table_hash
        for name in ("_cache_covers_waves", "_restrict_to_waves", "_grain_reports_for_waves"):
            setattr(receiver, name, MethodType(getattr(C.Country, name), receiver))
        gate = MethodType(C.Country._read_l2_country_cache, receiver)
        def read_gate():
            frame, covered, reason = gate(TABLE, h.output, [YEAR])
            assert covered == [YEAR]
            return frame, reason

    frozen = h.output.read_bytes(), h.output.stat().st_mtime_ns
    h.fast_result = endpoint()
    # A fresh process starts with empty memo/ledger. Both gates see the footer,
    # warn and serve; the failed fast probe is memoised, without a source read.
    for state in (C._DEGRADED_ATTEMPTED, C._SOURCE_PROBES, C._BUILD_DROP_LEDGER):
        state.clear()
    for _ in range(2):
        with pytest.warns(C.DegradedCacheWarning) as caught:
            warm, reason = read_gate()
        pd.testing.assert_frame_equal(warm, expected, check_exact=True)
        assert reason is None and len(caught) == 1
        assert caught[0].message.missing == (("Latitude",) if required else ())
        assert caught[0].message.missing_optional == (() if required else ("Latitude",))
        assert caught[0].message.files == ("Alpha/w1/Data/secondary.fake",)
    assert len(h.fast_calls) == 1 and h.read_calls == list(FILES)
    assert (h.output.read_bytes(), h.output.stat().st_mtime_ns) == frozen
    # New process can fetch again: one miss per key, then serve if not rebuilt.
    C._DEGRADED_ATTEMPTED.clear()
    C._SOURCE_PROBES.clear()
    h.fast_result = True
    with warnings.catch_warnings(record=True) as caught:
        miss, reason = read_gate()
    assert miss is None and caught == []
    assert tier == "wave" or ("degraded:" in reason and "Latitude" in reason)
    with pytest.warns(C.DegradedCacheWarning):
        warm, _ = read_gate()
    pd.testing.assert_frame_equal(warm, expected, check_exact=True)
    assert len(h.fast_calls) == 2
    # Follow the miss with the actual builder and a now-available invented source.
    h.errors.clear()
    monkeypatch.setattr(C, "to_parquet", LT.to_parquet)
    with warnings.catch_warnings(record=True) as caught:
        healed, origin = call(h, origin=True)
    assert caught == [] and origin == "materialized"
    pd.testing.assert_frame_equal(healed, expected_frame(h, complete=True, stored=True), check_exact=True)
    assert LT.read_parquet_build_drops(h.output) is None
    C._DEGRADED_ATTEMPTED.clear()
    C._SOURCE_PROBES.clear()
    with warnings.catch_warnings(record=True) as caught:
        healthy, reason = read_gate()
    pd.testing.assert_frame_equal(healthy, expected_frame(h, complete=True, stored=True), check_exact=True)
    assert caught == [] and reason is None and len(h.fast_calls) == 2
