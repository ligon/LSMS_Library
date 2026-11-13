"""Actual Nigeria concatenator, invented parquet-reader/writer leaves only."""
import importlib.util
from pathlib import Path
import runpy
import sys

import pandas as pd
import pytest

import lsms_library.local_tools as local_tools


ROOT = Path(__file__).resolve().parents[1] / "lsms_library/countries/Nigeria"


@pytest.fixture
def concatenator(monkeypatch):
    # Reuse actual physical-folder registry, not the public quarter list
    # (ledger 1287 §§2–4). Module import defines helpers; it reads no payload.
    spec = importlib.util.spec_from_file_location("nigeria", ROOT / "_/nigeria.py")
    nigeria = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(nigeria)
    monkeypatch.setitem(sys.modules, "nigeria", nigeria)
    frames = {}
    for folder in nigeria.Waves:
        frames[folder] = pd.DataFrame(
            {"Area": pd.Series([1.25, pd.NA], dtype="Float64").array},
            index=pd.MultiIndex.from_tuples(
                [(nigeria.PP_QUARTER[folder], "invented-hh", "1"),
                 (nigeria.PP_QUARTER[folder], "invented-hh", "2")],
                names=["t", "i", "plot_id"]),
        )
    reads, writes = [], []

    def reader(filename, **kwargs):
        parts = Path(filename).parts
        assert parts[0] == ".." and parts[2:] == ("_", "plot_features.parquet")
        reads.append(parts[1])
        return frames[parts[1]].copy(deep=True)

    def writer(frame, filename, **kwargs):
        assert filename == "../var/plot_features.parquet"
        writes.append(frame.copy(deep=True))

    monkeypatch.setattr(local_tools, "get_dataframe", reader)
    monkeypatch.setattr(local_tools, "to_parquet", writer)
    return frames, reads, writes


def test_all_declared_folders_keep_pp_rows_nulls_and_dtypes(concatenator):
    frames, reads, writes = concatenator
    original = {key: value.copy(deep=True) for key, value in frames.items()}
    runpy.run_path(str(ROOT / "_/plot_features.py"))
    assert reads == list(frames)
    assert len(writes) == 1
    pd.testing.assert_frame_equal(writes[0], pd.concat(list(frames.values())))
    assert writes[0].index.is_unique
    for folder, frame in frames.items():
        pd.testing.assert_frame_equal(frame, original[folder])


@pytest.mark.parametrize("failure", [FileNotFoundError, RuntimeError, KeyError])
def test_declared_middle_wave_failure_prevents_partial_country_write(
    monkeypatch, concatenator, failure,
):
    frames, reads, writes = concatenator
    reader = local_tools.get_dataframe
    error = failure("invented declared W3 input failed")

    def failing_reader(filename, **kwargs):
        if Path(filename).parts[1] == "2015-16":
            raise error
        return reader(filename, **kwargs)

    monkeypatch.setattr(local_tools, "get_dataframe", failing_reader)
    with pytest.raises(failure) as caught:
        runpy.run_path(str(ROOT / "_/plot_features.py"))
    assert caught.value is error
    assert reads == ["2010-11", "2012-13"]
    assert writes == [], "a declared-wave failure must never publish a partial result"
