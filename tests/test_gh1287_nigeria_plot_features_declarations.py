"""Actual PP/PH declaration metadata; invented compiler reader/writer only.

The original four current-wiring controls stay in their original file. These
cases exercise a different boundary: retirement and YAML-only declarations.
No Country/Wave table method, native loader, Make or physical parquet is used.
"""
import hashlib
import importlib.util
from pathlib import Path
import runpy
import sys

import pandas as pd
import pytest
import yaml

from lsms_library import Country
import lsms_library.local_tools as local_tools
from lsms_library.paths import countries_root


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "lsms_library/countries/Nigeria"
SCRIPT = SOURCE / "_/plot_features.py"
YAML_DECLARATION = """
plot_features:
  file: ../Data/invented.dta
  idxvars: {i: hhid, plot_id: plotid}
  myvars: {Area: area}
"""
METADATA_RECEIPTS = []


@pytest.fixture
def compiler(tmp_path, monkeypatch, request):
    config_root = tmp_path / "countries"
    country_root = config_root / "Nigeria"
    (country_root / "_").mkdir(parents=True)
    for name in ("nigeria.py", "data_scheme.yml"):
        (country_root / "_" / name).write_text((SOURCE / "_" / name).read_text())
    spec = importlib.util.spec_from_file_location("nigeria", country_root / "_/nigeria.py")
    nigeria = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(nigeria)
    monkeypatch.setitem(sys.modules, "nigeria", nigeria)
    for folder in nigeria.Waves:
        directory = country_root / folder / "_"
        directory.mkdir(parents=True)
        for name in ("data_info.yml", "plot_features.py"):
            (directory / name).write_text((SOURCE / folder / "_" / name).read_text())

    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(config_root))
    countries_root.cache_clear()
    country = Country("Nigeria")
    assert country.waves == sorted(nigeria.waves)
    assert len(country.waves) == 2 * len(nigeria.Waves)
    assert country.wave_folder_map == nigeria.wave_folder_map
    # A physical year is not a valid public wave. The production predicate
    # must go through actual logical-quarter lookup to reach each folder.
    with pytest.raises(KeyError):
        country["2015-16"]

    frames = {}
    for position, folder in enumerate(nigeria.Waves):
        frames[folder] = pd.DataFrame(
            {"Area": pd.array([position + 1.25, pd.NA], dtype="Float64"),
             "Tenure": pd.array(["owned", pd.NA], dtype="string")},
            index=pd.MultiIndex.from_tuples(
                [(nigeria.PP_QUARTER[folder], "invented-native-hh", "2"),
                 (nigeria.PP_QUARTER[folder], "invented-native-hh", "2")],
                names=["t", "i", "plot_id"]),
        )
        frames[folder].attrs = {"invented": "same", "id_converted": True}
    reads, writes = [], []

    def reader(filename, **kwargs):
        assert kwargs == {}, "preserve the compiler's existing reader call"
        parts = Path(filename).parts
        assert parts[0] == ".." and parts[2:] == ("_", "plot_features.parquet")
        folder = parts[1]
        reads.append(folder)
        value = frames[folder]
        if isinstance(value, Exception):
            raise value
        return value.copy(deep=True)

    def writer(frame, filename, **kwargs):
        assert filename == "../var/plot_features.parquet" and kwargs == {}
        writes.append(frame.copy(deep=True))

    monkeypatch.setattr(local_tools, "get_dataframe", reader)
    monkeypatch.setattr(local_tools, "to_parquet", writer)

    def configure(route, retired=()):
        for folder in nigeria.Waves:
            directory = country_root / folder / "_"
            if route == "yaml" or folder in retired:
                (directory / "plot_features.py").unlink(missing_ok=True)
            if route == "yaml" and folder not in retired:
                with (directory / "data_info.yml").open("a") as file:
                    file.write(YAML_DECLARATION)
        # Assert the real metadata's two-to-one mapping and declarations.
        # No Country, __getitem__, Wave.resources or data_scheme stub.
        for quarter in country.waves:
            wave = country[quarter]
            assert wave.wave_folder == nigeria.wave_folder_map[quarter]
            assert ("plot_features" in wave.data_scheme) == (wave.wave_folder not in retired)

    def run():
        METADATA_RECEIPTS.append({
            "case": request.node.nodeid,
            "config_root": str(config_root),
            "logical_waves": country.waves,
            "wave_folder_map": dict(country.wave_folder_map),
            "metadata_sha256": {
                str(path.relative_to(config_root)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(config_root.rglob("*"))
                if path.is_file() and path.suffix in {".py", ".yml"}
            },
        })
        return runpy.run_path(str(SCRIPT))

    yield country_root, nigeria, frames, reads, writes, configure, run
    countries_root.cache_clear()


def assert_exact(frames, reads, writes, folders):
    assert reads == folders, "one physical read per declared folder, in registry order"
    assert len(writes) == 1
    expected = pd.concat([frames[folder] for folder in folders])
    pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)
    assert writes[0].attrs == expected.attrs


@pytest.mark.parametrize("route", ["native", "yaml"])
def test_current_declarations_preserve_frames_and_reader_calls(compiler, route):
    _, nigeria, frames, reads, writes, configure, run = compiler
    configure(route)
    originals = {key: value.copy(deep=True) for key, value in frames.items()}
    run()
    assert_exact(frames, reads, writes, list(nigeria.Waves))
    assert not writes[0].index.is_unique, "the compiler must not add a reducer"
    for folder in frames:
        pd.testing.assert_frame_equal(frames[folder], originals[folder], check_exact=True)
        assert frames[folder].attrs == originals[folder].attrs


@pytest.mark.parametrize("route", ["native", "yaml"])
@pytest.mark.parametrize("retired", ["2010-11", "2012-13", "2015-16", "2018-19", "2023-24"])
def test_retired_folder_skips_before_any_reader_even_when_output_is_offered(compiler, route, retired):
    _, nigeria, frames, reads, writes, configure, run = compiler
    configure(route, retired={retired})
    # A valid invented retired output is deliberately available; stale output
    # availability must never turn a missing declaration into a contribution.
    run()
    assert retired not in reads
    assert_exact(frames, reads, writes, [folder for folder in nigeria.Waves if folder != retired])


@pytest.mark.parametrize("route", ["native", "yaml"])
def test_all_retired_refuses_before_any_read_or_write(compiler, route):
    _, nigeria, _, reads, writes, configure, run = compiler
    configure(route, retired=set(nigeria.Waves))
    with pytest.raises(AssertionError, match="plot_features: no wave-level parquets found"):
        run()
    assert reads == [] and writes == []


@pytest.mark.parametrize("route", ["native", "yaml"])
@pytest.mark.parametrize("failure", [FileNotFoundError, RuntimeError, KeyError])
def test_declared_reader_failure_is_loud_and_prevents_partial_write(compiler, route, failure):
    _, _, frames, reads, writes, configure, run = compiler
    configure(route)
    error = failure("invented declared middle-folder failure")
    frames["2015-16"] = error
    with pytest.raises(failure) as caught:
        run()
    assert caught.value is error
    assert reads == ["2010-11", "2012-13", "2015-16"]
    assert writes == []


@pytest.mark.parametrize("route", ["native", "yaml"])
@pytest.mark.parametrize("empty", ["middle", "all"])
def test_declared_empty_frames_remain_valid_and_exact(compiler, route, empty):
    _, nigeria, frames, reads, writes, configure, run = compiler
    configure(route)
    selected = list(frames) if empty == "all" else ["2015-16"]
    for folder in selected:
        frames[folder] = frames[folder].iloc[:0].copy()
    run()
    assert_exact(frames, reads, writes, list(nigeria.Waves))


def test_mixed_native_and_yaml_only_declarations_use_the_same_metadata_api(compiler):
    country_root, nigeria, frames, reads, writes, configure, run = compiler
    configure("native")
    directory = country_root / "2015-16" / "_"
    (directory / "plot_features.py").unlink()
    with (directory / "data_info.yml").open("a") as file:
        file.write(YAML_DECLARATION)
    run()
    assert_exact(frames, reads, writes, list(nigeria.Waves))


def test_disagreeing_attrs_follow_existing_concat_rule(compiler):
    _, nigeria, frames, reads, writes, configure, run = compiler
    configure("native")
    frames["2015-16"].attrs = {"invented": "different"}
    run()
    assert_exact(frames, reads, writes, list(nigeria.Waves))
    assert writes[0].attrs == {}


def test_bad_declaration_yaml_is_not_reclassified_as_retirement(compiler):
    country_root, _, _, reads, writes, _, run = compiler
    (country_root / "2015-16/_/data_info.yml").write_text("plot_features: [\n")
    with pytest.raises(yaml.YAMLError):
        run()
    assert writes == []
