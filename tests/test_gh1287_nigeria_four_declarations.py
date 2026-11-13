"""Actual Nigeria metadata/compiler boundary with invented IO leaves only.

A new table selection is run twice: exact original source and candidate. No
prior plot or seven-case current-wiring cohort is replayed. See task ledger.
"""
import hashlib
import importlib.util
import os
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
SELECTED_TABLE = os.environ.get("NIGERIA_COMPILER_TABLE")
TABLES = ("community_prices", "food_coping", "household_roster", "housing")
TABLE = SELECTED_TABLE or "community_prices"
assert TABLE in {"community_prices", "food_coping", "household_roster", "housing"}
PRODUCT = Path(os.environ.get("NIGERIA_COMPILER_SOURCE", str(SOURCE / "_" / (TABLE + ".py"))))
METADATA_RECEIPTS = []


@pytest.fixture(autouse=True, params=[SELECTED_TABLE] if SELECTED_TABLE else TABLES)
def selected_table(request, monkeypatch):
    # Ordinary CI covers all families; finite source arms select one explicitly.
    table = request.param
    if table != "community_prices" and request.node.originalname == "test_yaml_declared_duplicate_full_key_still_refuses":
        pytest.skip("community-price uniqueness contract only")
    monkeypatch.setitem(globals(), "TABLE", table)
    monkeypatch.setitem(globals(), "PRODUCT", Path(os.environ.get(
        "NIGERIA_COMPILER_SOURCE", str(SOURCE / "_" / (table + ".py")))))


def frame_for(table, folder, nigeria, number):
    pp, ph = nigeria.PP_QUARTER[folder], nigeria.PH_QUARTER[folder]
    attrs = {"invented": "same", "id_converted": True}
    if table == "community_prices":
        index = pd.MultiIndex.from_tuples(
            [(ph, "1/101/7", "Rice", "Kg", "1512", 2),
             (ph, "1/101/7", "Rice", "Kg", "1511", 1),
             (ph, "1/101/7", "Rice", "Kg", "1512", 1)],
            names=["t", "v", "j", "u", "variety", "obs"])
        value = pd.DataFrame({"Price": pd.array([0, number + .25, pd.NA], dtype="Float64"),
                              "Derivation": pd.array([pd.NA, "invented", pd.NA], dtype="string")}, index=index)
    elif table == "food_coping":
        index = pd.MultiIndex.from_tuples(
            [(pp, "native-001", "Reduce portions"), (pp, "native-001", "Skip meals"),
             (pp, "native-001", "Reduce portions")], names=["t", "i", "Strategy"])
        value = pd.DataFrame({"Days": pd.array([0, 7, pd.NA], dtype="Int64")}, index=index)
    elif table == "household_roster":
        index = pd.MultiIndex.from_tuples(
            [(ph, "native-001", "02"), (pp, "native-001", "01"),
             (pp, "native-001", "01")], names=["t", "i", "pid"])
        value = pd.DataFrame({"Age": pd.array([0, number + .5, pd.NA], dtype="Float64"),
                              "Sex": pd.array(["female", "male", pd.NA], dtype="string")}, index=index)
    else:
        quarter = pp if folder in {"2015-16", "2018-19"} else ph
        index = pd.MultiIndex.from_tuples([(quarter, "native-001"), (quarter, "native-001")], names=["t", "i"])
        value = pd.DataFrame({"Roof": pd.array(["Metal", pd.NA], dtype="string"),
                              "Rooms": pd.array([0, number + 1], dtype="Int64")}, index=index)
    value.attrs = attrs
    return value


@pytest.fixture
def compiler(tmp_path, monkeypatch, request):
    config_root = tmp_path / "countries"
    country_root = config_root / "Nigeria"
    (country_root / "_").mkdir(parents=True)
    for name in ["nigeria.py", "data_scheme.yml"]:
        (country_root / "_" / name).write_bytes((SOURCE / "_" / name).read_bytes())
    spec = importlib.util.spec_from_file_location("nigeria", country_root / "_/nigeria.py")
    nigeria = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(nigeria)
    monkeypatch.setitem(sys.modules, "nigeria", nigeria)
    for folder in nigeria.Waves:
        directory = country_root / folder / "_"
        directory.mkdir(parents=True)
        (directory / "data_info.yml").write_bytes((SOURCE / folder / "_/data_info.yml").read_bytes())
        native = SOURCE / folder / "_" / (TABLE + ".py")
        if native.exists():
            (directory / native.name).write_bytes(native.read_bytes())
    # Execute the exact pinned compiler bytes at a genuine private config path.
    # This preserves the original food_coping __file__ membership semantics.
    script = country_root / "_" / (TABLE + ".py")
    script.write_bytes(PRODUCT.read_bytes())
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(config_root))
    countries_root.cache_clear()
    country = Country("Nigeria", preload_panel_ids=False)
    assert country.waves == sorted(nigeria.waves)
    assert country.wave_folder_map == nigeria.wave_folder_map
    with pytest.raises(KeyError):
        country["2015-16"]
    frames = {folder: frame_for(TABLE, folder, nigeria, number)
              for number, folder in enumerate(nigeria.Waves)}
    current = [folder for folder in nigeria.Waves
               if TABLE in country[nigeria.PP_QUARTER[folder]].data_scheme]
    assert current == (list(nigeria.Waves)[:3] if TABLE == "food_coping" else list(nigeria.Waves))
    reads, writes = [], []

    def reader(filename, **kwargs):
        assert kwargs == {}
        parts = Path(filename).parts
        assert parts[0] == ".." and parts[2:] == ("_", TABLE + ".parquet")
        folder = parts[1]
        reads.append(folder)
        value = frames[folder]
        if isinstance(value, Exception):
            raise value
        return value.copy(deep=True)

    def writer(frame, filename, **kwargs):
        assert filename == "../var/" + TABLE + ".parquet" and kwargs == {}
        writes.append(frame.copy(deep=True))

    monkeypatch.setattr(local_tools, "get_dataframe", reader)
    monkeypatch.setattr(local_tools, "to_parquet", writer)

    def configure(route, retired=()):
        for folder in nigeria.Waves:
            directory = country_root / folder / "_"
            native = directory / (TABLE + ".py")
            metadata = yaml.safe_load((directory / "data_info.yml").read_text())
            metadata.pop(TABLE, None)
            if folder in retired or route == "yaml":
                native.unlink(missing_ok=True)
            else:
                # Invented executable-free native metadata for fixture-only
                # newly wired folders; actual Wave.data_scheme reads its stem.
                if not native.exists():
                    native.write_text("# Invented native declaration; never executed.\n")
            if folder not in retired and route == "yaml":
                metadata[TABLE] = {"file": "../Data/invented.dta", "idxvars": {"i": "hhid"}}
            (directory / "data_info.yml").write_text(yaml.safe_dump(metadata, sort_keys=False))
        for quarter in country.waves:
            wave = country[quarter]
            assert wave.wave_folder == nigeria.wave_folder_map[quarter]
            assert (TABLE in wave.data_scheme) == (wave.wave_folder not in retired)

    def run():
        METADATA_RECEIPTS.append({
            "case": request.node.nodeid, "table": TABLE, "compiler_path": str(script),
            "compiler_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
            "config_root": str(config_root), "logical_waves": country.waves,
            "wave_folder_map": dict(country.wave_folder_map),
            "metadata_sha256": {str(path.relative_to(config_root)): hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in sorted(config_root.rglob("*"))
                                if path.is_file() and path.suffix in {".py", ".yml"}},
        })
        return runpy.run_path(str(script))

    yield country_root, nigeria, country, frames, reads, writes, current, configure, run
    countries_root.cache_clear()


def exact(compiler, folders):
    _, _, _, frames, reads, writes, _, _, _ = compiler
    assert reads == folders, "one read per declared physical folder, in original order"
    assert len(writes) == 1
    expected = pd.concat([frames[folder] for folder in folders])
    if TABLE == "community_prices":
        expected = expected.sort_index()
    pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)
    assert writes[0].attrs == expected.attrs


def test_current_real_metadata_preserves_native_shapes(compiler):
    _, _, _, frames, _, _, current, _, run = compiler
    originals = {folder: value.copy(deep=True) for folder, value in frames.items()}
    run()
    exact(compiler, current)
    for folder in frames:
        pd.testing.assert_frame_equal(frames[folder], originals[folder], check_exact=True)
        assert frames[folder].attrs == originals[folder].attrs


@pytest.mark.parametrize("route", ["native", "yaml"])
def test_metadata_routes_keep_healthy_frames(compiler, route):
    _, nigeria, _, _, _, _, _, configure, run = compiler
    configure(route)
    run()
    exact(compiler, list(nigeria.Waves))


@pytest.mark.parametrize("route", ["native", "yaml"])
@pytest.mark.parametrize("retired", ["2010-11", "2012-13", "2015-16", "2018-19", "2023-24"])
def test_retired_offered_output_is_not_read(compiler, route, retired):
    _, nigeria, _, _, reads, _, _, configure, run = compiler
    configure(route, retired={retired})
    run()
    assert retired not in reads
    exact(compiler, [folder for folder in nigeria.Waves if folder != retired])


@pytest.mark.parametrize("route", ["native", "yaml"])
def test_all_retired_keeps_existing_no_input_failure(compiler, route):
    _, nigeria, _, _, reads, writes, _, configure, run = compiler
    configure(route, retired=set(nigeria.Waves))
    error = AssertionError if TABLE in {"community_prices", "food_coping"} else ValueError
    with pytest.raises(error):
        run()
    assert reads == writes == []


@pytest.mark.parametrize("route", ["native", "yaml"])
@pytest.mark.parametrize("error_type", [FileNotFoundError, RuntimeError, KeyError])
def test_declared_error_object_stops_before_write(compiler, route, error_type):
    _, _, _, frames, reads, writes, _, configure, run = compiler
    configure(route)
    error = error_type("invented declared middle input")
    frames["2015-16"] = error
    with pytest.raises(error_type) as caught:
        run()
    assert caught.value is error
    assert reads == ["2010-11", "2012-13", "2015-16"]
    assert writes == []


@pytest.mark.parametrize("route", ["native", "yaml"])
@pytest.mark.parametrize("empty", ["middle", "all"])
def test_successful_empty_is_not_retirement(compiler, route, empty):
    _, nigeria, _, frames, _, _, _, configure, run = compiler
    configure(route)
    for folder in (list(nigeria.Waves) if empty == "all" else ["2015-16"]):
        frames[folder] = frames[folder].iloc[:0].copy()
    run()
    exact(compiler, list(nigeria.Waves))


def test_mixed_native_and_yaml_only(compiler):
    root, nigeria, _, _, _, _, _, configure, run = compiler
    configure("native")
    directory = root / "2015-16" / "_"
    (directory / (TABLE + ".py")).unlink()
    with (directory / "data_info.yml").open("a") as stream:
        stream.write("\n" + TABLE + ": {file: ../Data/invented.dta}\n")
    run()
    exact(compiler, list(nigeria.Waves))


def test_null_yaml_does_not_disable_native_union(compiler):
    root, nigeria, country, _, _, _, _, configure, run = compiler
    configure("native")
    with (root / "2015-16/_/data_info.yml").open("a") as stream:
        stream.write("\n" + TABLE + ": null\n")
    assert TABLE in country["2015Q3"].data_scheme
    run()
    exact(compiler, list(nigeria.Waves))


def test_malformed_metadata_is_not_retirement(compiler):
    root, _, _, _, reads, writes, _, _, run = compiler
    (root / "2015-16/_/data_info.yml").write_text(TABLE + ": [\n")
    with pytest.raises(yaml.YAMLError):
        run()
    assert reads == writes == []


def test_unregistered_folder_skips_even_with_native_and_offered_output(compiler):
    root, nigeria, country, _, reads, _, _, configure, run = compiler
    configure("native")
    with (root / "_/nigeria.py").open("a") as stream:
        stream.write("\nwaves = [q for q in waves if wave_folder_map[q] != '2015-16']\n")
    assert "2015Q3" not in country.waves and "2016Q1" not in country.waves
    run()
    assert "2015-16" not in reads
    exact(compiler, [folder for folder in nigeria.Waves if folder != "2015-16"])


@pytest.mark.parametrize("quarter", ["2015Q3", "2016Q1"])
def test_one_registered_quarter_still_reads_folder_once(compiler, quarter):
    root, nigeria, country, _, _, _, _, configure, run = compiler
    configure("native")
    with (root / "_/nigeria.py").open("a") as stream:
        stream.write("\nwaves = [q for q in waves if wave_folder_map[q] != '2015-16' or q == " + repr(quarter) + "]\n")
    assert ("2015Q3" in country.waves) != ("2016Q1" in country.waves)
    run()
    exact(compiler, list(nigeria.Waves))


def test_disagreeing_attrs_retain_existing_concat_rule(compiler):
    _, nigeria, _, frames, _, writes, _, configure, run = compiler
    configure("native")
    frames["2015-16"].attrs = {"invented": "different"}
    run()
    exact(compiler, list(nigeria.Waves))
    assert writes[0].attrs == {}


if TABLE == "community_prices":
    def test_yaml_declared_duplicate_full_key_still_refuses(compiler):
        _, nigeria, _, frames, reads, writes, _, configure, run = compiler
        configure("yaml")
        frames["2015-16"] = pd.concat([frames["2015-16"], frames["2015-16"]])
        with pytest.raises(AssertionError, match="not unique after concat"):
            run()
        assert reads == list(nigeria.Waves) and writes == []
