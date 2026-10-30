"""Invented footer metadata only; no survey/cache build or baseline repinning."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def generator(monkeypatch):
    import lsms_library as ll

    # Exercise dynamic metadata lookup, without constructing a survey Country.
    calls = []
    def country(name):
        calls.append(name)
        return SimpleNamespace(waves=["a", "b"])
    monkeypatch.setattr(ll, "Country", country)
    module = _module("_baseline1146", "generate_baseline.py")
    module.country_calls = calls
    return module


@pytest.fixture
def nigeria():
    return _module("_nigeria_sold1146", "test_nigeria_sold_unit.py")


def _parquet(path, coverage):
    path.parent.mkdir(parents=True, exist_ok=True)
    # The sole observed wave intentionally cannot prove coverage of wave b.
    table = pa.Table.from_pandas(pd.DataFrame({"t": ["a"], "Quantity": [1.]}))
    if coverage is not None:
        metadata = dict(table.schema.metadata or {})
        metadata[b"lsms_cache_waves"] = json.dumps(coverage).encode()
        table = table.replace_schema_metadata(metadata)
    pq.write_table(table, path)


@pytest.mark.parametrize("fallback", [False, True])
def test_generator_refuses_present_partial_in_either_root(generator, tmp_path, fallback):
    primary = tmp_path / "Uganda"
    primary.mkdir()
    cache = tmp_path / "cache"
    _parquet((cache if fallback else primary) / "var/table.parquet", ["a"])
    with pytest.raises(generator.IncompleteCoverageError, match="b"):
        generator.build_manifest(primary, cache)


@pytest.mark.parametrize("coverage", [None, ["a", "b"], ["a", "b", "future"]])
def test_generator_preserves_legacy_and_accepts_covered_empty_wave(generator, tmp_path, coverage):
    _parquet(tmp_path / "var/table.parquet", coverage)
    result = generator.build_manifest(tmp_path)
    assert result[str(Path("var") / "table.parquet")]["shape"] == [1, 2]
    assert generator.country_calls == ["Uganda"]


def test_generator_refuses_explicit_empty_coverage(generator, tmp_path):
    _parquet(tmp_path / "var/table.parquet", [])
    with pytest.raises(generator.IncompleteCoverageError):
        generator.build_manifest(tmp_path)


def test_generator_uses_current_declared_future_wave(generator, monkeypatch, tmp_path):
    import lsms_library as ll

    _parquet(tmp_path / "var/table.parquet", ["a", "b"])
    monkeypatch.setattr(ll, "Country", lambda name: SimpleNamespace(waves=["a", "b", "new"]))
    with pytest.raises(generator.IncompleteCoverageError, match="new"):
        generator.build_manifest(tmp_path)


def test_generator_does_not_fall_through_partial_preferred_file(generator, monkeypatch, tmp_path):
    primary, cache = tmp_path / "Uganda", tmp_path / "cache"
    _parquet(primary / "var/table.parquet", ["a"])
    _parquet(cache / "var/table.parquet", ["a", "b"])
    def no_payload(*args, **kwargs):
        raise AssertionError("partial preferred file must be refused before payload/fallback")
    monkeypatch.setattr(generator.pd, "read_parquet", no_payload)
    with pytest.raises(generator.IncompleteCoverageError):
        generator.build_manifest(primary, cache)


def test_generator_rechecks_coverage_after_atomic_replacement(generator, monkeypatch, tmp_path):
    path = tmp_path / "var/table.parquet"
    _parquet(path, ["a", "b"])
    original = generator.pd.read_parquet
    def swapped(*args, **kwargs):
        frame = original(*args, **kwargs)
        replacement = path.with_name("replacement.parquet")
        _parquet(replacement, ["a"])
        replacement.replace(path)
        return frame
    monkeypatch.setattr(generator.pd, "read_parquet", swapped)
    with pytest.raises(generator.IncompleteCoverageError):
        generator.build_manifest(tmp_path)


def test_generator_refusal_leaves_existing_baseline_untouched(generator, monkeypatch, tmp_path):
    root = tmp_path / "Uganda"
    _parquet(root / "var/table.parquet", ["a"])
    script = tmp_path / "tests/generate_baseline.py"
    baseline = script.parent / "fixtures/uganda_baseline.json"
    baseline.parent.mkdir(parents=True)
    baseline.write_text('{"preserve": "existing approved baseline"}')
    before = baseline.read_bytes()
    monkeypatch.setattr(generator, "__file__", str(script))
    monkeypatch.setattr(generator.sys, "argv", [str(script), str(root), str(tmp_path / "empty")])
    with pytest.raises(generator.IncompleteCoverageError):
        generator.main()
    assert baseline.read_bytes() == before


def test_generator_native_entries_do_not_require_all_country_waves(generator, monkeypatch, tmp_path):
    import lsms_library as ll

    _parquet(tmp_path / "a/_/table.parquet", ["a"])
    monkeypatch.setattr(ll, "Country", lambda name: pytest.fail("native entry needs no country coverage lookup"))
    assert str(Path("a") / "_" / "table.parquet") in generator.build_manifest(tmp_path)


@pytest.mark.parametrize("coverage", [None, ["2011Q1", "2013Q1"], ["2011Q1", "2013Q1", "2024Q1"]])
def test_nigeria_gate_preserves_legacy_and_accepts_required_superset(nigeria, tmp_path, coverage):
    path = tmp_path / "crop_production.parquet"
    _parquet(path, coverage)
    nigeria._skip_partial_sold_unit_cache(path)


@pytest.mark.parametrize("coverage", [[], ["2011Q1"], ["2013Q1"]])
def test_nigeria_gate_skips_present_missing_required_wave(nigeria, tmp_path, coverage):
    path = tmp_path / "crop_production.parquet"
    _parquet(path, coverage)
    with pytest.raises(pytest.skip.Exception, match="required sold-unit waves"):
        nigeria._skip_partial_sold_unit_cache(path)


@pytest.mark.parametrize("method", ["test_unit_sold_populated_only_in_w1_w2", "test_api_frame_serves_unit_sold_through_finalize"])
def test_nigeria_partial_skips_before_payload_or_api(nigeria, monkeypatch, tmp_path, method):
    import lsms_library as ll
    import lsms_library.local_tools as lt

    _parquet(tmp_path / "Nigeria/var/crop_production.parquet", ["2011Q1"])
    monkeypatch.setattr(lt, "data_root", lambda: tmp_path)
    monkeypatch.setattr(nigeria.pd, "read_parquet", lambda *a, **k: pytest.fail("no partial payload read"))
    monkeypatch.setattr(ll, "Country", lambda *a, **k: pytest.fail("no cold/API build"))
    with pytest.raises(pytest.skip.Exception, match="required sold-unit waves"):
        getattr(nigeria.TestBuiltTable(), method)()


@pytest.mark.parametrize("method", ["test_unit_sold_populated_only_in_w1_w2", "test_api_frame_serves_unit_sold_through_finalize"])
def test_nigeria_rechecks_footer_after_payload_read(nigeria, monkeypatch, tmp_path, method):
    import lsms_library as ll
    import lsms_library.local_tools as lt

    path = tmp_path / "Nigeria/var/crop_production.parquet"
    _parquet(path, ["2011Q1", "2013Q1"])
    monkeypatch.setattr(lt, "data_root", lambda: tmp_path)
    def swapped(*args, **kwargs):
        _parquet(path, ["2011Q1"])
        return pd.DataFrame({"Unit_sold": pd.array(["Kg"], dtype="string")})
    monkeypatch.setattr(nigeria.pd, "read_parquet", swapped)
    monkeypatch.setattr(ll, "Country", lambda *a, **k: pytest.fail("no API build after partial replacement"))
    with pytest.raises(pytest.skip.Exception, match="required sold-unit waves"):
        getattr(nigeria.TestBuiltTable(), method)()
