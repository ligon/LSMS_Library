"""A malformed declared wave must not become a current partial country cache.

Exercise real YAML extraction and cache writes using synthetic CSV sources.
No survey data, credentialed reader or network is required.
"""
from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import country as C
from lsms_library.local_tools import read_parquet_cache_hash, to_parquet
from lsms_library.paths import countries_root, data_root


@pytest.fixture
def fake_country(tmp_path, monkeypatch):
    croot, droot = tmp_path / "countries", tmp_path / "data"
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(croot))
    monkeypatch.setenv("LSMS_DATA_DIR", str(droot))
    monkeypatch.setenv("LSMS_ISSUES_LOG", str(tmp_path / "issues.log"))
    monkeypatch.setenv("LSMS_BUILD_WORKERS", "1")
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    monkeypatch.setenv("LSMS_ALLOW_INTREE_SOURCES", "1")  # synthetic CSV only
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    C._REBUILD_HISTORY.clear()

    def no_dvc(*args, **kwargs):
        raise C.DvcException("synthetic country has no DVC repository")

    monkeypatch.setattr(C, "Repo", no_dvc)
    root = croot / "Testland1281"
    (root / "_").mkdir(parents=True)
    (root / "_" / "data_scheme.yml").write_text(
        "Country: Testland1281\nWaves: ['2000', '2001']\n"
        "Data Scheme:\n  facts:\n    index: (t, i)\n    x: float\n"
    )
    for year, value in (("2000", 2.0), ("2001", 3.0)):
        (root / year / "_").mkdir(parents=True)
        (root / year / "Data").mkdir()
        pd.DataFrame({"hhid": ["h" + year], "source_x": [value]}).to_csv(
            root / year / "Data" / "facts.csv", index=False
        )
        (root / year / "_" / "data_info.yml").write_text(
            "facts:\n  file: facts.csv\n  idxvars:\n    i: hhid\n"
            "  myvars:\n    x: source_x\n"
        )

    class Fixture:
        cache = droot / "Testland1281" / "var" / "facts.parquet"

        def country(self):
            return C.Country("Testland1281", preload_panel_ids=False)

        def load(self, waves=None):
            return self.country()._aggregate_wave_data(waves, "facts")

        def config(self, year="2001"):
            return root / year / "_" / "data_info.yml"

        def misspell(self, field="source_x"):
            path = self.config()
            path.write_text(path.read_text().replace(field, "typo_1281"))

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield Fixture()
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()
        C._REBUILD_HISTORY.clear()


@pytest.mark.parametrize("field", ["source_x", "hhid"])
@pytest.mark.parametrize("backend", ["dvc", "make"])
def test_declared_wave_keyerror_aborts_before_partial_cache(
    fake_country, monkeypatch, field, backend
):
    monkeypatch.setenv("LSMS_BUILD_BACKEND", backend)
    fake_country.misspell(field)
    with pytest.raises(RuntimeError, match=r"Testland1281/2001.*facts.*KeyError") as exc:
        fake_country.load()
    assert isinstance(exc.value.__cause__, KeyError)
    assert not fake_country.cache.exists()


def test_failed_rebuild_never_restamps_old_partial_cache(fake_country):
    fake_country.misspell()
    partial = pd.DataFrame({"t": ["2000"], "i": ["poison"], "x": [99.0]}).set_index(["t", "i"])
    fake_country.cache.parent.mkdir(parents=True)
    to_parquet(partial, fake_country.cache, absolute_path=True,
               cache_hash="previous-build", cache_waves=["2000", "2001"])
    before = fake_country.cache.read_bytes()
    with pytest.raises(RuntimeError, match=r"Testland1281/2001.*facts"):
        fake_country.load()
    assert fake_country.cache.read_bytes() == before
    assert read_parquet_cache_hash(fake_country.cache) == "previous-build"


def test_healthy_cold_and_warm_results_are_identical(fake_country):
    cold = fake_country.load()
    warm = fake_country.load()
    pd.testing.assert_frame_equal(cold, warm)
    assert set(cold.index.get_level_values("t")) == {"2000", "2001"}
    assert cold["x"].tolist() == [2.0, 3.0]
    assert fake_country.cache.exists()


def test_undeclared_wave_remains_valid_absence(fake_country):
    fake_country.config().write_text("Country: Testland1281\nWave: '2001'\n")
    result = fake_country.load()
    assert list(result.index.get_level_values("t")) == ["2000"]
    assert result["x"].tolist() == [2.0]
    assert fake_country.cache.exists()


def test_empty_declared_source_remains_valid_empty_wave(fake_country):
    source = fake_country.config().parent.parent / "Data" / "facts.csv"
    source.write_text("hhid,source_x\n")
    result = fake_country.load()
    assert list(result.index.get_level_values("t")) == ["2000"]
    assert result["x"].tolist() == [2.0]


def test_unrequested_bad_wave_does_not_break_valid_subset(fake_country):
    fake_country.misspell()
    result = fake_country.load(["2000"])
    assert list(result.index.get_level_values("t")) == ["2000"]
    assert result["x"].tolist() == [2.0]


def test_attributeerror_keeps_existing_legacy_fallback(fake_country, monkeypatch):
    original = C.Wave.grab_data

    def loader(self, request, **kwargs):
        if self.year == "2001" and request == "facts":
            raise AttributeError("synthetic legacy loader")
        return original(self, request, **kwargs)

    monkeypatch.setattr(C.Wave, "grab_data", loader)
    result = fake_country.load()
    assert list(result.index.get_level_values("t")) == ["2000"]
    assert result["x"].tolist() == [2.0]
