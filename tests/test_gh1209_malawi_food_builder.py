"""GH1209: Malawi's canonical waves are assembled by the framework.

The real Malawi registry and Makefile choose the route. Synthetic wave frames
keep the regression independent of microdata and preserve the source axis,
native units, zero values and exact kilogram products the old assembler lost.
Actual held-source cold/warm comparisons are recorded separately.
"""
from __future__ import annotations

import shutil
import subprocess

import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library.paths import data_root


needs_make = pytest.mark.skipif(shutil.which("make") is None,
                                reason="GNU make not on PATH")


@needs_make
def test_malawi_has_no_country_food_builder():
    c = C.Country("Malawi", preload_panel_ids=False)
    assert c._has_country_builder("food_acquired") is False


@needs_make
def test_direct_make_food_target_builds_native_waves_only(tmp_path):
    c = C.Country("Malawi", preload_panel_ids=False)
    root = tmp_path / "data"
    result = subprocess.run(
        ["make", "-n", f"LSMS_DATA_ROOT={root}", "food_acquired"],
        cwd=c.file_path / "_", capture_output=True, text=True, check=True,
    )
    lines = [line for line in result.stdout.splitlines()
             if "python food_acquired.py" in line]
    assert len(lines) == len(c.waves)
    for wave in c.waves:
        assert f"../{wave}/_/" in result.stdout
    assert "var/food_acquired.parquet" not in result.stdout
    assert not root.exists(), "a dry-run Make probe wrote data"


@pytest.mark.parametrize("backend", ["dvc", "make"])
@pytest.mark.parametrize("waves", [None, ["2013-14"]])
@needs_make
def test_native_wave_assembly_preserves_requested_food(tmp_path, monkeypatch,
                                                       backend, waves):
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LSMS_BUILD_BACKEND", backend)
    monkeypatch.setenv("LSMS_BUILD_WORKERS", "1")
    data_root.cache_clear()
    c = C.Country("Malawi", preload_panel_ids=False)
    c._panel_ids_attempted = True
    c._updated_ids_cache = {}
    seen = []

    def native_wave(self, request):
        assert request == "food_acquired", request
        seen.append(self.year)
        frame = pd.DataFrame({
            "t": [self.year, self.year], "i": ["house", "house"],
            "j": ["Rice", "Rice"], "u": ["Cup", "kg"],
            "s": ["purchased", "produced"],
            "Quantity": [0.0, 2.5], "Expenditure": [0.0, float("nan")],
            "Quantity_kg": [0.0, 2.5],
        }).set_index(["t", "i", "j", "u", "s"])
        # Native scripts write this parquet before returning. Its presence
        # also tells Wave.food_acquired to keep already-canonical columns.
        out = data_root("Malawi") / self.year / "_" / "food_acquired.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(out)
        return frame

    # Keep API metadata/finalization real, without asking a second table for
    # microdata. This test is about assembly, not the already-tested v join.
    monkeypatch.setattr(C.Wave, "grab_data", native_wave)
    monkeypatch.setattr(C.Country, "_join_v_from_sample", lambda self, df, *a, **k: df)
    run = C.subprocess.run

    def probe_only(command, *args, **kwargs):
        if isinstance(command, list) and command and command[0] == "make" and "-n" in command:
            return run(command, *args, **kwargs)
        raise AssertionError("selected the retired country food builder")

    monkeypatch.setattr(C.subprocess, "run", probe_only)
    try:
        result = c.food_acquired(waves=waves, currency=None)
        requested = c.waves if waves is None else waves
        assert seen == requested
        assert len(result) == 2 * len(requested)
        # Country._finalize_result orders household i before wave t.
        assert list(result.index.names) == ["i", "t", "j", "u", "s"]
        assert result.index.is_unique
        assert list(result.columns) == ["Quantity", "Expenditure", "Quantity_kg"]
        assert set(result.index.get_level_values("t")) == set(requested)
        assert set(result.index.get_level_values("s")) == {"purchased", "produced"}
        assert set(result.index.get_level_values("u")) == {"Cup", "kg"}
        purchased = result.xs("purchased", level="s")
        assert (purchased[["Quantity", "Expenditure", "Quantity_kg"]] == 0).all().all()
        produced = result.xs("produced", level="s")
        assert (produced[["Quantity", "Quantity_kg"]] == 2.5).all().all()
        assert produced["Expenditure"].isna().all()
    finally:
        data_root.cache_clear()
