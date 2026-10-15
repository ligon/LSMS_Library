"""GH #909: ``diagnostics._compute_panel_spine`` must not trust a partial
L2-country ``household_roster.parquet``.

The parquet is served only when its ``lsms_cache_waves`` manifest (GH #891)
covers every declared wave; a partial or unstamped parquet sends the spine
through the API path, and the provenance string says how many waves the
parquet covered.
"""
from __future__ import annotations

import pandas as pd
import pytest

from lsms_library import diagnostics
from lsms_library.local_tools import to_parquet
from lsms_library.paths import data_root


class _Stub:
    name = "Stubland"
    waves = ["w1", "w2", "w3"]

    def __init__(self):
        self.api_calls = 0

    def household_roster(self):
        self.api_calls += 1
        return _roster(self.waves)


def _roster(waves):
    idx = pd.MultiIndex.from_tuples(
        [(w, f"hh{k}", "1") for w in waves for k in range(3)],
        names=["t", "i", "pid"])
    return pd.DataFrame({"Age": 1.0}, index=idx)


@pytest.fixture
def private_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
    data_root.cache_clear()          # memoised; re-read the env var
    root = data_root("Stubland")
    assert str(root).startswith(str(tmp_path)), root
    (root / "var").mkdir(parents=True)
    yield root / "var" / "household_roster.parquet"
    data_root.cache_clear()


def _write(path, waves, *, stamp):
    to_parquet(_roster(waves), path, absolute_path=True,
               cache_waves=list(waves) if stamp else None)


def test_full_manifest_serves_the_parquet(private_root):
    _write(private_root, ["w1", "w2", "w3"], stamp=True)
    c = _Stub()
    spine, prov = diagnostics._compute_panel_spine(c)
    assert c.api_calls == 0
    assert prov == "cached household_roster.parquet [3 of 3 waves]"
    assert set(spine.index.get_level_values("t")) == {"w1", "w2", "w3"}


def test_partial_manifest_takes_the_api_path(private_root):
    _write(private_root, ["w3"], stamp=True)          # household_roster(waves=['w3'])
    c = _Stub()
    spine, prov = diagnostics._compute_panel_spine(c)
    assert c.api_calls == 1, "a one-wave parquet must not be trusted as the spine"
    assert prov == "household_roster (API)"
    assert set(spine.index.get_level_values("t")) == {"w1", "w2", "w3"}


def test_unstamped_parquet_takes_the_api_path(private_root):
    _write(private_root, ["w1", "w2", "w3"], stamp=False)   # pre-#891: no manifest
    c = _Stub()
    _, prov = diagnostics._compute_panel_spine(c)
    assert c.api_calls == 1 and prov == "household_roster (API)"


def test_parquet_covers_waves_counts(private_root):
    assert diagnostics._parquet_covers_waves(private_root, ["w1", "w2"]) == (False, 0, 2)
    _write(private_root, ["w1"], stamp=True)
    assert diagnostics._parquet_covers_waves(private_root, ["w1", "w2"]) == (False, 1, 2)
    assert diagnostics._parquet_covers_waves(private_root, ["w1"]) == (True, 1, 1)
