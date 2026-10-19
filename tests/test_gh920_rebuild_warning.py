"""GH #920(a), @ligon 2026-10-18: say why a table is rebuilt, on every install.

``Country._report_rebuild`` runs once at the top of the rebuild descent in
``load_dataframe_with_dvc`` -- before the git-checkout / pip split -- and
emits :class:`CacheRebuildWarning`:

- quiet for the expected misses: ``no parquet`` and ``LSMS_NO_CACHE``;
- a warning naming ``STALE`` / ``coverage`` / ``read-error``;
- a ``repeat`` warning, whatever the reason, when the same table rebuilds
  again in one process without its wave coverage growing.

Config-only: the helper is driven directly with gate reasons; no data read.
"""
import ast
import inspect
import warnings

import pytest

import lsms_library.country as country_mod
from lsms_library.country import CacheRebuildWarning, Country

COUNTRY, TABLE = "Albania", "household_roster"


@pytest.fixture
def c(tmp_path, monkeypatch):
    monkeypatch.setattr(country_mod, "_REBUILD_HISTORY", {})
    return Country(COUNTRY, preload_panel_ids=False)


def _report(c, reason, waves, tmp_path):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        c._report_rebuild(TABLE, reason, waves, tmp_path / "x.parquet")
    return [x.message for x in w if isinstance(x.message, CacheRebuildWarning)]


@pytest.mark.parametrize("reason", ["no parquet", "LSMS_NO_CACHE"])
def test_expected_misses_are_quiet(c, reason, tmp_path):
    assert _report(c, reason, list(c.waves), tmp_path) == []


@pytest.mark.parametrize("reason", [
    "STALE: parquet hash differs from the current inputs",
    "STALE: parquet carries no cache hash (GH #809)",
    "coverage: parquet covers ['2002']",
    "read-error: OSError('boom')",
])
def test_unexpected_misses_warn_with_their_reason(c, reason, tmp_path):
    msgs = _report(c, reason, list(c.waves), tmp_path)
    assert len(msgs) == 1
    m = msgs[0]
    assert (m.country, m.table, m.reason) == (COUNTRY, TABLE, reason)
    assert m.waves == tuple(sorted(map(str, c.waves)))
    assert "FULL REBUILD" in str(m) and reason in str(m)


def test_second_rebuild_of_the_same_waves_is_a_repeat(c, tmp_path):
    assert _report(c, "no parquet", list(c.waves), tmp_path) == []
    msgs = _report(c, "no parquet", list(c.waves), tmp_path)
    assert len(msgs) == 1 and msgs[0].reason == "repeat: no parquet"
    assert "AGAIN" in str(msgs[0])


def test_repeat_fires_for_a_quiet_reason_and_a_loud_one_alike(c, tmp_path):
    _report(c, "STALE: parquet hash differs", list(c.waves), tmp_path)
    msgs = _report(c, "STALE: parquet hash differs", list(c.waves), tmp_path)
    assert [m.reason for m in msgs] == ["repeat: STALE: parquet hash differs"]


def test_growing_coverage_is_not_a_repeat(c, tmp_path):
    """GH #891's union: alternating subsets rebuild a STRICTLY larger set.

    Each coverage miss still warns as ``coverage`` (it is unexpected), but
    none is reported as a repeat.
    """
    w = [str(x) for x in c.waves]
    assert len(w) >= 3
    assert _report(c, "no parquet", w[:1], tmp_path) == []
    for k in (2, 3):
        msgs = _report(c, "coverage: x", w[:k], tmp_path)
        assert [m.reason for m in msgs] == ["coverage: x"]


def test_no_cache_never_counts_toward_a_repeat(c, tmp_path):
    for _ in range(3):
        assert _report(c, "LSMS_NO_CACHE", list(c.waves), tmp_path) == []
    assert _report(c, "no parquet", list(c.waves), tmp_path) == []


def test_tables_and_countries_are_tracked_separately(c, tmp_path):
    _report(c, "no parquet", list(c.waves), tmp_path)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        c._report_rebuild("sample", "no parquet", list(c.waves), tmp_path / "y.parquet")
    assert not [x for x in w if isinstance(x.message, CacheRebuildWarning)]


def test_called_once_at_the_top_of_the_rebuild_descent():
    """Both install shapes pass through it: it precedes the Repo() try."""
    src = inspect.getsource(Country._aggregate_wave_data)
    call = src.index("self._report_rebuild(method_name, miss_reason, build_waves")
    assert src.count("self._report_rebuild(") == 1
    assert call < src.index("repo = Repo(")
    assert call > src.index("build_waves = list(waves) if covered is None")


def test_report_rebuild_is_excluded_from_the_build_fingerprint():
    from lsms_library._build_registry import _EXCLUDED_CALLABLES
    assert "lsms_library.country.Country._report_rebuild" in _EXCLUDED_CALLABLES


def test_inputs_changing_between_rebuilds_is_not_a_repeat(c, tmp_path, monkeypatch):
    """A config edit mid-session moves the expected hash: graded by reason."""
    _report(c, "no parquet", list(c.waves), tmp_path)
    monkeypatch.setattr(Country, "_table_cache_hash", lambda self, t, w: "edited")
    msgs = _report(c, "STALE: parquet hash differs", list(c.waves), tmp_path)
    assert [m.reason for m in msgs] == ["STALE: parquet hash differs"]
    # ...but a further rebuild under the SAME (edited) inputs is a repeat.
    msgs = _report(c, "STALE: parquet hash differs", list(c.waves), tmp_path)
    assert [m.reason for m in msgs] == ["repeat: STALE: parquet hash differs"]
