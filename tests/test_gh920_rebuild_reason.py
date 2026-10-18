"""GH #920(a) / #908: the L2-country read gate names WHY it did not serve.

``Country._read_l2_country_cache`` (extracted from the tagged
``load_dataframe_with_dvc`` closure by #908) returns ``(df, covered, reason)``.
These tests drive it against a private ``LSMS_DATA_DIR`` holding a
hand-written ``var/<table>.parquet`` for a real country config, so every
miss class is exercised without building anything.  Config-only: no source
data is read.
"""
import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.local_tools import to_parquet

COUNTRY, TABLE = "Albania", "household_roster"


@pytest.fixture
def gate(tmp_path, monkeypatch):
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    from lsms_library.paths import data_root
    data_root.cache_clear()
    c = Country(COUNTRY, preload_panel_ids=False)
    cache_path = data_root(COUNTRY) / "var" / f"{TABLE}.parquet"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {"Age": [30, 40]},
        index=pd.MultiIndex.from_tuples([(c.waves[0], "1", "1"), (c.waves[-1], "2", "1")],
                                        names=["t", "i", "pid"]))
    yield c, cache_path, df
    data_root.cache_clear()


def _write(df, path, **kw):
    to_parquet(df, path, absolute_path=True, **kw)


def test_absent_parquet_is_named(gate):
    c, cache_path, _ = gate
    out, covered, reason = c._read_l2_country_cache(TABLE, cache_path, c.waves)
    assert out is None and covered is None and reason == "no parquet"


def test_stale_hash_is_named_stale(gate):
    c, cache_path, df = gate
    _write(df, cache_path, cache_hash="0" * 16, cache_waves=list(c.waves))
    out, covered, reason = c._read_l2_country_cache(TABLE, cache_path, c.waves)
    assert out is None
    assert reason.startswith("STALE") and "hash differs" in reason
    # A stale parquet still reports its coverage so the rebuild takes the union.
    assert covered == list(c.waves)


def test_hashless_parquet_is_named_stale_809(gate):
    c, cache_path, df = gate
    _write(df, cache_path)
    out, _covered, reason = c._read_l2_country_cache(TABLE, cache_path, c.waves)
    assert out is None and reason.startswith("STALE") and "#809" in reason


def test_coverage_short_is_named_coverage(gate):
    c, cache_path, df = gate
    expected = c._table_cache_hash(TABLE, c.waves)
    _write(df.loc[[c.waves[0]]], cache_path, cache_hash=expected, cache_waves=[c.waves[0]])
    out, covered, reason = c._read_l2_country_cache(TABLE, cache_path, [c.waves[-1]])
    assert out is None and reason.startswith("coverage") and covered == [c.waves[0]]


def test_no_cache_lever_is_named(gate, monkeypatch):
    c, cache_path, df = gate
    _write(df, cache_path, cache_hash=c._table_cache_hash(TABLE, c.waves),
           cache_waves=list(c.waves))
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    out, covered, reason = c._read_l2_country_cache(TABLE, cache_path, c.waves)
    assert out is None and covered is None and reason == "LSMS_NO_CACHE"


def test_fresh_parquet_is_served_and_restricted(gate):
    c, cache_path, df = gate
    _write(df, cache_path, cache_hash=c._table_cache_hash(TABLE, c.waves),
           cache_waves=list(c.waves))
    out, covered, reason = c._read_l2_country_cache(TABLE, cache_path, [c.waves[0]])
    assert reason is None and covered == list(c.waves)
    assert set(out.index.get_level_values("t")) == {c.waves[0]}
