"""GH1091: Tanzania's framework owns country community-price aggregation.

Use the ACTUAL Tanzania Makefile and country-script presence, with two small,
distinct script-written waves. No selector or existence result is patched.
The same file is run against the parent to demonstrate the wrong subset
returns, then against the retirement. Real held-source comparisons live in
the task's private validation, separately from these credential-free tests.
"""
from __future__ import annotations

import hashlib
import shutil
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import Country
from lsms_library.local_tools import to_parquet
from lsms_library.paths import countries_root, data_root


SOURCE = countries_root() / "Tanzania"
WAVES = ["2008-09", "2010-11", "2012-13", "2014-15", "2019-20", "2020-21"]
SUPPORTED = WAVES[-2:]
FOLDERS = dict.fromkeys(WAVES[:4], "2008-15") | dict(zip(SUPPORTED, SUPPORTED))
REQUESTS = [None, *[[w] for w in WAVES], WAVES[:4], SUPPORTED, [WAVES[0], SUPPORTED[0]]]
INDEX = ["t", "v", "j", "u", "variety"]
pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="GNU make required")

SCHEME = """Country: Tanzania
Waves: ['2008-09', '2010-11', '2012-13', '2014-15', '2019-20', '2020-21']
Data Scheme:
  community_prices:
    index: (t, v, j, u, variety)
    Price: float
    Description:
      type: str
      optional: true
    materialize: make
  panel_ids: !make
  updated_ids: !make
"""

SCRIPT = """from pathlib import Path
import pandas as pd
from lsms_library.local_tools import to_parquet
w = Path.cwd().parent.name
base = 10 if w == '2019-20' else 20
df = pd.DataFrame({
    't': [w, w], 'v': ['1091-v1', '1091-v2'],
    'i': ['1091-v1', '1091-v2'], 'j': ['Mil', 'Sorgho'],
    'u': ['Kg', 'Kg'], 'variety': ['7011', '7012'],
    'Price': [float(base + 1), float(base + 2)],
    'Description': ['first', 'second'],
}).set_index(['t', 'v', 'i', 'j', 'u', 'variety'])
{drop}
to_parquet(df, 'community_prices.parquet')
"""


def _typed(value):
    """Keep immutable record types as well as their values in attrs checks."""
    kind = type(value)
    if hasattr(kind, '_FIELDS'):
        return (kind, tuple(kind._FIELDS),
                [(key, key in value, _typed(value[key]) if key in value else None)
                 for key in kind._FIELDS],
                [(key, _typed(item)) for key, item in value.items()])
    if isinstance(value, dict):
        return kind, [(key, _typed(item)) for key, item in value.items()]
    if isinstance(value, (tuple, list)):
        return kind, [_typed(item) for item in value]
    return kind, value


def _exact(left, right):
    pd.testing.assert_frame_equal(left, right, check_exact=True, rtol=0, atol=0)
    assert type(left.index) is type(right.index)
    assert type(left.columns) is type(right.columns)
    pd.testing.assert_index_equal(left.columns, right.columns, exact=True,
                                  check_exact=True, rtol=0, atol=0)
    pd.testing.assert_index_equal(left.index, right.index, exact=True,
                                  check_exact=True, rtol=0, atol=0)
    if isinstance(left.index, pd.MultiIndex):
        assert left.index.sortorder == right.index.sortorder
        for a, b in zip(left.index.levels, right.index.levels):
            pd.testing.assert_index_equal(a, b, exact=True, check_exact=True,
                                          rtol=0, atol=0)
        for a, b in zip(left.index.codes, right.index.codes):
            assert a.dtype == b.dtype and (a == b).all()
    assert _typed(left.attrs) == _typed(right.attrs)


def _receipts(root):
    return {
        str(p.relative_to(root)): (hashlib.sha256(p.read_bytes()).hexdigest(),
                                   p.stat().st_mode, p.stat().st_mtime_ns)
        for p in root.rglob("*.parquet")
    }


@pytest.fixture
def native_country(tmp_path, monkeypatch):
    root = tmp_path / "countries"
    country = root / "Tanzania"
    build = country / "_"
    build.mkdir(parents=True)
    (build / "data_scheme.yml").write_text(SCHEME)
    shutil.copy2(SOURCE / "_" / "Makefile", build / "Makefile")
    # Dependencies are real files; the native test producers need no data helper.
    # A country module takes precedence over the scheme's Waves declaration.
    (build / "tanzania.py").write_text(
        f"waves = {WAVES!r}\nwave_folder_map = {FOLDERS!r}\n")
    (build / "CONTENTS.org").write_text("Fixture native community observations.\n")
    for name in ["panel_ids.json", "updated_ids.json", "community_prices.py"]:
        if (SOURCE / "_" / name).exists():
            shutil.copy2(SOURCE / "_" / name, build / name)
    for w in WAVES:
        (country / FOLDERS[w] / "_").mkdir(parents=True, exist_ok=True)
    for w in SUPPORTED:
        (country / w / "_" / "community_prices.py").write_text(SCRIPT.replace("{drop}", ""))
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(root))
    monkeypatch.setenv("LSMS_BUILD_WORKERS", "1")
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    monkeypatch.delenv("VAR_DIR", raising=False)
    monkeypatch.delenv("LSMS_DATA_ROOT", raising=False)
    countries_root.cache_clear()

    class Native:
        config = country

        def select(self, name, backend):
            self.data = tmp_path / name
            monkeypatch.setenv("LSMS_DATA_DIR", str(self.data))
            monkeypatch.setenv("LSMS_BUILD_BACKEND", backend)
            monkeypatch.setenv("LSMS_ISSUES_LOG", str(tmp_path / (name + "-issues.log")))
            data_root.cache_clear()
            return Country("Tanzania")

        def drop(self, column):
            for w in SUPPORTED:
                (country / w / "_" / "community_prices.py").write_text(
                    SCRIPT.replace("{drop}", f"df = df.drop(columns=[{column!r}])"))

    try:
        yield Native()
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


def _read(country, requested):
    return country.community_prices(**({} if requested is None else {"waves": requested}))


def test_real_country_discovery_and_wave_make_rules(native_country):
    c = native_country.select("discovery", "make")
    assert c.waves == WAVES and c.wave_folder_map == FOLDERS
    assert c._has_country_builder("community_prices") is False
    empty = native_country.data / "empty-probe"
    target = empty / "Tanzania" / "var" / "community_prices.parquet"
    common = ["make", "-n", f"LSMS_DATA_ROOT={empty}", f"VAR_DIR={target.parent}"]
    absent = subprocess.run(common + [str(target)], cwd=native_country.config / "_",
                            capture_output=True, text=True)
    assert absent.returncode != 0 and "No rule to make target" in absent.stderr
    for w in SUPPORTED:
        wave = subprocess.run(common + [f"../{w}/_/community_prices.parquet"],
                              cwd=native_country.config / "_", capture_output=True, text=True)
        assert wave.returncode == 0
        assert f"cd ../{w}/_ && python community_prices.py" in wave.stdout
    assert not list(empty.rglob("*.parquet"))


@pytest.mark.parametrize("backend", ["dvc", "make"])
@pytest.mark.parametrize("requested", REQUESTS)
def test_requests_use_only_the_requested_native_waves(native_country, backend, requested):
    c = native_country.select("target", backend)
    result = _read(c, requested)
    expected_waves = set(SUPPORTED) & set(WAVES if requested is None else requested)
    assert len(result) == 2 * len(expected_waves)
    assert set(result.index.get_level_values("t")) == expected_waves
    assert list(result.index.names) == INDEX and result.index.is_unique
    if expected_waves:
        assert sorted(result.Price.tolist()) == sorted(
            float(value) for w in expected_waves
            for value in ([11, 12] if w == SUPPORTED[0] else [21, 22]))
        assert "Quantity" not in result.columns
    else:
        assert not list(native_country.data.rglob("*.parquet"))
    if backend == "make":
        assert not (native_country.data / "Tanzania" / "var" / "community_prices.parquet").exists()
    before = _receipts(native_country.data)
    _exact(result, _read(Country("Tanzania"), requested))
    if backend == "dvc":
        assert _receipts(native_country.data) == before
    # Compare to a genuinely separate normal default build, with the same args.
    reference = _read(native_country.select("reference", "dvc"), requested)
    _exact(result, reference)
    assert not list(native_country.config.rglob("*.parquet"))


@pytest.mark.parametrize("requested", [[SUPPORTED[0]], WAVES[:4]])
def test_make_never_reads_an_existing_wrong_wave_country_payload(native_country, requested):
    c = native_country.select("existing", "make")
    var = native_country.data / "Tanzania" / "var" / "community_prices.parquet"
    var.parent.mkdir(parents=True)
    wrong = pd.DataFrame({"t": [WAVES[0]], "v": ["1091-wrong"], "j": ["Mil"],
                          "u": ["Kg"], "variety": ["7011"],
                          "Price": [999.0], "Description": ["wrong"]})
    to_parquet(wrong.set_index(INDEX), var)
    before = _receipts(native_country.data)[str(var.relative_to(native_country.data))]
    result = _read(c, requested)
    assert len(result) == (2 if SUPPORTED[0] in requested else 0)
    assert 999.0 not in result.Price.tolist()
    assert _receipts(native_country.data)[str(var.relative_to(native_country.data))] == before


@pytest.mark.parametrize("column", ["Price"])
def test_retirement_preserves_required_script_columns(native_country, column):
    native_country.drop(column)
    c = native_country.select("missing-column", "make")
    with pytest.raises(RuntimeError, match=f"missing required declared column.*{column}"):
        c.community_prices()


def test_description_is_optional_without_quantity_fabrication(native_country):
    native_country.drop("Description")
    c = native_country.select("optional-description", "make")
    result = c.community_prices(waves=SUPPORTED)
    assert len(result) == 4 and "Price" in result.columns
    assert "Quantity" not in result.columns
    assert "Description" not in result.columns or result.Description.isna().all()


@pytest.mark.parametrize("requested", [[SUPPORTED[0]], WAVES[:4]])
def test_populated_make_preserves_verified_default_country_cache(native_country, requested):
    c = native_country.select("populated", "dvc")
    full = c.community_prices()
    var = native_country.data / "Tanzania" / "var" / "community_prices.parquet"
    from lsms_library.local_tools import read_parquet_cache_hash, read_parquet_cache_waves
    assert read_parquet_cache_hash(var) == c._table_cache_hash("community_prices", c.waves)
    assert set(read_parquet_cache_waves(var)) == set(WAVES)
    before = _receipts(native_country.data)[str(var.relative_to(native_country.data))]
    # A new actual Make object uses the same genuinely current data root.
    make = native_country.select("populated", "make")
    result = _read(make, requested)
    assert len(result) == (2 if SUPPORTED[0] in requested else 0)
    assert _receipts(native_country.data)[str(var.relative_to(native_country.data))] == before
    reference = _read(native_country.select("populated-reference", "dvc"), requested)
    _exact(result, reference)
