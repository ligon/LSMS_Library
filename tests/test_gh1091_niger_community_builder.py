"""GH1091: Niger's framework owns country community-price aggregation.

Use the ACTUAL Niger Makefile and country-script presence, with two small,
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


SOURCE = countries_root() / "Niger"
WAVES = ["2011-12", "2014-15", "2018-19", "2021-22"]
REQUESTS = [None, [WAVES[0]], [WAVES[1]], [WAVES[2]], [WAVES[3]], WAVES[2:]]
INDEX = ["t", "v", "j", "u", "variety", "visit", "obs"]
pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="GNU make required")

SCHEME = """Country: Niger
Waves: ['2011-12', '2014-15', '2018-19', '2021-22']
Data Scheme:
  community_prices:
    index: (t, v, j, u, variety, visit, obs)
    Price: float
    Quantity: float
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
base = 10 if w == '2011-12' else 20
df = pd.DataFrame({
    't': [w, w], 'v': ['1091-v1', '1091-v2'],
    'i': ['1091-v1', '1091-v2'], 'j': ['Mil', 'Sorgho'],
    'u': ['Kg', 'Kg'], 'variety': ['7011', '7012'],
    'visit': [1, 1], 'obs': [1, 2],
    'Price': [float(base + 1), float(base + 2)],
    'Quantity': [1.25, float('nan')], 'Description': ['first', 'second'],
}).set_index(['t', 'v', 'i', 'j', 'u', 'variety', 'visit', 'obs'])
{drop}
to_parquet(df, 'community_prices.parquet')
"""


def _typed(value):
    """Keep immutable record types as well as their values in attrs checks."""
    kind = type(value)
    if isinstance(value, dict):
        return kind, [(key, _typed(item)) for key, item in value.items()]
    if isinstance(value, (tuple, list)):
        return kind, [_typed(item) for item in value]
    return kind, value


def _exact(left, right):
    pd.testing.assert_frame_equal(left, right, check_exact=True, rtol=0, atol=0)
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
    country = root / "Niger"
    build = country / "_"
    build.mkdir(parents=True)
    (build / "data_scheme.yml").write_text(SCHEME)
    shutil.copy2(SOURCE / "_" / "Makefile", build / "Makefile")
    # Dependencies are real files; the native test producers need no data helper.
    # A country module takes precedence over the scheme's Waves declaration.
    (build / "niger.py").write_text(f"Waves = dict.fromkeys({WAVES!r}, ())\n")
    shutil.copy2(SOURCE / "_" / "categorical_mapping.org", build / "categorical_mapping.org")
    for name in ["panel_ids.json", "updated_ids.json", "community_prices.py"]:
        if (SOURCE / "_" / name).exists():
            shutil.copy2(SOURCE / "_" / name, build / name)
    for w in WAVES:
        (country / w / "_").mkdir(parents=True)
    for w in WAVES[:2]:
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
            return Country("Niger")

        def drop(self, column):
            for w in WAVES[:2]:
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
    assert c._has_country_builder("community_prices") is False
    empty = native_country.data / "empty-probe"
    target = empty / "Niger" / "var" / "community_prices.parquet"
    common = ["make", "-n", f"LSMS_DATA_ROOT={empty}", f"VAR_DIR={target.parent}"]
    absent = subprocess.run(common + [str(target)], cwd=native_country.config / "_",
                            capture_output=True, text=True)
    assert absent.returncode != 0 and "No rule to make target" in absent.stderr
    for w in WAVES[:2]:
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
    expected_waves = set(WAVES[:2]) & set(WAVES if requested is None else requested)
    assert len(result) == 2 * len(expected_waves)
    assert set(result.index.get_level_values("t")) == expected_waves
    assert list(result.index.names) == INDEX and result.index.is_unique
    if expected_waves:
        assert sorted(result.Price.tolist()) == sorted(
            float(value) for w in expected_waves
            for value in ([11, 12] if w == WAVES[0] else [21, 22]))
        assert int(result.Quantity.isna().sum()) == len(expected_waves)
    else:
        assert not list(native_country.data.rglob("*.parquet"))
    if backend == "make":
        assert not (native_country.data / "Niger" / "var" / "community_prices.parquet").exists()
    before = _receipts(native_country.data)
    _exact(result, _read(Country("Niger"), requested))
    if backend == "dvc":
        assert _receipts(native_country.data) == before
    # Compare to a genuinely separate normal default build, with the same args.
    reference = _read(native_country.select("reference", "dvc"), requested)
    _exact(result, reference)
    assert not list(native_country.config.rglob("*.parquet"))


@pytest.mark.parametrize("requested", [[WAVES[0]], WAVES[2:]])
def test_make_never_reads_an_existing_wrong_wave_country_payload(native_country, requested):
    c = native_country.select("existing", "make")
    var = native_country.data / "Niger" / "var" / "community_prices.parquet"
    var.parent.mkdir(parents=True)
    wrong = pd.DataFrame({"t": [WAVES[3]], "v": ["1091-wrong"], "j": ["Mil"],
                          "u": ["Kg"], "variety": ["7011"], "visit": [1], "obs": [1],
                          "Price": [999.0], "Quantity": [9.0], "Description": ["wrong"]})
    to_parquet(wrong.set_index(INDEX), var)
    before = _receipts(native_country.data)[str(var.relative_to(native_country.data))]
    result = _read(c, requested)
    assert len(result) == (2 if WAVES[0] in requested else 0)
    assert 999.0 not in result.Price.tolist()
    assert _receipts(native_country.data)[str(var.relative_to(native_country.data))] == before


@pytest.mark.parametrize("column", ["Price", "Quantity"])
def test_retirement_preserves_required_script_columns(native_country, column):
    native_country.drop(column)
    c = native_country.select("missing-column", "make")
    with pytest.raises(RuntimeError, match=f"missing required declared column.*{column}"):
        c.community_prices()
