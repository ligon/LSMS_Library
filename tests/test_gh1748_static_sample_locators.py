"""Public source locators for the existing private CI sample assertion."""
import ast
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import lsms_library as ll

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_TEST = ROOT / "tests" / "test_sample.py"
ISA = {"Burkina_Faso", "Ethiopia", "Malawi", "Mali", "Niger", "Nigeria",
       "Tanzania", "Uganda"}
PREFIX = "test_covers_all_waves_"


def _sample(waves):
    index = pd.MultiIndex.from_arrays(
        [[f"invented-{n}" for n in range(len(waves))], waves], names=["i", "t"])
    return pd.DataFrame({"v": ["invented-cluster"] * len(waves)}, index=index)


@pytest.fixture
def sample_tests():
    spec = importlib.util.spec_from_file_location("sample_tests_1748", SAMPLE_TEST)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_named_cases_are_static_and_match_their_public_country_wave():
    seen = set()
    current = set()
    declared = {c: set(ll.Country(c).waves) for c in ISA}
    for fn in ast.parse(SAMPLE_TEST.read_text(encoding="utf-8")).body:
        if not isinstance(fn, ast.FunctionDef) or not fn.name.startswith(PREFIX):
            continue
        assert len(fn.body) == 1 and isinstance(fn.body[0], ast.Expr)
        call = fn.body[0].value
        assert isinstance(call, ast.Call) and not call.keywords
        c, w = (ast.literal_eval(arg) for arg in call.args)
        assert fn.name == PREFIX + c + "_" + w.replace("-", "_")
        assert call.func.id == "_require_named_sample_wave"
        assert c in ISA and (c, w) not in seen
        # Do not require new waves to have a static diagnostic immediately.
        # The existing dynamic test continues to check them.
        seen.add((c, w))
        if w in declared[c]:
            current.add((c, w))
    assert {c for c, w in current} == ISA


def test_named_and_dynamic_checks_share_one_frame(monkeypatch, sample_tests):
    m = sample_tests
    calls = []
    partial = _sample(["2021-22"])

    def read_sample():
        calls.append("sample")
        return partial

    monkeypatch.setattr(m.ll, "Country", lambda name: SimpleNamespace(
        waves=["2011-12", "2021-22"], sample=read_sample))
    with pytest.raises(AssertionError, match="Ethiopia sample missing waves"):
        m.TestSample().test_covers_all_waves("Ethiopia", m._require_sample("Ethiopia"))
    with pytest.raises(AssertionError, match="declared wave absent"):
        m.test_covers_all_waves_Ethiopia_2011_12()
    m.test_covers_all_waves_Ethiopia_2021_22()
    assert calls == ["sample"]
    assert m._require_sample("Ethiopia") is partial


def test_unknown_new_wave_still_fails_the_original_assertion(monkeypatch, sample_tests):
    m = sample_tests
    monkeypatch.setattr(m.ll, "Country", lambda name: SimpleNamespace(
        waves=["2021-22", "2098-99"]))
    monkeypatch.setitem(m._sample_cache, "Ethiopia", _sample(["2021-22"]))
    m.test_covers_all_waves_Ethiopia_2021_22()
    with pytest.raises(AssertionError, match="2098-99"):
        m.TestSample().test_covers_all_waves("Ethiopia", m._require_sample("Ethiopia"))


def test_named_case_keeps_shared_exemptions_and_unavailable_skips(monkeypatch, sample_tests):
    m = sample_tests
    monkeypatch.setattr(m.ll, "Country", lambda name: SimpleNamespace(
        waves=["2011-12", "2021-22"]))
    monkeypatch.setitem(m.PARTIALLY_WIRED_SAMPLE, "Ethiopia", {"2011-12"})
    monkeypatch.setitem(m._sample_cache, "Ethiopia", _sample(["2021-22"]))
    with pytest.raises(pytest.skip.Exception, match="wave no longer expected"):
        m.test_covers_all_waves_Ethiopia_2011_12()
    monkeypatch.setitem(m._sample_cache, "Ethiopia", None)

    def unexpected_metadata_read(name):
        pytest.fail("did not skip unavailable sample before reading metadata")

    monkeypatch.setattr(m.ll, "Country", unexpected_metadata_read)
    with pytest.raises(pytest.skip.Exception, match="could not be built"):
        m.test_covers_all_waves_Ethiopia_2021_22()
    assert m._sample_cache == {"Ethiopia": None}
