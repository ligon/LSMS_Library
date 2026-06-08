"""GH #223 Layer 2: Malawi other-specify unit cleanup must not lose magnitude.

`_clean_freetext_unit` drops a leading count of *one* ('1 Basket' -> 'Basket')
but must NEVER strip/relabel a larger leading number, which can be a magnitude
that defines the unit's kg-equivalence ('10Kgs', '10G Packet') -- doing so
would corrupt the kg quantity by an order of magnitude.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

_MALAWI = (Path(__file__).resolve().parents[1]
           / "lsms_library" / "countries" / "Malawi" / "_" / "malawi.py")


def _load_mod():
    spec = importlib.util.spec_from_file_location("malawi_for_test", _MALAWI)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:  # pragma: no cover - environment-dependent imports
        pytest.skip(f"malawi.py not importable here: {exc}")
    return mod


def _load_clean():
    return _load_mod()._clean_freetext_unit


def _na(v):
    return v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NA


def test_drops_leading_count_of_one():
    f = _load_clean()
    assert f("1 BASKET") == "Basket"
    assert f("1 NSIMA PLATE (PHAZI)") == "Nsima Plate (Phazi)"
    assert f("1 KG") == "Kg"


def test_preserves_magnitudes_no_order_of_magnitude_error():
    f = _load_clean()
    # These MUST stay intact -- the leading number is a magnitude, not a count.
    assert f("10Kgs") == "10Kgs"
    assert f("149G") == "149G"
    assert f("10G Packet") == "10G Packet"
    assert f("10 GRAM PACKET") == "10 Gram Packet"
    assert f("1 5 LITER CONTAINER") == "5 Liter Container"  # only the '1' drops


def test_bare_quantity_becomes_na():
    f = _load_clean()
    assert _na(f("1/4"))
    assert _na(f("0.5"))
    assert _na(f("0"))
    assert _na(f("nan"))


def test_metric_kg_factor_scales_not_relabels():
    mf = _load_mod()._metric_kg_factor
    # pure metric magnitudes -> kg-equivalent factor (scale the quantity)
    assert mf("10Kgs") == 10.0
    assert abs(mf("149G") - 0.149) < 1e-9
    assert abs(mf("250 ml") - 0.25) < 1e-9
    assert mf("5 Litre") == 5.0
    # metric-sized containers, spaced OR glued, convert by the same rule
    # ('50 kg bag' and '90Kgbag' are the same <num><unit><container>).
    assert abs(mf("10G Packet") - 0.01) < 1e-9
    assert abs(mf("10Gpacket") - 0.01) < 1e-9
    assert mf("50 kg Bag") == 50.0
    assert mf("90Kgbag") == 90.0
    assert mf("5 Litre Bucket") == 5.0
    assert abs(mf("500Gtin") - 0.5) < 1e-9
    assert abs(mf("250 Milimiter") - 0.25) < 1e-9  # ml typo tolerated
    # first-word container in a multi-word / punctuated tail; 'of <item>'
    assert mf("15 Litre Pale") == 15.0
    assert mf("1Litre Bottle Super") == 1.0
    assert mf("5 Litre Bucket(Chigoba)") == 5.0
    assert abs(mf("25Gram Of Uchi") - 0.025) < 1e-9
    # non-metric container / count / non-container word -> None (left as-is)
    assert mf("Basket") is None
    assert mf("2 Cup") is None
    assert mf("10 Pieces") is None
    assert mf("12 Bottles") is None      # count of bottles, no per-unit size
    assert mf("2Sachet") is None         # sachet, no metric magnitude
    assert mf("1Mlambe") is None         # 'ml' prefix but 'ambe' not a container
    assert mf(pd.NA) is None


def test_metric_factor_rejects_glued_nonmetric_words():
    # The greedy letter-run guard: a number glued to a NON-metric word must
    # not match a metric prefix ('10Giraffes' is not 10 g!).  '5Lions' must
    # not match the litre unit 'l' either.
    mf = _load_mod()._metric_kg_factor
    assert mf("10Giraffes") is None
    assert mf("10Goats") is None
    assert mf("5Lions") is None
    assert mf("3Bottles") is None
    # but a real glued metric token still works
    assert abs(mf("3Grams") - 0.003) < 1e-9
    assert mf("10Kg") == 10.0
