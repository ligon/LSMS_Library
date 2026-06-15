"""GH #223 Layer 2: numeric unit-code normalization + leak audit.

- ``_augment_numeric_code_keys`` lets a Code-keyed ``#+name:u`` table match
  data that arrives as float-strings ('1.0').
- ``diagnostics.food_acquired_u_code_leaks`` flags leaked codes so
  regressions in already-clean countries surface.
"""
from __future__ import annotations

import pandas as pd
import pytest

from lsms_library.country import _augment_numeric_code_keys
from lsms_library import diagnostics


# --- _augment_numeric_code_keys --------------------------------------------

def test_augment_adds_float_and_int_string_variants():
    # Nigeria pattern: integer Code keys, data arrives as '1.0'.
    out = _augment_numeric_code_keys({1: "Kg", 2: "g"})
    assert out["1.0"] == "Kg" and out["1"] == "Kg"
    assert out["2.0"] == "g" and out["2"] == "g"
    assert out[1] == "Kg"  # original key preserved


def test_augment_leaves_non_numeric_labels_untouched():
    out = _augment_numeric_code_keys({"Tas": "Tas", "kg": "Kg"})
    assert out == {"Tas": "Tas", "kg": "Kg"}  # no spurious variants


def test_augment_skips_genuine_non_integers():
    out = _augment_numeric_code_keys({"1.5": "Half"})
    assert out == {"1.5": "Half"}  # 1.5 is not an integer code


def test_augment_original_key_wins_on_collision():
    # An explicit '1.0' label must not be clobbered by a synthesized variant.
    out = _augment_numeric_code_keys({1: "FromInt", "1.0": "Explicit"})
    assert out["1.0"] == "Explicit"


# --- food_acquired_u_code_leaks -------------------------------------------

def _fa(u_values):
    idx = pd.MultiIndex.from_arrays(
        [["x"] * len(u_values), u_values], names=["i", "u"]
    )
    return pd.DataFrame({"Quantity": [1.0] * len(u_values)}, index=idx)


def test_leak_detector_flags_codes_prefixes_and_itemnames():
    df = _fa(["Kg", "Tas", "116", "6.0", "1. Kilogram", "0 [Butter]"])
    leaks = diagnostics.food_acquired_u_code_leaks("X", df=df)
    assert leaks == ["0 [Butter]", "1. Kilogram", "116", "6.0"]


def test_leak_detector_clean_frame():
    df = _fa(["Kg", "Tas", "Litre", "Sachet"])
    assert diagnostics.food_acquired_u_code_leaks("X", df=df) == []


def test_leak_detector_no_u_level():
    df = pd.DataFrame({"Quantity": [1.0]},
                      index=pd.MultiIndex.from_tuples([("a", "b")], names=["i", "t"]))
    assert diagnostics.food_acquired_u_code_leaks("X", df=df) == []


def test_leak_detector_categorical_u_with_nan():
    # Regression (master CI, post-v0.7.3): a *categorical* ``u`` level with a
    # NaN keeps NaN as a float through ``.astype(str)`` (object dtype gives
    # 'nan'), feeding ``re.match`` a float -> TypeError.  A NaN ``u`` means no
    # unit was recorded, not a leaked code, so it must be skipped.
    u_cat = pd.Categorical(["Kg", "Tas", "116", float("nan")])
    idx = pd.MultiIndex.from_arrays([["x"] * 4, u_cat], names=["i", "u"])
    df = pd.DataFrame({"Quantity": [1.0] * 4}, index=idx)
    assert diagnostics.food_acquired_u_code_leaks("X", df=df) == ["116"]


# --- cross-country regression (skips when caches/data unavailable) ----------

# Clean per the 2026-06-07 audit; must stay clean.
_KNOWN_CLEAN = ["Mali", "Niger", "CotedIvoire", "Guinea-Bissau"]
# Tracked dirty (driven down by Layer 2 / #347 / #348). Progress:
#   Ethiopia  45 -> 0  (prefix strip, #370)
#   Togo      25 -> 4  (EHCVM codebook; residual 114/659/660/662 ACCEPTED*)
#   Burkina   22 -> 2  (EHCVM codebook; residual 254/255 ACCEPTED*)
#   Nigeria   32 -> 3  (WB .dta labels; residual 131/151/152 ACCEPTED*)
#   Senegal    1 -> 0  (code '1' data-entry anomaly, 2 rows -> u='NA')
#   Malawi   217 -> 19 (#382/#391/#399/#383; 19 residual ACCEPTED*)
#   GhanaLSS  13 -> 0  (#348; 1991-92 units dict was empty -- fixed value-col
#                       + float->str code lookup; all codes now decode)
# *ACCEPTED = item-specific / data-entry codes with no label in ANY source
#  (questionnaire scheme differs from the data coding); documented per-country
#  and left as codes -- they still yield valid food_expenditures + unitvalue,
#  only the kg aggregate excludes them.  No food_acquired country now leaks
#  non-accepted unit codes (Layer-2 u-harmonization complete).


def test_clean_countries_have_no_u_code_leaks():
    import warnings

    import lsms_library as ll

    checked = 0
    for c in _KNOWN_CLEAN:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                fa = ll.Country(c, preload_panel_ids=False).food_acquired()
        except Exception:
            continue  # microdata not available in this environment
        checked += 1
        leaks = diagnostics.food_acquired_u_code_leaks(c, df=fa)
        assert not leaks, f"{c}: leaked u codes {leaks[:10]}"
    if checked == 0:
        pytest.skip("no food_acquired data available for clean-country audit")
