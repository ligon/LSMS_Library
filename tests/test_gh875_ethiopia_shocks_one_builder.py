"""GH #875 -- Ethiopia `shocks` must have exactly ONE builder, with ONE rule.

The defect: the YAML path (`{wave}/_/data_info.yml`, which is what
`data_scheme.yml` selects -- it declares no `materialize:`) mapped
`Increase -> True`, the corpus convention; four wave scripts plus the country
concatenator `Ethiopia/_/shocks.py`, still wired into `_/Makefile`, mapped
`Increase -> False`, imputed missing answers to `False`, and named the
household level `j` against the declared `(t, i, Shock)`.  Both wrote the same
cache locations, so which rule a caller got depended on build path and cache
state rather than on the code.

Fixed by deleting the script path.  These tests are config-only (no build), so
they are cheap and cannot be defeated by a warm cache.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import SchemeLoader

ETHIOPIA = Path(countries_root()) / "Ethiopia"
WAVES = ["2011-12", "2013-14", "2015-16", "2018-19", "2021-22"]
EFFECT_COLUMNS = [
    "AffectedIncome",
    "AffectedAssets",
    "AffectedProduction",
    "AffectedConsumption",
    "AffectedFoodStock",
]


def _shocks_myvars(wave: str) -> dict:
    with open(ETHIOPIA / wave / "_" / "data_info.yml", encoding="utf-8") as f:
        info = yaml.safe_load(f)
    assert "shocks" in info, f"{wave}: no shocks block -- the YAML path IS the builder"
    return info["shocks"]["myvars"]


def test_no_shocks_build_script_anywhere_under_ethiopia():
    """The second builder is gone, wave-level and country-level."""
    strays = sorted(str(p.relative_to(ETHIOPIA)) for p in ETHIOPIA.glob("*/_/shocks.py"))
    strays += [str(p.relative_to(ETHIOPIA)) for p in ETHIOPIA.glob("_/shocks.py")]
    assert strays == [], f"shocks build script(s) resurrected: {strays}"


def test_makefile_has_no_shocks_rule():
    text = (ETHIOPIA / "_" / "Makefile").read_text(encoding="utf-8")
    recipe_lines = [
        ln for ln in text.splitlines()
        if "shocks" in ln and not ln.lstrip().startswith("#")
    ]
    assert recipe_lines == [], f"Makefile still builds shocks: {recipe_lines}"


def test_data_scheme_declares_no_materialize_for_shocks():
    """`shocks` is a YAML-path table; a `materialize:` key would re-open the split."""
    with open(ETHIOPIA / "_" / "data_scheme.yml", encoding="utf-8") as f:
        scheme = yaml.load(f, Loader=SchemeLoader)["Data Scheme"]["shocks"]
    assert "materialize" not in scheme
    assert scheme["index"] == "(t, i, Shock)"
    for col in EFFECT_COLUMNS:
        assert scheme[col] == "bool", col
    # W1-W3 only, hence optional (data_scheme is country-grain).
    assert scheme["Occurrence"] == {"type": "int", "optional": True}


@pytest.mark.parametrize("wave", WAVES)
def test_any_change_counts_as_affected(wave):
    """The corpus convention: Increase -> True, Did Not Change -> False.

    `Increase -> False` is the rule the deleted scripts used; it must never
    come back, in any wave, for any effect column.
    """
    myvars = _shocks_myvars(wave)
    for col in EFFECT_COLUMNS:
        assert col in myvars, f"{wave}: {col} not read"
        decl = myvars[col]
        assert isinstance(decl, list) and len(decl) == 2, f"{wave}/{col}: {decl!r}"
        mapping = decl[1]["mapping"]
        for label, value in mapping.items():
            key = str(label).upper()
            if "INCREASE" in key or "DECREASE" in key:
                assert value is True, f"{wave}/{col}: {label!r} -> {value!r}"
            elif "DID NOT CHANGE" in key or "DID NOT" in key:
                assert value is False, f"{wave}/{col}: {label!r} -> {value!r}"


@pytest.mark.parametrize("wave", WAVES)
def test_affected_food_stock_is_read(wave):
    """Script-only until #875; now on the sanctioned path in all five waves."""
    assert "AffectedFoodStock" in _shocks_myvars(wave)


@pytest.mark.parametrize("wave", WAVES)
def test_occurrence_read_exactly_where_the_instrument_asks_it(wave):
    """`hh_s8q05` is W1-W3 only -- W4/W5 dropped the question."""
    myvars = _shocks_myvars(wave)
    if wave in ("2011-12", "2013-14", "2015-16"):
        assert myvars.get("Occurrence") == "hh_s8q05"
    else:
        assert "Occurrence" not in myvars
