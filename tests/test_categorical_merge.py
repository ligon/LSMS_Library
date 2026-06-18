"""GH #223 Layer 2 / DESIGN_u_consolidation: additive global<->country
categorical-table merge.

`u` and `harmonize_assets` (GH #168) are row-unioned (global core +
country-specific rows, country wins on key collision); every other table
keeps full-table override.
"""
from __future__ import annotations

import pandas as pd

from lsms_library.country import (
    _ADDITIVE_CATEGORICAL_TABLES,
    _merge_categorical_tables,
    _row_union_categorical,
)


def _tbl(rows, cols=("Original Label", "Preferred Label")):
    return pd.DataFrame(rows, columns=list(cols))


def test_u_is_additive():
    assert "u" in _ADDITIVE_CATEGORICAL_TABLES


def test_harmonize_assets_is_additive():
    # GH #168: the global harmonize_assets.org carries the shared vocabulary;
    # per-country tables override only their country-specific rows.
    assert "harmonize_assets" in _ADDITIVE_CATEGORICAL_TABLES


def test_harmonize_education_is_additive():
    # GH #171: the global harmonize_education.org ordinal vocabulary is the
    # shared base; per-country tables override only their own attainment labels.
    assert "harmonize_education" in _ADDITIVE_CATEGORICAL_TABLES


def test_row_union_country_wins_and_inherits_global():
    glob = _tbl([["kg", "Kg"], ["g", "g"], ["l", "Litre"]])
    ctry = _tbl([["kg", "KILO"], ["Tas", "Tas"]])   # overrides kg, adds Tas
    out = _row_union_categorical(glob, ctry)
    d = out.set_index("Original Label")["Preferred Label"].to_dict()
    assert d["kg"] == "KILO"      # country wins on collision
    assert d["g"] == "g"          # inherited from global
    assert d["l"] == "Litre"      # inherited from global
    assert d["Tas"] == "Tas"      # country-specific added


def test_row_union_preserves_wave_columns():
    glob = _tbl([["kg", "Kg"], ["g", "g"]])
    ctry = pd.DataFrame(
        [["Tas", "Tas", "tas_2018"]],
        columns=["Original Label", "Preferred Label", "2018-19"],
    )
    out = _row_union_categorical(glob, ctry)
    assert "2018-19" in out.columns
    row = out[out["Original Label"] == "Tas"].iloc[0]
    assert row["2018-19"] == "tas_2018"
    # global rows keep their values; wave column is NaN for them
    assert out[out["Original Label"] == "kg"]["Preferred Label"].iloc[0] == "Kg"


def test_row_union_falls_back_when_key_columns_disagree():
    glob = _tbl([["kg", "Kg"]], cols=("Original Label", "Preferred Label"))
    ctry = _tbl([[1, "Kg"]], cols=("Code", "Preferred Label"))
    out = _row_union_categorical(glob, ctry)
    # can't align Original Label vs Code -> country wins wholesale
    assert list(out.columns) == ["Code", "Preferred Label"]
    assert out["Code"].tolist() == [1]


def test_merge_additive_tables_rowunion_others_override():
    glob = {
        "u": _tbl([["kg", "Kg"], ["g", "g"]]),
        "harmonize_assets": _tbl([["bike", "Bicycle"], ["car", "Car"]]),
        "Roof": _tbl([["grass", "Thatch"], ["tin", "Iron Sheets"]]),
    }
    ctry = {
        "u": _tbl([["Tas", "Tas"]]),                   # additive -> inherits kg/g
        "harmonize_assets": _tbl([["bike", "Bike"]]),  # additive (#168) -> inherits car
        "Roof": _tbl([["grass", "Grass"]]),            # non-additive -> full override
    }
    merged = _merge_categorical_tables(glob, ctry)
    u = merged["u"].set_index("Original Label")["Preferred Label"].to_dict()
    assert set(u) == {"kg", "g", "Tas"}                # u row-unioned
    ha = merged["harmonize_assets"].set_index("Original Label")["Preferred Label"].to_dict()
    assert set(ha) == {"bike", "car"}                  # assets row-unioned (#168)
    assert ha["bike"] == "Bike"                        # country wins on collision
    assert ha["car"] == "Car"                          # global row inherited
    roof = merged["Roof"]
    assert roof["Original Label"].tolist() == ["grass"]  # non-additive: fully replaced
    assert roof["Preferred Label"].tolist() == ["Grass"]


def test_merge_global_only_and_country_only_tables_pass_through():
    glob = {"u": _tbl([["kg", "Kg"]]), "kinship": _tbl([["x", "X"]])}
    ctry = {"u": _tbl([["Tas", "Tas"]]), "region": _tbl([["n", "North"]])}
    merged = _merge_categorical_tables(glob, ctry)
    assert "kinship" in merged and "region" in merged          # both kept
    assert set(merged["u"].set_index("Original Label")["Preferred Label"]
               .to_dict()) == {"kg", "Tas"}
