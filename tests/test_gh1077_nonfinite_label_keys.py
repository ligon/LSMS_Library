"""GH #1077: additive numeric spelling expansion must retain nonfinite keys.

The real table/target cells below reproduce the live metadata census at
f8d236694, not the historical 36-cell premise. No microdata is needed.
"""
from functools import lru_cache
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from lsms_library import Country
from lsms_library.country import _augment_numeric_code_keys, _categorical_key_column


@pytest.mark.parametrize("key", [
    np.nan, np.inf, -np.inf, "nan", "NaN", " NAN ",
    "inf", "+inf", "-inf", "Infinity", "+Infinity", "-Infinity", "1e1000",
])
def test_nonfinite_original_key_and_value_survive_without_variants(key):
    value = object()
    raw = {key: value}
    out = _augment_numeric_code_keys(raw)
    assert out is raw
    assert next(iter(out)) is key
    assert next(iter(out.values())) is value


def test_mixed_nonfinite_keys_do_not_change_finite_fractional_or_collision_rules():
    nan = float("nan")
    raw = {nan: "missing", float("inf"): "positive infinity",
           float("-inf"): "negative infinity", " NaN ": "spelled missing",
           "1e1000": "overflow spelling", 1: "integer", "1": "explicit int",
           "1.0": "explicit float", "-2": "negative", "+003": "padded",
           "1.5": "fraction string", 2.5: "fraction numeric", "Tas": "native"}
    out = _augment_numeric_code_keys(raw)
    assert len(out) == len(raw) + 3
    for key, value in raw.items():
        assert out[key] == value
    assert out["1"] == "explicit int" and out["1.0"] == "explicit float"
    assert out["-2.0"] == "negative"
    assert out["3"] == out["3.0"] == "padded"
    assert set(out).difference(raw) == {"-2.0", "3", "3.0"}


@lru_cache(maxsize=None)
def _country(name):
    return Country(name, preload_panel_ids=False)


# Actual current nonfinite-crashing cells; independent original-key replacement
# below retains every nonmissing-key replacement; the caller omits null keys.
REAL_CELLS = [
    ("Ethiopia", "harmonize_food", "Preferred Label"),
    ("Ethiopia", "harmonize_food", "2011-12"),
    ("Ethiopia", "harmonize_food", "2013-14"),
    ("Ethiopia", "harmonize_food", "2015-16"),
    ("Ethiopia", "harmonize_food", "2018-19"),
    ("Ethiopia", "harmonize_food", "2021-22"),
    ("Ethiopia", "harmonize_food", "FTC Code"),
    ("Ethiopia", "harmonize_food", "FDC ID"),
    ("Guatemala", "harmonize_food", "2000"),
    ("Mali", "u", "Code"),
    ("Mali", "harmonize_food", "Code"),
    ("GhanaLSS", "nonfood_items", "Preferred Label"),
    ("GhanaLSS", "nonfood_items", "Code_2005-06"),
    ("GhanaLSS", "nonfood_items", "Code_2012-13"),
    ("GhanaLSS", "nonfood_items", "Code_2016-17"),
    ("GhanaLSS", "nonfood_items", "Label_2005-06"),
    ("GhanaLSS", "nonfood_items", "Label_2012-13"),
    ("GhanaLSS", "nonfood_items", "Label_2016-17"),
    ("GhanaLSS", "nonfood_items_9b", "Preferred Label"),
    ("GhanaLSS", "nonfood_items_9b", "Code_2005-06"),
    ("GhanaLSS", "nonfood_items_9b", "Code_2012-13"),
    ("GhanaLSS", "nonfood_items_9b", "Code_2016-17"),
    ("GhanaLSS", "nonfood_items_9b", "Label_2005-06"),
    ("GhanaLSS", "nonfood_items_9b", "Label_2012-13"),
    ("GhanaLSS", "nonfood_items_9b", "Label_2016-17"),
    ("Nigeria", "u", "Code"),
    ("Nigeria", "harmonize_industry", "Code"),
    ("Tanzania", "u", "Original Label"),
    ("Tanzania", "harmonize_input", "Code"),
    ("Tanzania", "harmonize_input", "Preferred Label"),
    ("Tanzania", "harmonize_input", "2019-20"),
    ("Tanzania", "harmonize_input", "2020-21"),
    ("Tanzania", "harmonize_species", "Code"),
    ("Uganda", "u", "Code"),
]


@pytest.mark.parametrize("name,table_name,target", REAL_CELLS)
def test_real_table_selection_preserves_original_replacements_and_other_data(
        name, table_name, target):
    country = _country(name)
    table = country.categorical_mapping[table_name]
    source = _categorical_key_column(table, exclude=(target,))
    assert source is not None
    original = table.set_index(source)[target].to_dict()
    # There is a genuinely nonfinite parseable key, even if a caller's data
    # itself never uses that key. The whole replacement dictionary is visited.
    nonfinite = []
    for key in original:
        try:
            nonfinite.append(not np.isfinite(float(str(key).strip())))
        except (TypeError, ValueError):
            pass
    assert any(nonfinite)
    values = table[source].astype(object).reset_index(drop=True)
    frame = pd.DataFrame({table_name: values, "Measurement": np.arange(len(values), dtype=float)})
    frame["Unchanged"] = np.nan
    frame.index.name = "observation"
    frame.attrs = {"id_converted": True, "control": {"nested": "preserve"}}
    expected = frame.copy()
    # Org cells are trimmed by the same public mapping contract, independent
    # of numeric augmentation. Direct pandas replacement is the key oracle.
    nonmissing = {k: v for k, v in original.items() if pd.notna(k)}
    expected[table_name] = frame[table_name].map(
        lambda x: x.strip() if isinstance(x, str) else x).replace(nonmissing)
    actual = country._apply_categorical_mappings(
        frame, labels={table_name: target})
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert actual.attrs == expected.attrs


def test_explicit_niger_key_cannot_be_excluded_by_its_requested_target():
    country = _country("Niger")
    table = country.categorical_mapping["u"]
    assert _categorical_key_column(table) == "Original Label"
    assert _categorical_key_column(table, exclude=("Original Label",)) is None
    frame = pd.DataFrame({"u": ["Kg"], "Quantity": [1.]})
    with pytest.raises(KeyError, match="no key column left"):
        country._apply_categorical_mappings(frame, labels={"u": "Original Label"})


@pytest.mark.parametrize("kind", ["column", "index"])
@pytest.mark.parametrize("name,table_name,target", REAL_CELLS)
def test_real_table_missing_source_key_never_labels_a_null_input(
        name, table_name, target, kind):
    country = _country(name)
    table = country.categorical_mapping[table_name]
    source = _categorical_key_column(table, exclude=(target,))
    known = next(x for x in table[source]
                 if pd.notna(x) and str(x).strip() not in ("", "---"))
    values = [known, "__1077_unmatched__", np.nan, None, pd.NA, "nan", "inf"]
    if kind == "column":
        frame = pd.DataFrame({table_name: pd.Series(values, dtype=object),
                              "Measurement": np.arange(len(values), dtype=float)})
        frame.index.name = "observation"
    else:
        index = pd.MultiIndex.from_arrays(
            [values, range(len(values))], names=[table_name, "observation"])
        frame = pd.DataFrame({"Measurement": np.arange(len(values), dtype=float)},
                             index=index)
    frame.attrs = {"id_converted": True, "control": {"nested": "preserve"}}
    out = country._apply_categorical_mappings(frame, labels={table_name: target})
    got = (out[table_name].tolist() if kind == "column"
           else list(out.index.get_level_values(table_name)))
    assert all(pd.isna(x) for x in got[2:5]), (name, table_name, target, kind)
    assert got[1] == values[1]
    assert got[5:] == ["nan", "inf"]
    assert out.attrs == frame.attrs
    if kind == "column":
        pd.testing.assert_frame_equal(out.drop(columns=[table_name]),
                                      frame.drop(columns=[table_name]), check_exact=True)
    else:
        pd.testing.assert_frame_equal(out.droplevel(table_name),
                                      frame.droplevel(table_name), check_exact=True)


@pytest.mark.parametrize("kind", ["column", "index"])
def test_literal_curated_nonfinite_names_targets_and_sentinels_are_distinct(kind):
    from lsms_library.local_tools import CATEGORICAL_KEY_ATTR

    source = [np.nan, None, pd.NA, "nan", "inf", np.inf, -np.inf,
              "known", "null-target", "kg", "Value"]
    targets = ["invented A", "invented B", "invented C", "literal nan",
               "literal inf", "numeric positive infinity", "numeric negative infinity",
               "known label", None, "bad kg", "bad Value"]
    table = pd.DataFrame({"Code": range(len(source)), "Recorded key": source,
                          "Preferred Label": targets, "Detail Label": targets})
    # Filter the resolved declared source, not the first column or the target.
    table.attrs[CATEGORICAL_KEY_ATTR] = "Recorded key"
    fake = SimpleNamespace(name="Fixtureland", categorical_mapping={"u": table})
    method = Country._apply_categorical_mappings.__get__(fake)
    values = [np.nan, None, pd.NA, "nan", "inf", np.inf, -np.inf, "known",
              "null-target", "kg", "Value", "Unknown", "unmatched"]
    if kind == "column":
        frame = pd.DataFrame({"u": pd.Series(values, dtype=object),
                              "Quantity": np.arange(len(values), dtype=float)})
    else:
        index = pd.MultiIndex.from_arrays([values, range(len(values))],
                                          names=["u", "observation"])
        frame = pd.DataFrame({"Quantity": np.arange(len(values), dtype=float)}, index=index)
    frame.attrs = {"id_converted": True, "control": "preserve"}
    out = method(frame, labels={"u": "Detail"}, protect_u_sentinels=True)
    got = out["u"].tolist() if kind == "column" else list(out.index.get_level_values("u"))
    assert all(pd.isna(x) for x in got[:3])
    assert got[3:8] == ["literal nan", "literal inf", "numeric positive infinity",
                        "numeric negative infinity", "known label"]
    assert pd.isna(got[8])  # A nonmissing key's explicitly null target survives.
    assert got[9:] == ["kg", "Value", "Unknown", "unmatched"]
    assert out.attrs == frame.attrs
    if kind == "column":
        pd.testing.assert_frame_equal(out.drop(columns="u"), frame.drop(columns="u"),
                                      check_exact=True)
    else:
        pd.testing.assert_frame_equal(out.droplevel("u"), frame.droplevel("u"),
                                      check_exact=True)
