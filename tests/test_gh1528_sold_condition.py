"""Own EHCVM sold answers; no harvest-state inference or sale placement change.

The dictionaries below were checked separately against the released Burkina2018,
Mali2018 and Mali2021 sold variables. Source/API equality is a separate data gate.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.derivations import resolve_callable, records_for
from lsms_library.paths import countries_root
from lsms_library import local_tools


FIELDS = ["Condition_sold", "condition_sold_native"]
SALE = ["Quantity_sold", "Unit_sold", "Value_sold"]
FOLD = {1: "unshelled", 2: "shelled", 3: "shelled", 4: "unshelled",
        5: "shell_not_applicable"}
LABELS = ["Epis", "Grain", "Décortiqué", "Non-décortiqué", "Non concerné"]
ROOT = Path(countries_root())


def _mali(name):
    return resolve_callable(f"lsms_library.countries.Mali._.mali:{name}")


def _burkina(*names):
    path = ROOT / "Burkina_Faso/2018-19/_/crop_production.py"
    nodes = [n for n in ast.parse(path.read_text()).body
             if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == set(names)
    env = {"pd": pd, "get_categorical_mapping": local_tools.get_categorical_mapping}
    exec(compile(ast.Module(nodes, []), str(path), "exec"), env)
    return env


@pytest.mark.parametrize("country,wave", [("Burkina_Faso", "2018-19"),
                                          ("Mali", "2018-19"), ("Mali", "2021-22")])
def test_sold_fold_is_own_five_codes_with_na_not_harvest_sentinel(country, wave, monkeypatch):
    monkeypatch.chdir(ROOT / country / wave / "_")
    fn = (_burkina("_crop_conditions", "_crop_sold_conditions")["_crop_sold_conditions"]
          if country == "Burkina_Faso" else _mali("ehcvm_crop_sold_conditions"))
    codes = pd.Series([1, 2, 3, 4, 5, None, 77], dtype="float64", index=list("abcdefg"))
    expected = pd.Series([*FOLD.values(), pd.NA, pd.NA], dtype="string", index=codes.index)
    pd.testing.assert_series_equal(fn(codes), expected)


@pytest.mark.parametrize("country", ["Burkina_Faso", "Mali"])
def test_native_answer_is_literal_and_does_not_use_fold_vocabulary(country):
    fn = (_burkina("_crop_condition_labels")["_crop_condition_labels"]
          if country == "Burkina_Faso" else _mali("ehcvm_crop_condition_labels"))
    labels = pd.Series([*LABELS, "", None, "  Décortiqué  "], dtype="string")
    expected = pd.Series([*LABELS, pd.NA, pd.NA, "Décortiqué"], dtype="string")
    pd.testing.assert_series_equal(fn(labels), expected)


class _Loader(yaml.SafeLoader):
    pass


_Loader.add_constructor("!make", lambda loader, node: None)


@pytest.mark.parametrize("country", ["Burkina_Faso", "Mali"])
def test_new_fields_are_optional_columns_and_never_keys(country):
    scheme = yaml.load((ROOT / country / "_/data_scheme.yml").read_text(), Loader=_Loader)
    entry = scheme["Data Scheme"]["crop_production"]
    for field in FIELDS:
        assert entry[field] == {"type": "str", "optional": True}
        assert field not in entry["index"]


@pytest.mark.parametrize("count", [0, 1, 2, 3])
def test_explicit_sold_payload_follows_existing_attachment_without_moving_old_values(count):
    harvest = pd.DataFrame({"i": ["h"] * count, "crop": ["Mil"] * count,
                            "plot_id": [str(n) for n in range(count)],
                            "Quantity": [13.0] * count, "condition": ["unshelled"] * count}).astype(
                                {"i": "string", "crop": "string", "plot_id": "string"})
    sales = pd.DataFrame({"i": ["h"], "crop": ["Mil"], "Quantity_sold": [2.0],
                          "Unit_sold": ["Sac (50 Kg)"], "Value_sold": [301.0],
                          "Condition_sold": ["shelled"], "condition_sold_native": ["Grain"]})
    fn = _mali("derive_hh_crop_sale_attachment")
    before = fn(harvest, sales.drop(columns=FIELDS))
    got = fn(harvest, sales, columns=SALE + FIELDS)
    pd.testing.assert_frame_equal(got.drop(columns=FIELDS), before)
    sold = got[got.Quantity_sold.notna()]
    assert len(sold) == 1
    assert sold[FIELDS].iloc[0].tolist() == ["shelled", "Grain"]
    assert sold.plot_id.iloc[0] == ("0" if count == 1 else "Farm-level")
    assert got.loc[got.Quantity_sold.isna(), FIELDS].isna().all(axis=None)
    # A sold state cannot overwrite the independently unshelled harvest.
    assert got.loc[got.Quantity.notna(), "condition"].eq("unshelled").all()


def test_attachment_default_still_ignores_extra_fields_and_refuses_duplicate_sales():
    fn = _mali("derive_hh_crop_sale_attachment")
    harvest = pd.DataFrame({"i": ["h"], "crop": ["Mil"], "plot_id": ["p"]})
    sales = pd.DataFrame({"i": ["h"], "crop": ["Mil"], "Quantity_sold": [2.0],
                          "Unit_sold": ["Kg"], "Value_sold": [3.0],
                          "Condition_sold": ["shelled"], "condition_sold_native": ["Grain"]})
    assert not set(FIELDS).intersection(fn(harvest, sales).columns)
    with pytest.raises(ValueError, match="more than one sale"):
        fn(harvest, pd.concat([sales, sales]), columns=SALE + FIELDS)


@pytest.mark.parametrize("with_answer", [False, True])
def test_ehcvm_finish_preserves_old_values_and_types_new_columns(with_answer):
    frame = pd.DataFrame({"i": ["h"], "crop": ["Mil"], "plot_id": ["p"],
                          "Quantity": [7.0], "Quantity_kg": [123.0], "u": ["Kg"],
                          "condition": ["unshelled"], "condition_native": ["Epis"],
                          "Quantity_sold": [2.0], "Value_sold": [3.0], "Unit_sold": ["Kg"],
                          "intercropped": [pd.NA]})
    if with_answer:
        frame[FIELDS] = ["shelled", "Grain"]
    got = _mali("finish_ehcvm_crop_production")(frame, "2021-22")
    assert got.index.names == ["t", "i", "plot_id", "crop", "condition"]
    assert got.Quantity.tolist() == [7.0] and got.Quantity_kg.tolist() == [123.0]
    assert got.condition_native.tolist() == ["Epis"]
    assert all(str(got[f].dtype) == "string" for f in FIELDS)
    if with_answer:
        assert got[FIELDS].iloc[0].tolist() == ["shelled", "Grain"]
    else:
        assert got[FIELDS].isna().all(axis=None)


@pytest.mark.parametrize("country,wave,variable,frame", [
    ("Burkina_Faso", "2018-19", "s16cq16c", "src_codes"),
    ("Mali", "2018-19", "s16cq16c", "src_codes"),
    ("Mali", "2021-22", "s16dq05d", "records_codes"),
])
def test_each_new_field_reads_its_own_sold_variable_not_harvest(country, wave, variable, frame):
    tree = ast.parse((ROOT / country / wave / "_/crop_production.py").read_text())
    dicts = [n for n in ast.walk(tree) if isinstance(n, ast.Dict)]
    fields = {k.value: v for n in dicts for k, v in zip(n.keys, n.values)
              if isinstance(k, ast.Constant) and k.value in FIELDS}
    assert set(fields) == set(FIELDS)
    for name, expr in fields.items():
        text = ast.unparse(expr)
        assert repr(variable) in text
        assert "s16cq12c" not in text and "s16cq16d" not in text
        if name == "Condition_sold":
            assert frame in text and "sold_conditions(" in text
        else:
            assert "condition_labels(" in text
    raw_reads = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == frame for t in n.targets)]
    assert len(raw_reads) == 1
    assert any(k.arg == "convert_categoricals" and k.value.value is False
               for k in raw_reads[0].value.keywords)


def test_attachment_registry_names_new_payload_and_own_source():
    rec = records_for("Mali", "crop_production")["Mali::crop_production::hh-crop-sale-single-row"]
    assert set(FIELDS) <= set(rec.columns)
    assert "s16dq05d" in rec.raw_variables


@pytest.mark.requires_s3
@pytest.mark.parametrize("country,wave,file,state,crop", [
    ("Burkina_Faso", "2018-19", "s16c_me_bfa2018.dta", "s16cq16c", "s16cq04"),
    ("Mali", "2018-19", "s16c_me_mli2018.dta", "s16cq16c", "s16cq04"),
    ("Mali", "2021-22", "s16d_me_mli2021.dta", "s16dq05d", "s16dq01"),
])
def test_served_answers_equal_own_raw_codes_and_literal_labels(country, wave, file, state, crop):
    """No country helper or production state table supplies the expected answer.

    Crop alignment reuses the unchanged harmonize_food table; this is not an
    independent audit of crop labels. The sold dictionary and native answers are
    independent of that table and of every production condition helper.
    """
    import json
    from lsms_library import Country

    path = ROOT / country / wave / "Data" / file
    numeric = local_tools.get_dataframe(str(path), convert_categoricals=False)
    literal = local_tools.get_dataframe(str(path), convert_categoricals=True)
    table = local_tools.df_from_orgfile(ROOT / country / "_/categorical_mapping.org",
                                       name="harmonize_food")
    source_header = "Original Label" if country == "Burkina_Faso" else "Code"
    mapping = dict(zip(table[source_header].astype(str).str.strip(),
                       table["Preferred Label"].astype(str).str.strip()))
    ids = (numeric.grappe.astype(int).astype(str) + "0"
           + numeric.menage.astype(int).astype(str).str.zfill(2))
    walk = json.loads((ROOT / country / "_/updated_ids.json").read_text()).get(wave, {})
    ids = ids.map(lambda value: walk.get(value, value))
    labels = literal[crop].astype("string").str.strip()
    crops = labels.map(mapping)
    if country == "Burkina_Faso":
        crops = crops.fillna(labels)
    raw = pd.DataFrame({"i": ids, "crop": crops,
                        "Condition_sold": pd.to_numeric(numeric[state]).map(FOLD).astype("string"),
                        "condition_sold_native": literal[state].astype("string").str.strip().replace("", pd.NA)})
    got = Country(country).crop_production(waves=[wave]).reset_index()
    if wave == "2021-22":
        sale = (pd.to_numeric(numeric.s16dq05a).gt(0) | pd.to_numeric(numeric.s16dq06).gt(0))
        raw = raw[sale]
        got = got[got.Quantity_sold.gt(0) | got.Value_sold.gt(0)]
        keys = ["i", "crop"]
    else:
        raw["plot_id"] = (numeric.s16cq02.astype(int).astype(str) + "_"
                          + numeric.s16cq03.astype(int).astype(str))
        # Rows the existing API discards contain no payload, including no sold answer.
        raw = raw[raw.Condition_sold.notna() | raw.condition_sold_native.notna()]
        got = got[got.Condition_sold.notna() | got.condition_sold_native.notna()]
        keys = ["i", "plot_id", "crop"]
    expected = raw.set_index(keys)[FIELDS].sort_index()
    actual = got.set_index(keys)[FIELDS].sort_index().astype("string")
    assert expected.index.is_unique and actual.index.is_unique
    pd.testing.assert_frame_equal(actual, expected, check_index_type=False)
