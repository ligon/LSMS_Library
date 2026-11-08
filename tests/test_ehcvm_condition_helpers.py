"""Exact old-helper parity using invented inputs; no producer or data access.

The repository-relative fixture freezes only original pure definitions and
literal instrument configuration at b8346cf1. It is not a source-data oracle.
Country modules and top-level wave producers are never imported or executed.
"""
from __future__ import annotations

import ast
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from lsms_library.ehcvm import crop_condition_codes, crop_condition_labels


ROOT = Path(__file__).resolve().parents[1]
ORIGINALS = json.loads((ROOT / "tests/fixtures/ehcvm_condition_originals.json").read_text())
HOMES = ORIGINALS["homes"]
DOMAINS = [(c, t) for c, h in HOMES.items() for t in h["tables"]]


def _names(country):
    prefix = "ehcvm_crop" if country == "Mali" else "_crop"
    return prefix + "_conditions", prefix + "_condition_labels", prefix + "_sold_conditions"


def _namespace(country, *, original=False, tables=None, failure=None):
    """Execute a finite allowlist of pure function definitions, never a module."""
    home = HOMES[country]
    tables = copy.deepcopy(home["tables"] if tables is None else tables)

    def mapping(*, tablename, idxvars, **columns):
        assert idxvars == "Code"
        assert columns in ({"Preferred Label": "Preferred Label"},
                           {"Source Label": "Source Label"})
        if failure is not None:
            raise failure("invented missing table")
        column = 1 if "Preferred Label" in columns else 2
        return {row[0]: row[column] for row in tables[tablename]}

    env = {"pd": pd, "CONDITION_NA": "unknown_condition",
           "tools": SimpleNamespace(get_categorical_mapping=mapping),
           "get_categorical_mapping": mapping,
           "crop_condition_codes": crop_condition_codes,
           "crop_condition_labels": crop_condition_labels}
    if original:
        nodes = [ast.parse(s).body[0] for s in home["definitions"].values()]
    else:
        tree = ast.parse((ROOT / home["path"]).read_text())
        nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name in home["definitions"]]
    assert {n.name for n in nodes} == set(home["definitions"])
    assert all(isinstance(n, ast.FunctionDef) and not n.decorator_list for n in nodes)
    exec(compile(ast.Module(nodes, []), home["path"], "exec"), env)
    return env


def _exact(left, right):
    assert type(left) is type(right)
    pd.testing.assert_series_equal(left, right, check_exact=True)
    assert left.attrs == right.attrs
    pd.testing.assert_series_equal(left.isna(), right.isna())


def _parity(country, name, args, *, tables=None, failure=None):
    snapshots = [arg.copy(deep=True) if isinstance(arg, pd.Series) else copy.deepcopy(arg)
                 for arg in args]
    outcomes = []
    for original in (True, False):
        env = _namespace(country, original=original, tables=tables, failure=failure)
        try:
            outcomes.append(("value", env[name](*args)))
        except Exception as exc:
            outcomes.append(("error", (type(exc), str(exc))))
        for arg, before in zip(args, snapshots):
            if isinstance(arg, pd.Series):
                _exact(arg, before)
            else:
                assert arg == before
    assert outcomes[0][0] == outcomes[1][0]
    if outcomes[0][0] == "error":
        assert outcomes[0][1] == outcomes[1][1]
    else:
        _exact(outcomes[0][1], outcomes[1][1])
    return outcomes[1]


CODE_CASES = ["int64", "float64", "Int64", "Float64", "string", "category",
              "object", "unknown", "fraction", "empty", "empty-Int64",
              "empty-string", "empty-category", "multiindex", "list", "scalar"]


def _codes(case, domain):
    values = list(range(1, domain + 1))
    if case in ("list", "scalar"):
        return values if case == "list" else 1
    if case.startswith("empty"):
        dtype = case.removeprefix("empty-") if case != "empty" else "float64"
        out = pd.Series([], dtype=dtype, index=pd.Index([], name="records"), name="state")
    else:
        if case == "unknown":
            values = [0, -1, 77, None, "", "bad", " 2 ", "2.0"]
        elif case == "fraction":
            values = [1, 1.5, None]
        dtype = case if case in ("int64", "float64", "Int64", "Float64", "string", "category") else "object"
        if dtype != "int64" and case != "fraction":
            values += [None]
        index = pd.Index([9, 9] + list(range(len(values) - 2)), name="records")
        if case == "multiindex":
            index = pd.MultiIndex.from_arrays([["x"] * len(values), index], names=["batch", "record"])
        out = pd.Series(values, dtype=dtype, index=index, name="state")
    out.attrs = {"instrument": "invented", "nested": {"keep": [1, 2]}}
    return out


@pytest.mark.parametrize("country,table", DOMAINS)
@pytest.mark.parametrize("case", CODE_CASES)
def test_every_own_code_carrier_matches_original(country, table, case):
    codes = _codes(case, len(HOMES[country]["tables"][table]))
    harvest, _, _ = _names(country)
    args = (codes, table) if country == "Niger" else (codes,)
    outcome = _parity(country, harvest, args)
    if case in ("fraction", "list", "scalar"):
        assert outcome[0] == "error"
    else:
        assert outcome[0] == "value"
        if case in ("int64", "float64", "Int64", "Float64", "string", "category", "object"):
            assert outcome[1].iloc[:len(HOMES[country]["tables"][table])].tolist() == [
                row[1] for row in HOMES[country]["tables"][table]]


@pytest.mark.parametrize("country", list(HOMES))
@pytest.mark.parametrize("case", ["object", "string", "category", "empty", "empty-string", "multiindex"])
def test_every_literal_answer_matches_original(country, case):
    rows = next(iter(HOMES[country]["tables"].values()))
    values = [row[2] for row in rows] + [" Grain ", "", " \t", None, "l'état", "Não descascado"]
    if case.startswith("empty"):
        values = []
    dtype = case if case in ("object", "string", "category") else "string"
    labels = pd.Series(values, dtype=dtype, name="answer")
    labels.index = pd.Index([i // 2 for i in range(len(labels))], name="record")
    if case == "multiindex":
        labels.index = pd.MultiIndex.from_arrays([["x"] * len(labels), labels.index], names=["a", "b"])
    labels.attrs = {"literal": True}
    args = (labels,)
    if country == "CotedIvoire":
        codes = pd.Series([int(row[0]) for row in rows] + [2, None, None, None, 77, 77],
                          dtype="Float64", index=labels.index) if values else pd.Series([], dtype="Float64", index=labels.index)
        args += (codes,)
    outcome = _parity(country, _names(country)[1], args)
    assert outcome[0] == "value"
    assert outcome[1].iloc[:len(rows)].tolist() == [row[2] for row in rows] if values else outcome[1].empty


@pytest.mark.parametrize("country", list(HOMES))
@pytest.mark.parametrize("failure", [FileNotFoundError, KeyError])
def test_mapping_failures_keep_original_exception(country, failure):
    harvest, native, _ = _names(country)
    codes = pd.Series([1.0])
    args = (codes, next(iter(HOMES[country]["tables"]))) if country == "Niger" else (codes,)
    assert _parity(country, harvest, args, failure=failure)[0] == "error"
    if country == "CotedIvoire":
        assert _parity(country, native, (pd.Series(["Epis"]), codes), failure=failure)[0] == "error"


@pytest.mark.parametrize("country", list(HOMES))
def test_wrong_domain_validation_and_evaluation_order_match_original(country):
    tables = copy.deepcopy(HOMES[country]["tables"])
    table = next(iter(tables))
    tables[table] = tables[table][:-1]
    args = (pd.Series([1.0]), table) if country == "Niger" else (pd.Series([1.0]),)
    outcome = _parity(country, _names(country)[0], args, tables=tables)
    assert outcome[0] == ("value" if country == "Niger" else "error")
    # A bad carrier and absent table together preserve which operation fails first.
    args = (pd.DataFrame({"code": [1]}), table) if country == "Niger" else (pd.DataFrame({"code": [1]}),)
    for original in (True, False):
        env = _namespace(country, original=original, failure=KeyError)
        with pytest.raises(TypeError if country == "Niger" else KeyError):
            env[_names(country)[0]](*args)


@pytest.mark.parametrize("country,table", [(c, t) for c, t in DOMAINS if c in ("Burkina_Faso", "Mali", "Niger")])
def test_sold_na_and_harvest_disagreement_match_original(country, table):
    codes = pd.Series([1, 2, None, 77], dtype="Float64", index=[8, 8, 2, 3], name="sold")
    codes.attrs = {"sold": True}
    harvest, _, sold = _names(country)
    args = (codes, table) if country == "Niger" else (codes,)
    assert _parity(country, sold, args)[0] == "value"
    env = _namespace(country)
    assert env[sold](*args).iloc[2:].isna().all()
    assert env[harvest](*args).iloc[2:].eq("unknown_condition").all()
    frame = pd.DataFrame({"Quantity": pd.Series([4, None, 7, None], index=codes.index, dtype="Float64"),
                          "condition": env[harvest](pd.Series([2, 1, None, 1], index=codes.index), table)
                          if country == "Niger" else env[harvest](pd.Series([2, 1, None, 1], index=codes.index)),
                          "Condition_sold": env[sold](*args)})
    before = frame.copy(deep=True)
    _parity(country, sold, args)
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    assert frame.iloc[0]["condition"] != frame.iloc[0]["Condition_sold"]


def test_civ_seven_answer_oracle_rejects_global_five_answer_mapping():
    env = _namespace("CotedIvoire")
    codes = pd.Series([1, 2, 3, 4, 5, 6, 7], name="state")
    expected = pd.Series(["unshelled", "shelled", "shelled", "unshelled",
                          "shell_not_applicable", "other_condition", "other_condition"],
                         dtype="string", name="state")
    _exact(env["_crop_conditions"](codes), expected)
    labels = pd.Series(["Epis", "Grain", "Décortiqué", "Non-décortiqué", "Non concerné", "6", "7"])
    assert env["_crop_condition_labels"](labels, codes).tolist() == [
        "Epis", "Grain", "Décortiqué", "Non-décortiqué", "Non concerné", "Fève", "Autre à préciser"]
    wrong = crop_condition_codes(codes, dict(enumerate(expected.iloc[:5], 1))).fillna("unknown_condition")
    with pytest.raises(AssertionError):
        _exact(wrong, expected)
    # Vague 1's literal five-answer domain is distinct from the seven-answer table.
    assert HOMES["CotedIvoire"]["tables"]["harmonize_crop_condition_ehcvm1"][:5] == HOMES["Benin"]["tables"]["harmonize_crop_condition_ehcvm1"]


@pytest.mark.parametrize("codes", [pd.Series([1.5]), pd.Series([None], dtype="Float64"),
                                 pd.Series([], dtype="Int64")])
def test_civ_native_code_errors_and_missing_carriers_match_original(codes):
    labels = pd.Series(["6"] * len(codes), dtype="string", index=codes.index)
    _parity("CotedIvoire", "_crop_condition_labels", (labels, codes))


@pytest.mark.parametrize("country", list(HOMES))
def test_committed_instrument_tables_match_frozen_literal_configuration(country):
    lines = (ROOT / "lsms_library/countries" / country / "_/categorical_mapping.org").read_text().splitlines()
    for table, expected in HOMES[country]["tables"].items():
        start = lines.index("#+NAME: " + table) + 1
        rows = []
        for line in lines[start:]:
            if not line.startswith("|"):
                break
            cells = [v.strip() for v in line.strip("|").split("|")]
            if cells[0].isdigit():
                rows.append(cells)
        assert rows == expected


@pytest.mark.parametrize("country", list(HOMES))
def test_actual_caller_binds_shared_functions_and_keeps_other_helpers(country):
    home = HOMES[country]
    tree = ast.parse((ROOT / home["path"]).read_text())
    imports = [n for n in tree.body if isinstance(n, ast.ImportFrom) and n.module == "lsms_library.ehcvm"]
    assert len(imports) == 1
    assert [(n.name, n.asname) for n in imports[0].names] == [
        ("crop_condition_codes", None), ("crop_condition_labels", None)]
    functions = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    for name, shared in zip(_names(country)[:2], ("crop_condition_codes", "crop_condition_labels")):
        calls = [n for n in ast.walk(functions[name]) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == shared]
        assert len(calls) == 1
        assert _namespace(country)[shared] is globals()[shared]
    for name, source in home["definitions"].items():
        if name not in _names(country)[:2]:
            assert ast.dump(functions[name]) == ast.dump(ast.parse(source).body[0])


def test_guinea_producer_remains_byte_exact():
    guinea = ORIGINALS["guinea_producer"]
    assert hashlib.sha256((ROOT / guinea["path"]).read_bytes()).hexdigest() == guinea["sha256"]
