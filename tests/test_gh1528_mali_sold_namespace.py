"""Pure controls for GH1528's independent raw-ID oracle (ledger1528 §5).

Compile only its ID statements, so collection imports no LSMS/data-test module.
Invented operands exercise the actual oracle source without a country formatter.
"""
import ast
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest


REPO = Path(__file__).resolve().parents[1]
ORACLE = REPO / "tests/test_gh1528_sold_condition.py"
MAP_SHA256 = "7c17086a611c44bdd1f99b79305636feb67c8273cd349b3e81a580cdbe6649dd"


def _id_nodes():
    tree = ast.parse(ORACLE.read_text(encoding="utf-8"))
    fn, = [n for n in tree.body if isinstance(n, ast.FunctionDef)
           and n.name == "test_served_answers_equal_own_raw_codes_and_literal_labels"]
    start, = [i for i, n in enumerate(fn.body) if isinstance(n, ast.Assign)
              and isinstance(n.targets[0], ast.Name) and n.targets[0].id == "ids"
              and isinstance(n.value, ast.BinOp)]
    nodes = fn.body[start:start + 4]
    # The four original/changed statements end before crop/answer construction.
    assert [type(n) for n in nodes] == [ast.Assign, ast.If, ast.Assign, ast.Assign]
    assert ast.unparse(nodes[-1]) == "ids = ids.map(lambda value: walk.get(value, value))"
    return nodes


def _ids(root, country="Mali", wave="2021-22", grappe=(12,), menage=(4,)):
    env = {"numeric": pd.DataFrame({"grappe": grappe, "menage": menage}),
           "country": country, "wave": wave, "ROOT": root, "json": json}
    exec(compile(ast.Module(_id_nodes(), []), str(ORACLE), "exec"), env)
    return env["ids"]


@pytest.fixture
def panel(tmp_path):
    # Nonidentity approved edge plus another edge detects accidental chain closure.
    # Deliberately no entry for Non, unknown-answer or absent-cover/orphan cases.
    maps = {"2018-19": {}, "2021-22": {"EHCVM21_12003": "99007", "99007": "88001"}}
    for country in ("Mali", "Burkina_Faso"):
        directory = tmp_path / country / "_"
        directory.mkdir(parents=True)
        (directory / "updated_ids.json").write_text(json.dumps(maps), encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize("cluster,household,want", [
    pytest.param(12, 3, "99007", id="approved-linked-nonidentity-single-pass"),
    pytest.param(12, 4, "EHCVM21_12004", id="unlinked-Non"),
    pytest.param(12, 5, "EHCVM21_12005", id="unknown-panel-answer"),
    pytest.param(77, 8, "EHCVM21_77008", id="orphan-no-cover-entry"),
])
def test_namespace_then_only_approved_map(panel, cluster, household, want):
    assert _ids(panel, grappe=[cluster], menage=[household]).tolist() == [want]


def test_same_bare_2018_and_unlinked_2021_stay_distinct(panel):
    assert _ids(panel, wave="2018-19").tolist() == ["12004"]
    assert _ids(panel).tolist() == ["EHCVM21_12004"]


@pytest.mark.parametrize("country,wave", [
    ("Mali", "2018-19"), ("Burkina_Faso", "2018-19"), ("Burkina_Faso", "2021-22"),
])
def test_other_routes_keep_original_bare_composite_and_map(panel, country, wave):
    path = panel / country / "_/updated_ids.json"
    path.write_text(json.dumps({wave: {"12004": "51002"}}), encoding="utf-8")
    assert _ids(panel, country=country, wave=wave).tolist() == ["51002"]


@pytest.mark.parametrize("bad", ["EHCVM21_12", "EHCVM21_EHCVM21_12", "12x", "", None])
@pytest.mark.parametrize("coordinate", ["grappe", "menage"])
def test_prefixed_or_malformed_numeric_coordinates_are_not_silently_repaired(panel, coordinate, bad):
    kwargs = {"grappe": [12], "menage": [4], coordinate: [bad]}
    with pytest.raises((TypeError, ValueError)):
        _ids(panel, **kwargs)


def test_prefix_occurs_once_before_map_and_order_is_preserved(panel):
    got = _ids(panel, grappe=[77, 12, 12, 12], menage=[8, 3, 5, 4])
    assert got.tolist() == ["EHCVM21_77008", "99007", "EHCVM21_12005", "EHCVM21_12004"]
    assert not got.str.contains("EHCVM21_EHCVM21_", regex=False).any()
    assert got.index.tolist() == [0, 1, 2, 3]


def test_unicode_committed_map_is_read_as_utf8_text(panel, monkeypatch):
    path = panel / "Mali/_/updated_ids.json"
    path.write_text(json.dumps({"2021-22": {"EHCVM21_12004": "cible_é"}},
                               ensure_ascii=False), encoding="utf-8")
    original = Path.read_text
    calls = []

    def read_text(self, *args, **kwargs):
        if self == path:
            calls.append(kwargs.get("encoding"))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    assert _ids(panel).tolist() == ["cible_é"]
    assert calls == ["utf-8"]


def test_exact_committed_map_version_and_namespace_domain():
    path = REPO / "lsms_library/countries/Mali/_/updated_ids.json"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == MAP_SHA256
    mapping = json.loads(path.read_text(encoding="utf-8"))
    links = mapping["2021-22"]
    assert links and mapping["2018-19"] == {}
    assert all(k.startswith("EHCVM21_") and k.removeprefix("EHCVM21_").isdigit()
               and isinstance(v, str) and v.isdigit() for k, v in links.items())
    # Compare all approved source edges without exporting source identifiers.
    raw = [k.removeprefix("EHCVM21_") for k in links]
    got = _ids(path.parents[2], grappe=[int(k[:-3]) for k in raw],
               menage=[int(k[-2:]) for k in raw])
    assert got.tolist() == list(links.values())
