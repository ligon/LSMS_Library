"""Synthetic controls for the independent crop-test household oracles.

All household coordinates/edges below are invented. No served frame, country
formatter, raw getter or panel generator supplies an expected result.
"""
import hashlib
import json

import pandas as pd
import pytest

import test_gh824_mali_ehcvm_crop_production as crop
import test_mali_crop_survey_kg as survey


@pytest.fixture
def panel(tmp_path, monkeypatch):
    # A deliberately nonidentity target and a second edge expose chain following.
    payload = json.dumps({"2018-19": {}, "2021-22": {
        "EHCVM21_12003": "99007", "99007": "88001"}}).encode()
    (tmp_path / "_").mkdir()
    path = tmp_path / "_" / "updated_ids.json"
    path.write_bytes(payload)
    monkeypatch.setattr(crop, "_root", lambda: tmp_path)
    monkeypatch.setattr(survey, "ROOT", tmp_path)
    # This synthetic map is positively bound, separately from the committed map.
    for module in (crop, survey):
        monkeypatch.setattr(module, "UPDATED_IDS_SHA256", hashlib.sha256(payload).hexdigest())
    return path


def _expected(module, cluster, household):
    raw = pd.DataFrame({"grappe": [cluster], "menage": [household]})
    if module is crop:
        return crop._walk("2021-22", crop._household(raw)).iloc[0]
    return survey._household(raw).iloc[0]


@pytest.mark.parametrize("module", [crop, survey], ids=["gh824", "survey-kg"])
@pytest.mark.parametrize("cluster,household,want", [
    pytest.param(12, 3, "99007", id="approved-linked-nonidentity-target"),
    pytest.param(12, 4, "EHCVM21_12004", id="unlinked"),
    pytest.param(12, 5, "EHCVM21_12005", id="unknown-panel-answer"),
    pytest.param(77, 8, "EHCVM21_77008", id="orphan-not-in-cover"),
])
def test_raw_namespace_precedes_one_approved_substitution(panel, module, cluster, household, want):
    assert _expected(module, cluster, household) == want


def test_same_bare_coordinates_stay_distinct_between_2018_and_unlinked_2021(panel):
    raw = pd.DataFrame({"grappe": [12], "menage": [4]})
    assert crop._walk("2018-19", crop._household(raw)).tolist() == ["12004"]
    assert crop._walk("2021-22", crop._household(raw)).tolist() == ["EHCVM21_12004"]
    assert survey._household(raw).tolist() == ["EHCVM21_12004"]


@pytest.mark.parametrize("key", ["EHCVM21_12004", "EHCVM21_EHCVM21_12004", "", "12x004", None])
def test_gh824_refuses_prefixed_or_malformed_raw_keys(panel, key):
    with pytest.raises(ValueError, match="bare numeric raw household keys"):
        crop._walk("2021-22", pd.Series([key], dtype="string"))


@pytest.mark.parametrize("module", [crop, survey], ids=["gh824", "survey-kg"])
def test_survey_coordinates_cannot_already_contain_a_namespace(panel, module):
    with pytest.raises(ValueError):
        _expected(module, "EHCVM21_12", 4)


@pytest.mark.parametrize("module", [crop, survey], ids=["gh824", "survey-kg"])
def test_an_unreviewed_map_is_refused_before_it_can_change_expected_keys(panel, module):
    panel.write_bytes(b'{"2021-22": {}}')
    with pytest.raises(AssertionError, match="unreviewed Mali panel map"):
        _expected(module, 12, 4)


def test_both_oracles_bind_the_committed_approved_map():
    approved = "7c17086a611c44bdd1f99b79305636feb67c8273cd349b3e81a580cdbe6649dd"
    assert crop.UPDATED_IDS_SHA256 == survey.UPDATED_IDS_SHA256 == approved
    assert hashlib.sha256((crop._root() / "_" / "updated_ids.json").read_bytes()).hexdigest() == approved
