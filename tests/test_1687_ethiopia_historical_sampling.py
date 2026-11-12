"""Pure historical-operand controls; invented graphs, no survey/API/cache reads."""
import copy
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tests import test_sampling as sampling
from tests.test_gh1196_parquet_index_policy import _exact


EARLIER, WAVE = "2011-12", "2013-14"
FIXTURE = Path(__file__).parent / "fixtures" / "ethiopia_updated_ids_fbc213417.json"


def links():
    old = {"native-1": "h1", "native-2": "h2", "native-3": "h3"}
    return old, dict(old, **{"native-new": "added"})


def frame():
    # A -> B and C -> D are old bijective edges. An added A -> X edge
    # disqualifies A in the full graph without changing the old cohort.
    index = pd.MultiIndex.from_arrays([
        pd.array(["h1", "h2", "h3", "added"] * 2, dtype="string[pyarrow]"),
        pd.array([EARLIER] * 4 + [WAVE] * 4, dtype="string[pyarrow]"),
    ], names=["i", "t"])
    index = index.set_levels([level.insert(len(level), "unused") for level in index.levels])
    result = pd.DataFrame({
        "v": pd.array(["A", "A", "C", "A", "B", "B", "D", "X"],
                      dtype="string[pyarrow]"),
        "weight": pd.array([np.nextafter(1., 2.), None, 0., 5., 2., 3., 4., 9.],
                           dtype="Float64"),
        "panel_weight": pd.array([0., 1., None, 3., 4., 5., 6., 7.], dtype="Float64"),
        "Rural": pd.Categorical(["Rural", None, "Urban", "Rural"] * 2,
                                categories=["Unused", "Urban", "Rural"], ordered=True),
        "untouched": pd.array(["None", "nan", None, "<NA>"] * 2, dtype="string[pyarrow]"),
    }, index=index)
    result.attrs = {"id_converted": True, "nested": {
        "typed": [1, 1.0, True], "missing": None, "literal": "nan"}}
    return result


def original(monkeypatch, sample, country="Ethiopia", n=3, d=3, k=2):
    """Run the committed reproduction body with independent invented operands."""
    old, current = links()
    monkeypatch.setattr(sampling, "_ethiopia_w2_links",
                        lambda path, historical=False: old if historical else current)
    monkeypatch.setattr(sampling, "sampling_records", lambda _: {
        WAVE: {"panel": {"cluster_id_measurement": {
            "linked_households": n, "households_changed": d,
            "clusters_renumbered": k}}}})
    check = sampling.TestTheReproduction()
    requests = []

    def public_sample(*, waves):
        requests.append(waves)
        return sample

    monkeypatch.setattr(check, "_fresh", lambda _: SimpleNamespace(sample=public_sample))
    before = copy.deepcopy(sample)
    try:
        check.test_the_curated_size_matches_the_served_sample(country, EARLIER, WAVE)
    finally:
        _exact(sample, before)
        assert requests == [[EARLIER, WAVE]]  # One full, unchanged API request.


def test_exact_members_and_full_typed_current_conservation():
    sample = frame()
    before = copy.deepcopy(sample)
    old, current = links()
    selected = sampling._ethiopia_historical_cluster_view(sample, EARLIER, WAVE, old, current)
    _exact(selected, sample.iloc[[0, 1, 2, 4, 5, 6]])
    _exact(sample, before)
    assert len(sample) == 8 and ("added", WAVE) in sample.index
    # Return is independent, including nested attrs; current nullable weights stay exact.
    selected.loc[("h1", EARLIER), "weight"] = 99.
    selected.attrs["nested"]["typed"].append("changed")
    _exact(sample, before)


def test_added_sender_destination_changes_full_k_only(monkeypatch):
    sample = frame()
    # Independent graph discriminators: old A has only B; full A also has X.
    assert set(sample.loc[[("h1", WAVE), ("h2", WAVE)], "v"]) == {"B"}
    assert sample.loc[("added", WAVE), "v"] == "X"
    original(monkeypatch, sample)
    original(monkeypatch, sample, country="GhanaLSS", n=4, d=4, k=1)


def test_same_n_d_wrong_k_mutant_is_detected(monkeypatch):
    sample = frame()
    sample.loc[("h2", WAVE), "v"] = "X"
    # All three old households still link and all change, but A splits.
    with pytest.raises(AssertionError, match=r"\(1, 2\)"):
        original(monkeypatch, sample)


@pytest.mark.parametrize("quantity,want", [("n", 4), ("d", 2)])
def test_original_n_d_assertions_remain_active(monkeypatch, quantity, want):
    with pytest.raises(AssertionError, match=rf"\(3, {want}\)"):
        original(monkeypatch, frame(), **{quantity: want})


@pytest.mark.parametrize("kind", ["changed", "missing"])
def test_old_edge_drift_refuses(kind):
    old, current = links()
    if kind == "changed":
        current["native-1"] = "different"
    else:
        del current["native-1"]
    with pytest.raises(AssertionError, match=kind + " old Ethiopia link"):
        sampling._ethiopia_historical_cluster_view(frame(), EARLIER, WAVE, old, current)


@pytest.mark.parametrize("side", ["old", "current"])
def test_noninjective_maps_refuse(side):
    old, current = links()
    (old if side == "old" else current)["alias"] = "h1"
    with pytest.raises(AssertionError, match="noninjective Ethiopia links"):
        sampling._ethiopia_historical_cluster_view(frame(), EARLIER, WAVE, old, current)


@pytest.mark.parametrize("text", [
    '{"2013-14":{"native":"h1","native":"h2"}}',
    '{"2013-14":{"native":"h1"},"2013-14":{"other":"h2"}}',
])
def test_duplicate_json_keys_refuse(tmp_path, text):
    path = tmp_path / "links.json"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(AssertionError, match="duplicate Ethiopia map key"):
        sampling._ethiopia_w2_links(path)


def test_exact_historical_fixture_and_sha_failure(tmp_path):
    # Entire public fixture is read by the real parser, no Git/shared path at runtime.
    assert sampling._ethiopia_w2_links(FIXTURE, historical=True)
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_bytes(FIXTURE.read_bytes() + b" ")  # JSON meaning unchanged, bytes wrong.
    with pytest.raises(AssertionError, match="historical Ethiopia fixture SHA256"):
        sampling._ethiopia_w2_links(corrupt, historical=True)


@pytest.mark.parametrize("pair", [(WAVE, EARLIER), (EARLIER, "2015-16")])
def test_wrong_boundary_refuses(pair):
    with pytest.raises(AssertionError, match="historical Ethiopia boundary"):
        sampling._ethiopia_historical_cluster_view(frame(), *pair, *links())


@pytest.mark.parametrize("kind,error", [
    ("missing-v", "missing or duplicate v"),
    ("duplicate-key", "duplicate current household/wave"),
    ("wrong-wave", "incorrect sample waves"),
    ("wrong-index", "historical operand index"),
    ("null-key", "null sample key"),
])
def test_bad_full_current_input_refuses(kind, error):
    sample = frame()
    if kind == "missing-v":
        sample = sample.drop(columns="v")
    elif kind == "duplicate-key":
        sample = pd.concat([sample, sample.iloc[[0]]])
    elif kind == "wrong-wave":
        sample = sample.rename(index={WAVE: "wrong"}, level="t")
    elif kind == "wrong-index":
        sample = sample.reorder_levels(["t", "i"])
    else:
        values = sample.index.to_frame(index=False)
        values.loc[0, "i"] = pd.NA
        sample.index = pd.MultiIndex.from_frame(values)
    before = copy.deepcopy(sample)
    with pytest.raises(AssertionError, match=error):
        sampling._ethiopia_historical_cluster_view(sample, EARLIER, WAVE, *links())
    _exact(sample, before)


def test_extra_unlinked_record_cannot_enter_old_cohort():
    sample = frame()
    extra = sample.iloc[[0]].copy(deep=True)
    extra.index = pd.MultiIndex.from_tuples([("unlinked", WAVE)], names=["i", "t"])
    extra["v"] = pd.array(["Q"], dtype="string[pyarrow]")
    full = pd.concat([sample, extra])
    before = copy.deepcopy(full)
    selected = sampling._ethiopia_historical_cluster_view(full, EARLIER, WAVE, *links())
    _exact(selected, full.iloc[[0, 1, 2, 4, 5, 6]])
    _exact(full, before)


def test_other_country_full_intersection_is_unchanged(monkeypatch):
    original(monkeypatch, frame(), country="Tanzania", n=4, d=4, k=1)


def test_outside_domain_null_v_is_conserved(monkeypatch):
    sample = frame()
    sample.loc[("added", WAVE), "v"] = pd.NA
    before = copy.deepcopy(sample)
    selected = sampling._ethiopia_historical_cluster_view(sample, EARLIER, WAVE, *links())
    _exact(selected, sample.iloc[[0, 1, 2, 4, 5, 6]])
    _exact(sample, before)
    original(monkeypatch, sample)


def test_historical_null_v_reaches_original_n_assertion(monkeypatch):
    sample = frame()
    sample.loc[("h1", WAVE), "v"] = pd.NA
    before = copy.deepcopy(sample)
    # The helper preserves the missing value. Original dropna/N owns the refusal.
    selected = sampling._ethiopia_historical_cluster_view(sample, EARLIER, WAVE, *links())
    _exact(selected, sample.iloc[[0, 1, 2, 4, 5, 6]])
    with pytest.raises(AssertionError, match=r"\(2, 3\)"):
        original(monkeypatch, sample)
    _exact(sample, before)
