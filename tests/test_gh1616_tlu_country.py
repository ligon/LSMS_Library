"""GH1616: explicit country is household identity, never a factor choice.

Invented animals/keys only; ordinary country-less arithmetic remains protected
by GH1485 controls. No sources, Country getters or scientific factors are added.
"""
from copy import deepcopy
import warnings

import pandas as pd
import pytest

from lsms_library import transformations as T


def herd(country_axis="index", animal_axis="index", countries=("A", "B")):
    x = pd.DataFrame({"country": list(countries), "t": ["wave"] * len(countries),
                      "i": ["same-household"] * len(countries),
                      "animal": ["Chicken"] * len(countries),
                      "HeadCount": pd.array([10, 20][:len(countries)], dtype="Float64")})
    levels = (["country"] if country_axis == "index" else []) + ["t", "i"]
    if animal_axis == "index": levels += ["animal"]
    return x.set_index(levels)


def run(x, **kwargs):
    before = x.copy(deep=True)
    attrs = deepcopy(x.attrs)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = T.tlu(x, **kwargs)
    pd.testing.assert_frame_equal(x, before, check_exact=True)
    assert x.attrs == attrs
    return out, caught


@pytest.mark.parametrize("country_axis", ["index", "column"])
@pytest.mark.parametrize("animal_axis", ["index", "column"])
@pytest.mark.parametrize("reverse", [False, True])
def test_collision_is_two_exact_households(country_axis, animal_axis, reverse):
    x = herd(country_axis, animal_axis)
    if reverse: x = x.iloc[::-1]
    out, seen = run(x)
    assert out.index.names == ["country", "t", "i"]
    assert out.index.tolist() == [("A", "wave", "same-household"), ("B", "wave", "same-household")]
    assert out.TLU.tolist() == [.1, .2]
    assert not seen
    assert out.attrs["tlu_species_coverage"]["input_rows"] == 2


@pytest.mark.parametrize("axis", ["index", "column"])
@pytest.mark.parametrize("kind", ["singleton", "empty", "unsupported", "missing", "zero"])
def test_country_presence_survives_cardinality_and_filtering(axis, kind):
    x = herd(axis, countries=("A",))
    if kind == "empty": x = x.iloc[:0]
    if kind == "unsupported":
        x = x.reset_index("animal"); x["animal"] = "Cow"
    if kind == "missing": x["HeadCount"] = pd.array([pd.NA], dtype="Float64")
    if kind == "zero": x["HeadCount"] = pd.array([0], dtype="Float64")
    out, seen = run(x)
    assert out.index.names == ["country", "t", "i"]
    assert len(out) == (0 if kind in {"empty", "unsupported", "missing"} else 1)
    if kind == "zero": assert out.TLU.tolist() == [0.]
    assert bool(seen) == (kind == "unsupported")


@pytest.mark.parametrize("axis", ["index", "column"])
@pytest.mark.parametrize("all_null", [False, True])
@pytest.mark.parametrize("filtered", [False, True])
def test_null_country_refuses_before_contribution_filter(axis, all_null, filtered):
    x = herd(axis, countries=(None, None if all_null else "B"))
    if filtered: x["HeadCount"] = pd.array([pd.NA, pd.NA], dtype="Float64")
    with pytest.raises(ValueError, match="country.*non-null"):
        run(x)


@pytest.mark.parametrize("column", [[None, None], ["wrong-B", "wrong-A"]])
def test_index_precedes_conflicting_or_null_column_and_attrs(column):
    x = herd(); x["country"] = column; x.attrs["country"] = "wrong"
    out, _ = run(x)
    assert out.index.get_level_values("country").tolist() == ["A", "B"]
    assert out.TLU.tolist() == [.1, .2]


def test_null_index_cannot_be_repaired_by_column_or_attrs():
    x = herd(countries=(None, "B")); x["country"] = ["A", "B"]; x.attrs["country"] = "A"
    with pytest.raises(ValueError, match="country.*non-null"): run(x)


def test_country_named_custom_measure_is_preserved_before_redundant_column_removal():
    x = herd(); x["country"] = pd.array([10, 20], dtype="Int64")
    out, _ = run(x, head_col="country")
    assert out.TLU.tolist() == [.1, .2]
    assert out.attrs["tlu_species_coverage"]["head_col"] == "country"


@pytest.mark.parametrize("local", [["t"], ["i"], []])
@pytest.mark.parametrize("supplied", [False, True])
def test_existing_partial_local_key_contract(local, supplied):
    x = herd().reset_index()
    levels = (["country"] if supplied else []) + local + ["animal"]
    if not supplied: x = x.drop(columns="country")
    x = x.set_index(levels)
    if not local:
        with pytest.raises(ValueError, match="No group keys passed"): run(x)
    else:
        out, _ = run(x)
        assert out.index.names == (["country"] if supplied else []) + local
        assert out.TLU.tolist() == ([.1, .2] if supplied else [.30000000000000004])


def test_scalar_attrs_do_not_manufacture_country_and_remain_unmodified():
    x = herd().droplevel("country"); x.attrs["country"] = "invented-country"
    out, _ = run(x)
    assert out.index.names == ["t", "i"]
    assert out.TLU.tolist() == [.30000000000000004]


@pytest.mark.parametrize("heads", [[10, None], [-10, 0], [0, 20]])
@pytest.mark.parametrize("animal_axis", ["index", "column"])
def test_separate_country_calls_equal_pooled_payload_and_global_coverage(heads, animal_axis):
    x = herd(animal_axis=animal_axis); x["HeadCount"] = pd.array(heads, dtype="Float64")
    out, _ = run(x, tlu_factors={"chicken": .03})
    parts = {}
    for name in ["A", "B"]:
        parts[name] = run(x.xs(name, level="country"), tlu_factors={"chicken": .03})[0]
    expected = pd.concat(parts, names=["country"])
    # Coverage is intentionally the complete input tally, not per-country attrs.
    expected.attrs = out.attrs
    pd.testing.assert_frame_equal(out, expected, check_exact=True)
    assert out.attrs["tlu_species_coverage"]["input_rows"] == 2


def test_reordered_index_and_unused_country_categories_preserve_observed_identity():
    x = herd().reset_index()
    x["country"] = pd.Categorical(x.country, categories=["unused", "B", "A"])
    x = x.set_index(["animal", "i", "country", "t"])
    out, _ = run(x)
    assert len(out) == 2
    observed = out
    assert observed.index.names == ["country", "t", "i"]
    assert observed.index.get_level_values("country").tolist() == ["B", "A"]
    assert observed.TLU.tolist() == [.2, .1]


def test_actual_feature_livestock_assembly_keeps_country_households(monkeypatch):
    import lsms_library as ll
    providers = {name: herd(countries=(name,)).droplevel('country')
                 for name in ['A', 'B']}
    providers['B']['HeadCount'] = pd.array([20], dtype='Float64')

    class Provider:
        def __init__(self, name, trust_cache=False): self.name = name
        def livestock(self, **kwargs): return providers[self.name]

    monkeypatch.setattr(ll, 'Country', Provider)
    feature = ll.Feature('livestock'); feature._countries = ['A', 'B']
    assembled = feature(['B', 'A'], currency=None)
    out, _ = run(assembled)
    assert out.index.names == ['country', 't', 'i']
    assert out.index.get_level_values('country').tolist() == ['A', 'B']
    assert out.TLU.tolist() == [.1, .2]


@pytest.mark.parametrize('supplied', [False, True])
def test_empty_with_no_local_index_keys_retains_existing_empty_result(supplied):
    x = herd().iloc[:0].reset_index()
    x = x.set_index(['country', 'animal'] if supplied else ['animal'])
    if not supplied: x = x.drop(columns='country')
    out, _ = run(x)
    assert out.empty and out.columns.tolist() == ['TLU']
    # Unlike a nonempty herd, pandas' existing empty groupby([]) does not raise.
    # A country axis cannot legalize nonempty local-key-free pooling (above).
