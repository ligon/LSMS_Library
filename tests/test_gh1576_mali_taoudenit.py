"""Exact cosmetic Region fold through existing machinery; all rows invented.

Ledger: 1576-mali-taoudenit.md §§2–5. Source acceptance is a separate gate.
"""
import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.ehcvm import derive_region_milieu_stratum


@pytest.fixture
def mali(monkeypatch):
    # Metadata construction is permitted; no getter/provider may read a source.
    from lsms_library import local_tools, data_access
    import lsms_library.country as country_module
    def refused(*args, **kwargs):
        pytest.fail("Hermetic mapping controls must not read/acquire a source")
    monkeypatch.setattr(local_tools, "get_dataframe", refused)
    monkeypatch.setattr(country_module, "get_dataframe", refused)
    monkeypatch.setattr(data_access, "get_data_file", refused)
    return Country("Mali")


@pytest.mark.parametrize("raw,preferred", [
    ("Taoudenit", "Taoudénit"), ("Taoudénit", "Taoudénit"),
    ("Segou", "Ségou"), ("Tomboctou", "Tombouctou"),
    ("Menaka", "Menaka"), ("Tombouctou", "Tombouctou"),
    ("TAOUDENIT", "TAOUDENIT"), ("Taoudeni", "Taoudeni"),
])
def test_exact_column_mapping_and_no_new_aliases(mali, raw, preferred):
    native = pd.DataFrame({"Region": pd.array([raw, pd.NA], dtype="string"),
                           "Quantity": pd.array([0., pd.NA], dtype="Float64")})
    native.attrs = {"id_converted": True, "source": "invented"}
    expected = native.copy(deep=True)
    expected.loc[0, "Region"] = preferred
    out = mali._apply_categorical_mappings(native.copy(deep=True))
    pd.testing.assert_frame_equal(out, expected)
    assert out.attrs == expected.attrs


@pytest.mark.parametrize("milieu", ["Urbain", "Rural"])
def test_cover_and_consumption_labels_converge_without_changing_milieu(mali, milieu):
    labels = [derive_region_milieu_stratum(region, milieu)
              for region in ["Taoudenit", "Taoudénit"]]
    native = pd.DataFrame({"Region": ["Taoudenit", "Taoudénit"], "strata": labels,
                           "Unrelated": ["Taoudenit", "Taoudénit"]})
    out = mali._apply_categorical_mappings(native.copy(deep=True))
    assert out.Region.tolist() == ["Taoudénit"] * 2
    assert out.strata.tolist() == [f"Taoudénit {milieu}"] * 2
    pd.testing.assert_series_equal(out.Unrelated, native.Unrelated)


def test_named_index_mapping_preserves_keys_rows_order_nulls_and_attrs(mali):
    index = pd.MultiIndex.from_tuples([
        ("second", "Taoudenit", "Taoudenit Rural"),
        ("first", "Taoudénit", "Taoudénit Rural"),
        ("missing", pd.NA, pd.NA),
    ], names=["invented_key", "Region", "strata"])
    native = pd.DataFrame({"Amount": pd.array([2.5, 0, pd.NA], dtype="Float64")}, index=index)
    native.attrs = {"id_converted": True, "source": "invented"}
    expected = native.rename(index={"Taoudenit": "Taoudénit"}, level="Region")
    expected = expected.rename(index={"Taoudenit Rural": "Taoudénit Rural"}, level="strata")
    out = mali._apply_categorical_mappings(native.copy(deep=True))
    pd.testing.assert_frame_equal(out, expected)
    assert out.attrs == native.attrs
    assert out.index.get_level_values("invented_key").tolist() == ["second", "first", "missing"]


def test_eaci_design_codes_empty_strata_and_other_schemes_pass_unchanged(mali):
    native = pd.DataFrame({"strata": pd.array(["1", "45", "50", "51", "", pd.NA,
                                               "Agadez urbain", "Niamey"], dtype="string")})
    pd.testing.assert_frame_equal(mali._apply_categorical_mappings(native.copy(deep=True)), native)


@pytest.mark.parametrize("method", ["cluster_features", "sample"])
def test_real_finalizer_matches_already_canonical_control_without_native_mutation(mali, method):
    names = ["t", "v"] if method == "cluster_features" else ["i", "t", "v"]
    keys = [("invented-wave", "c2"), ("invented-wave", "c1")]
    if method == "sample":
        keys = [("h2", *keys[0]), ("h1", *keys[1])]
    native = pd.DataFrame({"Region": pd.array(["Taoudenit", pd.NA], dtype="string"),
                           "strata": pd.array(["Taoudénit Rural", pd.NA], dtype="string"),
                           "Unrelated": pd.array(["Taoudenit", pd.NA], dtype="string")},
                          index=pd.MultiIndex.from_tuples(keys, names=names))
    native.attrs = {"id_converted": True, "source": "invented"}
    original = native.copy(deep=True)
    canonical = native.copy(deep=True)
    canonical.loc[keys[0], "Region"] = "Taoudénit"
    scheme = {"index": names, "Region": "str", "strata": "str", "Unrelated": "str"}
    actual = mali._finalize_result(native.copy(deep=True), scheme, method)
    control = mali._finalize_result(canonical, scheme, method)
    pd.testing.assert_frame_equal(actual, control)
    assert actual.attrs == control.attrs
    assert actual.Region.iloc[0] == "Taoudénit"
    assert actual.strata.iloc[0] == "Taoudénit Rural"
    assert actual.Unrelated.iloc[0] == "Taoudenit"
    pd.testing.assert_frame_equal(native, original)
    assert native.attrs == original.attrs
