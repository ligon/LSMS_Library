"""GH #797: categorical mappings run BEFORE kinship expansion in ``_finalize_result``.

Ordering contract (``Country._finalize_result``):

1. ``Relation`` -> ``Relationship`` rename;
2. ``_apply_categorical_mappings`` -- a country ``#+name: Relationship`` table
   with a ``Preferred Label`` column canonicalises the raw survey label;
3. ``_expand_kinship`` -- the kinship lookup (``categorical_mapping/kinship.yml``)
   therefore sees the CANONICAL label, not the raw one.

The swap was measured byte-identical on 34 of 34 cached ``household_roster``
tables before landing (measurement posted on GH #797): no
country curates a Relationship table with ``Preferred Label`` whose keys occur
in delivered data (Peru's is ``Code``-keyed and decoded at grab time; GhanaLSS's
two ``| Code | Label |`` tables have no ``Preferred Label`` and are not
mappings).  These tests pin the contract with a synthetic frame so it stays
true without microdata.

``_build_replace_dict`` keys on the FIRST column that is neither ``Preferred
Label`` nor the requested variant, so a mapping table must be written
``| Original Label | Preferred Label |`` (raw label first) -- see
``tests/test_label_selection.py::test_source_cols_ordering_is_the_key_column``.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pandas.testing as pdt
import pytest

from lsms_library.country import _expand_kinship, _load_kinship_map
from tests.test_label_selection import _fake_country, _stub_finalize

# A raw survey wording that kinship.yml does NOT know (asserted below), and the
# canonical label it should be mapped onto, which kinship.yml DOES know.
RAW = "Wife/Husband Of Head (Q3 Code 2)"
CANON = "Spouse"
CANON_TUPLE = (0, 0, "affinal")


def _roster(labels):
    idx = pd.MultiIndex.from_tuples(
        [("2000", f"h{n}", "1") for n in range(1, len(labels) + 1)],
        names=["t", "i", "pid"])
    df = pd.DataFrame({"Sex": ["F"] * len(labels),
                       "Age": [30] * len(labels),
                       "Relationship": list(labels)}, index=idx)
    df.attrs["id_converted"] = True
    return df


def _preferred_label_table():
    """``| Original Label | Preferred Label |`` -- raw label FIRST (the key)."""
    return pd.DataFrame({
        "Original Label": [RAW, "Spouse (Wife/Husband)"],
        "Preferred Label": [CANON, CANON],
    })


def _code_label_table():
    """GhanaLSS-style ``| Code | Label |``: documentation, not a mapping."""
    return pd.DataFrame({"Code": [1, 2], "Label": ["Head", "Spouse"]})


def test_fixture_labels_are_what_the_tests_assume():
    kin = _load_kinship_map()
    assert RAW not in kin and RAW.title() not in kin, "RAW must be unknown to kinship.yml"
    assert kin[CANON] == CANON_TUPLE


def test_preferred_label_table_feeds_the_kinship_lookup():
    """(a) The canonical label reaches kinship: ``[0, 0, affinal]`` from RAW."""
    fake = _stub_finalize(_fake_country({"Relationship": _preferred_label_table()}))
    with warnings.catch_warnings():
        # An 'Unknown relationship labels' warning here would mean kinship saw RAW.
        warnings.simplefilter("error", UserWarning)
        out = fake._finalize_result(_roster([RAW, "Spouse (Wife/Husband)"]),
                                    {}, "household_roster")
    # The mapping REPLACES the raw label in the returned column ...
    assert list(out["Relationship"]) == [CANON, CANON]
    # ... and the kinship tuple is the canonical label's, for BOTH rows.
    assert list(out["Generation"]) == [0, 0]
    assert list(out["Distance"]) == [0, 0]
    assert list(out["Affinity"]) == ["affinal", "affinal"]


def test_old_order_would_not_have_resolved_the_raw_label():
    """Counterfactual, run explicitly: expansion BEFORE mapping leaves the tuple
    null and only relabels after the fact.  Pins *why* the order matters."""
    fake = _fake_country({"Relationship": _preferred_label_table()})
    with pytest.warns(UserWarning, match="Unknown relationship labels"):
        old = fake._apply_categorical_mappings(_expand_kinship(_roster([RAW])))
    assert old["Relationship"].iloc[0] == CANON          # relabelled ...
    assert pd.isna(old["Affinity"].iloc[0])              # ... but kinship never saw it
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        new = _expand_kinship(fake._apply_categorical_mappings(_roster([RAW])))
    assert (new["Generation"].iloc[0], new["Distance"].iloc[0],
            new["Affinity"].iloc[0]) == CANON_TUPLE


def test_code_label_table_without_preferred_label_changes_nothing():
    """(b) A ``| Code | Label |`` table (no ``Preferred Label``) is not a mapping:
    ``_build_replace_dict`` returns None and the result is byte-identical to
    having no table at all.  Lower-case name, as GhanaLSS spells it."""
    labels = ["Spouse", "Head", RAW]
    with_table = _stub_finalize(_fake_country({"relationship": _code_label_table()}))
    no_table = _stub_finalize(_fake_country({}))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)     # RAW is unknown either way
        a = with_table._finalize_result(_roster(labels), {}, "household_roster")
        b = no_table._finalize_result(_roster(labels), {}, "household_roster")
    pdt.assert_frame_equal(a, b, check_exact=True)
    assert list(a["Relationship"]) == labels             # raw labels untouched
    assert pd.isna(a["Affinity"].iloc[2])                # RAW still unknown


def test_comparable_variant_coarsens_the_label_and_the_tuple_follows():
    """``labels={'Relationship': 'Comparable'}`` selects a second column of
    the same table (GH #682); under the post-#797 order the SELECTED label
    feeds kinship, so the coarse view's tuples are the coarse label's."""
    table = pd.DataFrame({
        "Original Label":   ["Sibling", "Father/mother in-law", "Tenant"],
        "Preferred Label":  ["Sibling", "Parent-in-law", "Tenant"],
        "Comparable Label": ["Other relative", "Parent", "Non-relative"],
    })
    fake = _stub_finalize(_fake_country({"Relationship": table}))
    raw = ["Sibling", "Father/mother in-law", "Tenant"]
    default = fake._finalize_result(_roster(raw), {}, "household_roster")
    assert list(default["Relationship"]) == ["Sibling", "Parent-in-law", "Tenant"]
    assert list(default["Distance"]) == [1, 0, 0]
    assert list(default["Affinity"]) == ["consanguineal", "affinal", "guest"]
    coarse = fake._finalize_result(_roster(raw), {}, "household_roster",
                                   labels={"Relationship": "Comparable"})
    assert list(coarse["Relationship"]) == ["Other relative", "Parent", "Non-relative"]
    assert pd.isna(coarse["Generation"].iloc[0]) and pd.isna(coarse["Distance"].iloc[0])
    assert coarse["Affinity"].iloc[0] == "consanguineal"          # Other relative's cell
    assert (coarse["Generation"].iloc[1], coarse["Affinity"].iloc[1]) == (1, "consanguineal")
    assert coarse["Affinity"].iloc[2] == "unrelated"
