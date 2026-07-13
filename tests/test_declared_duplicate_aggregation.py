"""GH #323: a non-unique DECLARED index must never collapse SILENTLY.

_normalize_dataframe_index used to reduce any duplicate declared index with
groupby().first(), discarding the dropped rows.  For Mali that vaporises 32,026
people from household_roster; for Timor-Leste 2007-08 cluster_features the same
code path performs a *correct*, lossless de-duplication (4477 household rows of
a cover file -> 300 clusters).  The defect is that the two are indistinguishable.

The fix: a table may DECLARE the reduction in its data_scheme.yml --

    aggregation:
      on_duplicate_index: unique

-- which VERIFIES the payload is constant within each index group and RAISES if
it is not.  A declared+verified collapse is silent; an undeclared one still
warns; a declared collapse over an inconstant payload is a hard error rather
than a silent wrong answer.
"""
from __future__ import annotations

import re
import warnings

import pandas as pd
import pytest
import yaml

from lsms_library.country import _normalize_dataframe_index
from lsms_library.paths import countries_root

UNIQUE = {"index": "(t, v)", "aggregation": {"on_duplicate_index": "unique"}}


def _cover_page_frame():
    """Timor-Leste 2007-08 in miniature: household rows carrying a redundant
    copy of their cluster's attributes.  5 households, 2 clusters."""
    idx = pd.MultiIndex.from_tuples(
        [("2007-08", "c1"), ("2007-08", "c1"), ("2007-08", "c1"),
         ("2007-08", "c2"), ("2007-08", "c2")],
        names=["t", "v"],
    )
    return pd.DataFrame(
        {"Region": ["East", "East", "East", "West", "West"],
         "Rural": ["Rural", "Rural", "Rural", "Urban", "Urban"]},
        index=idx,
    )


def test_unique_reducer_collapses_constant_payload_losslessly():
    """The declared collapse yields one row per group, with the right values,
    and does NOT emit the #323 data-loss warning (it is not data loss)."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)  # any #323 warning -> failure
        out = _normalize_dataframe_index(_cover_page_frame(), UNIQUE, None,
                                         "cluster_features")

    assert len(out) == 2, "5 household rows -> 2 clusters"
    assert out.index.is_unique
    assert out.loc[("2007-08", "c1"), "Region"] == "East"
    assert out.loc[("2007-08", "c2"), "Region"] == "West"
    assert out.loc[("2007-08", "c1"), "Rural"] == "Rural"
    assert out.loc[("2007-08", "c2"), "Rural"] == "Urban"


def test_unique_reducer_raises_when_payload_is_not_constant():
    """THE POINT OF THE DECLARATION.  If a cluster carries two different
    Regions the collapse would have to CHOOSE -- .first() would do that
    silently and wrongly (class-1).  We raise instead (class-2, loud)."""
    df = _cover_page_frame()
    df.iloc[1, df.columns.get_loc("Region")] = "West"  # c1 now East *and* West

    with pytest.raises(ValueError, match=r"(?s)constant.*Region"):
        _normalize_dataframe_index(df, UNIQUE, None, "cluster_features")


def test_unique_reducer_coalesces_missing_values():
    """A group of (NaN, 'East') is UNAMBIGUOUS -- one observed value -- so it
    must NOT raise, and the observed value must survive.  Raising here would be
    a false alarm, and false alarms are how a check gets turned off."""
    df = _cover_page_frame()
    df.iloc[0, df.columns.get_loc("Region")] = None  # c1: (NaN, East, East)

    out = _normalize_dataframe_index(df, UNIQUE, None, "cluster_features")
    assert out.loc[("2007-08", "c1"), "Region"] == "East", "observed value must win over NaN"


def test_unique_reducer_keeps_an_all_missing_group_missing():
    """Missing stays missing (class-2), never fabricated."""
    df = _cover_page_frame()
    df.loc[("2007-08", "c2"), "Region"] = None  # c2 entirely unobserved

    out = _normalize_dataframe_index(df, UNIQUE, None, "cluster_features")
    assert pd.isna(out.loc[("2007-08", "c2"), "Region"])
    assert out.loc[("2007-08", "c1"), "Region"] == "East"


def test_undeclared_duplicate_index_still_warns():
    """Regression guard: the new escape hatch must not disarm #323 detection
    for every table that has NOT declared a policy (i.e. Mali must still be
    loud).  Same frame, no `aggregation:` block -> still warns."""
    with pytest.warns(RuntimeWarning, match="GH #323"):
        out = _normalize_dataframe_index(_cover_page_frame(), {"index": "(t, v)"},
                                         None, "cluster_features")
    assert len(out) == 2


def test_timor_leste_declares_the_aggregation_it_relies_on():
    """Timor-Leste's cluster_features MUST carry the declaration -- otherwise
    the 4477->300 collapse is back to being silent."""
    scheme = yaml.safe_load(
        re.sub(r"!make", "",
               (countries_root() / "Timor-Leste" / "_" / "data_scheme.yml").read_text())
    )["Data Scheme"]["cluster_features"]

    assert scheme.get("aggregation", {}).get("on_duplicate_index") == "unique", (
        "Timor-Leste cluster_features is built from the household cover page "
        "(basicvars.dta) and relies on a 4477->300 duplicate collapse; it must "
        "declare that collapse so it is machine-checked, not silent (GH #323)."
    )
