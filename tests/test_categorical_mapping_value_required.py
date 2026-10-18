"""GH #674: a bare ``get_categorical_mapping(tablename=...)`` must RAISE.

Without a value-column keyword ``df_data_grabber`` keeps only the key, so the
call used to return ``{}`` (or an empty frame with ``asdict=False``) and every
later lookup missed -- GhanaLSS "Trap 1" (``GhanaLSS/_/CONTENTS.org``), which
shipped 100%-null columns three times.  GH #953 widened it: a name held only at
the cross-country level now resolves, so a bare call against it returned ``{}``
too.

Temp org files and a temp ``.dta`` only -- no microdata, CI-safe.
"""
from __future__ import annotations

import textwrap

import pandas as pd
import pytest

from lsms_library.local_tools import code_label_map, get_categorical_mapping

ORG = textwrap.dedent(
    """\
    #+name: region
    | Code | Label   |
    |------+---------|
    |    1 | Western |
    |    2 | Central |

    #+name: empty
    | Code | Label |
    |------+-------|
    """
)


@pytest.fixture()
def level(tmp_path):
    (tmp_path / "categorical_mapping.org").write_text(ORG, encoding="utf-8")
    return [str(tmp_path) + "/"]


@pytest.mark.parametrize("asdict", [True, False])
def test_bare_call_raises(level, asdict):
    with pytest.raises(TypeError, match="code_label_map"):
        get_categorical_mapping(tablename="region", dirs=level, asdict=asdict)


@pytest.mark.parametrize("opt", ["encoding", "missing_ok", "convert_categoricals"])
def test_grabber_options_are_not_value_columns(level, opt):
    # df_data_grabber's own options are not myvars; passing one alone is
    # still a bare call.
    with pytest.raises(TypeError):
        get_categorical_mapping(tablename="region", dirs=level, **{opt: None})


def test_bare_call_raises_before_resolution(tmp_path):
    # Argument error first: no level holds 'nonesuch', yet the message is the
    # TypeError, not the KeyError a resolution failure would give.
    with pytest.raises(TypeError):
        get_categorical_mapping(tablename="nonesuch", dirs=[str(tmp_path) + "/"])


def test_explicit_value_column_unchanged(level):
    assert get_categorical_mapping(tablename="region", dirs=level,
                                   Label="Label") == {"1": "Western",
                                                      "2": "Central"}
    s = get_categorical_mapping(tablename="region", dirs=level,
                                Label="Label", asdict=False)
    assert isinstance(s, pd.Series) and s.to_dict() == {"1": "Western",
                                                        "2": "Central"}


def test_code_label_map_is_the_alternative(level):
    assert code_label_map("region", level)[1] == "Western"


def test_global_only_name_bare_raises(tmp_path):
    # 'u_kg' lives only in lsms_library/categorical_mapping/u.org; a local
    # level without it falls through to the global rung (GH #953).
    dirs = [str(tmp_path) + "/"]
    with pytest.raises(TypeError):
        get_categorical_mapping(tablename="u_kg", dirs=dirs)
    got = get_categorical_mapping(tablename="u_kg", dirs=dirs,
                                  idxvars="Unit", Kg="Kg")
    assert len(got) > 0


def test_zero_row_table_is_a_separate_case(level):
    # A declared-but-empty table is not a missing-argument error: with a value
    # column it never reaches the TypeError branch (it fails resolution, as
    # before -- df_data_grabber treats a zero-row table as absent).
    with pytest.raises(KeyError):
        get_categorical_mapping(tablename="empty", dirs=level, Label="Label")


def test_dta_form_unchanged(tmp_path):
    p = tmp_path / "f.dta"
    pd.DataFrame({"x": pd.Categorical(["a", "b", "a"])}).to_stata(
        p, write_index=False)
    allcats = get_categorical_mapping(str(p))
    assert set(allcats) == {"x"}
    assert sorted(get_categorical_mapping(str(p), "x").values()) == ["a", "b"]


ONE_ROW = "#+name: one\n| Code | Label |\n|------+-------|\n| 1 | Only |\n"


@pytest.mark.parametrize("asdict", [True, False])
def test_one_row_table_is_not_squeezed_to_a_scalar(tmp_path, asdict):
    (tmp_path / "categorical_mapping.org").write_text(ONE_ROW, encoding="utf-8")
    got = get_categorical_mapping(tablename="one", dirs=[str(tmp_path) + "/"],
                                  Label="Label", asdict=asdict)
    if asdict:
        assert got == {"1": "Only"}
    else:
        assert isinstance(got, pd.Series) and got.to_dict() == {"1": "Only"}
