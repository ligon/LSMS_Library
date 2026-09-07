"""'Other relative' has its own kinship cell (decided 2026-09-07, GH #698 / #797).

Until then every "other / unspecified relative" label was filed on Head's
cell ``[0, 0, consanguineal]`` -- the tuple could not tell a head from an
unspecified relative -- and two of them sat on the cousin cell.  The
convention now:

* a plain "other relative" label -> ``[null, null, consanguineal]``: a blood
  relative whose generation and collateral distance the instrument does not
  record (partial null, the #698 shape);
* a label that names the SPOUSE's relatives too ("other relative of head or
  spouse", EHCVM "autres parents du CM ou du conjoint") -> ``[null, null,
  null]``: the instrument conflates blood and marriage, so affinity is
  unknown as well (Peru's ``Otro pariente`` precedent);
* Head, Cousin and every other cell are untouched.
"""
from __future__ import annotations

import pandas as pd
import pytest

from lsms_library.country import _expand_kinship, _load_kinship_map

PARTIAL = ["Other Relative", "Other relatives", "Autre parent", "OTHER RELATIVE (SPECIFY)",
           "13. Other Relative", "Other relative of head"]
FULL = ["Autres Parents Du Cm/Conjoint", "Other relative of head or spouse",
        "Outros parentes do CAF/cônjuge", "Otro Pariente"]


def _roster(labels):
    return pd.DataFrame({"Relationship": labels},
                        index=pd.Index([f"p{i}" for i in range(len(labels))], name="pid"))


@pytest.mark.parametrize("label", PARTIAL)
def test_other_relative_is_a_blood_relative_of_unknown_generation(label):
    out = _expand_kinship(_roster([label]))
    assert pd.isna(out["Generation"].iloc[0])
    assert pd.isna(out["Distance"].iloc[0])
    assert out["Affinity"].iloc[0] == "consanguineal"


@pytest.mark.parametrize("label", FULL)
def test_relative_of_head_or_spouse_is_fully_unknown(label):
    out = _expand_kinship(_roster([label]))
    assert out[["Generation", "Distance", "Affinity"]].isna().all(axis=None)


def test_head_and_cousin_keep_their_cells():
    out = _expand_kinship(_roster(["Head", "Cousin", "Chef de ménage"]))
    assert out["Generation"].tolist() == [0, 0, 0]
    assert out["Distance"].tolist() == [0, 2, 0]
    assert out["Affinity"].tolist() == ["consanguineal"] * 3


def test_no_unknown_label_warning_and_dtypes_survive_partial_nulls():
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        out = _expand_kinship(_roster(PARTIAL + FULL + ["Head"]))
    assert str(out["Generation"].dtype) == "Int64" and str(out["Distance"].dtype) == "Int64"
    assert out["Relationship"].notna().all()


def test_the_head_cell_holds_no_other_relative_key():
    """The collision this convention removes must not come back: no key whose
    label says 'other'/'autre'/'outros'/'relation' sits on Head's cell."""
    kin = _load_kinship_map()
    offenders = [k for k, v in kin.items()
                 if tuple(v) == (0, 0, "consanguineal")
                 and any(w in k.lower() for w in ("other", "autre", "outros", "relation ", "relatives", "family"))]
    assert offenders == [], offenders
