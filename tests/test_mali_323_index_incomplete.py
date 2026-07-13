"""Regression tests for GH #323 (Mali): a DECLARED index that does not identify
the row, silently collapsed by ``_normalize_dataframe_index``'s
``groupby().first()``.

Two independent defects, both of which silently DROPPED rows:

1. ``household_roster`` / ``individual_education`` (2014-15) declared
   ``pid: [grappe, menage, s01q]``.  In ``EACIIND_p1.dta``, ``s01q`` is
   "Code du répondant" -- the roster line of whoever ANSWERED section 1 on the
   household's behalf -- NOT the person the row is about ("Numero d'ordre" =
   ``s01q00``).  The declared index therefore mapped 37,175 people onto 5,149
   keys and 32,026 real people were collapsed away.

2. ``interview_date`` declared ``(t, v, i)``, making the ``visit`` level
   undeclared.  The EACI waves visit every household TWICE (passage 1 and 2,
   a median 124 days apart), so one genuine interview date per household was
   discarded: 3,804 rows in 2014-15 and 8,390 in 2017-18.

The cheap structural tests run everywhere.  The integration tests need the
microdata and skip cleanly without it.
"""
import ast

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import SchemeLoader

WAVE = "2014-15"


def _wave_data_info():
    p = countries_root() / "Mali" / WAVE / "_" / "data_info.yml"
    return yaml.load(p.read_text(), Loader=SchemeLoader) or {}


def _data_scheme():
    p = countries_root() / "Mali" / "_" / "data_scheme.yml"
    d = yaml.load(p.read_text(), Loader=SchemeLoader) or {}
    return d.get("Data Scheme") or {}


# --------------------------------------------------------------------------
# Structural: the declaration itself.  These fail on the pre-fix config.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("table", ["household_roster", "individual_education"])
def test_pid_is_the_person_not_the_respondent(table):
    """pid must be built from s01q00 ("Numero d'ordre"), never s01q
    ("Code du répondant" -- a household-level attribute)."""
    pid = (_wave_data_info()[table]["idxvars"]["pid"])
    assert "s01q00" in pid, f"Mali/{WAVE}/{table}: pid={pid} must key on s01q00"
    assert "s01q" not in pid, (
        f"Mali/{WAVE}/{table}: pid={pid} keys on s01q, which is the RESPONDENT's "
        "line number, not the person's -- it collapses ~9.8 people/HH onto ~1.35 keys"
    )


def test_interview_date_declares_visit():
    """The canonical interview_date index is (t, v, i, visit) -- per-visit is
    the thing to keep (GH #506).  Dropping `visit` throws away one of the two
    real EACI interview dates per household."""
    idx = str(_data_scheme()["interview_date"]["index"])
    assert "visit" in idx, (
        f"Mali interview_date index is {idx}; without `visit` the framework drops "
        "the level and collapses the two EACI passages to one date per household"
    )


def test_interview_date_visit_is_ground_truth_not_inferred():
    """EACICONTROLE_p1/_p2 carry the passage only in the FILENAME (identical
    column sets, no `passage` column), so each file must inject it as a literal.
    It must NOT be inferred from row order or by ranking Int_t (that mislabels
    the 66 households whose passage-2 date precedes their passage-1 date)."""
    files = _wave_data_info()["interview_date"]["file"]
    consts = {}
    for entry in files:
        assert isinstance(entry, dict), (
            f"interview_date file entry {entry!r} carries no per-file visit constant"
        )
        fn, overrides = next(iter(entry.items()))
        assert overrides.get("visit", {}).get("const") is not None, (
            f"{fn}: no `visit: {{const: ...}}` override"
        )
        consts[fn] = overrides["visit"]["const"]
    assert len(set(consts.values())) == len(consts) == 2, (
        f"expected two files with distinct visit constants, got {consts}"
    )

    # And the wave hook must not re-derive it.  Inspect the CODE, not the file
    # text: the docstring deliberately quotes the old rank-based line to explain
    # why it is gone, so a substring search over the source would match itself.
    src = (countries_root() / "Mali" / WAVE / "_" / "mapping.py").read_text()
    fn = next(
        node for node in ast.parse(src).body
        if isinstance(node, ast.FunctionDef) and node.name == "interview_date"
    )
    body = [n for n in fn.body if not (isinstance(n, ast.Expr)
                                       and isinstance(n.value, ast.Constant)
                                       and isinstance(n.value.value, str))]
    code = "\n".join(ast.unparse(n) for n in body)
    assert "rank" not in code, (
        "the 2014-15 interview_date hook must not synthesize `visit` by ranking "
        f"Int_t -- passage is recorded in the source and needs no inference:\n{code}"
    )
    assert "visit" not in code, (
        f"the 2014-15 interview_date hook must not derive `visit`:\n{code}"
    )


# --------------------------------------------------------------------------
# Integration: the rows actually come back.  Needs microdata.
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def mali():
    import lsms_library as ll
    return ll.Country("Mali")


def _table(country, name):
    try:
        df = getattr(country, name)()
    except Exception as exc:  # broad: skip when microdata/DVC is unavailable
        pytest.skip(f"Mali.{name}() could not be built ({type(exc).__name__})")
    if not isinstance(df, pd.DataFrame) or df.empty:
        pytest.skip(f"Mali.{name}() is empty (missing data)")
    return df.reset_index()


def test_roster_2014_15_keeps_every_person(mali):
    """37,175 people in EACIIND_p1.dta -> 37,175 rows.  Pre-fix: 5,149."""
    f = _table(mali, "household_roster")
    n = int((f["t"] == WAVE).sum())
    assert n == 37175, (
        f"Mali household_roster {WAVE} returned {n:,} rows; the source has 37,175 "
        "people (pre-fix this was 5,149 -- 32,026 silently collapsed)"
    )


def test_roster_2014_15_pid_is_unique(mali):
    """The declared (t, i, pid) index must actually identify a person."""
    f = _table(mali, "household_roster")
    w = f[f["t"] == WAVE]
    dups = int(w.duplicated(subset=["t", "i", "pid"]).sum())
    assert dups == 0, f"Mali household_roster {WAVE} has {dups:,} duplicate (t,i,pid)"


def test_roster_2014_15_household_size_is_sane(mali):
    """The collapse made mean household size ~1.35.  The truth is ~9.77."""
    f = _table(mali, "household_roster")
    w = f[f["t"] == WAVE]
    size = w.groupby("i", observed=True).size().mean()
    assert 9.0 < size < 10.5, (
        f"Mali {WAVE} mean household size is {size:.2f}; the source says 9.77 "
        "(37,175 people / 3,804 households)"
    )


def test_interview_date_keeps_both_eaci_visits(mali):
    """Every EACI household is interviewed twice; both dates must survive."""
    f = _table(mali, "interview_date")
    assert "visit" in f.columns, "interview_date lost its `visit` level"
    for wave, expected in [("2014-15", 7608), ("2017-18", 16780)]:
        n = int((f["t"] == wave).sum())
        assert n == expected, (
            f"Mali interview_date {wave} returned {n:,} rows, expected {expected:,} "
            "(2 passages x every household)"
        )
    w = f[f["t"] == WAVE]
    per_hh = w.groupby("i", observed=True).size()
    assert set(per_hh.unique()) == {2}, (
        f"expected exactly 2 visits per household in {WAVE}, saw {sorted(per_hh.unique())}"
    )
