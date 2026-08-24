"""The coverage matrix's feature axis, and the `undeclared` tier (GH #724).

`build_matrix` used to take its feature axis from each country's own
`data_scheme`, so a feature a country never declared produced NO ROW AT ALL.
GhanaSPS reported 21/21 = 100% `sane` with ~300 of its 328 source files
unwired.  These tests pin the fix and, more importantly, pin the property that
makes it safe: an `undeclared` cell must be CLOSEABLE with evidence, or the
change just manufactures hundreds of permanently-red cells.
"""
from __future__ import annotations

import pandas as pd
import pytest

from lsms_library import coverage_matrix as cm


# --- the vocabulary itself ------------------------------------------------

def test_vocabulary_loads_and_is_nonempty():
    vocab = cm.feature_vocabulary()
    assert vocab, "Feature Vocabulary missing from data_info.yml"
    assert len(vocab) == len(set(vocab)), "duplicate entries"


def test_vocabulary_excludes_derived_and_country_level():
    """A derived feature is its source restated; grading both double-counts."""
    from lsms_library.country import JSON_CACHE_METHODS
    from lsms_library.feature import _DERIVED_SOURCE
    vocab = set(cm.feature_vocabulary())
    assert not (vocab & set(_DERIVED_SOURCE)), (
        f"derived features in the vocabulary: {sorted(vocab & set(_DERIVED_SOURCE))}")
    assert not (vocab & set(JSON_CACHE_METHODS)), (
        f"country-level-only features in the vocabulary: "
        f"{sorted(vocab & set(JSON_CACHE_METHODS))}")


def test_vocabulary_is_in_both_tier_lists():
    assert "undeclared" in cm.TIER_ORDER
    assert "undeclared" in cm.ROLLUP_PRIORITY, (
        "ROLLUP_PRIORITY is a SECOND list and is easy to forget; a tier missing "
        "from it breaks the grid rollup rather than failing loudly")


# --- the ratchet ----------------------------------------------------------

@pytest.mark.slow
def test_every_declared_source_feature_is_in_the_vocabulary_or_is_a_known_local_table():
    """Growth must be loud: a NEW cross-country feature forces a vocabulary edit.

    Deliberately not 'every declared feature is in the vocabulary' -- the tail
    of one- and two-country helper tables (Tanzania `community_cluster_xwalk`,
    EthiopiaRHS `hhsize`, Uganda `fct`) is legitimately excluded.  The ratchet
    is on features declared by >= 3 countries, which is what the vocabulary was
    measured from.
    """
    from collections import Counter

    import lsms_library as ll
    from lsms_library import catalog
    from lsms_library.country import JSON_CACHE_METHODS
    from lsms_library.feature import _DERIVED_SOURCE

    counts: Counter = Counter()
    for c in catalog.countries():
        try:
            counts.update(set(ll.Country(c, preload_panel_ids=False).data_scheme))
        except Exception:                                    # noqa: BLE001
            continue
    skip = set(_DERIVED_SOURCE) | set(JSON_CACHE_METHODS)
    widespread = {f for f, n in counts.items() if n >= 3 and f not in skip}
    missing = widespread - set(cm.feature_vocabulary())
    assert not missing, (
        f"features declared by >=3 countries but absent from `Feature "
        f"Vocabulary` in data_info.yml: {sorted(missing)}.  Add them (and say "
        f"why in the block comment) -- the matrix cannot grade what it does "
        f"not know about.")


# --- the load-bearing property: undeclared cells can be CLOSED ------------

def test_undeclared_cell_is_closeable_by_a_blank_wave_verdict():
    """#724's whole argument: the closing machinery already exists.

    If a blank-wave verdict could not close an `undeclared` cell, the change
    would create ~400 permanently-red cells and the noise objection would be
    correct.
    """
    verdicts = {("Armenia", "plot_features", ""): {
        "verdict": "not-asked",
        "evidence": "ILCS has no agriculture module (catalog 2001-2018)"}}
    tier, detail = cm._absent_tier(
        "Armenia", "plot_features", None, verdicts,
        default_tier="undeclared", default_detail="not declared ...")
    assert tier == "not-asked", tier
    assert "no agriculture module" in detail


def test_todo_verdict_leaves_an_undeclared_cell_OPEN_and_undeclared():
    """`todo` is not a close -- and must not silently relabel the cell `absent`.

    `absent` means 'declared for other waves, missing here'.  Saying that about
    a feature the country never declared would be a false statement about the
    config.
    """
    verdicts = {("GhanaSPS", "housing", ""): {
        "verdict": "todo",
        "evidence": "12a_housingcharacteristics1iii_structure.dta present"}}
    tier, detail = cm._absent_tier(
        "GhanaSPS", "housing", None, verdicts,
        default_tier="undeclared", default_detail="not declared ...")
    assert tier == "undeclared", tier
    assert "12a_housing" in detail, "evidence must carry forward"


def test_unadjudicated_undeclared_cell_stays_undeclared():
    tier, detail = cm._absent_tier(
        "GhanaSPS", "housing", None, {},
        default_tier="undeclared", default_detail="not declared by this country")
    assert tier == "undeclared"
    assert detail == "not declared by this country"


def test_absent_default_is_unchanged_by_the_generalisation():
    """Negative control: the pre-#724 caller must behave exactly as before."""
    tier, detail = cm._absent_tier("Albania", "assets", "2003", {})
    assert tier == "absent"
    assert detail == "source not declared for wave"


# --- the ghost-row property ----------------------------------------------

def _snap(tmp_path, rows):
    p = tmp_path / "latest.csv"
    pd.DataFrame(rows, columns=cm.COLUMNS).to_csv(p, index=False)
    return p


def test_authoritative_run_removes_a_stale_undeclared_row(tmp_path):
    """Declare-then-regrade must not leave the old `undeclared` row behind.

    Upsert alone only ever adds or overwrites, so a row whose KEY stops being
    emitted survives for ever -- and `(c, feature, "")` -> `(c, feature, wave)`
    is exactly a key change.
    """
    path = _snap(tmp_path, [
        {"country": "GhanaSPS", "feature": "housing", "wave": "",
         "tier": "undeclared", "coverage": "absent", "n_rows": "", "detail": "x"},
        {"country": "Uganda", "feature": "housing", "wave": "2009-10",
         "tier": "sane", "coverage": "declared", "n_rows": "1", "detail": ""},
    ])
    fresh = pd.DataFrame([
        {"country": "GhanaSPS", "feature": "housing", "wave": "2013-14",
         "tier": "sane", "coverage": "declared", "n_rows": 5, "detail": ""},
    ], columns=cm.COLUMNS)
    cm.save_snapshot(fresh, path, authoritative_countries=["GhanaSPS"])
    out = pd.read_csv(path, dtype=str, keep_default_na=False)

    ghosts = out[(out.country == "GhanaSPS") & (out.tier == "undeclared")]
    assert ghosts.empty, f"stale undeclared row survived:\n{ghosts}"
    assert ((out.country == "GhanaSPS") & (out.wave == "2013-14")).any()
    # and an untouched country is not collateral damage
    assert ((out.country == "Uganda") & (out.tier == "sane")).any(), \
        "a country the run did not grade must be preserved"


def test_non_authoritative_run_is_purely_additive(tmp_path):
    """A feature-scoped run knows nothing about features it skipped."""
    path = _snap(tmp_path, [
        {"country": "GhanaSPS", "feature": "housing", "wave": "",
         "tier": "undeclared", "coverage": "absent", "n_rows": "", "detail": "x"},
    ])
    fresh = pd.DataFrame([
        {"country": "GhanaSPS", "feature": "sample", "wave": "2013-14",
         "tier": "sane", "coverage": "declared", "n_rows": 5, "detail": ""},
    ], columns=cm.COLUMNS)
    cm.save_snapshot(fresh, path, authoritative_countries=None)
    out = pd.read_csv(path, dtype=str, keep_default_na=False)
    assert ((out.feature == "housing") & (out.tier == "undeclared")).any(), \
        "a scoped run must NOT delete rows it had no opinion about"
