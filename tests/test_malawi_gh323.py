"""GH #323 (Malawi): the silent groupby().first() collapse of a non-unique index.

cluster_features is EXTRACTED at household grain (t, v, i) but DECLARED at
(t, v), so `_normalize_dataframe_index` reduces it with groupby().first().  A
reduction like that is invisible -- it is baked into the L2 cache on the first
(cold) build, and the GH #323 RuntimeWarning never fires again -- so it has to
be pinned by tests rather than trusted.

The tests below are the enforcement for four defects that were live in Malawi:

  1. 2004-05 keyed `v` on `ea`, a within-TA sequence with only 110 distinct
     values, not a cluster.  66 of those buckets straddled a REGION, so
     .first() handed 9,940 households an arbitrary (often wrong-region)
     cluster -- and because `sample` owns `v`, that wrong cluster propagated to
     every household-level table via _join_v_from_sample.
  2. 2004-05 shocks read Stata LABELS for ab02, where codes 117 and 118 BOTH
     label as "Other" -- merging the questionnaire's two distinct
     "Other (specify)" slots and making (t, i, Shock) non-unique for 11,077
     rows, of which 10,831 were pure label artefact.
  3. 2010-11 / 2019-20 merged a HOUSEHOLD-grain geo file on `v`, a within-EA
     cartesian product that fabricated ~355k rows.
  4. 2016-17 merged its geo file on the RAW case_id while df_main had rewritten
     case_id to 'cs-17-...', so the merge matched NOTHING: 100% of that wave's
     geovariables were dropped and 12,447 phantom NaN rows were created.

`test_cluster_attributes_constant_within_cluster` is the class-level guard: it
asserts the property that makes the (t, v) collapse SAFE, rather than any one
of the four bugs.  It fails on every one of them.
"""
from __future__ import annotations

import importlib.util
import warnings
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
_COUNTRY = _ROOT / "lsms_library" / "countries" / "Malawi"

WAVES = ["2004-05", "2010-11", "2013-14", "2016-17", "2019-20"]

# Households per wave -- what cluster_features must extract.  Anything much
# larger means a merge has gone cartesian again.
HOUSEHOLDS = {
    "2004-05": 11_280,
    "2010-11": 12_271,
    "2013-14": 4_000,
    "2016-17": 14_955,   # 12,447 cross-sectional + 2,508 panel
    "2019-20": 14_612,   # 11,434 cross-sectional + 3,178 panel
}


def _country():
    """A Malawi Country, or skip when the microdata is not reachable here."""
    try:
        import lsms_library as ll
        c = ll.Country("Malawi")
        c.waves
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Malawi data not available: {exc}")
    return c


def _grab(country, wave, table):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = country[wave].grab_data(table)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"cannot build Malawi/{wave}/{table}: {exc}")
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        pytest.skip(f"Malawi/{wave}/{table} empty here")
    return df.reset_index()


def _malawi_module():
    spec = importlib.util.spec_from_file_location(
        "malawi_gh323", _COUNTRY / "_" / "malawi.py")
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"malawi.py not importable here: {exc}")
    return mod


# --------------------------------------------------------------------------
# THE CLASS-LEVEL INVARIANT
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wave", WAVES)
def test_cluster_attributes_constant_within_cluster(wave):
    """Every cluster_features attribute must be single-valued inside its cluster.

    This is what makes the declared (t, v) index safe to collapse.  Where it
    does NOT hold, groupby().first() publishes one arbitrary household's value
    as the cluster's -- silently, and permanently once cached.

    Fails pre-fix on 2004-05 (Region 66 / District 79 / Rural 19 clusters),
    2013-14 (93 / 165 / 131 + 188 on lat & lon), 2016-17 (Rural 75, lat/lon 7)
    and 2019-20 (66 / 98 / 86).
    """
    flat = _grab(_country(), wave, "cluster_features")
    assert "v" in flat.columns, f"{wave}: cluster_features has no `v`"

    payload = [c for c in flat.columns if c not in ("t", "v", "i")]
    assert payload, f"{wave}: cluster_features carries no attributes"

    offenders = {}
    grouped = flat.groupby("v", dropna=False)
    for col in payload:
        n_bad = int((grouped[col].nunique(dropna=True) > 1).sum())
        if n_bad:
            offenders[col] = n_bad

    assert not offenders, (
        f"{wave}: cluster_features attributes disagree inside a cluster "
        f"{offenders} -- groupby(['t','v']).first() would silently publish one "
        f"arbitrary household's value as the whole cluster's (GH #323)."
    )


@pytest.mark.parametrize("wave", WAVES)
def test_cluster_features_is_not_cartesian(wave):
    """cluster_features must carry one row per household, not a merge blow-up.

    Pre-fix, 2010-11 merged a household-grain geo file on `v` and produced
    196,083 rows for 12,271 households (sum of n_hh_per_ea^2); 2019-20 produced
    185,842 for 14,612; and 2016-17's failed merge left 27,402 for 14,955.
    """
    flat = _grab(_country(), wave, "cluster_features")
    assert len(flat) == HOUSEHOLDS[wave], (
        f"{wave}: cluster_features has {len(flat):,} rows for "
        f"{HOUSEHOLDS[wave]:,} households -- a merge has gone cartesian, or "
        f"unmatched rows are being fabricated (GH #323)."
    )
    # A failed merge shows up as phantom NaN-key rows (GH #606).
    assert flat["v"].notna().all(), f"{wave}: cluster_features has NaN `v` rows"
    if "i" in flat.columns:
        assert flat["i"].notna().all(), f"{wave}: cluster_features has NaN `i` rows"


# --------------------------------------------------------------------------
# 1. the 2004-05 cluster key
# --------------------------------------------------------------------------

def test_2004_cluster_key_is_psu_not_ea():
    """IHS2 has 564 EAs of exactly 20 households, not 110 `ea` buckets."""
    c = _country()
    flat = _grab(c, "2004-05", "cluster_features")
    n_v = flat["v"].nunique()
    assert n_v == 564, (
        f"2004-05 cluster_features has {n_v} clusters; IHS2 has 564 "
        f"(`ea` is a within-TA sequence with only 110 values -- use `psu`)."
    )
    sizes = flat.groupby("v").size()
    assert set(sizes.unique()) == {20}, (
        f"2004-05: IHS2 samples exactly 20 households per EA; got {sorted(sizes.unique())}"
    )


def test_2004_sample_v_matches_cluster_features():
    """`sample` owns `v` and feeds every table via _join_v_from_sample.

    If it kept the broken `ea` key, the corrupt cluster -- and the wrong
    Region/District attached to it -- would leak into every Malawi 2004-05
    table, whatever cluster_features said.
    """
    c = _country()
    smp = _grab(c, "2004-05", "sample")
    assert smp["v"].nunique() == 564, (
        f"2004-05 sample has {smp['v'].nunique()} clusters, not 564 -- the "
        f"broken `ea` key is still leaking into every household-level table."
    )
    cf = _grab(c, "2004-05", "cluster_features")
    assert set(smp["v"]) == set(cf["v"]), (
        "2004-05: sample and cluster_features disagree about the cluster keyspace"
    )


# --------------------------------------------------------------------------
# 2. the 2004-05 shocks label collision
# --------------------------------------------------------------------------

def test_2004_shocks_other_slots_stay_distinct():
    """ab02 codes 117 and 118 both label "Other"; they must not be merged."""
    flat = _grab(_country(), "2004-05", "shocks")
    labels = set(flat["Shock"].dropna().unique())
    assert {"Other (specify) 1", "Other (specify) 2"} <= labels, (
        "2004-05 shocks: the questionnaire's two 'Other (specify)' roster slots "
        "(ab02 = 117 and 118) have been collapsed into a single 'Other' label. "
        "That makes (t, i, Shock) non-unique for 11,077 rows, which the "
        "framework then drops silently (GH #323)."
    )
    assert len(labels) == 19, f"2004-05 shocks: expected 19 shock codes, got {len(labels)}"

    dups = int(flat.duplicated(subset=["i", "Shock"]).sum())
    # 246 genuine (household, shock-code) repeats survive in the source itself;
    # the other 10,831 were pure label artefact and must be gone.
    assert dups <= 246, (
        f"2004-05 shocks: {dups} duplicate (i, Shock) rows -- the 117/118 label "
        f"collision is back (it accounted for 10,831 of the original 11,077)."
    )


def test_shock_label_map_still_matches_the_source():
    """Pin the transcribed code->label maps to the .dta they were read from.

    The premise of the fix -- that 117 and 118 really do share a label -- is
    itself asserted here, so if a re-release of the source ever fixes the
    collision, this test says so instead of the fix quietly becoming a no-op.
    """
    from lsms_library.local_tools import get_dataframe
    import os

    mod = importlib.util.spec_from_file_location(
        "mw04", _COUNTRY / "2004-05" / "_" / "mapping.py")
    m = importlib.util.module_from_spec(mod)
    try:
        mod.loader.exec_module(m)
        cwd = os.getcwd()
        os.chdir(_COUNTRY / "2004-05" / "_")
        try:
            lab = get_dataframe("../Data/sec_ab.dta")
            raw = get_dataframe("../Data/sec_ab.dta", convert_categoricals=False)
        finally:
            os.chdir(cwd)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"2004-05 sec_ab.dta not reachable: {exc}")

    pairs = pd.DataFrame({"code": raw["ab02"], "label": lab["ab02"].astype(str)})
    pairs = pairs.dropna().drop_duplicates()
    pairs = pairs[pairs["label"] != "nan"]

    # the collision that motivates the whole fix
    by_label = pairs.groupby("label")["code"].nunique()
    assert (by_label > 1).any(), (
        "sec_ab.dta no longer has colliding ab02 labels -- re-check the fix"
    )

    # our transcription must still name every code the source carries
    for code in pairs["code"].unique():
        assert int(code) in m._SHOCK, f"ab02 code {code} missing from _SHOCK"
    # and must keep the two Other slots apart
    assert m._SHOCK[117] != m._SHOCK[118]


# --------------------------------------------------------------------------
# 3. the EA-code decoder (2013-14 / 2016-17 / 2019-20 movers)
# --------------------------------------------------------------------------

def test_ea_code_decoder_matches_the_source():
    """`_EA3_DISTRICT` / `_EA1_REGION` must still agree with the microdata.

    They are the baseline geography used wherever the IHPS panel's mover-
    contaminated region/district columns cannot be trusted, so a silent drift
    here would put clusters in the wrong district.
    """
    from lsms_library.local_tools import get_dataframe
    import os

    m = _malawi_module()
    cwd = os.getcwd()
    os.chdir(_COUNTRY / "2019-20" / "_")
    try:
        d = get_dataframe("../Data/Cross_Sectional/hh_mod_a_filt.dta")
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"2019-20 cross-section not reachable: {exc}")
    finally:
        os.chdir(cwd)

    ea = d["ea_id"].astype(str)
    for pre, dist in zip(ea.str[:3], d["district"].astype(str)):
        assert m._EA3_DISTRICT.get(pre) == dist, (
            f"EA prefix {pre} decodes to {m._EA3_DISTRICT.get(pre)!r} but the "
            f"2019-20 cross-section says {dist!r}"
        )
    for pre, reg in zip(ea.str[:1], d["region"].astype(str)):
        assert m._EA1_REGION.get(pre) == reg, (
            f"EA prefix {pre} decodes to {m._EA1_REGION.get(pre)!r} but the "
            f"2019-20 cross-section says {reg!r}"
        )


def test_2016_geovariables_actually_merge():
    """The cs_i keyspace fix: 2016-17 lat/lon must not be empty.

    Pre-fix, df_geo keyed on the raw case_id while df_main had rewritten it to
    'cs-17-...', so Latitude was non-null on 0 of 14,955 rows.
    """
    flat = _grab(_country(), "2016-17", "cluster_features")
    assert "Latitude" in flat.columns

    # Count only rows that are REAL HOUSEHOLDS (a `v`).  A bare notna() count
    # over the whole frame passes on the broken config: the failed merge parks
    # every lat/lon on a phantom NaN-`v`/NaN-`i` row, so the coordinates are
    # "present" while being attached to nothing.  Pre-fix this is 0 of 14,955.
    real = flat[flat["v"].notna()]
    n = int(real["Latitude"].notna().sum())
    assert n > 10_000, (
        f"2016-17: Latitude is non-null on only {n} of the {len(real):,} rows "
        f"that have a cluster -- df_geo is keyed on the raw case_id while "
        f"df_main rewrites it to 'cs-17-...', so the merge matches nothing and "
        f"the whole wave's geovariables are lost (GH #323/#606)."
    )
