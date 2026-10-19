"""A label that conflates ONLY the Affinity slot is ``[G, D, null]`` (GH #698).

Ruling, @ligon 2026-10-18: *"'X of head or spouse' and 'X or X-in-law'
labels become [G, D, null], Affinity only; the Guyana triple becomes
[null, null, null].  This is the same null-component rule as 2026-09-07."*

Two label forms, one defect -- the instrument names a determinate generation
and collateral distance but not whether the tie runs through blood or
marriage:

* **"X of head or spouse"** -- a child of the spouse who is not the head's
  child is a *stepchild*; a parent of the spouse is a *parent-in-law*.  The
  survey code does not say which, so Affinity is null.
* **"X or X-in-law"** -- blood relation and its in-law in one survey code
  (Peru ``Padre/Suegro``, Guyana ``Father/Mother/T-in-law/M-in-law``,
  Serbia 2007's sibling-or-sibling-in-law code).

Until this ruling every one of these served ``consanguineal`` -- the modal
guess, written as a fact.  The Guyana triple ``Bro/Sis/B-in-law/S-in-law/
Other relative`` also folds in "other relative", so Generation and Distance
are undetermined too: ``[null, null, null]``.

The labels below are the spellings the surveys actually SERVE (census of the
warm corpus, 2026-10-19), not the kinship.yml keys, so the lookup's
``.title()`` step is exercised exactly as it runs on a real roster.
"""
from __future__ import annotations

import re
import warnings

import pandas as pd
import pytest

from lsms_library.country import _expand_kinship, _load_kinship_map

# served label -> (kinship.yml key it resolves to, Generation, Distance,
#                  people in the warm corpus, where)
# Generation and Distance are their values BEFORE the ruling: the ruling
# moves Affinity only, and pinning G/D here is what proves it.
AFFINITY_ONLY = {
    # --- "X of head or spouse" ---
    "Son / daughter of head or spouse":
        ("Son / Daughter Of Head Or Spouse", -1, 0, 17_411, "Uganda 2018-19, 2019-20"),
    "Sister/Brother of head or spouse":
        ("Sister/Brother Of Head Or Spouse", 0, 1, 2_008, "Uganda 2009-10 .. 2015-16"),
    "sister/brother of head or spouse":
        ("Sister/Brother Of Head Or Spouse", 0, 1, 460, "Uganda 2005-06"),
    "Sister / brother of head or spouse":
        ("Sister / Brother Of Head Or Spouse", 0, 1, 363, "Uganda 2018-19, 2019-20"),
    "Parent Of Head Or Spouse":
        ("Parent Of Head Or Spouse", 1, 0, 3_161, "CotedIvoire 1985-89"),
    "Parent of head or spouse":
        ("Parent Of Head Or Spouse", 1, 0, 721, "Uganda 2009-10 .. 2019-20"),
    "parent of head or spouse":
        ("Parent Of Head Or Spouse", 1, 0, 96, "Uganda 2005-06"),
    "Child of the head / spouse":
        ("Child Of The Head / Spouse", -1, 0, 8_290, "Serbia and Montenegro 2002, 2003"),
    "Grandchild of the head / spouse":
        ("Grandchild Of The Head / Spouse", -2, 0, 1_966, "Serbia and Montenegro 2002, 2003"),
    "Parents of the head / spouse":
        ("Parents Of The Head / Spouse", 1, 0, 838, "Serbia and Montenegro 2002, 2003"),
    "child of head of household/spouse, partner":
        ("child of head of household/spouse, partner", -1, 0, 5_103, "Serbia 2007"),
    "grandson/daughter of head of household/spouse, partner":
        ("grandson/daughter of head of household/spouse, partner", -2, 0, 1_454, "Serbia 2007"),
    "parent of head of household/spouse, partner":
        ("parent of head of household/spouse, partner", 1, 0, 593, "Serbia 2007"),
    # --- "X or X-in-law" ---
    "brother/sister or brother/sister-in-law of head of household":
        ("brother/sister or brother/sister-in-law of head of household", 0, 1, 903, "Serbia 2007"),
    "Padre/Suegro":
        ("Padre/Suegro", 1, 0, 279, "Peru 1994"),
    "Father/Mother/T-in-law/M-in-law":
        ("Father/Mother/T-In-Law/M-In-Law", 1, 0, 136, "Guyana 1992"),
}

# The Guyana triple: sibling, sibling-in-law OR "other relative".
FULLY_UNKNOWN = {
    "Bro/Sis/B-in-law/S-in-law/Other relative":
        ("Bro/Sis/B-In-Law/S-In-Law/Other Relative", 459, "Guyana 1992"),
}

# Keys of the same family whose Affinity IS determinate -- they must keep it.
DETERMINATE = {
    "Son/Daug. In Law Of The Head/Spouse": (
        (-1, 0, "affinal"), "an in-law of the head OR of the spouse is affinal either way"),
    "Personne Non Apparentée Au Cm/Conjoint": (
        (0, 0, "unrelated"), "unrelated to the head AND to the spouse is a determinate claim"),
    "Personne Non Apparentée Au Cm Ou À La Conjointe": (
        (0, 0, "unrelated"), "same claim, Niger/Mali 2021-22 spelling"),
    "Personne Non Apparentée Au Cm Ni Au Conjoint": (
        (0, 0, "unrelated"), "'ni ... ni' -- neither; determinate"),
    "Pessoa não aparentada com o CAF/cônjuge": (
        (0, 0, "unrelated"), "Guinea-Bissau's spelling of the same determinate claim"),
    "Father/Mother Of The Wife Of Head": (
        (1, 0, "affinal"), "names the SPOUSE's parent only: a parent-in-law"),
    "Father/Mother Of The Husband Of Head": (
        (1, 0, "affinal"), "names the SPOUSE's parent only: a parent-in-law"),
    "Son/Daughter Of Head": (
        (-1, 0, "consanguineal"), "the head's own child; no 'or spouse'"),
}


def _roster(labels):
    return pd.DataFrame({"Relationship": list(labels)},
                        index=pd.Index([f"p{i}" for i in range(len(labels))], name="pid"))


def _expand(labels):
    with warnings.catch_warnings():
        # An unknown-label warning would mean the served spelling no longer
        # reaches its key -- a silent all-NA tuple, not this ruling's null.
        warnings.simplefilter("error", UserWarning)
        return _expand_kinship(_roster(labels))


@pytest.mark.parametrize("label", sorted(AFFINITY_ONLY))
def test_affinity_only_conflation_nulls_affinity_and_keeps_generation_and_distance(label):
    key, gen, dist, _n, _where = AFFINITY_ONLY[label]
    assert tuple(_load_kinship_map()[key]) == (gen, dist, None), key
    out = _expand([label])
    assert out["Generation"].iloc[0] == gen
    assert out["Distance"].iloc[0] == dist
    assert pd.isna(out["Affinity"].iloc[0]), (label, out["Affinity"].iloc[0])
    assert out["Relationship"].iloc[0] == label      # the survey's own label rides along


@pytest.mark.parametrize("label", sorted(FULLY_UNKNOWN))
def test_the_guyana_triple_is_fully_unknown(label):
    key, _n, _where = FULLY_UNKNOWN[label]
    assert tuple(_load_kinship_map()[key]) == (None, None, None)
    out = _expand([label])
    assert out[["Generation", "Distance", "Affinity"]].isna().all(axis=None)


@pytest.mark.parametrize("key", sorted(DETERMINATE))
def test_determinate_members_of_the_family_keep_their_affinity(key):
    expected, _why = DETERMINATE[key]
    assert tuple(_load_kinship_map()[key]) == expected


def test_dtypes_survive_the_partial_nulls():
    out = _expand(sorted(AFFINITY_ONLY) + sorted(FULLY_UNKNOWN) + ["Head"])
    assert str(out["Generation"].dtype) == "Int64"
    assert str(out["Distance"].dtype) == "Int64"
    assert str(out["Affinity"].dtype).startswith("string")
    assert out["Relationship"].notna().all()
    assert out["Affinity"].iloc[-1] == "consanguineal"       # Head untouched


# ---------------------------------------------------------------------------
# Structural guards: the NEXT such key must arrive null, not consanguineal.
# ---------------------------------------------------------------------------

# "X of head or spouse" in English, French and Portuguese: a relation word,
# then the head, then "or"/"/" and the spouse.
_HEAD_OR_SPOUSE = re.compile(
    r"\b(of|du|do|au|com o)\b.*\b(head|cm|caf)\b.*(\bor\b|/|\bou\b)\s*"
    r"(du |à la |the )?(spouse|conjoint|cônjuge)",
    re.IGNORECASE,
)
_INLAW = re.compile(r"in[- ]?law|inlaw|in-l\b|suegr|yerno|nuera|cuñad|beau|belle", re.IGNORECASE)
_DISTRIBUTIVE_TAIL = re.compile(r"(in[- ]?law|inlaw|in-l)\s*$", re.IGNORECASE)


def _mixes_blood_and_in_law(key: str) -> bool:
    """True when one survey code names a blood relation AND an in-law.

    Split on ``/``, ``,`` and ``or``/``ou``.  English ``Father/Mother-in-law``
    is a gendered PAIR of in-laws -- the suffix distributes over the slash --
    so a key whose ONLY in-law part is its last part, ending in the in-law
    suffix, is not a mix.  Anything else that has an in-law part beside a
    part with none is: ``Padre/Suegro`` (``Suegro`` is a word, it does not
    distribute), ``Father/Mother/T-in-law/M-in-law`` (two in-law parts),
    ``brother/sister or brother/sister-in-law of head of household`` (the
    in-law part does not END in the suffix).
    """
    parts = [p.strip(" -.") for p in re.split(r"/|,|\bor\b|\bou\b", key, flags=re.IGNORECASE)]
    parts = [p for p in parts if p and not p.isdigit()]
    inlaw = [i for i, p in enumerate(parts) if _INLAW.search(p)]
    if not inlaw or len(inlaw) == len(parts):
        return False
    distributive = inlaw == [len(parts) - 1] and _DISTRIBUTIVE_TAIL.search(parts[-1])
    return not distributive


def test_every_head_or_spouse_key_has_null_affinity_unless_determinate():
    kin = _load_kinship_map()
    hits = {k: v for k, v in kin.items() if _HEAD_OR_SPOUSE.search(k)}
    # The guard must actually see the family it guards.
    assert {"Son / Daughter Of Head Or Spouse", "Child Of The Head / Spouse",
            "parent of head of household/spouse, partner",
            "Autres Parents Du Cm/Conjoint"} <= set(hits)
    offenders = {k: v for k, v in hits.items()
                 if v[2] is not None and k not in DETERMINATE}
    assert offenders == {}, (
        "a label naming a relation 'of head OR spouse' does not say whether the "
        "tie runs through blood or marriage: file it [G, D, null] (GH #698), or "
        f"add it to DETERMINATE with the reason it is not conflated: {offenders}")


def test_every_blood_or_in_law_key_has_null_affinity_unless_determinate():
    kin = _load_kinship_map()
    hits = {k: v for k, v in kin.items() if _mixes_blood_and_in_law(k)}
    assert {"Padre/Suegro", "Father/Mother/T-In-Law/M-In-Law",
            "Bro/Sis/B-In-Law/S-In-Law/Other Relative",
            "brother/sister or brother/sister-in-law of head of household"} <= set(hits)
    # Gendered in-law pairs are NOT mixes -- the suffix distributes.
    assert not {"Father/Mother-In-Law", "Brother/Sister In Law", "Son- Or Daughter-In-Law",
                "Mother- Or Father-In-Law", "10. Father/month-in-Law",
                "Yerno/Nuera", "Beau-Frère/Belle-Soeur"} & set(hits)
    offenders = {k: v for k, v in hits.items()
                 if v[2] is not None and k not in DETERMINATE}
    assert offenders == {}, (
        "one survey code naming a blood relation AND an in-law: file it "
        f"[G, D, null] (GH #698), or add it to DETERMINATE with the reason: {offenders}")
