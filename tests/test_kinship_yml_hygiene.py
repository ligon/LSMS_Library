"""kinship.yml may not carry a key the lookup can never reach (GH #945).

``_expand_kinship`` resolves a label in exactly two steps::

    mapped = stripped.str.title().map(kinship).fillna(stripped.map(kinship))

``str.title()`` is idempotent, so its output is always a title-cased string.
A key ``k`` that is **not** title-cased therefore can never be hit by the
first lookup; and if ``k.title()`` is *also* a key, then any row whose label
strips to ``k`` is already resolved by that first lookup, so ``fillna`` never
consults the raw map either.  Such a key is **unreachable by construction**,
whatever any survey emits.

70 such keys were removed in the commit that added this test.  The test is
the point of #945: the set regrew (149 -> 157 spelling variants between #797
and #945) because nothing rejected a variant at the moment it was added.
This turns the next one into a red run at the moment someone can fix it,
at zero runtime cost.

Deliberately NOT asserted here: that no two keys collide under ``.title()``
at all.  Four pairs do (``"LODGER/LODGER'S RELATIVE"`` vs
``"Lodger/Lodger's relative"``, and three like it) where *neither* member is
title-cased and the title-cased form is not itself a key -- both members are
reachable through the raw lookup, so removing either would change behaviour.
Which spelling is canonical is GH #813's decision, not this test's.
"""
from __future__ import annotations

import re
from pathlib import Path

from lsms_library import country
from lsms_library.country import _load_kinship_map


def test_every_kinship_key_is_a_string():
    kin = _load_kinship_map()
    assert [k for k in kin if not isinstance(k, str)] == []


def test_title_is_idempotent_so_the_shadowing_rule_is_exact():
    """The premise of the rule below: ``.title()`` output is a fixed point."""
    kin = _load_kinship_map()
    assert [k for k in kin if k.title().title() != k.title()] == []


def test_no_key_is_shadowed_by_a_title_cased_twin():
    kin = _load_kinship_map()
    keys = set(kin)
    offenders = {k: (kin[k], k.title(), kin[k.title()])
                 for k in kin if k != k.title() and k.title() in keys}
    assert not offenders, (
        "kinship.yml keys unreachable by construction: _expand_kinship looks up "
        "label.title() first, so a non-title-cased key whose title-cased twin is "
        "also a key is never consulted. Delete the shadowed spelling (its twin "
        "already answers for it) -- {key: (its tuple, twin, twin's tuple)}: "
        f"{offenders}"
    )


# ---------------------------------------------------------------------------
# GH #974 / #945: the file as TEXT, and the adjudicated-dead keys.
# ---------------------------------------------------------------------------

_KINSHIP_YML = Path(country.__file__).with_name("categorical_mapping") / "kinship.yml"
_ADJUDICATION = "slurm_logs/2026-09-18_kinship_813/ADJUDICATION.org"
_KEY_LINE = re.compile(r"^(?P<key>.*?):\s+\[")


def _key_lines():
    """``[(lineno, key)]`` for every mapping line, read as TEXT.

    ``yaml.safe_load`` keeps the LAST copy of a repeated key silently, so a
    duplicate can only be seen before the parser gets to it.
    """
    out = []
    for n, line in enumerate(_KINSHIP_YML.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = _KEY_LINE.match(line)
        assert m, f"kinship.yml:{n}: not a `key: [G, D, A]` line: {line!r}"
        key = m.group("key").strip()
        if key[0] in "\"'" and key[-1] == key[0]:
            key = key[1:-1]
        out.append((n, key))
    return out


def test_the_text_reader_sees_every_loaded_key():
    """Premise of the duplicate test: the regex and the parser agree."""
    assert {k for _, k in _key_lines()} == set(_load_kinship_map())


def test_no_key_line_is_duplicated():
    """Each key is written exactly once (GH #945, the free hygiene finding).

    Until 2026-10-14 the file had 418 key lines for 403 keys: 11 keys twice
    and ``Father/Mother`` / ``Sister/Brother`` three times.  All copies agreed,
    so nothing was served wrongly -- but the first edit that changed one copy
    would have been silently overruled by whichever copy came last.
    """
    seen = {}
    dups = {}
    for n, k in _key_lines():
        if k in seen:
            dups.setdefault(k, [seen[k]]).append(n)
        else:
            seen[k] = n
    assert not dups, (
        "kinship.yml writes these keys more than once; yaml.safe_load keeps "
        f"only the LAST copy, silently. Delete the extra lines -- {{key: lines}}: {dups}"
    )


def _fold(label: str) -> str:
    """Spelling-insensitive form: case, whitespace and punctuation ignored.

    This is NOT the lookup's normalisation (``_expand_kinship`` matches
    ``label.strip().title()`` and then the raw label, exactly).  It is only
    the equivalence under which two keys are *the same word* -- which is
    what makes a tuple disagreement between them a contradiction.
    """
    return re.sub(r"[\W_]+", "", label.casefold())


def test_spelling_variants_of_one_key_agree_on_their_tuple():
    """Variants may coexist (the lookup is exact-match, and every spelling a
    survey emits must stay a key -- GH #945's canonical-form rule is still
    open); but two spellings of ONE word may not decompose differently."""
    kin = _load_kinship_map()
    groups = {}
    for k, v in kin.items():
        groups.setdefault(_fold(k), {})[k] = tuple(v)
    disagree = {f: g for f, g in groups.items() if len(set(g.values())) > 1}
    assert not disagree, (
        "spelling variants of one kinship label carry different tuples; a "
        "canonicalisation must not change a decomposition (GH #698/#804 own "
        f"that), so fix the contradiction first: {disagree}"
    )


#: Keys the 2026-09-18 adjudication ruled dead for an ORTHOGRAPHIC reason
#: (accent-stripped / re-spaced guesses at a form a survey emits, source
#: typos from a decode path that no longer exists, or a phrase no held
#: instrument uses).  Deleted on 2026-10-14 (GH #974).  Re-adding one
#: re-opens the door GH #801 closed: a future encoding regression would
#: resolve silently through the guess instead of warning.
ADJUDICATED_DEAD = frozenset({
    # row 3 -- ASCII / spacing twins of emitted EHCVM labels
    "Conjoint (E)", "Conjoint( E )", "Pere, Mere", "Frere, Soeur",
    "Personne Non Apparentee Au Cm/Conjoint", "Personne Non Apparentee Au Cm",
    # row 4 -- EthiopiaRHS 1999 value-label typos; the wave now decodes by code
    "Other reative", "Other unrelated", "Tenant/Broader",
    # row 5 -- a phrase from an instrument the corpus does not hold
    "Nurse, Assistant Girl",
    # row 6 -- short forms of the Tajikistan pair GH #800 removed
    "father/mother of husband", "father/mother of wife",
    # row 7 -- spacing twin of Uganda's `Son / Daughter Of Head Or Spouse`
    "Son/Daughter Of Head Or Spouse",
    # row 8 -- the bare form; every wave emits `.../Conjoint`
    "Autres Parents Du Cm",
    # row 9 -- the ACCENTED bare form (row 3's is the unaccented one)
    "Personne Non Apparentée Au Cm",
})


def test_adjudicated_dead_keys_stay_out():
    assert len(ADJUDICATED_DEAD) == 15
    back = ADJUDICATED_DEAD & set(_load_kinship_map())
    assert not back, (
        f"{sorted(back)} were ruled dead in {_ADJUDICATION} and deleted under "
        "GH #974; if a survey really emits one now, say which in the commit "
        "and remove it from ADJUDICATED_DEAD"
    )
