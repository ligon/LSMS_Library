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
