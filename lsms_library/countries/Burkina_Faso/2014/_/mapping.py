"""Formatting functions for Burkina Faso 2014 (EMC).

``food_acquired`` is NOT built here.  It moved to the SCRIPT path
(``2014/_/food_acquired.py``) under GH #323: the wave's four
``emc2014_p{1..4}_conso7jours`` files are four quarterly PASSAGES over the
same households, and a YAML ``file:`` list cannot give them the distinct
``visit`` index level they need.  The old
``food_acquired_to_canonical as food_acquired`` post-processor registered
here is therefore gone -- see the script's docstring for the full story.
"""

import pandas as pd

from lsms_library.local_tools import format_id
from lsms_library.transformations import reduce_to_agreed


def strata(x):
    return format_id(x)


def cluster_features(df):
    """Collapse the household-level cover page to ONE row per cluster.

    GH #323.  ``cluster_features`` is declared at cluster grain ``(t, v)``, but
    the source (``emc2014_p1_logement``) is one row per HOUSEHOLD, so the frame
    arriving here carries 10,800 rows on 900 clusters.  The framework's
    duplicate-index reducer used to collapse that 10,800 -> 900 via
    ``groupby().first()`` -- silently, and with an arbitrary winner.

    The collapse is correct in INTENT (the cluster attributes really are meant
    to be redundant copies), but it must be DECLARED rather than left to a
    silent framework fallback, and the reducer has to be honest about the cases
    where the copies do NOT agree:

      Region   constant within zd for 900/900 clusters -> collapses cleanly
      Rural    constant within zd for 900/900 clusters -> collapses cleanly
      District (province) VARIES within zd for 94 of 900 clusters (10.4%)

    Those 94 are real: e.g. zd=9 holds 7 households in province BALE and 5 in
    TUY -- both inside the Boucle du Mouhoun region.  Region and rural status
    never disagree, so ``v = zd`` is a sound cluster id; it is only the province
    that straddles.  ``first()`` picked one of the two provinces arbitrarily.
    There is no way to know which is right, so ``reduce_to_agreed`` sets
    District to NA for exactly those clusters: the ambiguity stays visible and
    countable instead of being resolved by a coin flip.
    """
    return reduce_to_agreed(df)


def shocks(df):
    """Dedup the two households whose entire shock roster was entered twice.

    GH #323.  Exactly two households -- (zd=47, menage=5) and (zd=556,
    menage=13) -- have their whole 19-row shock roster recorded TWICE in
    ``emc2014_p3_chocs`` (38 rows each against the modal 19; the other 10,219
    households are clean).  That made the declared ``(t, i, Shock)`` index
    non-unique, so the framework collapsed it with ``groupby().first()`` and
    (because the index was non-unique) ALSO silently deleted this wave's 9
    NaN-key shock rows via the groupby's ``dropna=True`` default.

    Deduping here removes the trigger: 37 of the 38 duplicate rows are
    byte-identical re-entries, so dropping them loses nothing.  The 38th pair
    -- (47, 5) x 'Sécheresse/Pluies irrégulières' -- is one ALL-NULL row plus
    one populated row, so ``_agree_or_na`` keeps the populated one.

    A NOTE ON WHAT IS *NOT* WRONG HERE, because it is easy to talk yourself
    into it: in the RAW file that pair disagrees on CS1 (one entry says the
    household did not suffer the drought, the other says it did and records the
    impacts).  That looks like it should produce a ``first()`` chimera --
    ``first()`` is per-column first-NON-NULL rather than first-row, so it CAN
    synthesize a record present in no source row.  It does not here: CS1 is not
    part of this table's schema (the columns are the CS3*/CS4* impact and
    coping fields), and the not-suffered entry is therefore ALL-NULL in the
    extracted frame -- it contributes nothing to any column.  Both ``first()``
    and agree-or-NA return exactly the populated row.  The output was never a
    chimera; the CS1 contradiction simply is not representable in this schema.
    The real defect this closes is the duplicate index itself (and the NaN-key
    deletion it triggered).
    """
    idx = [n for n in df.index.names if n is not None]
    flat = df.reset_index()

    n_before = len(flat)
    flat = flat.drop_duplicates()
    n_exact = n_before - len(flat)

    dup = flat.duplicated(subset=idx, keep=False)
    n_conflict = int(dup.sum())
    if n_conflict:
        resolved = reduce_to_agreed(flat[dup].set_index(idx)).reset_index()
        flat = pd.concat([flat[~dup], resolved], ignore_index=True)

    if n_exact or n_conflict:
        print(f'Burkina_Faso 2014 shocks: dropped {n_exact} byte-identical '
              f'duplicate row(s); {n_conflict} conflicting row(s) reduced to '
              f'agree-or-NA.')

    return flat.set_index(idx)
