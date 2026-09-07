"""Formatting functions for Burkina Faso 2014 (EMC).

food_acquired canonical reshape (GH #169 / #107): the wave's
``data_info.yml`` extracts the wide form (Quantity-total, Produced-subset,
Expenditure); importing ``food_acquired_to_canonical as food_acquired``
below registers it as the ``mapping.py`` post-processor, so grab_data
reshapes the wide form into the canonical long form on the ``s``
(acquisition-source) axis — matching the 2018-19 / 2021-22 waves.  Before
this, 2014 emitted the legacy wide shape (a ``Produced`` column, no ``s``),
which leaked into ``Country('Burkina_Faso').food_acquired()`` and broke the
``food_quantities`` kg derivation (0% resolved).
"""

from lsms_library.local_tools import format_id
from lsms_library.transformations import food_acquired_to_canonical as food_acquired


def strata(x):
    return format_id(x)


# Relationship (B5) code 6 is stored in emc2014_p1_individu_27022015.dta
# as ``Fr\xe8re/s?ur`` -- a literal 0x3F where the oe ligature belongs
# (the rest of the label set is clean Latin-1: ``P\xe8re / m\xe8re``,
# ``Sans lien de parent\xe9``).  The ``?`` is in the file, so no
# ``encoding=`` choice recovers it (utf-8 fails, latin-1 and cp1252 both
# return the ``?``; measured 2026-09-07): a lossy conversion upstream of
# the published file.  The clean label keeps the EMC 2014 vocabulary's
# ``/`` (``Petit fils/fille``, ``P\xe8re / m\xe8re``) rather than the
# EHCVM comma form the 2018-19 / 2021-22 waves serve, and resolves in
# kinship.yml through the existing ``Fr\xe8re/S\u0153ur`` key.  Before this
# hook ``Fr\xe8re/s?ur`` was itself a kinship.yml key (2,599 rows); it is
# not any more (GH #801).  Whole-value replacement, never a substring.
_RELATIONSHIP_FIX = {'Fr\u00e8re/s?ur': 'Fr\u00e8re/s\u0153ur'}


def household_roster(df):
    """``household_roster`` post-processor: restore the one Relationship
    label the source file ships with a literal ``?`` (see
    ``_RELATIONSHIP_FIX``; GH #801).  No other transformation."""
    if 'Relationship' in df.columns:
        df['Relationship'] = df['Relationship'].replace(_RELATIONSHIP_FIX)
    return df
