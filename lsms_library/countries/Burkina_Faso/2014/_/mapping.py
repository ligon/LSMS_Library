"""Formatting functions for Burkina Faso 2014 (EMC).

food_acquired canonical reshape (GH #169 / #107): the wave's
``data_info.yml`` extracts the wide form (Quantity-total, Produced-subset,
Expenditure); the ``food_acquired`` post-processor below wraps
``food_acquired_to_canonical``, so grab_data reshapes the wide form into the
canonical long form on the ``s`` (acquisition-source) axis — matching the
2018-19 / 2021-22 waves.  Before this, 2014 emitted the legacy wide shape (a
``Produced`` column, no ``s``), which leaked into
``Country('Burkina_Faso').food_acquired()`` and broke the ``food_quantities``
kg derivation (0% resolved).
"""

import pandas as pd

from lsms_library.build_transforms import add_visit_level
from lsms_library.local_tools import format_id
from lsms_library.transformations import (
    food_acquired_to_canonical as _food_acquired_canonical)


def strata(x):
    return format_id(x)


# ---------------------------------------------------------------------------
# The four quarterly passages (GH #851 / charter §2.7)
# ---------------------------------------------------------------------------
#
# The EMC 2014 is a CONTINUOUS survey.  ``Report.pdf`` p. 20, quoted verbatim
# in ``Burkina_Faso/_/CONTENTS.org``: « Le dispositif de l'EMC a prévu la
# reconduite de l'ensemble des ménages enquêtés au premier passage aux trois
# passages suivants ».  Each passage re-asks the 7-day food module, so the
# wave delivers 4 x 7 = 28 days of exposure in four INDEPENDENT recalls.
#
# None of the four ``emc2014_p{1..4}_conso7jours_16032015.dta`` files carries a
# passage column — the passage lives in the FILENAME.  ``data_info.yml`` maps
# ``visit`` onto one of these constants per file (see the comment there for why
# the constant has to ride on a column reference).  They take the row that
# df_data_grabber hands them and ignore it.
def visit_p1(_row):
    """Passage 1 of the EMC 2014 continuous survey.  Constant."""
    return 1


def visit_p2(_row):
    """Passage 2 of the EMC 2014 continuous survey.  Constant."""
    return 2


def visit_p3(_row):
    """Passage 3 of the EMC 2014 continuous survey.  Constant."""
    return 3


def visit_p4(_row):
    """Passage 4 of the EMC 2014 continuous survey.  Constant."""
    return 4


# The `u` (unit) sentinel — GH #842 / #847, and here it is load-bearing.
#
# Only passage 1 fields the quantity grid (`qachat` / `uachat` / `qautocons`);
# passages 2-4 record MONEY ONLY (`achat`, `autocons`, `cadeau`).  So `uachat`
# does not exist in three of the four files and `u` arrives NaN for every one
# of their rows (and for the 55,205 passage-1 rows whose `uachat` is blank).
#
# `u` is a DECLARED index level, and pandas `groupby(dropna=True)` DELETES a
# row with a NaN key.  Measured on the pre-fix build: 460,438 of 557,822 rows
# (82.5%) were deleted by `_normalize_dataframe_index`, taking 76.5% of the
# wave's reported food expenditure with them — CFA 342,110,515 extracted,
# CFA 80,286,096 served.  That is the worst instance in the corpus of the
# NaN-index-key class (CLAUDE.md "Grain Collapse" §3b; `.coder/ledger/
# 323-framework.md` §6 names this exact cell).  Without the sentinel the
# `visit` level below would be cosmetic: passages 2-4 never reach the index.
#
# `Unknown` is the corpus-canonical spelling (lsms_library/data_info.yml `u`;
# Niger `niger.py::U_NA`, Uganda `crop_production`).
#
# CONSUMER RULE: `u == 'Unknown'` is NOT a unit.  These rows carry an
# Expenditure and no physical quantity; exclude them before any per-unit price
# or kg conversion, and report how many you excluded.
U_NA = 'Unknown'


def _fill_missing_u(df):
    """Replace a missing ``u`` index level with :data:`U_NA`.

    Rows are never added or removed — only the value of an already-present
    ``u`` changes.  Mirrors ``Niger/_/niger.py::fill_missing_u``.
    """
    names = list(df.index.names or [])
    if 'u' not in names:
        return df
    flat = df.reset_index()
    flat['u'] = flat['u'].astype('string').fillna(U_NA)
    out = flat.set_index(names)
    out.attrs = dict(df.attrs)
    return out


def food_acquired(df):
    """``food_acquired`` post-processor: per-passage canonical reshape.

    ``grab_data`` concatenates the four passage files before calling this hook,
    so by the time we see the frame the only thing distinguishing a passage-3
    row from a passage-1 row is the ``visit`` column stamped per file in
    ``data_info.yml``.  Reshape each passage SEPARATELY and re-stamp ``visit``
    onto the index with :func:`build_transforms.add_visit_level`, which is the
    library's declared spelling for a recall-occasion level (its docstring
    names this very wave).

    Why the level has to exist: ``food_acquired`` is in
    ``feature._ADDITIVE_MEASURE_COLUMNS``, so without ``visit`` the four
    passages land on ONE ``(t, v, i, j, u, s)`` tuple and are SUMMED into a
    single figure presented as a 7-day number.  Adding a level never reduces
    grain — the analyst who wants the 28-day total sums ``visit`` themselves,
    and ``Feature('food_acquired')`` still does exactly that (it drops the
    non-canonical level and sums the additive columns, as it does for
    GhanaLSS's ~12 visits, GH #501).

    ``visit`` is an int 1-4 matching the passage number in the source
    filename.
    """
    if 'visit' not in df.columns:
        # Defensive: a caller (or a future config edit) that does not stamp the
        # passage still gets the canonical shape, but must not silently serve a
        # 4x-summed figure as if it were one recall.
        raise KeyError(
            "Burkina_Faso/2014 food_acquired: no `visit` column.  The four EMC "
            "passages are independent 7-day recalls and MUST carry a visit "
            "level; see the food_acquired block of 2014/_/data_info.yml.")

    # `groupby` drops NaN keys, so an unstamped row would VANISH here -- the
    # very failure mode this whole change exists to close (a NaN on `u` was
    # deleting 82.5% of the wave).  A NaN `visit` can only mean the per-file
    # override lost its column, which is a config bug: say so, do not eat rows.
    n_missing = int(df['visit'].isna().sum())
    if n_missing:
        raise ValueError(
            f"Burkina_Faso/2014 food_acquired: {n_missing} of {len(df)} rows "
            "carry no `visit`.  The per-file `visit_p{n}` stamp in "
            "2014/_/data_info.yml did not fire for some file (a missing source "
            "column degrades to NaN under `missing_ok`); groupby would delete "
            "those rows silently.")

    out = []
    for passage, part in df.groupby('visit', sort=True):
        canonical = _food_acquired_canonical(part)   # drops the visit column
        out.append(add_visit_level(canonical, visit=int(passage)))
    return _fill_missing_u(pd.concat(out))


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
