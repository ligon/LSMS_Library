"""Formatting functions for Burkina Faso 2014 (EMC).

food_acquired canonical reshape (GH #169 / #107, #864): the wave's
``data_info.yml`` extracts one wide row per (household, item, passage) carrying
the questionnaire's three DISJOINT section-C blocks -- C04 purchases consumed,
C05 own production, C06 gifts / in-kind / barter -- each with its own quantity
and unit.  The ``food_acquired`` post-processor below serves each block on its
own ``s`` row (``purchased`` / ``produced`` / ``inkind``) in its own unit, the
canonical long form of the 2018-19 / 2021-22 waves.  It deliberately does NOT
use ``food_acquired_to_canonical``: that helper reads ``Quantity`` as a TOTAL
and subtracts ``Produced`` from it, which is wrong for this instrument (see
:func:`_section_c_to_canonical`).
"""

from pathlib import Path as _Path

import pandas as pd

from lsms_library.build_transforms import add_visit_level
from lsms_library.local_tools import format_id, get_formatting_functions as _get_formatting_functions

# Bind the helper to this config tree, including when another checkout's bare
# burkina_faso module is already cached. Reuse the framework's exact-path loader.
_emc_i = _get_formatting_functions(
    _Path(__file__).resolve().parents[2] / '_' / 'burkina_faso.py',
    '_burkina_emc_namespace')['emc_i']


def strata(x):
    return format_id(x)


def i(value):
    """2014 household id, NAMESPACED: ``'EMC_' + zd + menage.zfill(3)``.

    GH #1244 (@ligon's ruling): the EMC 2014 sample is independent of the
    EHCVM waves, yet its ids used to collide with 2018-19's -- ``zd +
    menage.zfill(3)`` here and ``grappe + '0' + menage.zfill(2)`` there spell
    the same string for a two-digit menage whenever the cluster serials agree:
    583 shared ids, whose clusters' Region agreed for 80.  See
    ``burkina_faso.EMC_PREFIX`` for the prefix and why.  Overrides the country
    ``i`` for every 2014 table; ``value`` is the row of ``[zd, menage]`` (or
    ``[grappe, menage]`` in emc2014_welfare.dta, which spells the ZD
    ``grappe``).
    """
    return _emc_i(value.iloc[0], value.iloc[1])


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


# ---------------------------------------------------------------------------
# Section C: three disjoint blocks, each in its own unit (GH #864)
# ---------------------------------------------------------------------------
#
# Questionnaire_EMC_2014_Passage_1.pdf, section C (PDF pp. 15-18), asks for each
# item C04 « la quantite totale et la valeur des achats consommes [...] », C05
# the same for own production and C06 for gifts / in-kind / barter -- each block
# with its OWN quantity, unit and value boxes.  The file's labels bind them:
# qachat/uachat/achat = C04, qautocons/uautocons/autocons = C05,
# qcadeau/ucadeau/cadeau = C06.  So:
#
# * ``qachat`` is the PURCHASED quantity.  The shared
#   ``food_acquired_to_canonical`` read it as a total and served
#   ``(qachat - qautocons).clip(0)``: 283 passage-1 rows carry both, 238 of
#   them clipped to 0 (the 219 served zeros) and 45 subtracted across units.
# * own production is served in ``uautocons``, not in the purchase unit
#   ``uachat``: on the old shape 11,123 of 11,418 produced rows carried
#   ``u = 'Unknown'`` although their own unit was recorded.
# * the C06 gift quantity ``qcadeau`` (1,468 rows > 0) is served as
#   ``s = 'inkind'`` in ``ucadeau``.
#
# @ligon's ruling, 2026-11-03.  Every served number is a direct survey answer
# (C04 quantity and value, C05 quantity, C06 quantity), so nothing here is a
# registered derivation.  ``Expenditure`` is C04's ``achat`` and is NaN on the
# produced / inkind rows, as in the EHCVM waves; the C05 / C06 VALUES
# (``autocons`` / ``cadeau``) and C02 ``mtt_dep`` stay unread.
#
# A quantity the survey did not ask for stays NaN: passages 2-4 have no
# quantity grid, and the printed passage-1 grid has no quantity boxes for nine
# items (Sel, Oignons, Soumbala, Feuilles, Gombo, Pate d'arachide, Haricot,
# Beurre de Karite, Fruits).  Nothing is filled with 0, so no NaN has to be
# restored afterwards.
_BLOCKS = (
    # s,           quantity column, unit column (None = the index's `u`), value
    ('purchased', 'Quantity',      None,           'Expenditure'),
    ('produced',  'Produced',      'ProducedUnit', None),
    ('inkind',    'GiftQuantity',  'GiftUnit',     None),
)


def _unit_label(value):
    """The block's unit label, formatted as the ``u`` idxvar is (``format_id``
    strips surrounding whitespace); missing stays missing."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    return format_id(value)


def _section_c_to_canonical(part):
    """One passage's wide frame -> canonical long form on ``s``.

    ``part`` is indexed ``(t, v, i, j, u[, visit])`` with ``u`` = ``uachat``
    and columns ``Quantity`` / ``Expenditure`` (C04), ``Produced`` /
    ``ProducedUnit`` (C05), ``GiftQuantity`` / ``GiftUnit`` (C06).  A block's
    columns may be absent (a test frame, or a passage file that lacks them);
    they are then treated as all-NaN.  Returns a frame indexed
    ``(t, v, i, j, u, s)`` with ``Quantity`` / ``Expenditure``:

    * ``purchased`` -- kept where ``Quantity > 0`` or ``Expenditure > 0``,
      ``Quantity`` exactly as reported (NaN where not asked);
    * ``produced`` / ``inkind`` -- kept where their quantity is ``> 0``, in
      their own unit, ``Expenditure`` NaN.

    A missing unit is left NaN here; the caller's :func:`_fill_missing_u`
    turns it into the ``Unknown`` sentinel.
    """
    work = part.reset_index()
    if 'visit' in work.columns:
        work = work.drop(columns=['visit'])
    keys = ['t', 'v', 'i', 'j']
    pieces = []
    for source, qcol, ucol, xcol in _BLOCKS:
        qty = pd.to_numeric(work[qcol], errors='coerce') if qcol in work \
            else pd.Series(float('nan'), index=work.index)
        value = pd.to_numeric(work[xcol], errors='coerce') if xcol and xcol in work \
            else pd.Series(float('nan'), index=work.index)
        if ucol is None:
            unit = work['u']
        elif ucol in work:
            unit = work[ucol].astype(object).map(_unit_label)
        else:
            unit = pd.Series(None, index=work.index, dtype=object)
        keep = (qty > 0) | (value > 0)
        block = work.loc[keep, keys].copy()
        block['u'] = unit[keep].values
        block['s'] = source
        block['Quantity'] = qty[keep].values
        block['Expenditure'] = value[keep].values
        pieces.append(block)
    out = pd.concat(pieces, ignore_index=True)
    return out.set_index(keys + ['u', 's'])


# ---------------------------------------------------------------------------
# Encoding safety net for the `food_acquired` labels (GH #794 / #805)
# ---------------------------------------------------------------------------
#
# `emc2014_p{1..4}_conso7jours_16032015.dta` write their value labels in
# cp1252.  Since GH #805 the reader re-decodes them (``get_dataframe`` ->
# ``local_tools._redecode_cp1252``), so the `product` labels arrive clean --
# ``\u0152ufs``, ``Cube (Maggi, Jumbo,\u2026)``, ``Feuilles (oseilles,
# baobab, boulvaka\u2026`` -- and the parent `_/categorical_mapping.org`
# keys them clean.  This hook used to carry a wave-level ``_decode_cp1252``
# and a whole-value ``_J_FIX`` for the Feuilles label (whose U+0085 the org
# reader stripped from any key); both are retired with #805.
#
# THE CROSSWALK RUNS FIRST: `data_info.yml` maps `j` through `harmonize_food`
# on the raw label inside df_data_grabber, before this hook sees the frame.
# What remains below is a SAFETY NET for a label the crosswalk did not
# absorb: a UTF-8-as-latin-1 double encoding (`_decode_mojibake`, the 2018-19
# class -- a clean label round-trips unchanged; #805 deliberately leaves
# that class alone).  It exists so that a future uncovered label is served
# legible rather than as mojibake.


def _decode_mojibake(value):
    """Reverse a UTF-8-read-as-latin-1 double encoding (``bÃ©`` -> ``bé``;
    mirrors ``Niger/2018-19/_/mapping.py``).  A string that does not
    round-trip is returned unchanged, so clean French survives."""
    if not isinstance(value, str):
        return value
    try:
        return value.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value


def _repair_label(value):
    if not isinstance(value, str):
        return value
    return _decode_mojibake(value)


def _repair_index_labels(df, levels=('j', 'u')):
    """Apply :func:`_repair_label` to the named string index levels.  Rows
    are never added or removed; only label values change."""
    names = list(df.index.names or [])
    todo = [n for n in levels if n in names]
    if not todo:
        return df
    arrays = []
    changed = False
    for name in names:
        level = df.index.get_level_values(name)
        if name in todo:
            repaired = level.map(_repair_label)
            changed = changed or not repaired.equals(level)
            arrays.append(repaired)
        else:
            arrays.append(level)
    if not changed:
        return df
    out = df.copy()
    out.index = pd.MultiIndex.from_arrays(arrays, names=names)
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

    Each passage is reshaped by :func:`_section_c_to_canonical` (GH #864):
    C04 / C05 / C06 become ``purchased`` / ``produced`` / ``inkind`` rows, each
    in its own unit, with a quantity that was not asked left NaN.  Then two
    value-only steps (no row is added or removed): a missing ``u`` becomes the
    ``Unknown`` sentinel (:func:`_fill_missing_u`), and
    :func:`_repair_index_labels` applies the GH #794 safety net on ``j`` /
    ``u`` (a UTF-8 double encoding; the cp1252 class is repaired by the reader
    since GH #805), after the build-time crosswalk.
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
        canonical = _section_c_to_canonical(part)    # drops the visit column
        out.append(add_visit_level(canonical, visit=int(passage)))
    result = _fill_missing_u(pd.concat(out))
    return _repair_index_labels(result)


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
