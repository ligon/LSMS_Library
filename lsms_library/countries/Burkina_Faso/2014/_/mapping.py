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


# ---------------------------------------------------------------------------
# Quantity is NaN where the survey asked for none (GH #864)
# ---------------------------------------------------------------------------
#
# `food_acquired_to_canonical` computes the purchased quantity as
# `(Quantity.fillna(0) - Produced.fillna(0)).clip(lower=0)`, so a source row
# with NO quantity answer is served as a purchased quantity of ZERO -- and 0
# means "bought none", not "not asked".  In this wave the not-asked case is
# the RULE, not the exception: passages 2-4 field no quantity grid at all
# (`qachat` does not exist in their files, 405,127 served rows), and in
# passage 1 `qachat` is null on 43,906 of the 140,908 rows with `achat > 0`,
# concentrated on items for which the grid was evidently not administered
# (Sel 10,244 of 10,259; Oignons 7,668 of 7,672; Soumbala 7,138 of 7,190;
# Feuilles, Gombo, Pate d'arachide, Haricot, Beurre de Karite, Fruits all at
# >= 99%).  `qachat` is NEVER 0 where `achat > 0` (0 of 140,908), so a served
# purchased 0 beside an expenditure can only be a filled NaN or a clipped
# `Produced >= Quantity` row.
#
# `_restore_missing_quantity` puts the NaN back on purchased rows whose
# SOURCE quantity was null, keyed on the wide frame's own index, so the rule
# is "NaN in, NaN out" for every passage rather than a per-passage special
# case.  Expenditure is untouched; no row is added or removed; the wave's
# Quantity total is unchanged (a NaN and a 0 sum alike).  Downstream,
# `food_prices` sees `Expenditure / NaN` instead of `Expenditure / 0` and
# can tell a money-only recall occasion from the GH #591 zero-quantity
# defect (`transformations._money_only_rows`).  The corpus-wide `fillna(0)`
# in `build_transforms` is deliberately NOT changed here: it is
# `@build_transform`-tagged and every reshaped country would re-warm.
def _restore_missing_quantity(wide, canonical):
    """Set purchased ``Quantity`` back to NaN where ``wide``'s own Quantity
    was null, after :func:`food_acquired_to_canonical` filled it with 0.

    ``wide`` is one passage's wide frame (index ``(t, v, i, j, u)``, columns
    ``Quantity`` / ``Produced`` / ``Expenditure``); ``canonical`` is its
    reshaped long form (``s`` appended to the index).  Only a purchased row
    that is exactly 0 and shares its key with a null-quantity source row is
    touched -- a reported positive quantity on the same key (a genuine
    within-passage duplicate) is left alone.
    """
    src_null = wide.index[wide['Quantity'].isna()]
    if len(src_null) == 0:
        return canonical
    s = canonical.index.get_level_values('s')
    key = canonical.index.droplevel('s')
    q = pd.to_numeric(canonical['Quantity'], errors='coerce')
    mask = (s == 'purchased') & (q == 0) & key.isin(src_null)
    if mask.any():
        canonical = canonical.copy()
        canonical.loc[mask, 'Quantity'] = float('nan')
    return canonical


# ---------------------------------------------------------------------------
# Encoding repair for the 2014 `conso7jours` value labels (GH #794 / #805)
# ---------------------------------------------------------------------------
#
# `emc2014_p{1..4}_conso7jours_16032015.dta` write their value labels in
# cp1252 and the reader decodes pre-118 files as latin-1, so four of the
# `product` labels reach us with a C1 control where cp1252 has a printable
# character: 0x85 (ellipsis), 0x8C / 0x9C (OE / oe ligature).  THE CROSSWALK
# RUNS FIRST: `data_info.yml` maps `j` through `harmonize_food` on the RAW
# label inside df_data_grabber, before this hook sees the frame, and that
# table is keyed on the damage (strategy B, `_/CONTENTS.org` "Encoding";
# the parent `_/categorical_mapping.org` says which keys and why).  So the
# decoders below are a SAFETY NET for a label the crosswalk did not absorb:
# they run after the mapping and only change a string that still carries a
# C1 control (`_decode_cp1252`) or a UTF-8-as-latin-1 double encoding
# (`_decode_mojibake`, the 2018-19 class; a clean label round-trips
# unchanged).  Measured on the 2026-10-14 rebuild they change 0 rows -- the
# repaired crosswalk does the work -- and they exist so that a future
# uncovered label is served legible rather than as a control character.
# If the decode is ever moved AHEAD of the crosswalk, every damage-keyed
# row must be re-keyed in the same commit.
#
# `_J_FIX` is the one label the crosswalk structurally cannot reach:
# "Feuilles (oseilles, baobab, boulvaka\x85" ENDS in the 0x85 byte, and the
# org reader strips every cell (U+0085 is whitespace to str.strip), so no
# key can carry it, while the dict is applied before format_id strips the
# value.  Whole-value replacement to the Preferred Label the crosswalk
# intends, exactly as `_RELATIONSHIP_FIX` does for the roster (GH #801).
# Never a substring.
_J_FIX = {'Feuilles (oseilles, baobab, boulvaka': 'Feuilles'}


def _decode_cp1252(value):
    """Re-decode a label the reader took as latin-1 but the file wrote as
    cp1252 (GH #801; mirrors ``Niger/2018-19/_/mapping.py``).  Changes only a
    string carrying a C1 control (U+0080-U+009F); returns anything else, and
    the five undecodable cp1252 holes, unchanged."""
    if not isinstance(value, str) or not any('\x80' <= ch <= '\x9f' for ch in value):
        return value
    try:
        return value.encode('latin-1').decode('cp1252')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value


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
    value = _J_FIX.get(value, value)
    # Double-encoding FIRST: the latin-1 -> utf-8 round trip succeeds only
    # on a genuine UTF-8-read-as-latin-1 string (a lone C1 byte is an
    # invalid UTF-8 continuation and falls through unchanged).  cp1252
    # first would turn `b\xc5\x93uf` into `b\xc5\u201cuf` -- the 0x93 is a
    # curly quote in cp1252 -- and destroy the round trip.
    return _decode_cp1252(_decode_mojibake(value))


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

    Two further steps, both value-only (no row is added or removed):

    * :func:`_restore_missing_quantity` -- a purchased ``Quantity`` is NaN
      where the source quantity was null (GH #864): all of passages 2-4,
      and the passage-1 items the grid was not administered for.  The
      ``Unknown`` ``u`` fill is taken BEFORE the reshape so the wide and
      long frames share one key.
    * :func:`_repair_index_labels` -- the GH #794 safety net on ``j`` / ``u``
      (whole-value ``_J_FIX``, then cp1252, then double-encoding), after the
      build-time crosswalk has already absorbed the keyed damage.
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
        part = _fill_missing_u(part)                  # one key for both tiers
        canonical = _food_acquired_canonical(part)   # drops the visit column
        canonical = _restore_missing_quantity(part, canonical)
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
