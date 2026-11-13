# Formatting Functions for Niger 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import food_acquired_to_canonical as _food_acquired_canonical

from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_with_unit_weights as _food_acquired_with_unit_weights,
    inputs_last_purchase as _inputs_last_purchase,
)

# GH #876.  The wide frame's `Expenditure` used to be `s07bq08`, the value of
# ONE purchase made up to 30 days ago, sitting beside a 7-day consumption
# `Quantity`.  `_food_acquired_ehcvm` derives it instead -- purchased Quantity
# x (s07bq08 / s07bq07a), NA where the last purchase used a different unit --
# and stamps the registry key on every purchased row.  See lsms_library/ehcvm.py
# and Niger/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Niger')

# GH #812.  `strata` is the documented stratum, region x milieu, as a label.  It
# is a construction, so `sample` carries its registry key: see
# lsms_library/ehcvm.py and Niger/_/derivations.yml.
from lsms_library.ehcvm import (  # noqa: E402
    inputs_region_milieu_stratum as _inputs_region_milieu_stratum,
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def inputs_strata(wave):
    """The registry `inputs` callable: the cover's region and milieu at (t, i)."""
    return _inputs_region_milieu_stratum('Niger', wave)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Niger')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Niger/_/niger.py -- which is in EVERY Niger table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Niger', wave)

# The country module holds the shared `u` sentinel (GH #842).
import sys as _sys
from pathlib import Path as _Path
_sys.path.append(str(_Path(__file__).resolve().parents[2] / '_'))
from niger import fill_missing_u as _fill_missing_u  # noqa: E402


# Pre-decode byte fixups.  The export pipeline upstream of pyreadstat
# replaced the second byte of certain UTF-8 codepoints with literal
# ``__`` (0x5f 0x5f).  For Niger 2018-19, two food items use the
# OE/oe ligature (Œ / œ); their UTF-8 second bytes (``\\x92``,
# ``\\x93``) were lost and replaced with ``__``, so pyreadstat
# returns mojibake fragments like ``Å__ufs`` (= Œufs, UTF-8
# ``\\xc5\\x92...``) and ``bÅ__uf`` (= bœuf, UTF-8 ``b\\xc5\\x93uf``,
# appearing in ``Viande de bœuf``).  We restore the lost UTF-8
# continuation byte BEFORE the byte-level decode by mapping ``Å__``
# (mojibake ``\\xc5\\x5f\\x5f``) back to ``Å\\x92`` or ``Å\\x93`` --
# Latin-1-encodable strings whose round-trip through
# ``encode('latin-1').decode('utf-8')`` yields the canonical Œ / œ.
# Keys are mojibake substrings; values are mojibake-form replacements
# that contain the recovered UTF-8 continuation byte.
_LOST_PREFIX_REPLACEMENTS = {
    'Å__ufs': 'Å\x92ufs',  # Œufs (UTF-8: \xc5\x92)
    'bÅ__uf': 'bÅ\x93uf',  # bœuf (UTF-8: \xc5\x93)
}


def _decode_mojibake(s):
    """Reverse the UTF-8-as-Latin-1-as-UTF-8 double-encoding that pyreadstat
    produces for Niger's 2018-19 .dta value labels.

    The .dta file's value-labels block stores French strings as raw
    UTF-8 bytes (e.g. ``UnitÃ©`` should be ``Unité``: the ``é`` is
    UTF-8 ``b'\\xc3\\xa9'``), but the file metadata declares Latin-1
    encoding.  pyreadstat respects the metadata, decoding the UTF-8
    bytes as Latin-1 and re-encoding as Python ``str`` -- producing
    ``UnitÃ©`` instead of ``Unité``.  None of pyreadstat's
    ``encoding=`` options correct this (verified for the analogous
    Guinea-Bissau case 2026-05-07).  The fix is post-decode: encode
    the mojibake string back to Latin-1 bytes (recovers the original
    UTF-8 byte sequence), then decode as UTF-8.

    Two j-values carry an additional lossy substitution upstream of
    pyreadstat where the second byte of certain UTF-8 codepoints was
    replaced with literal ``__`` (i.e. ``Œufs`` -> ``Å__ufs``,
    ``bœuf`` -> ``bÅ__uf``).  Those can't be recovered byte-wise, so
    we handle them via ``_LOST_PREFIX_REPLACEMENTS`` above.

    Strings without non-ASCII characters round-trip cleanly through
    ``encode('latin-1').decode('utf-8')`` (every byte 0x00-0x7F is
    valid in both encodings and identical), so this is safe to apply
    to all string values; only the mojibake-bearing ones change.

    Returns the input unchanged on any error or when ``s`` isn't a
    string -- best-effort, never raises.
    """
    if not isinstance(s, str):
        return s
    # Restore lost UTF-8 continuation bytes BEFORE the byte-level
    # decode.  The replacement values are still in mojibake form
    # (Latin-1-encodable codepoints that map back to the original
    # UTF-8 byte sequence after ``encode('latin-1')``).  Substring
    # match -- the fragments may appear at any position (e.g.
    # ``bÅ__uf`` inside ``Viande de bÅ__uf``).
    for bad, good in _LOST_PREFIX_REPLACEMENTS.items():
        if bad in s:
            s = s.replace(bad, good)
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def _decode_mojibake_in_df(df):
    """Apply ``_decode_mojibake`` to every string column and string
    index level.  Used by per-feature ``df_edit`` hooks to fix
    value-label mojibake that affects French strings in multiple
    columns at once (food items, units, etc.) -- 7 of 44 ``u`` values
    and 49 of 135 ``j`` values carry it before this fix.
    """
    new_levels = []
    new_names = list(df.index.names)
    changed = False
    for i, name in enumerate(df.index.names):
        level = df.index.get_level_values(i)
        if level.dtype == object or pd.api.types.is_string_dtype(level):
            mapped = level.map(_decode_mojibake)
            if not mapped.equals(level):
                changed = True
            new_levels.append(mapped)
        else:
            new_levels.append(level)
    if changed:
        df = df.copy()
        df.index = pd.MultiIndex.from_arrays(new_levels, names=new_names) \
            if len(new_levels) > 1 else new_levels[0]
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(_decode_mojibake)
    return df


def food_acquired(df):
    """``food_acquired`` post-processor: canonical reshape + mojibake fix.

    Wraps ``transformations.food_acquired_to_canonical`` (the Phase 3
    s-axis reshape), sweeps mojibake out of every string column / index
    level, then fills a missing ``u``.  See ``_decode_mojibake`` for the
    encoding rationale.

    The reshape drops every row left with no measurement, so the rows that
    reach the fill are exactly the rows the table serves; ``fill_missing_u``
    changes a value, never a row count.  A row that reported an amount with no
    unit gets the ``Unknown`` sentinel instead of a NaN on the declared ``u``
    index level, which the next ``groupby`` would delete silently (GH #842 --
    the rationale is in ``niger.py`` at ``U_NA``).

    This wave serves 0 NaN-``u`` rows today (the 12 #842 counted are in
    2021-22); the fill is here so the two EHCVM waves cannot drift apart.

    GH #1708: the purchased rows built from a record with a computed
    ``s07bq07a`` are flagged (:func:`_stamp_computed_last_purchase`), and those
    #876 values are revalued at a local median of genuine last-purchase unit
    values (:func:`_revalue_flagged_purchases`).
    """
    raw = _computed_raw()
    flagged = _computed_last_purchase_positions(df, raw)
    valuation = _valuation_frame(raw, df, _region_of(raw))
    # GH #1224: `decode` -- the served base unit of a sized row is read
    # through the same repair the sweep below applies to every other row.
    out = _food_acquired_with_unit_weights(df, FOOD_ACQUIRED_DERIVATION, wave='2018-19',
                                           decode=_decode_mojibake)
    out = _revalue_flagged_purchases(out, df, valuation)
    out = _decode_mojibake_in_df(out)
    # Fill a missing unit with the `Unknown` sentinel, AFTER the mojibake
    # sweep so the two never fight over the same value.
    out = _fill_missing_u(out)
    return _stamp_computed_last_purchase(out, df, flagged)


# ---------------------------------------------------------------------------
# GH #1708: a computed last-purchase quantity is flagged (ruling @ligon
# 2026-11-12, "Flag now, diagnose"), and its purchase is revalued at a local
# median (ruling @ligon 2026-11-12, "Revalue at local median")
# ---------------------------------------------------------------------------
#
# 15,264 of this file's 107,263 positive s07bq07a (14.2%) are off the quarter
# grid, and 14,908 of them carry 13-18 decimals: double-precision arithmetic, not
# an entry.  2021-22, the same instrument, has 15 such values in 112,923.  15,263
# of the 15,264 sit on a record where the questionnaire ENABLES s07bq07a (s07bq06
# = 1, 2 or 3), and their values were reconstructed from a reference unit value
# per product x unit x size x region.  They are not the 21,063 records where the
# CAPI disables s07bq07a and the file fills it anyway: that is a separate set,
# with no s07bq08 and no served value, and it is not keyed.  Until the second
# ruling the served Expenditure of these rows was Quantity x s07bq08 / s07bq07a,
# i.e. it passed through the reconstructed unit value (26.0% of 2018-19's).  Each
# purchased row built from such a record carries
# `Niger::food_acquired::computed-last-purchase-quantity` beside its #876 key, and
# since the second ruling, where #876 values it, `Niger::food_acquired::
# flagged-purchase-local-median` too, with its Expenditure revalued (below).  How
# the values were built is in Niger/_/CONTENTS.org.
COMPUTED_LAST_PURCHASE_KEY = 'Niger::food_acquired::computed-last-purchase-quantity'
_COMPUTED_SOURCE = 's07b_me_ner2018.dta'


class ComputedLastPurchaseJoinError(Exception):
    """The wide food_acquired frame is not ``s07b_me_ner2018.dta`` row for row.

    Subclasses ``Exception``: a derived food table must not read it as a failed
    build (GH #1546)."""


def _computed_raw(wave='2018-19'):
    """``s07b_me_ner2018.dta`` in its codes, every record, in file order."""
    if wave != '2018-19':
        raise ValueError(f'{COMPUTED_LAST_PURCHASE_KEY} covers 2018-19 only, not {wave!r}')
    from lsms_library.paths import countries_root
    path = countries_root() / 'Niger' / wave / 'Data' / _COMPUTED_SOURCE
    return tools.get_dataframe(str(path), convert_categoricals=False)


def computed_last_purchase_rows(last_quantity):
    """Boolean ndarray: ``s07bq07a`` positive and off the quarter grid (GH #1708).

    Off the grid means ``|4x - round(4x)| >= 1e-6``: not a multiple of 0.25.  The
    instrument asks a decimal, so a typed 0.1 is off the grid too; 2021-22, the
    same instrument, has 15 off-grid values in 112,923, which bounds how many of
    2018-19's 15,264 can be typed.  The other way round, a value computed to land
    on the grid cannot be seen.
    """
    x = pd.to_numeric(pd.Series(np.ravel(last_quantity)), errors='coerce').to_numpy(dtype=float)
    with np.errstate(invalid='ignore'):
        return (x > 0) & (np.abs(4 * x - np.round(4 * x)) >= 1e-6)


def _computed_last_purchase_positions(df, raw=None):
    """:func:`computed_last_purchase_rows` on the wide frame, which is the file row
    for row (the shared hook's unit-weight join refuses anything else)."""
    if raw is None:
        raw = _computed_raw()
    mine = pd.to_numeric(df['LastPurchaseQuantity'], errors='coerce').to_numpy(dtype=float)
    theirs = pd.to_numeric(raw['s07bq07a'], errors='coerce').to_numpy(dtype=float)
    if len(raw) != len(df) or not np.array_equal(mine, theirs, equal_nan=True):
        raise ComputedLastPurchaseJoinError(
            f'the wide food_acquired frame is not {_COMPUTED_SOURCE} row for row')
    return computed_last_purchase_rows(raw['s07bq07a'])


def _stamp_computed_last_purchase(out, df, flagged):
    """Add the GH #1708 key to the purchased rows the flagged records produce.

    The flagged records are run through the same reshape on their own, to learn
    the served keys they become, and the key is stamped on those keys in ``out``.
    Marking them inside the main call would change which keys ``index.duplicated``
    sees there and so which rows keep a ``Quantity_kg``.  A purchased row that
    shares its key with an unflagged record's (core sums the two) is stamped too:
    its Expenditure includes a revalued one.  No such key exists today (measured
    2026-11-12: no purchased key of this wave is shared by two records).
    """
    if not flagged.any():
        return out
    from lsms_library.derivations import union_keys
    from lsms_library.ehcvm import food_acquired_ehcvm
    sub = food_acquired_ehcvm(df[flagged], FOOD_ACQUIRED_DERIVATION, decode=_decode_mojibake)
    sub = _fill_missing_u(_decode_mojibake_in_df(sub))
    keys = sub.index[sub.index.get_level_values('s') == 'purchased']
    hit = out.index.isin(keys) & (out.index.get_level_values('s') == 'purchased')
    out = out.copy()
    current = out['Derivation'].astype(object)
    out['Derivation'] = pd.Series(
        [union_keys([c, COMPUTED_LAST_PURCHASE_KEY]) if h else c
         for c, h in zip(current, hit)], index=out.index, dtype='string')
    return out


def inputs_computed_last_purchase(wave='2018-19'):
    """The registry ``inputs`` callable: the flagged records at ``(t, i, j)``.

    The raw section-7B answers of every record whose ``s07bq07a`` is flagged,
    with the file's own names (``s07bq06`` in its code: 1 hier, 2 7 jours,
    3 30 jours, 4 plus de 30 jours, 5 jamais), indexed as the served table is.
    """
    raw = _computed_raw(wave)
    frame = _inputs_last_purchase('Niger', wave)
    mine = pd.to_numeric(frame['s07bq07a'], errors='coerce').to_numpy(dtype=float)
    theirs = pd.to_numeric(raw['s07bq07a'], errors='coerce').to_numpy(dtype=float)
    if len(raw) != len(frame) or not np.array_equal(mine, theirs, equal_nan=True):
        raise ComputedLastPurchaseJoinError(
            f'the input frame is not {_COMPUTED_SOURCE} row for row')
    flagged = computed_last_purchase_rows(raw['s07bq07a'])
    out = frame.loc[flagged].copy()
    out.insert(out.columns.get_loc('s07bq07a'), 's07bq06',
               pd.to_numeric(raw.loc[flagged, 's07bq06'], errors='coerce').to_numpy())
    return out


# ---------------------------------------------------------------------------
# GH #1708, second ruling: the flagged purchases are revalued at a local median
# ---------------------------------------------------------------------------
#
# Ruling @ligon 2026-11-12, "Revalue at local median".  On a purchased row built
# from a flagged record, Expenditure = the served purchased Quantity x the median
# GENUINE last-purchase unit value, s07bq08 / s07bq07a over the records that are
# NOT flagged, for the same product x unit x size in the same cluster (grappe),
# else in the same region (s00q01 of the cover, s00_me_ner2018.dta); NA where
# neither has enough genuine reports.  The unit rule is #876's: a genuine record
# prices the cell of its OWN last-purchase unit and size, the flagged row reads the
# cell of its consumption unit and size, and the two are compared through the
# identity `ehcvm.unit_match` uses (`ehcvm._unit_key`); nothing is converted
# between units.  The product is the s07bq01 code.  Consumed quantities are not
# touched, and the producer's values stay recoverable from derivation_inputs.
#
# Two details were ruled afterwards (@ligon 2026-11-12, "GH1708 revaluation details,
# and the release plan"): "Region floor of 3": a local median needs >= 3 genuine
# reports at cluster AND at region level; otherwise NA.  "Only previously valued":
# revalue only flagged rows that already carried a served Expenditure under #876;
# rows the library left NA (2,787) stay NA.  "The ruling corrects a construction, it
# does not add new values."
LOCAL_MEDIAN_KEY = 'Niger::food_acquired::flagged-purchase-local-median'
#: Genuine reports a CLUSTER median needs; with fewer the region's is used.  Held-out
#: genuine prices (each record against the median of the OTHER genuine reports in its
#: cell, 2026-11-12): within 25% of the held-out price, the cluster median of 2
#: others scores 63.6% against the region median's 67.4%, of 3 others 71.7% against
#: 69.1%, of 5 others 73.5% against 70.8%, of 10 or more 87.2% against 84.9%.  From 3
#: the cluster median is about as good as the region's or better, and 3 is the
#: smallest median one stray report cannot move.
LOCAL_MEDIAN_MIN_CLUSTER_REPORTS = 3
#: Genuine reports a REGION median needs; with fewer the row is served NA.  Ruled
#: "Region floor of 3" (@ligon 2026-11-12): the same floor as the cluster's.  For a
#: genuine report whose cluster has fewer than 3 others, a region median of 1 or 2
#: other reports was within 25% of it only 48.5% / 43.1% of the time.  The counts
#: behind every value are in the function's output (`cluster_reports`,
#: `region_reports`), so another floor is a re-derivation from derivation_inputs,
#: not a rebuild.
LOCAL_MEDIAN_MIN_REGION_REPORTS = 3
_REGION_SOURCE = 's00_me_ner2018.dta'
#: The raw variables the revaluation reads, with the file's own names.
LOCAL_MEDIAN_RAW_VARIABLES = ['grappe', 's00q01', 's07bq01', 's07bq03a', 's07bq03b',
                              's07bq03c', 's07bq04', 's07bq05', 's07bq06', 's07bq07a',
                              's07bq07b', 's07bq07c', 's07bq08']
#: Wide-frame names (the hook's MYVARS) of the raw variables, where they differ.
_WIDE_NAMES = {'s07bq03a': 'Quantity', 's07bq04': 'Produced', 's07bq05': 'GiftQuantity',
               's07bq03b': 'ConsumptionUnitCode', 's07bq03c': 'ConsumptionUnitSize',
               's07bq07a': 'LastPurchaseQuantity', 's07bq07b': 'LastPurchaseUnitCode',
               's07bq07c': 'LastPurchaseUnitSize', 's07bq08': 'LastPurchaseValue'}


class LocalMedianJoinError(Exception):
    """A frame the revaluation reads is not what it expects (GH #1708).

    Raised when the wide frame or the input frame is not s07b row for row, when the
    cover repeats a household, or when the shared hook's purchased rows are not the
    records they are expected to be, so a value cannot be written by position.
    Subclasses ``Exception``: a derived food table must not read it as a failed
    build (GH #1546)."""


def _num(values):
    return pd.to_numeric(pd.Series(np.asarray(values)), errors='coerce').to_numpy(dtype=float)


def _region_of(raw, wave='2018-19'):
    """Each s07b record's region, ``s00q01`` of the cover, by (grappe, menage).

    A build input of food_acquired since the second #1708 ruling: declared in the
    wave's data_info.yml (``household_region:``) so its sidecar is hashed.  If the
    cover cannot be fetched the table raises; a record the cover does not list gets
    no region, and so no region fallback."""
    from lsms_library.paths import countries_root
    path = countries_root() / 'Niger' / wave / 'Data' / _REGION_SOURCE
    cover = tools.get_dataframe(str(path), convert_categoricals=False)

    def key(frame):
        return pd.MultiIndex.from_arrays([_num(frame['grappe']), _num(frame['menage'])])

    region = pd.Series(_num(cover['s00q01']), index=key(cover))
    if region.index.duplicated().any():
        raise LocalMedianJoinError(f'{_REGION_SOURCE} repeats a (grappe, menage)')
    return region.reindex(key(raw)).to_numpy(dtype=float)


def _valuation_frame(raw, labelled, region):
    """What the revaluation reads, one row per record of s07b, in file order.

    ``raw`` is the file in its codes.  ``labelled`` is the hook's wide frame (its
    MYVARS names) or :func:`ehcvm.inputs_last_purchase`'s frame (the raw names): it
    supplies the unit and size LABELS, which are what #876's unit match compares.
    Its quantities must be the file's, row for row.  ``region`` is :func:`_region_of`.
    ``flagged`` is the #1708 class (:func:`computed_last_purchase_rows`).
    """
    def col(name):
        wide = _WIDE_NAMES.get(name)
        return labelled[wide] if wide in labelled.columns else labelled[name]

    if len(raw) != len(labelled):
        raise LocalMedianJoinError(f'the frame is not {_COMPUTED_SOURCE} row for row')
    for name in ('s07bq03a', 's07bq04', 's07bq05', 's07bq07a', 's07bq08'):
        if not np.array_equal(_num(col(name)), _num(raw[name]), equal_nan=True):
            raise LocalMedianJoinError(
                f'the frame is not {_COMPUTED_SOURCE} row for row ({name})')
    frame = pd.DataFrame({
        'grappe': _num(raw['grappe']), 's00q01': np.asarray(region, dtype=float),
        's07bq01': _num(raw['s07bq01']), 's07bq03a': _num(raw['s07bq03a']),
        's07bq03b': np.asarray(col('s07bq03b'), dtype=object),
        's07bq03c': np.asarray(col('s07bq03c'), dtype=object),
        's07bq04': _num(raw['s07bq04']), 's07bq05': _num(raw['s07bq05']),
        's07bq06': _num(raw['s07bq06']), 's07bq07a': _num(raw['s07bq07a']),
        's07bq07b': np.asarray(col('s07bq07b'), dtype=object),
        's07bq07c': np.asarray(col('s07bq07c'), dtype=object),
        's07bq08': _num(raw['s07bq08'])})
    frame['flagged'] = computed_last_purchase_rows(frame['s07bq07a'])
    return frame


def _purchased_quantity(frame):
    """The served purchased Quantity of each record, as the shared reshape computes it:
    ``((s07bq03a - s07bq05) - s07bq04).clip(0)``, a missing gift or production as 0."""
    gifts = pd.Series(frame['s07bq05'].to_numpy(dtype=float)).fillna(0)
    net = pd.Series(frame['s07bq03a'].to_numpy(dtype=float)) - gifts
    produced = pd.Series(frame['s07bq04'].to_numpy(dtype=float)).fillna(0)
    return (net.fillna(0) - produced).clip(lower=0).to_numpy(dtype=float)


def local_median_unit_values(frame):
    """The local median genuine unit value in each record's consumption cell.

    ``frame`` is :func:`_valuation_frame` (or :func:`inputs_flagged_purchase_local_median`).
    A genuine report is a record that is not flagged, with ``s07bq07a`` and
    ``s07bq08`` positive and its last-purchase unit and size recorded; it prices
    the cell (product, last-purchase unit, last-purchase size).  Each record reads
    the cell (product, consumption unit, consumption size): in its cluster when the
    cell has :data:`LOCAL_MEDIAN_MIN_CLUSTER_REPORTS` genuine reports there, else
    in its region when it has :data:`LOCAL_MEDIAN_MIN_REGION_REPORTS`, else none.

    Returns, positionally: ``unit_value``, ``level`` ('cluster', 'region' or NA),
    ``cluster_reports`` and ``region_reports`` (genuine reports in the two cells).
    """
    from lsms_library.ehcvm import _unit_key

    def keys(name):
        return pd.Series(frame[name].to_numpy(dtype=object)).map(_unit_key).astype(object)

    q7 = frame['s07bq07a'].to_numpy(dtype=float)
    q8 = frame['s07bq08'].to_numpy(dtype=float)
    product = frame['s07bq01'].to_numpy(dtype=float)
    cu, cs, lu, ls = keys('s07bq03b'), keys('s07bq03c'), keys('s07bq07b'), keys('s07bq07c')
    flagged = frame['flagged'].to_numpy(dtype=bool)
    with np.errstate(divide='ignore', invalid='ignore'):
        genuine = (~flagged & (q7 > 0) & np.isfinite(q8) & (q8 > 0) & np.isfinite(product)
                   & lu.notna().to_numpy() & ls.notna().to_numpy())
        uv = np.where(genuine, q8 / np.where(q7 > 0, q7, np.nan), np.nan)
    pool = pd.DataFrame({'grappe': frame['grappe'].to_numpy(dtype=float),
                         's00q01': frame['s00q01'].to_numpy(dtype=float),
                         'p': product, 'u': lu.to_numpy(), 's': ls.to_numpy(), 'uv': uv})[genuine]
    has_cell = cu.notna().to_numpy() & cs.notna().to_numpy() & np.isfinite(product)

    def cell(area):
        stats = (pool[np.isfinite(pool[area].to_numpy(dtype=float))]
                 .groupby([area, 'p', 'u', 's'], sort=False)['uv'].agg(['median', 'size'])
                 .reset_index())
        target = pd.DataFrame({area: frame[area].to_numpy(dtype=float), 'p': product,
                               'u': cu.to_numpy(), 's': cs.to_numpy()})
        got = target.merge(stats, on=[area, 'p', 'u', 's'], how='left', validate='many_to_one')
        ok = has_cell & np.isfinite(target[area].to_numpy(dtype=float))
        median = np.where(ok, got['median'].to_numpy(dtype=float), np.nan)
        reports = np.where(ok, got['size'].fillna(0).to_numpy(dtype=float), 0).astype(int)
        return median, reports

    cl_median, cl_reports = cell('grappe')
    rg_median, rg_reports = cell('s00q01')
    use_cluster = cl_reports >= LOCAL_MEDIAN_MIN_CLUSTER_REPORTS
    use_region = ~use_cluster & (rg_reports >= LOCAL_MEDIAN_MIN_REGION_REPORTS)
    value = np.where(use_cluster, cl_median, np.where(use_region, rg_median, np.nan))
    level = np.where(use_cluster, 'cluster', np.where(use_region, 'region', None))
    return pd.DataFrame({'unit_value': value,
                         'level': pd.array(level, dtype='string'),
                         'cluster_reports': cl_reports, 'region_reports': rg_reports},
                        index=frame.index)


def _value_876(frame):
    """The #876 value of each record's purchased row: purchased Quantity x its OWN
    ``s07bq08 / s07bq07a`` where the last purchase used the consumption unit and size,
    else NA (:func:`ehcvm.derive_ehcvm_purchase_value`, the function itself)."""
    from lsms_library.ehcvm import derive_ehcvm_purchase_value
    return np.asarray(derive_ehcvm_purchase_value(
        _purchased_quantity(frame), frame['s07bq03b'], frame['s07bq03c'],
        frame['s07bq07a'], frame['s07bq07b'], frame['s07bq07c'], frame['s07bq08']),
        dtype=float)


def _revalued_records(frame):
    """Boolean ndarray: the records whose purchased row the ruling revalues.

    A flagged record with a positive purchased Quantity whose #876 value is not NA
    (:func:`_value_876`): ruled "Only previously valued" (@ligon 2026-11-12).  The
    2,787 flagged purchased rows #876 serves NA (2,786 whose own last purchase used
    another unit or size, 1 with no ``s07bq08``) are not revalued; they stay NA and
    carry no :data:`LOCAL_MEDIAN_KEY`.
    """
    return (frame['flagged'].to_numpy(dtype=bool) & (_purchased_quantity(frame) > 0)
            & np.isfinite(_value_876(frame)))


def derive_flagged_purchase_local_median(inputs):
    """The registered function: the revalued Expenditure of each flagged purchase.

    ``inputs`` is :func:`inputs_flagged_purchase_local_median`: every record of the
    file, because the genuine ones are the prices.  Returns, indexed as ``inputs``
    is and for the records whose purchased row is revalued (:func:`_revalued_records`):
    ``Expenditure`` = purchased Quantity x ``unit_value`` (NA where no local price),
    ``unit_value``, the ``level`` it came from, and the genuine report counts of the
    record's cluster and region cells.  No options.
    """
    frame = inputs.reset_index(drop=True)
    local = local_median_unit_values(frame)
    hit = _revalued_records(frame)
    local['Expenditure'] = _purchased_quantity(frame) * local['unit_value'].to_numpy(dtype=float)
    local.index = inputs.index
    return local.loc[hit, ['Expenditure', 'unit_value', 'level',
                           'cluster_reports', 'region_reports']]


def _revalue_flagged_purchases(out, df, frame):
    """Write the revalued Expenditure onto the purchased rows the flagged records become.

    ``out`` is the shared hook's output, before any collapse: its first block is one
    purchased row per record whose purchased Quantity or #876 value is positive, in
    file order (``ehcvm.food_acquired_ehcvm`` -> ``food_acquired_to_canonical``).
    That is checked -- the count, the ``s`` level, each row's Quantity, #876
    Expenditure and household -- and :class:`LocalMedianJoinError` is raised if it
    does not hold, so the value is written by position, never by key.  Only the rows
    #876 values are revalued (:func:`_revalued_records`).  Each revalued row gains
    :data:`LOCAL_MEDIAN_KEY` beside the keys it carries, including one the region
    floor serves NA: the rule produced that NA.  A flagged row #876 serves NA is left
    as it is.
    """
    from lsms_library.derivations import union_keys
    from lsms_library.ehcvm import derive_ehcvm_purchase_value

    gifts = df['GiftQuantity'].fillna(0)
    value_876 = derive_ehcvm_purchase_value(
        (df['Quantity'].fillna(0) - df['Produced'].fillna(0) - gifts).clip(lower=0),
        df['ConsumptionUnitCode'], df['ConsumptionUnitSize'], df['LastPurchaseQuantity'],
        df['LastPurchaseUnitCode'], df['LastPurchaseUnitSize'], df['LastPurchaseValue'])
    quantity = _purchased_quantity(frame)
    with np.errstate(invalid='ignore'):
        keep = (quantity > 0) | (value_876 > 0)
    n = int(keep.sum())
    s = np.asarray(out.index.get_level_values('s'), dtype=object)
    is_purchased = s == 'purchased'
    # The scope ("Only previously valued") is read off the valuation frame; it must be
    # the #876 value the hook itself served.
    ok = (np.array_equal(_value_876(frame), np.asarray(value_876, dtype=float), equal_nan=True)
          and int(is_purchased.sum()) == n and bool(is_purchased[:n].all())
          and np.array_equal(_num(out['Quantity'].to_numpy()[:n]), quantity[keep])
          and np.array_equal(_num(out['Expenditure'].to_numpy()[:n]), value_876[keep],
                             equal_nan=True)
          and np.array_equal(pd.Index(out.index.get_level_values('i')[:n]).astype(str).to_numpy(),
                             pd.Index(df.index.get_level_values('i')).astype(str).to_numpy()[keep]))
    if not ok:
        raise LocalMedianJoinError(
            "the shared hook's purchased rows are not the file's records in order; "
            "the revaluation cannot be written by position")
    revalued = _revalued_records(frame) & keep
    if not revalued.any():
        return out
    positions = np.flatnonzero(revalued[keep])
    local = local_median_unit_values(frame)['unit_value'].to_numpy(dtype=float)
    out = out.copy()
    expenditure = _num(out['Expenditure'].to_numpy()).copy()
    expenditure[positions] = quantity[revalued] * local[revalued]
    out['Expenditure'] = expenditure
    derivation = out['Derivation'].astype(object).to_numpy(copy=True)
    for p in positions:
        derivation[p] = union_keys([derivation[p], LOCAL_MEDIAN_KEY])
    out['Derivation'] = pd.Series(derivation, index=out.index, dtype='string')
    return out


def inputs_flagged_purchase_local_median(wave='2018-19'):
    """The registry ``inputs`` callable: every record of the file at ``(t, i, j)``.

    The genuine records are the prices, so all are returned, with the file's own
    names (:data:`LOCAL_MEDIAN_RAW_VARIABLES`: ``s07bq01`` as its code, the unit and
    size variables as the labels the unit match compares, ``s00q01`` from the cover)
    and ``flagged``, the #1708 class.  On a flagged record ``s07bq07a`` and
    ``s07bq08`` are the producer's values: the Expenditure served before the
    revaluation was purchased Quantity x ``s07bq08 / s07bq07a`` where the units
    match (:func:`ehcvm.derive_ehcvm_purchase_value`).
    """
    raw = _computed_raw(wave)
    labelled = _inputs_last_purchase('Niger', wave)
    frame = _valuation_frame(raw, labelled, _region_of(raw, wave))
    frame.index = labelled.index
    return frame


# GH #823: community_prices from s05_co_ner2018.dta.  The registry callables
# for Niger::community_prices::one-item-per-price live HERE (wave-scoped, the
# Nigeria #834 / #900 placement) and the shared logic in niger.py.  Placement
# saves nothing on the cache (every .py in this dir is in every 2018-19
# table's input hash); it keeps the registry path off the country module.
COMMUNITY_PRICES_DERIVATION = 'Niger::community_prices::one-item-per-price'


def derive_s05_one_item_per_price(df):
    """``Quantity = 1`` on every s05 community price row, stamped with the
    registry key in ``Derivation``.

    s05_co_ner2018 records, per (grappe, product, unit-size), two prices and
    NO quantity.  The Niger CAPI price questionnaire (``Qx Communautaire Prix-
    NIGER_V1_VF``, UEMOA catalogue entry 48) asks the enumerator to select up
    to six unit-sizes of the product present in the market (5.02) and to
    record "5.03 Relevé 1 Prix (en FCFA)" and "5.04 Relevé 2 Prix (en FCFA)"
    for each -- the price of ONE item of that unit-size.  So the quantity a
    price was recorded for is one ``u``, by the instrument's construction, not
    by any answer: the served 1 is a construction and carries the key.

    Takes the finished frame (after ``_finish_community_prices``, i.e. after
    every drop) and returns a copy; no options.  The alternative -- serving
    ``Quantity`` NA because the file has no quantity column -- is recoverable
    from ``derivation_inputs``."""
    out = df.copy()
    out['Quantity'] = pd.Series(1.0, index=out.index, dtype='Float64')
    out['Derivation'] = pd.Series(COMMUNITY_PRICES_DERIVATION, index=out.index,
                                  dtype='string')
    return out


def inputs_community_prices(wave='2018-19'):
    """The registry ``inputs`` callable: the raw s05 rows (original variable
    names, all 42,247 source rows) keyed by the served ``(t, v, j, u,
    visit)``, so each served price row finds the record it came from."""
    from niger import _s05_raw_inputs
    return _s05_raw_inputs(_decode_mojibake, wave=wave)

_FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
               'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def _to_fies_bool(s):
    '''Coerce a FIES item column to nullable boolean.  The YAML
    ``mapping: {Oui: True, Non: False}`` runs through df_data_grabber,
    which leaves UNMAPPED values ("Ne sait pas" / "Refus" / NaN)
    UNCHANGED rather than turning them into NaN.  So we keep only genuine
    True/False and send everything else (residual strings, NaN) to
    pd.NA.'''
    return s.map(lambda x: True if x is True else (False if x is False else pd.NA)).astype('boolean')


def food_security(df):
    '''food_security post-processor: coerce the 8 FIES items to nullable
    boolean and compute FIES_score.

    FIES_score is the count of True across the 8 items, and is NaN only
    when ALL 8 items are NaN (the question was not asked at all).
    Partial NaNs count as not-True (FAO scoring).
    '''
    for c in _FIES_ITEMS:
        df[c] = _to_fies_bool(df[c])
    items = df[_FIES_ITEMS]
    all_na = items.isna().all(axis=1)
    score = items.eq(True).sum(axis=1)
    df['FIES_score'] = score.where(~all_na).astype('Int64')
    return df


COPING_LABELS = {
    1: "Utilisation de son épargne",
    2: "Aide de parents ou d'amis",
    3: "Aide du gouvernement/l'Etat",
    4: "Aide d'organisations religieuses ou d'ONG",
    5: "Marier les enfants",
    6: "Changement des habitudes de consommation",
    7: "Achat d'aliments moins chers",
    8: "Membres actifs ont pris des emplois supplémentaires",
    9: "Membres adultes inactifs/chômeurs ont pris des emplois",
    10: "Enfants de moins de 15 ans amenés à travailler",
    11: "Les enfants ont été déscolarisés",
    12: "Migration de membres du ménage",
    13: "Réduction des dépenses de santé/d'éducation",
    14: "Obtention d'un crédit",
    15: "Vente des actifs agricoles",
    16: "Vente des biens durables du ménage",
    17: "Vente de terrain/immeubles/Maisons",
    18: "Louer/mettre ses terres en gages",
    19: "Vente du stock de vivres",
    20: "Pratique plus importante des activités de pêche",
    21: "Vente de bétail",
    22: "Confiage des enfants à d'autres ménages",
    23: "Engagé dans des activités spirituelles",
    24: "Pratique de la culture de contre saison",
    25: "Autre stratégie",
    26: "Aucune stratégie",
}


def shocks(df):
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: [], 2: []}
    for _, row in df[cope_cols].iterrows():
        found = []
        for c in cope_cols:
            num = int(c.replace('Cope', ''))
            val = row[c]
            try:
                val = float(val)
            except (ValueError, TypeError):
                continue
            if val >= 1:
                found.append(COPING_LABELS.get(num, f'Strategy {num}'))
            if len(found) == 3:
                break
        for k in range(3):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df['HowCoped2'] = how_coped[2]
    df = df.drop(columns=cope_cols)
    return df


def v(value):
    '''
    Formatting cluster id
    '''
    return tools.format_id(value)


def i(value):
    '''
    Formatting household id from (grappe, menage).
    Uses '0' separator + zero-padded menage to prevent collisions
    (e.g., grappe=1,menage=23 vs grappe=12,menage=3).
    Adds 'E_' prefix for EHCVM waves to prevent panel collision with ECVMA waves.
    '''
    grappe = tools.format_id(value.iloc[0])
    menage = tools.format_id(value.iloc[1], zeropadding=2)
    if grappe is None or menage is None:
        return None
    return 'E_' + grappe + '0' + menage


def Sex(value):
    '''
    Formatting sex variable (Stata label: 1=Masculin, 2=Féminin)
    '''
    if value in ('Masculin', 'Masculin'):
        return 'M'
    if value in ('Féminin', 'Feminin'):
        return 'F'
    s = str(value).strip()
    if s == '1':
        return 'M'
    if s == '2':
        return 'F'
    return pd.NA


def Age(value):
    '''
    Pre-process Age list: convert French month name (s01q03b) to integer.
    Sentinel 9999 for any component is left as-is; age_handler's is_valid()
    rejects values >= 2100 (covers 9999).

    The function name ``Age`` collides with the ``assets`` myvar
    ``Age: s12q07`` (item age in years), which the framework binds to
    this formatter by name (column_mapping in country.py).  That path
    passes a *scalar*, not the roster's DOB Series; pass scalars
    through unchanged so assets extraction does not crash on
    ``list(<float>)`` (closes #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    month_map = {
        'Janvier': 1, 'Fevrier': 2, 'Février': 2, 'Mars': 3, 'Avril': 4,
        'Mai': 5, 'Juin': 6, 'Juillet': 7, 'Aout': 8, 'Août': 8,
        'Septembre': 9, 'Octobre': 10, 'Novembre': 11, 'Decembre': 12,
        'Décembre': 12,
    }
    result = list(value)
    # s01q03b (index 2) may be a French month name or numeric/sentinel
    raw_month = value.iloc[2]
    if isinstance(raw_month, str):
        result[2] = month_map.get(raw_month, None)
    return result


def Relationship(value):
    '''
    Formatting relationship variable
    '''
    if value:
        return str(value).title()


def household_roster(df):
    '''
    Compute Age from date-of-birth components using canonical age_handler.
    s01q04a = age in years (direct)
    s01q03a = day of birth
    s01q03b = month of birth (French text, already converted to int by Age())
    s01q03c = year of birth
    Sentinel 9999 in any component is handled by age_handler's is_valid() check.

    Note the two roster/food files of this wave carry DIFFERENT defects:
    s07b's labels are raw UTF-8 bytes read as Latin-1 (``_decode_mojibake``
    above, a latin-1 -> utf-8 round trip); s01's are cp1252 bytes read as
    Latin-1, which the reader re-decodes since GH #805, so the
    ``Frère, Sœur`` Relationship label arrives clean and the GH #801
    ``_decode_cp1252`` / ``_fix_relationship`` pair is retired.
    '''

    def _age_from_row(x):
        age_raw = x['Age'][0]
        # 9999 is not a valid age (> 130); treat as missing
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) >= 9999) else age_raw
        result = tools.age_handler(
            age=age_val,
            d=x['Age'][1],
            m=x['Age'][2],
            y=x['Age'][3],
            interview_date=x['interview_date'],
            interview_year=2018,
        )
        return np.nan if (pd.notna(result) and result < 0) else result

    df['Age'] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df
