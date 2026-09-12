#!/usr/bin/env python
"""Malawi-specific helpers for wave-level food_acquired.py scripts.

The live surface is three functions used by the four IHS3+ wave scripts
(2010-11, 2013-14, 2016-17, 2019-20) to apply Malawi's region-keyed
unit-conversion CSV and to resolve the other-specify free-text unit label.
"300 grams"-style labels are NOT handled there: they are converted for
every wave at the single choke point in ``food_acquired_to_canonical``, by
``_metric_kg_factor`` (GH #878).
Other helpers (roster decomposition, get_other_features, etc.) were
removed in 2026-05-05 alongside the shadowed
food_prices_quantities_and_expenditures.py — see GH #218.

It has since grown the feature-assembly helpers the wave scripts share
(plot_features, crop_production, plot_inputs, livestock, plot_labor,
anthropometry, people_last7days, community_prices) and, as of GH #854,
``crop_conversion_factors`` / ``with_region`` -- the loader for the two
SHIPPED IHS5 crop conversion tables, for use as
``harvest_kg(cp, shipped_factors=...)``.
"""

import pandas as pd
import numpy as np
import re
import sys
import warnings
sys.path.append('../../../_/')
from lsms_library.local_tools import conversion_table_matching_global, format_id, melt_visit_intervals


class SaleAttachmentWarning(UserWarning):
    """A reported household-crop sale that could not be placed on a harvest row.

    Raised by :func:`assemble_crop_production` when a plot-crop is reported in
    more than one ``condition`` within the same unit: the sale module gives one
    household-crop-unit total and does not say which state it refers to, so
    attaching it to both rows would double-count it.  The sale is dropped and
    the warning names how many and how much -- see GH #854.
    """


def _extract_kg_conversion(series):
    """kg-per-unit factors parsed out of a free-text unit label.

    Reads a metric magnitude embedded in the label -- '300 grams' -> 0.3,
    '50kg bag' -> 50 -- and returns a float Series ALIGNED to *series*
    (NaN where neither pattern matches).

    Two things were wrong here until GH #878 (2026-09-12).  Grams were
    scaled by 0.01, so '300 grams' came back 3.0 kg instead of 0.3; and the
    return was a ``concat(...).dropna()``, which only happened to align on
    assignment because at most one of the two patterns ever matches a given
    label -- had both matched, the duplicate index labels would have raised.
    Neither defect ever reached a served number: the sole live caller is
    the 2004-05 wave script, whose unit column is a closed 22-label set in
    which nothing matches the grams pattern and only '50kg bag' / '90 kg
    bag' match the kilogram one.  Grams-style free text in the IHS3+ waves
    is converted at the single choke point in
    :func:`food_acquired_to_canonical` by :func:`_metric_kg_factor`, whose
    table has grams at the correct 0.001.

    A kilogram match wins over a grams match on the same label; no label in
    the corpus matches both, so that precedence is unexercised.
    """
    grams = r'(\d+)\s*g(?:\s+|r)'
    kgs = r'(\d+)\s*k(?:g|ilo)'

    lower = series.astype(str).str.lower()
    g = lower.str.extract(grams)[0].astype(float) * 0.001
    k = lower.str.extract(kgs)[0].astype(float)
    return k.combine_first(g)


def _clean_freetext_unit(value):
    """Tidy a respondent other-specify unit string (GH #223 Layer 2).

    These are free text like ``'1 BASKET'``, ``'1 NSIMA PLATE (PHAZI)'``,
    ``'1/4'``.  Drop ONLY a leading count of *one* (``'1 BASKET'`` ->
    ``'Basket'``) and title-case; a bare quantity with no unit (``'1/4'``,
    ``'0.5'``, ``'0'``) -> NA.

    Deliberately conservative on the leading number: a count of one is a
    safe no-op multiplier, but a *larger* leading number may be a magnitude
    that defines the unit's kg-equivalence (``'10Kgs'``, ``'10G Packet'``,
    ``'10 Litre Bucket'``).  Stripping or relabelling those would corrupt the
    kg quantity by an order of magnitude, so they are left untouched (their
    kg conversion, where known, is handled separately by the conversion
    factor; the residual ``u`` label simply stays as-is).  Only ever applied
    to the genuine other-specify column, never to standard/sized labels.
    """
    if pd.isna(value):
        return value
    s = str(value).strip()
    if s.lower() in ('nan', ''):
        return pd.NA
    if re.fullmatch(r'[\d.,/]+', s):          # bare quantity, no unit -> NA
        return pd.NA
    # Drop a leading count of one only ('1 Basket' -> 'Basket').  Never a
    # larger leading number -- it may be a magnitude (see docstring).
    s = re.sub(r'^\s*1\s+', '', s).strip()
    if (not s) or re.fullmatch(r'[\d.,/]+', s):
        return pd.NA
    return s.title()


_METRIC_KG = {  # unit token -> kg per 1 of that unit (volume at water density 1)
    'kg': 1.0, 'kgs': 1.0, 'kilo': 1.0, 'kilos': 1.0,
    'kilogram': 1.0, 'kilograms': 1.0, 'kilogramme': 1.0, 'kilogrammes': 1.0,
    'g': 0.001, 'gram': 0.001, 'grams': 0.001, 'gramme': 0.001, 'grammes': 0.001,
    'ml': 0.001, 'mls': 0.001, 'millilitre': 0.001, 'millilitres': 0.001,
    'millitre': 0.001, 'milimitre': 0.001, 'milimiter': 0.001, 'millimeter': 0.001,
    'l': 1.0, 'ls': 1.0, 'litre': 1.0, 'litres': 1.0, 'liter': 1.0, 'liters': 1.0,
}

# Known container nouns that may trail a metric magnitude.  A glued/spaced
# "<num><metric-unit><container>" converts ONLY when the trailing word is one
# of these -- so '90Kgbag'/'10G Packet' convert but '10Giraffes' does not.
_CONTAINERS = {
    'packet', 'packets', 'bag', 'bags', 'tin', 'tins', 'bottle', 'bottles',
    'sachet', 'sachets', 'satchet', 'satchets', 'pail', 'pails',
    'plate', 'plates', 'cup', 'cups', 'tube', 'tubes', 'can', 'cans',
    'crate', 'crates', 'pack', 'packs', 'bucket', 'buckets', 'carton', 'cartons',
    'pale', 'pales', 'container', 'containers', 'paint', 'jar', 'jars',
}


def _metric_kg_factor(value):
    """kg-equivalent of a unit string that *leads* with a metric magnitude.

    Table-driven (``_METRIC_KG`` units x ``_CONTAINERS``): a leading
    ``<number><metric-unit>`` -- optionally followed by a known container,
    spaced or glued -- converts to its kg weight by scaling the quantity
    (GH #223 Layer 2), so '50 kg bag' and '90Kgbag' both -> 50 / 90 kg, and
    '10G Packet'/'10Gpacket' -> 0.01.  litres/ml use water density 1.

    The trailing word, if any, MUST be a known container, so a number glued
    to a non-container word ('10Giraffes', '5Lions', '1Mlambe') returns None
    and is left as-is rather than being scaled by a spurious metric prefix.
    """
    if pd.isna(value):
        return None
    m = re.match(r'\s*(\d+(?:[.,]\d+)?)\s*([a-z].*)$', str(value).strip().lower())
    if not m:
        return None
    num = float(m.group(1).replace(',', '.'))
    rest = m.group(2).strip()
    for unit in sorted(_METRIC_KG, key=len, reverse=True):
        if rest == unit:
            return num * _METRIC_KG[unit]
        if rest.startswith(unit):
            tail = rest[len(unit):].strip(' .,-')
            if tail == '':
                return num * _METRIC_KG[unit]
            # Convert when the magnitude is followed by a known container
            # (first word, so 'bottle super' / 'bucket(chigoba)' count) or by
            # an 'of <item>' descriptor ('25Gram Of Uchi').  A non-container,
            # non-'of' word ('10Giraffes' -> 'iraffes') is rejected.
            first = re.split(r'[\s(),.]+', tail, maxsplit=1)[0]
            if first in _CONTAINERS or first == 'of':
                return num * _METRIC_KG[unit]
    return None


def _norm_unit_code(value):
    """Canonicalize a raw unit *code* to zero-padded-2 + lowercase suffix.

    Malawi's waves emit the same code in different forms -- 2013-14
    zero-pads ('01', '04A'), 2016-17/2019-20 don't ('1', '4A', '10A').
    Normalizing to the questionnaire/table form ('01', '04a', '10a') lets a
    single #+name:u table decode every wave (GH #383).  Only pure
    ``<digits><optional-single-letter>`` codes are touched; labels, the 'kg'
    sentinel, and multi-letter strings pass through unchanged.
    """
    if pd.isna(value):
        return value
    s = str(value).strip()
    m = re.fullmatch(r'(\d+)([A-Za-z]?)', s)
    if not m:
        return value
    return m.group(1).zfill(2) + m.group(2).lower()


def _titlecase_label(value):
    """Title-case a unit label so case-variants collapse (``'PIECE'`` ->
    ``'Piece'``); leaves the lowercase ``'kg'`` sentinel and sized labels
    (no leading number is stripped here) intact."""
    if pd.isna(value):
        return value
    s = str(value).strip()
    if s.lower() in ('nan', ''):
        return pd.NA
    return s.title()


def handling_unusual_units(df, suffixes=None):
    """Resolve each source's ``u`` label from its other-specify free text.

    For every suffix, the respondent's other-specify string
    (``unitsdetail_{suffix}``) is tidied (GH #223 Layer 2) and used as the
    unit label ``u_{suffix}``, falling back to the standard label
    (``units_{suffix}``) where there is none.  It does NOT touch
    ``cfactor_{suffix}`` (which comes from the wave's region-keyed
    conversion table) and does NOT convert quantities: the summable
    ``Quantity_kg`` is computed per source in
    :func:`food_acquired_to_canonical` (GH #378), and metric free-text
    labels are converted there by :func:`_metric_kg_factor` (GH #878).

    Parameters
    ----------
    df : DataFrame
    suffixes : list[str], optional
        Column suffixes to process (e.g. ``['consumed', 'bought']``).
        For each suffix, expects columns ``unitsdetail_{suffix}`` and
        ``units_{suffix}``.
        Defaults to ``['consumed', 'bought']`` for backward compatibility.
    """
    if suffixes is None:
        suffixes = ['consumed', 'bought']

    for suffix in suffixes:
        detail_col = f'unitsdetail_{suffix}'
        units_col = f'units_{suffix}'
        u_col = f'u_{suffix}'

        if detail_col not in df.columns:
            continue

        # GH #878: a "300 grams" regex fallback used to be written into
        # cfactor here as ``x[c] or conv_kg``.  It never fired -- ``x[c]`` is
        # the merged conversion-table factor, NaN where unmatched, and
        # ``bool(nan)`` is True, so the expression always returned ``x[c]``
        # (and the conversion tables hold no zero factor that could make it
        # falsy: IHS3 min 0.02 over 831 rows, IHS5 min 9.6e-05 over 1,768).
        # It is removed rather than repaired: the metric free-text labels it
        # aimed at are converted for every wave at the single choke point in
        # food_acquired_to_canonical by _metric_kg_factor, and making this
        # fallback live would set cfactor, hence has_cf, hence SKIP that
        # choke point -- serving the same kilograms under a different
        # (u, Quantity) row shape.  See CONTENTS.org 2026-09-12.

        # Tidy the other-specify free text for use as a `u` label:
        # "1 Basket" -> "Basket", "1/4" -> NA (#223 Layer 2).
        df[detail_col] = df[detail_col].map(_clean_freetext_unit)

        # Migration to GH #378's Quantity_kg: keep the NATIVE quantity and the
        # native unit label; do NOT multiply in place or stamp the 'kg'
        # sentinel here.  The summable Quantity_kg = quantity x cfactor is
        # computed per source in food_acquired_to_canonical, so food_acquired
        # regains native units (and unitvalue) while food_quantities(kgs)
        # stays identical.
        df[u_col] = df[detail_col].replace('nan', pd.NA).fillna(
            df[units_col].map(_titlecase_label))

    return df


def Sex(value):
    if isinstance(value, str) and value.strip():
        return value.strip().upper()[0]
    else:
        return np.nan


def malawi_date_ymd(row):
    """Combine a [year, month, day] row into a Timestamp.

    Used by the ``interview_date`` table for the waves that store the
    interview date as three separate columns (2004-05 IHS2: numeric
    a14a/b/c; 2013-14 IHS3: hh_a23a_* with the month as an English name
    like 'MAY').  Declare the columns in year, month, day order in the
    wave's data_info.yml ``int_t`` myvar with a trailing
    ``mapping: malawi_date_ymd``.

    The month component may be numeric (5) or a name ('MAY'); both are
    handled by building a 'DAY MONTH YEAR' string and letting
    ``pd.to_datetime`` parse it.  Returns ``pd.NaT`` when any part is
    missing or the date is unparseable.
    """
    y, m, d = row.iloc[0], row.iloc[1], row.iloc[2]
    if pd.isna(y) or pd.isna(m) or pd.isna(d):
        return pd.NaT
    # Month may be numeric (float/int) or an English name.
    if isinstance(m, str):
        month = m.strip()
    else:
        month = str(int(m))
    return pd.to_datetime(f"{int(d)} {month} {int(y)}", errors='coerce')


def interview_date(df):
    """Melt Malawi's per-visit interview dates onto a ``visit`` index level.

    Country-level ``df_edit`` hook for the ``interview_date`` table
    (auto-dispatched by table name; see country.py ``grab_data`` ->
    ``formatting_functions.get(request)``).  This is the first application
    of the grain-aggregation-policy design (SkunkWorks/
    grain_aggregation_policy.org): the raw cover files of Malawi's panel
    waves record TWO interview-visit dates per household, and we preserve
    BOTH on an ordinal ``visit`` level rather than discarding Visit 2.

    Input
    -----
    df : pd.DataFrame
        Indexed per the wave's ``idxvars`` (after the framework prepends
        ``t``, i.e. ``(t, i)``), carrying the Visit-1 interview date in a
        ``int_t`` / ``Int_t`` column and -- where the wave records a second
        visit -- the Visit-2 date in ``int_t_v2`` / ``Int_t_v2``.  The
        column casing depends on whether the canonical ``int_t -> Int_t``
        rejected-spelling rewrite has run yet (it has NOT at this wave-level
        hook stage), so both spellings are accepted defensively.

    Output
    ------
    pd.DataFrame indexed ``(<original idx>, visit)`` -- i.e. ``(t, i,
    visit)`` -- with a single ``Int_t`` datetime column.  ``visit`` is an
    ordinal Int64 level: ``1`` carries the Visit-1 date, ``2`` the Visit-2
    date.  Visit-2 rows whose date is NaT (households not revisited, the
    single-visit IHS2 wave, and the IHS4/IHS5 cross-sectional halves which
    have no Visit 2) are DROPPED -- never fabricated.  A wave with no
    Visit-2 column simply yields ``visit=1`` rows.

    The framework then prepends ``t`` (already present) and joins ``v``
    from ``sample()`` at API time, giving the returned grain
    ``(t, v, i, visit)``.  Collapsing ``visit`` with the declared ``first``
    aggregation (data_scheme.yml) reproduces the legacy Visit-1-only table.

    Implementation delegates to the shared
    :func:`lsms_library.local_tools.melt_visit_intervals` helper.  Malawi
    records only an interview *date* per visit (no separate start/end), so we
    pass ``start_base='int_t'`` and keep the legacy ``Int_t`` output column;
    the EHCVM family reuses the same helper with start/end timestamps.
    """
    return melt_visit_intervals(df, start_base='int_t', out_start='Int_t')


def harmonize_food_labels(df, level='i'):
    """Apply the cross-wave union of Malawi's harmonize_food map to ``df``.

    The wave-level food_acquired.py scripts apply
    ``df['i'].astype(str).str.capitalize()`` before renaming, which produces
    sentence-cased labels (e.g. ``'Sugar cane'``).  The per-wave columns of
    ``harmonize_food`` in ``categorical_mapping.org`` mix Title-case and
    sentence-case entries, so the per-wave rename via
    ``get_categorical_mapping(idxvars={'j': wave})`` silently misses any
    label whose harmonize_food entry is in a different case than the
    post-``.capitalize()`` data — see GH #216.

    This helper sidesteps the drift by building a single label map from
    *all* wave columns of ``harmonize_food`` (including each value's
    ``.capitalize()`` variant) and applying it once.  A label that's
    documented in *any* wave column gets resolved to its Preferred Label
    regardless of which wave's data we're processing.

    The Preferred Label column is honoured as-is; any truncation (e.g.
    ``'Maize Ufa Mgaiwa (Normal F'``) carries through to the output.
    Truncation cleanup is a separate concern (GH #169 / #216 follow-up).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame whose index includes the food-item level.
    level : str, default 'i'
        Index level name carrying the food labels.  In Malawi's wave-level
        builds the item lives on ``'i'`` (the framework's ``map_index``
        swaps it to canonical ``'j'`` downstream).

    Returns
    -------
    pd.DataFrame
        ``df`` with food labels remapped to Preferred Labels where the
        union map covers them.  Labels not in the map pass through
        unchanged.
    """
    import os
    from lsms_library.local_tools import all_dfs_from_orgfile

    org_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'categorical_mapping.org')
    hf = all_dfs_from_orgfile(org_path)['harmonize_food']

    unify = {}
    skip_cols = {'Preferred Label', 'GD Category'}
    for col in hf.columns:
        if col in skip_cols:
            continue
        for _, row in hf.iterrows():
            v = row.get(col)
            p = row.get('Preferred Label')
            if pd.isna(v) or pd.isna(p):
                continue
            v_str = str(v).strip()
            if v_str in ('', '---'):
                continue
            # Map both the literal harmonize_food entry and its
            # .capitalize() form (since wave scripts apply .capitalize()
            # to the data before this rename runs).
            unify.setdefault(v_str, p)
            unify.setdefault(v_str.capitalize(), p)

    return df.rename(index=unify, level=level)

def conversion_table_matching(df, conversions, conversion_label_name, num_matches=3, cutoff=0.6):
    return conversion_table_matching_global(df, conversions, conversion_label_name,
                                            num_matches=num_matches, cutoff=cutoff)


# ---- Food-label normalization & harmonize_food application ----------------
#
# Three flavors of mangled en-dash show up in the raw food-item .dta values
# across waves, depending on the source encoding and pyreadstat decode path:
#   - '\x96'  : cp1252 byte for en-dash, preserved when the file is read as
#               latin1 (seen in 2010-11, 2013-14).
#   - '�': Unicode replacement char from a failed UTF-8 decode (2016-17).
#   - 'ï¿½'   : UTF-8 mojibake of '�' (the bytes 0xef 0xbf 0xbd
#               re-decoded as latin1 and re-encoded as UTF-8) (2016-17).
# All three should become a proper en-dash before the harmonize_food rename,
# otherwise rows like 'Citrus – naartje, orange, etc.' fail to match.

_ENDASH_MOJIBAKE = [('\x96', '–'), ('ï¿½', '–'), ('�', '–')]


def normalize_food_label(s):
    """Replace mangled en-dashes in a food-label Series.

    Apply *after* ``.str.capitalize()`` in wave scripts so that the data
    side matches the dict keys produced by :func:`apply_harmonize_food`.
    """
    out = s
    for bad, good in _ENDASH_MOJIBAKE:
        out = out.str.replace(bad, good, regex=False)
    return out


def _normalize_label_key(k):
    """Normalize a single dict key to mirror the wave-script data path.

    Applies ``str.capitalize()`` (single-word title-case as in every wave
    script's ``df['i'] = ... .str.capitalize()`` line) followed by the same
    en-dash repair as :func:`normalize_food_label`.  2004-05's wave script
    skips ``capitalize()`` but its column entries in categorical_mapping.org
    are already in capitalize-form, so this is a no-op there.
    """
    if not isinstance(k, str):
        return k
    out = k.capitalize()
    for bad, good in _ENDASH_MOJIBAKE:
        out = out.replace(bad, good)
    return out


def food_acquired_to_canonical(df, wave):
    """Reshape Malawi wide-form ``food_acquired`` to canonical long form.

    Phase 3 of GH #169 / DESIGN_food_acquired_canonical_2026-05-05.org.

    Inputs
    ------
    df : DataFrame
        Wave-level wide-form output produced by the per-wave food_acquired.py
        scripts after all per-source unit-conversion machinery has run.
        Index ``(j, t, i)`` per Malawi's legacy convention where ``j`` is
        the household ID and ``i`` is the food item (opposite of the
        canonical LSMS convention).  Recognized columns:

        * ``quantity_bought``, ``u_bought``, ``expenditure``
          (purchased rows; Expenditure populated)
        * ``quantity_produced``, ``u_produced``  (produced rows;
          Expenditure NaN)
        * ``quantity_gifted``, ``u_gifted``      (in-kind rows;
          Expenditure NaN)

        Any of ``quantity_consumed``, ``u_consumed``, ``cfactor_*``,
        ``price per unit`` (and other vestigial columns) are silently
        ignored — only the per-source columns above are read.
    wave : str
        Wave label (e.g. ``'2010-11'``) — passed through to
        :func:`apply_harmonize_food` for the food-label rename.

    Output
    ------
    DataFrame indexed by canonical ``(t, i, j, u, s)`` where
    ``i`` is the household ID and ``j`` is the food item (the legacy
    Malawi ``j↔i`` swap is handled inside this function).
    Columns: ``Quantity``, ``Expenditure``.
    ``s`` ∈ ``{'purchased', 'produced', 'inkind'}``.

    Notes
    -----
    - Rows are kept where EITHER ``Quantity > 0`` OR ``Expenditure > 0``
      (matches the shared
      :func:`lsms_library.transformations.food_acquired_to_canonical`
      rule).  An expenditure-only row (HH reported food expenditure but
      no quantity) is legitimate data and is carried through with NaN
      ``Quantity``.
    - Food labels are normalized via :func:`apply_harmonize_food` at
      ``level='j'`` before returning.
    - ``v`` is intentionally absent — the framework joins it from
      ``sample()`` at API time; see CLAUDE.md "## ``sample()`` and
      Cluster Identity".
    """
    work = df.reset_index()
    # Swap legacy Malawi (j=HHID, i=item) to canonical (i=HHID, j=item).
    work = work.rename(columns={'j': '_i_canon', 'i': '_j_canon'})
    work = work.rename(columns={'_i_canon': 'i', '_j_canon': 'j'})

    # Convert any *pure metric* unit string ('10Kgs', '149G', '250 Ml') to the
    # 'kg' sentinel by scaling its quantity (GH #223 Layer 2).  '10Kgs' is
    # 10 kg -- relabelling it 'Kg' without scaling would be off by 10x, so we
    # scale instead.  Non-metric / container strings ('Basket', '10G Packet')
    # are left untouched.  Applies across every wave at this single choke point.
    for ucol, qcol, ccol in [('u_bought', 'quantity_bought', 'cfactor_bought'),
                             ('u_produced', 'quantity_produced', 'cfactor_produced'),
                             ('u_gifted', 'quantity_gifted', 'cfactor_gifted')]:
        if ucol in work.columns and qcol in work.columns:
            factor = work[ucol].map(_metric_kg_factor)
            # Skip rows that already have a per-row cfactor: those become an
            # exact Quantity_kg in _make, so scaling the quantity here as
            # well would double-count (GH #378 / Malawi migration).  Where
            # cfactor comes from is wave-dependent: IHS3+ take it solely
            # from the region-keyed conversion table, while IHS2 (2004-05)
            # has no such table and takes it only from a kilogram magnitude
            # read off the unit label itself ('50kg bag' -> 50), via
            # _extract_kg_conversion.  In neither case does it carry a
            # grams magnitude -- that is this loop's job (GH #878).
            has_cf = work[ccol].notna() if ccol in work.columns else False
            mask = factor.notna() & ~has_cf
            if mask.any():
                # Coerce the quantity column to float64 *in place* before the
                # masked write.  Stata reads quantities as float32; pandas 3.0
                # refuses to setitem a float64 product (e.g. q*1.3) back into a
                # float32 block when it isn't losslessly representable
                # (LossySetitemError).  Widening the column first keeps the
                # values identical and the assignment lossless.
                q = pd.to_numeric(work[qcol], errors='coerce').astype('float64')
                work[qcol] = q
                work.loc[mask, qcol] = q[mask] * factor[mask].astype(float)
                work.loc[mask, ucol] = 'kg'
            # Normalize any remaining raw unit *code* to the canonical
            # zero-padded-2 + lowercase-suffix form ('1'->'01', '4A'->'04a',
            # '10A'->'10a') so all waves match the #+name:u table (GH #383),
            # which then decodes them to Preferred Labels at finalize.
            work[ucol] = work[ucol].map(_norm_unit_code)

    def _make(source_label, quant_col, unit_col, value_col=None,
              cfactor_col=None):
        if quant_col not in work.columns:
            return None
        qty = pd.to_numeric(work[quant_col], errors='coerce')
        out = pd.DataFrame({
            't': work['t'].values,
            'i': work['i'].values,
            'j': work['j'].values,
            'u': (work[unit_col].values if unit_col in work.columns
                  else pd.NA),
            's': source_label,
            'Quantity': qty.values,
        })
        # Summable exact kg = native quantity x per-source cfactor (GH #378 /
        # Malawi migration).  NaN where no cfactor -> food_quantities falls
        # back to the unit->factor map.
        if cfactor_col is not None and cfactor_col in work.columns:
            out['Quantity_kg'] = (
                qty * pd.to_numeric(work[cfactor_col], errors='coerce')).values
        if value_col is not None and value_col in work.columns:
            out['Expenditure'] = pd.to_numeric(work[value_col],
                                               errors='coerce').values
        else:
            # Use np.nan (float64) rather than pd.NA so the all-missing
            # Expenditure column for produced/inkind pieces concatenates
            # with the same dtype as the populated 'purchased' piece.
            # Mismatched dtypes here trigger pandas 3.0's FutureWarning
            # about all-NA columns at pd.concat dtype inference.  Float
            # NaN is appropriate per CLAUDE.md "Pandas 3.0 Targets" --
            # numeric float columns prefer np.nan over pd.NA.
            out['Expenditure'] = np.nan
        return out

    pieces = []
    for src, qcol, ucol, vcol, ccol in [
        ('purchased', 'quantity_bought',   'u_bought',   'expenditure', 'cfactor_bought'),
        ('produced',  'quantity_produced', 'u_produced', None,          'cfactor_produced'),
        ('inkind',    'quantity_gifted',   'u_gifted',   None,          'cfactor_gifted'),
    ]:
        piece = _make(src, qcol, ucol, value_col=vcol, cfactor_col=ccol)
        if piece is not None:
            pieces.append(piece)

    if not pieces:
        raise ValueError(
            "food_acquired_to_canonical: no source columns "
            "(quantity_bought / quantity_produced / quantity_gifted) "
            "found in input"
        )

    from lsms_library.transformations import _finalize_canonical_food_acquired

    out = pd.concat(pieces, ignore_index=True)
    # Filter (qty>0 | exp>0; expenditure-only rows kept with NaN Quantity)
    # and sum genuine source-data duplicates -- e.g. two ``Other (Specify)``
    # rows under one (item, unit, source) key (observed 2013-14 HH 1508-006,
    # 2019-20) -- via the shared tail (GH #251).  Malawi has no Price column,
    # so Quantity/Expenditure summed with min_count=1 reproduces the prior
    # blanket ``.sum(min_count=1)`` exactly.
    out = _finalize_canonical_food_acquired(out)

    # Normalize food labels on the canonical 'j' level.
    out = apply_harmonize_food(out, wave, level='j')
    return out


def apply_harmonize_food(df, wave, level='i'):
    """Rename *level* of *df*'s index via Malawi's harmonize_food table.

    Builds a ``{wave-column-label -> Preferred Label}`` dict from
    ``../../_/categorical_mapping.org#harmonize_food``, normalizes each
    dict key with :func:`_normalize_label_key` so that case drift and
    encoding mojibake between the .dta source and the org table never
    silently break the mapping, then applies the rename at *level*.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame whose index includes a food-label level.
    wave : str
        Wave label (e.g. ``'2010-11'``) -- selects which column of
        ``harmonize_food`` carries the source-side labels.
    level : str, default 'i'
        Index level name carrying the food labels.  Phase 3 reshape
        passes ``'j'`` (food on the canonical j-axis).

    Returns
    -------
    pd.DataFrame
        ``df`` with food labels remapped to Preferred Labels where the
        wave column covers them; labels not in the map pass through
        unchanged.
    """
    from lsms_library.local_tools import get_categorical_mapping
    raw = get_categorical_mapping(tablename='harmonize_food',
                                  idxvars={'_k': wave},
                                  **{'_v': 'Preferred Label'})
    labelsd = {_normalize_label_key(k): v
               for k, v in raw.items()
               if pd.notna(k) and pd.notna(v)}
    return df.rename(index=labelsd, level=level)



# ---------------------------------------------------------------------------
# plot_features (GH #167)
# ---------------------------------------------------------------------------
# Lasting plot-level characteristics for the four buildable IHS/IHPS waves
# (2010-11, 2013-14, 2016-17, 2019-20).  Module C carries plot area (farmer
# estimate ag_c04a + unit ag_c04b, or GPS-measured ag_c04c in acres);
# Module D carries soil type (ag_d21), irrigation/water source (ag_d28a),
# and -- in 2010-11 & 2013-14 ONLY -- the tenure/acquire question ag_d03.
# 2016-17 & 2019-20 ag_mod_d have NO ag_d03 (ag_d02 there is "ID of
# Respondent", not tenure), so Tenure is NaN for those two waves.
#
# The C<->D merge is on (hhid, plotkey).  2004-05 (IHS2) is DEFERRED -- it
# has no standard plot roster.  See ../_/CONTENTS.org and the validated
# recon recipe slurm_logs/2026-06-03_session/RECON_Malawi.md.

ACRES_TO_HECTARES = 0.404686


def _malawi_code_map(tablename, here=None):
    """Load a {int code: Preferred Label} dict from the Malawi
    categorical_mapping.org table ``tablename`` (Code-keyed).

    Resolves the org file relative to this module first so wave-script
    CWDs (``Malawi/<wave>/_``) still find it.  Codes whose Preferred
    Label is missing / '---' map to pd.NA."""
    import os
    from lsms_library.local_tools import df_from_orgfile

    if here is None:
        here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, 'categorical_mapping.org'),
        os.path.abspath(os.path.join('..', '..', '_', 'categorical_mapping.org')),
        'categorical_mapping.org',
    ]
    orgfn = next((c for c in candidates if os.path.exists(c)), candidates[0])

    df = df_from_orgfile(orgfn, name=tablename, set_columns=True, to_numeric=True)
    out = {}
    for _, row in df.iterrows():
        c = row['Code']
        try:
            c = int(c)
        except (TypeError, ValueError):
            continue
        lab = row.get('Preferred Label')
        if pd.isna(lab) or str(lab).strip() in ('---', ''):
            out[c] = pd.NA
        else:
            out[c] = str(lab).strip()
    return out


def _map_codes(series, code_map):
    """Map a numeric (raw Stata code) Series through ``code_map``
    ({int: str}).  Returns a nullable-string Series, NaN where the code
    is absent from the map.  Source must be loaded with
    convert_categoricals=False so the codes are numeric."""
    out = pd.to_numeric(series, errors='coerce').astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_features_for_wave(t, df_c, df_d, colmap):
    """Build canonical ``plot_features`` for one Malawi IHS/IHPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2010-11"``), used as the ``t`` index value.
    df_c : pd.DataFrame
        Module C (area) rows, loaded with convert_categoricals=False,
        with an ``hhid`` column already set to the canonical wave
        household id string (cs-17 prefix applied for the 2016-17
        cross-sectional half by the caller) and a ``plotkey`` column
        uniquely identifying the plot within the household.
    df_d : pd.DataFrame | None
        Module D (soil / irrigation / tenure) rows, same ``hhid`` /
        ``plotkey`` convention.  ``None`` is permitted (Tenure / SoilType
        / Irrigated then all NaN), but every buildable wave has one.
    colmap : dict
        Column-name map.  Keys:
            area_est   — farmer-estimated area column in df_c (ag_c04a)
            area_unit  — area unit code column in df_c (ag_c04b)
            area_gps   — GPS-measured area in acres in df_c (ag_c04c)
            soil_type  — soil-type code column in df_d (ag_d21)
            water_source — water-source code column in df_d (ag_d28a)
            acquire    — tenure/acquire code column in df_d (ag_d03);
                         omit (or absent) -> Tenure / PlotOwned NaN
                         (2016-17/2019-20)
            fallow     — cultivation-status code column in df_d (ag_d14);
                         omit (or absent) -> Fallow NaN
            erosion    — 1st erosion-control facility code column in df_d
                         (ag_d25a); omit (or absent) -> ErosionProtection NaN

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares, float — GPS-preferred), ``AreaUnit`` (str,
        always 'acres'), ``AreaSelfReported`` (hectares, float — the
        farmer-estimate area, NOT GPS-overridden, the WB
        ``area_self_reported``), ``Tenure`` (str), ``TenureSystem`` (str),
        ``PlotOwned`` (nullable bool — the WB ``plot_owned``, acquire codes
        1-5 = owned, 6-11 = not owned, from the same ag_d03 that drives
        Tenure; NaN where acquire is absent), ``SoilType`` (str),
        ``Irrigated`` (nullable bool), ``Fallow`` (nullable bool — the WB
        per-plot fallow flag, ag_d14 == 4 "Fallow"; the WB nb_fallow_plots
        is a count transform over this), and ``ErosionProtection``
        (nullable bool — the WB ``erosion_protection``, ag_d25a != 1
        i.e. any facility other than "No erosion control").  Latitude /
        Longitude are deferred (Malawi plot GPS is offset / redacted;
        GH #167).
    """
    c = colmap

    # C<->D merge on (hhid, plotkey).  Left-join keeps every area row;
    # D attributes are NaN where the plot is absent from Module D.
    df = df_c.copy()
    if df_d is not None and not df_d.empty:
        d_cols = ['hhid', 'plotkey'] + [
            df_d_col for df_d_col in (c.get('soil_type'),
                                      c.get('water_source'),
                                      c.get('acquire'),
                                      c.get('fallow'),
                                      c.get('erosion'))
            if df_d_col and df_d_col in df_d.columns]
        df = df.merge(df_d[d_cols].drop_duplicates(['hhid', 'plotkey']),
                      on=['hhid', 'plotkey'], how='left')

    n = len(df)
    idx_i = df['hhid'].astype('string')
    plot_id = df['plotkey'].astype('string')

    # Area: prefer GPS-measured (ag_c04c, acres), else farmer estimate
    # (ag_c04a) converted via its unit code (ag_c04b: 1=Acre, 2=Hectare,
    # 3=Square Meters, 4=Other).
    area_ha = pd.Series(pd.NA, index=df.index, dtype='Float64')

    gps_col = c.get('area_gps')
    if gps_col and gps_col in df.columns:
        gps_acres = pd.to_numeric(df[gps_col], errors='coerce').astype('Float64')
        # Plausibility clamp: > 2500 acres (~1000 ha) is a data-entry
        # error for Malawi smallholder plots; drop to NaN (GH #167).
        gps_acres = gps_acres.where((gps_acres <= 2500) | gps_acres.isna(), pd.NA)
        area_ha = gps_acres * ACRES_TO_HECTARES

    # AreaSelfReported: the farmer-estimate area in hectares, kept as its
    # OWN column (the WB ``area_self_reported``), distinct from the
    # GPS-preferred ``Area`` above.  Reported item attribute, never
    # GPS-overridden.
    est_ha = pd.Series(pd.NA, index=df.index, dtype='Float64')
    est_col = c.get('area_est')
    unit_col = c.get('area_unit')
    if est_col and est_col in df.columns:
        est = pd.to_numeric(df[est_col], errors='coerce').astype('Float64')
        unit = (pd.to_numeric(df[unit_col], errors='coerce').astype('Int64')
                if unit_col and unit_col in df.columns
                else pd.Series(pd.NA, index=df.index, dtype='Int64'))
        # acre -> ha, hectare -> ha, sq metre -> ha; OTHER (4) / 0 -> NaN
        est_ha = est_ha.where(unit != 1, est * ACRES_TO_HECTARES)
        est_ha = est_ha.where(unit != 2, est)
        est_ha = est_ha.where(unit != 3, est / 10000.0)
        # Clamp implausible estimates too (>1000 ha)
        est_ha = est_ha.where((est_ha <= 1000) | est_ha.isna(), pd.NA)
        # GPS-preferred Area falls back to the self-reported estimate.
        area_ha = area_ha.where(area_ha.notna(), est_ha)

    area_self_reported = est_ha

    area_unit = pd.Series(['acres'] * n, index=df.index, dtype='string')
    area_unit = area_unit.where(area_ha.notna(), pd.NA)

    # SoilType
    soil_type = pd.Series(pd.NA, index=df.index, dtype='string')
    soil_col = c.get('soil_type')
    if soil_col and soil_col in df.columns:
        soil_type = _map_codes(df[soil_col], _malawi_code_map('harmonize_soil'))

    # Irrigated: derived from water-source code (ag_d28a).  Code 7 =
    # 'Rainfed/No irrigation' is the only non-irrigated value; any other
    # recorded code means the plot is irrigated.  NaN where unrecorded.
    irrigated = pd.Series(pd.NA, index=df.index, dtype='boolean')
    water_col = c.get('water_source')
    if water_col and water_col in df.columns:
        wcode = pd.to_numeric(df[water_col], errors='coerce').astype('Int64')
        irrigated = (wcode != 7).astype('boolean')
        irrigated = irrigated.where(wcode.notna(), pd.NA)

    # Tenure / TenureSystem / PlotOwned from the acquire code (ag_d03),
    # present in 2010-11 & 2013-14 only.  Absent -> all NaN (2016-17 /
    # 2019-20: the WB derives plot_owned from the Module B2 parcel-tenure
    # roster there, a different grain -- left NaN here rather than joined,
    # matching how Tenure already behaves for those waves).
    tenure = pd.Series(pd.NA, index=df.index, dtype='string')
    tenure_system = pd.Series(pd.NA, index=df.index, dtype='string')
    plot_owned = pd.Series(pd.NA, index=df.index, dtype='boolean')
    acq_col = c.get('acquire')
    if acq_col and acq_col in df.columns:
        acode = pd.to_numeric(df[acq_col], errors='coerce').astype('Int64')
        tenure = _map_codes(acode, _malawi_code_map('harmonize_tenure'))
        # Leasehold acquire code (6) -> TenureSystem 'leasehold'.
        tenure_system = pd.Series(pd.NA, index=df.index, dtype='string')
        tenure_system = tenure_system.where(acode != 6, 'leasehold')
        # PlotOwned: the WB plot_owned recode of ag_d03 -- codes 1-5
        # (granted / inherited / bride price / purchased with or without
        # title) = owned; 6-11 (leasehold / rent / tenant / borrowed /
        # squatter / other) = not owned.  NaN where the acquire code is
        # itself missing.
        plot_owned = ((acode >= 1) & (acode <= 5)).astype('boolean')
        plot_owned = plot_owned.where(acode.notna(), pd.NA)

    # Fallow: the WB per-plot fallow flag -- cultivation-status code
    # ag_d14 == 4 ("Fallow").  Any other recorded status (cultivated,
    # rented/given out, forest, pasture, other) = not fallow; NaN where
    # the status is unrecorded.  The WB nb_fallow_plots is a count
    # transform over this item flag (NOT stored here).
    fallow = pd.Series(pd.NA, index=df.index, dtype='boolean')
    fallow_col = c.get('fallow')
    if fallow_col and fallow_col in df.columns:
        fcode = pd.to_numeric(df[fallow_col], errors='coerce').astype('Int64')
        fallow = (fcode == 4).astype('boolean')
        fallow = fallow.where(fcode.notna(), pd.NA)

    # ErosionProtection: the WB erosion_protection recode of the 1st
    # erosion-control facility code ag_d25a -- code 1 = "No erosion
    # control" -> False; any other recorded facility (terraces, bunds,
    # gabions, vetiver, tree belts, drainage, ...) -> True.  NaN where
    # unrecorded.
    erosion_protection = pd.Series(pd.NA, index=df.index, dtype='boolean')
    ero_col = c.get('erosion')
    if ero_col and ero_col in df.columns:
        ecode = pd.to_numeric(df[ero_col], errors='coerce').astype('Int64')
        erosion_protection = (ecode != 1).astype('boolean')
        erosion_protection = erosion_protection.where(ecode.notna(), pd.NA)

    out = pd.DataFrame({
        't':                t,
        'i':                idx_i.values,
        'plot_id':          plot_id.values,
        'Area':             area_ha.values,
        'AreaUnit':         area_unit.values,
        'AreaSelfReported': area_self_reported.values,
        'Tenure':           tenure.values,
        'TenureSystem':     tenure_system.values,
        'PlotOwned':        plot_owned.values,
        'SoilType':         soil_type.values,
        'Irrigated':        irrigated.values,
        'Fallow':           fallow.values,
        'ErosionProtection': erosion_protection.values,
    })
    # GH #637 -- what this groupby actually does, measured rather than assumed.
    #
    # Audited cold across all six (wave x half) builds by patching
    # DataFrameGroupBy.first in-process and driving the wave scripts with
    # runpy (an in-process patch cannot see a `materialize: make` subprocess).
    # Result: ZERO groups of more than one row on a non-null plot_id, in every
    # wave.  So `.first()` collapses nothing real and cannot assemble the
    # skipna COMPOSITE that #637 is about -- there are no duplicate rows for it
    # to compose.  Module C is one row per plot, and (hhid, plotkey) identifies
    # that plot: no two plots collide on it, and format_id merges no two
    # distinct ids (both would show up here as duplicate groups, and neither
    # does).  Left in place as a guard, not removed: it is free, and the wave
    # scripts' `assert df.index.is_unique` depends on the frame being unique.
    #
    # "0 duplicates" ALONE would not license that verdict -- a key drawn from
    # the WRONG NAMESPACE also yields 0, because every row becomes its own
    # distinct "household" (the Tanzania shocks inversion, #637 trap 3).  So
    # the namespace was checked separately and PER WAVE against the roster:
    # 100% of `i` present in household_roster for 2010-11 / 2013-14 / 2016-17 /
    # 2019-20 alike, matching literally rather than vacuously.  Pinned by
    # tests/test_gh637_malawi_first_sites.py.
    #
    # What the audit DID find is a silent deletion, which is now explicit.
    # `groupby()` defaults to dropna=True, so a row whose plot_id is NULL was
    # being DROPPED here without a word (GH #323 §3b's delete-and-report, in a
    # country script rather than in core):
    #
    #     2010-11 275   2013-14 158   2016-17 27 + 6   2019-20 0 + 2   = 468
    #
    # Those rows are empty Module C roster stubs, not plots: 0 of all 468
    # carries a non-null value in ANY of ag_c04a / ag_c04b / ag_c04c, i.e. no
    # farmer-estimated area, no unit and no GPS area.  A row with no plot id
    # and no measurement cannot go into a (t, i, plot_id) table, so dropping it
    # is right -- but it should say so.  Dropping them here first leaves the
    # returned frame bit-for-bit what it was.
    out = out.dropna(subset=['plot_id'])
    out = out.groupby(['t', 'i', 'plot_id'], as_index=False).first()
    out = out.set_index(['t', 'i', 'plot_id'])
    return out


# --- crop_production / harvest (GAP 1) ----------------------------------
# Item-level (t, i, plot, crop) harvest feature.  Two source modules feed
# the plot-crop grain:
#   * Module G  — seasonal (rainy-season) crop harvest, one row per
#     (plot, crop).  Crop codes 1-48 (harmonize_crop).
#   * Module P  — perennial / tree-crop harvest, one row per (plot, crop).
#     Crop codes offset +1000 (1001-1018) to disambiguate the namespace,
#     exactly as the World Bank cleaning code does (MWI_IHPS1.do:59).
# Two further modules carry the SALE, but only at the household-crop grain
# (NO plot id in the questionnaire):
#   * Module I  — seasonal harvest sale, one row per (hh, crop).
#   * Module Q  — perennial harvest sale, one row per (hh, crop).
# We therefore attach the REPORTED Quantity_sold / Value_sold to a
# plot-crop row ONLY when that (i, crop) is grown on exactly one plot, so
# the reported value unambiguously belongs to that single plot-crop.
# Where a crop spans several plots, the sale is not plot-resolvable and
# those columns stay NaN -- we never fabricate a per-plot split of a
# household-level reported figure.  Everything aggregate (harvest_kg,
# yield, share_kg_sold, main_crop, value-shares) is a transformations.py
# concern, NOT a stored column.

# Module G/I/P/Q "non-standard unit" codes -> months map is for dates;
# the 1-12 month codes are the standard Stata month value labels.
_MONTH_CODES = {i: i for i in range(1, 13)}  # months stored as 1-12 ints


def _crop_codes(series, perennial=False):
    """Map a numeric crop-code Series through harmonize_crop, applying
    the +1000 perennial offset first when ``perennial`` is True.  Source
    must be loaded convert_categoricals=False (numeric codes)."""
    codes = pd.to_numeric(series, errors='coerce').astype('Int64')
    if perennial:
        codes = codes + 1000
    cmap = _malawi_code_map('harmonize_crop')
    out = codes.map(cmap)
    return out.astype('string').where(out.notna(), pd.NA), codes


# Sentinel for a harvest record whose condition code is NULL or outside the
# instrument's own 1/2/3 scheme, and for the perennial Module P, which asks
# no condition question at all.  A real value, never NA: a null index key is
# silently dropped by a duplicate collapse (GH #323 §3b), and `condition` is
# an index level of crop_production.  Distinct from `shell_not_applicable`,
# which is the survey's own answer 3 ("this crop has no shell").
_CONDITION_UNKNOWN = 'unknown_condition'

_CONDITION_TABLE = 'harmonize_crop_condition'

#: Code -> un-collapsed crop VARIETY label; see `categorical_mapping.org`.
_VARIETY_TABLE = 'harmonize_crop_variety'


def _crop_conditions(series):
    """Map a raw ag_g13c (S/U) code Series to the canonical ``condition``.

    Codes 1/2/3 -> shelled / unshelled / shell_not_applicable, through the
    ``harmonize_crop_condition`` table in ``categorical_mapping.org``.
    Everything else -- NULL, and the handful of off-scheme values the source
    carries (2013-14 has one 5; the 2016-17 panel has 15, 10 and a
    NON-INTEGRAL 4.5) -- becomes ``unknown_condition``.

    The non-integral value is why this does not go through
    ``.astype('Int64')`` like the other code maps: pandas 3.0 raises
    ``TypeError: cannot safely cast non-equivalent float64 to int64`` on
    4.5 rather than coercing it.
    """
    cond_map = _malawi_code_map(_CONDITION_TABLE)
    raw = pd.to_numeric(series, errors='coerce')

    def _one(x):
        if pd.isna(x) or not float(x).is_integer():
            return pd.NA
        return cond_map.get(int(x), pd.NA)

    out = raw.map(_one)
    return out.astype('object').where(out.notna(), _CONDITION_UNKNOWN)


def _harvest_block(df, *, hhid, plotkey, cropcode, qty, unit, condition=None,
                   plant_m=None, plant_y=None, harv_m=None, intercrop=None,
                   perennial=False, t=None):
    """Reshape one harvest module (G or P) to canonical long rows.

    All keyword args are RAW column names in ``df`` (or None when the
    wave/module lacks that field).  ``df`` must already carry an ``hhid``
    string column.  Returns a long DataFrame with the canonical columns
    (i, plot, crop, crop_code, Quantity, u, condition, planting_month,
    harvest_month, intercropped, perennial) -- NO aggregation.

    ``condition`` is Module G's ``ag_g13c`` ("(S/U)").  It was accepted and
    silently DISCARDED from this function's first version until GH #854;
    every one of the four wave scripts has passed ``condition='ag_g13c'``
    since GAP 1, so the column was always asserted to exist and never read.
    It is now an INDEX LEVEL of ``crop_production`` -- see
    ``_/data_scheme.yml`` and ``lsms_library/data_info.yml``.  Module P
    (perennial) asks no such question and passes None, which sentinels its
    rows to ``unknown_condition``.
    """
    unit_map = _malawi_code_map('harmonize_crop_unit')

    crop_label, crop_code_int = _crop_codes(df[cropcode], perennial=perennial)
    plot = df[plotkey].apply(format_id).astype('string')

    # The crop at its NATIVE grain, un-collapsed (GH #854 red-team item 2).
    # `harmonize_crop` maps codes 1-4 all to 'Maize' and 17-26 all to 'Rice';
    # the shipped IHS5 conversion tables key their kg factors on the VARIETY,
    # and the varieties genuinely differ.  Carried as a COLUMN, never an index
    # level: it is functionally determined by the crop code, so putting it in
    # the grain would split no key and only widen the index.
    crop_variety = _map_codes(crop_code_int, _malawi_code_map(_VARIETY_TABLE))

    u = (pd.to_numeric(df[unit], errors='coerce').astype('Int64').map(unit_map)
         if unit is not None and unit in df.columns
         else pd.Series(pd.NA, index=df.index, dtype='string'))
    u = u.astype('string').where(u.notna(), pd.NA)

    quantity = (pd.to_numeric(df[qty], errors='coerce').astype('Float64')
                if qty is not None and qty in df.columns
                else pd.Series(pd.NA, index=df.index, dtype='Float64'))

    def _month(col):
        if col is None or col not in df.columns:
            return pd.Series(pd.NA, index=df.index, dtype='Int64')
        m = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        # Calendar months are 1-12; 0 / out-of-range codes are "not
        # recorded" sentinels -> NaN.
        return m.where((m >= 1) & (m <= 12), pd.NA)

    planting_month = _month(plant_m)
    harvest_month = _month(harv_m)

    if condition is not None and condition in df.columns:
        crop_condition = _crop_conditions(df[condition])
    else:
        crop_condition = pd.Series(_CONDITION_UNKNOWN, index=df.index,
                                   dtype='object')

    # intercropped flag: Module G ag_g01 crop-stand code (1 = pure/sole;
    # >=2 = some form of intercrop/mixed stand).  Perennial rows: NaN.
    if intercrop is not None and intercrop in df.columns:
        stand = pd.to_numeric(df[intercrop], errors='coerce').astype('Int64')
        intercropped = (stand >= 2).astype('boolean')
        intercropped = intercropped.where(stand.notna(), pd.NA)
    else:
        intercropped = pd.Series(pd.NA, index=df.index, dtype='boolean')

    out = pd.DataFrame({
        't':              t,
        'i':              df['hhid'].astype('string').values,
        'plot_id':           plot.values,
        'crop':           crop_label.values,
        '_crop_code':     crop_code_int.values,
        'Quantity':       quantity.values,
        'u':              u.values,
        'condition':      crop_condition.values,
        'crop_variety':   crop_variety.values,
        'planting_month': planting_month.values,
        'harvest_month':  harvest_month.values,
        'intercropped':   intercropped.values,
        'perennial':      pd.array([perennial] * len(df), dtype='boolean'),
    })
    # Drop rows with no crop identity (crop code missing) — not a planted
    # item.  Keep rows with a crop even if Quantity is NaN (reported "no
    # harvest yet"/refused are legitimately item rows).
    out = out[out['crop'].notna() | out['_crop_code'].notna()]
    return out


def _sale_block(df, *, hhid, cropcode, sold_flag, qty_sold, value_sold,
                unit_sold=None, perennial=False):
    """Reshape one sale module (I or Q) to (i, _crop_code, u) reported sale.

    Returns a DataFrame with columns [i, _crop_code, u, Quantity_sold,
    Value_sold] at the household-crop-unit grain (no plot).  Summed within
    (i, _crop_code, u) because a household may report several sale rows for
    the same crop -- this is the REPORTED total the household sold of that
    crop IN THAT UNIT, not a derived aggregate over plots.

    ``unit_sold`` is the sale module's OWN unit column (Module I ag_i02b,
    Module Q ag_q02b).  The sale quantity is asked in its own unit, on the
    same code list as the harvest unit (verified from the Stata value
    labels: ag_i02b / ag_q02b / ag_g13b all carry
    {1 kilogram, 2 50 KG BAG, 3 90 KG BAG, 4 PAIL (SMALL), ... 13 OTHER}),
    and it DIFFERS from the harvest unit for 22.2% of 2010-11 sale rows.
    Carrying it is what lets ``assemble_crop_production`` attach a sale only
    to the harvest row measured in the SAME unit, so that
    ``Value_sold / Quantity_sold`` is a price per the row's declared ``u``.
    Before this was read, a sale in kilogrammes was routinely stamped on a
    row whose ``u`` said ``50 kg Bag`` -- a silent factor-of-50 error in any
    per-kg price.
    """
    unit_map = _malawi_code_map('harmonize_crop_unit')
    crop_label, crop_code_int = _crop_codes(df[cropcode], perennial=perennial)

    if unit_sold is not None and unit_sold in df.columns:
        u = pd.to_numeric(df[unit_sold], errors='coerce').astype('Int64').map(unit_map)
        u = u.astype('string').where(u.notna(), pd.NA)
    else:
        u = pd.Series(pd.NA, index=df.index, dtype='string')

    qs = (pd.to_numeric(df[qty_sold], errors='coerce').astype('Float64')
          if qty_sold is not None and qty_sold in df.columns
          else pd.Series(pd.NA, index=df.index, dtype='Float64'))
    vs = (pd.to_numeric(df[value_sold], errors='coerce').astype('Float64')
          if value_sold is not None and value_sold in df.columns
          else pd.Series(pd.NA, index=df.index, dtype='Float64'))

    out = pd.DataFrame({
        'i':            df['hhid'].astype('string').values,
        '_crop_code':   crop_code_int.values,
        'u':            u.values,
        'Quantity_sold': qs.values,
        'Value_sold':   vs.values,
    })
    out = out[out['_crop_code'].notna()]
    grp = out.groupby(['i', '_crop_code', 'u'], as_index=False,
                      dropna=False).agg(
        {'Quantity_sold': 'sum', 'Value_sold': 'sum'})
    return grp


def assemble_crop_production(t, harvest_pieces, sale_pieces):
    """Combine reshaped harvest (_harvest_block) and sale (_sale_block)
    pieces into the canonical crop_production DataFrame for wave ``t``.

    Parameters
    ----------
    t : str — wave id, used as the ``t`` index value.
    harvest_pieces : list[pd.DataFrame] — outputs of _harvest_block.
    sale_pieces : list[pd.DataFrame] — outputs of _sale_block (may be []).

    Returns
    -------
    pd.DataFrame indexed (t, i, plot, crop) with columns Quantity, u,
    condition, Quantity_sold, Value_sold, planting_month, harvest_month,
    intercropped, perennial.  Item-level reported values only.  ``u`` and
    ``condition`` are declared INDEX LEVELS in ``_/data_scheme.yml`` and are
    promoted by the framework; they are left as columns here for the same
    reason ``u`` always was.
    """
    harv = pd.concat(harvest_pieces, ignore_index=True)

    # Collapse exact duplicate (i, plot, crop, u, condition) harvest rows by
    # summing reported quantity (a plot-crop may be split across several
    # recorded lines in the same unit and state); keep the first non-null
    # date/flag.  This is a reported-line consolidation, NOT a cross-unit or
    # cross-state aggregation: rows in different units `u` OR different
    # `condition` stay distinct.  Adding `condition` to this key (GH #854) is
    # what stops shelled and unshelled kilograms of the same plot-crop from
    # being added together -- see `data_info.yml`, "Quantities in different
    # conditions are NOT commensurable".
    harv['u'] = harv['u'].astype('string')
    harv['condition'] = harv['condition'].astype('string')
    harv = harv.groupby(['i', 'plot_id', 'crop', 'u', 'condition', '_crop_code'],
                        as_index=False, dropna=False).agg({
        'Quantity':       'sum',
        # EXACT, not a reduction: `crop_variety` is a function of
        # `_crop_code`, which is in the key, so every row in a group carries
        # the same value.
        'crop_variety':   'first',
        'planting_month': 'first',
        'harvest_month':  'first',
        'intercropped':   'first',
        'perennial':      'first',
    })

    if sale_pieces:
        sale = pd.concat(sale_pieces, ignore_index=True)
        sale['u'] = sale['u'].astype('string')
        sale = sale.groupby(['i', '_crop_code', 'u'], as_index=False,
                            dropna=False).agg(
            {'Quantity_sold': 'sum', 'Value_sold': 'sum'})
        # Attach sale ONLY where the (i, crop) is grown on exactly one
        # plot, so the household-crop reported figure unambiguously
        # belongs to that single plot-crop.  Multi-plot crops keep NaN.
        # AND only to the harvest row measured in the SAME unit `u` as the
        # sale: the sale quantity is asked in its own unit (ag_i02b /
        # ag_q02b) and differs from the harvest unit for 22.2% of 2010-11
        # sale rows, so the old (i, _crop_code) merge stamped a
        # kilogramme-basis sale onto a `50 kg Bag` row -- Value_sold /
        # Quantity_sold then read as a price per 50 kg Bag when it was a
        # price per kg.  Merging on `u` too makes the ratio a price per the
        # row's declared unit BY CONSTRUCTION.  It cannot multiply rows:
        # harv is unique on (i, plot, crop, u) and, under the single-plot
        # gate, sale is unique on (i, _crop_code, u).  A sale whose unit is
        # unrecorded matches a harvest row whose unit is likewise
        # unrecorded (pd.merge matches null keys), which is the honest
        # pairing.
        nplots = (harv.groupby(['i', '_crop_code'])['plot_id']
                  .nunique().rename('_nplots').reset_index())
        # GH #854: the single-plot gate alone stopped being sufficient the
        # moment `condition` joined the harvest key.  Before it did, a
        # single-plot (i, crop) had at most ONE harvest row per unit, so the
        # household-crop sale total landed on exactly one row.  Now the same
        # plot-crop can carry a shelled AND an unshelled row in the same `u`,
        # and the m:1 merge would stamp the SAME Quantity_sold / Value_sold on
        # both -- double-counting the sale and inventing a per-unit price on
        # whichever row did not produce it.  The sale module records no
        # condition of its own that we read (ag_i02c is unwired; GH #854
        # scopes it out), so there is nothing to disambiguate WITH: the honest
        # answer is to attach nothing and count it.
        nrows = (harv.groupby(['i', '_crop_code', 'u'], dropna=False)
                 .size().rename('_nrows').reset_index())
        harv = harv.merge(nplots, on=['i', '_crop_code'], how='left')
        harv = harv.merge(nrows, on=['i', '_crop_code', 'u'], how='left')
        harv = harv.merge(sale, on=['i', '_crop_code', 'u'], how='left')
        single = (harv['_nplots'] == 1) & (harv['_nrows'] == 1)

        # "Attach nothing and COUNT IT" -- and the counting has to be code,
        # not a number frozen in CONTENTS.org that will not move when the data
        # does (GH #854 red-team item 6).  What is reported is the sales the
        # NEW `_nrows` clause suppresses that the old single-plot gate would
        # have attached: those are exactly the household-crop-unit totals that
        # a condition-split plot-crop would have DOUBLE-counted.
        suppressed = (harv['_nplots'] == 1) & (harv['_nrows'] > 1) \
            & harv['Value_sold'].notna()
        n_rows = int(suppressed.sum())
        if n_rows:
            hit = harv.loc[suppressed]
            # One sale is stamped on each of the `_nrows` rows, so the sale
            # COUNT is the number of distinct (i, crop, u) totals, not rows.
            sales = hit.drop_duplicates(['i', '_crop_code', 'u'])
            amount = float(pd.to_numeric(sales['Value_sold'],
                                         errors='coerce').sum())
            tally = {'wave': t, 'sales': int(len(sales)), 'rows': n_rows,
                     'value': amount}
            warnings.warn(
                f"Malawi/crop_production {t}: {len(sales)} household-crop "
                f"sale(s) totalling {amount:,.0f} MWK were NOT attached to "
                f"any harvest row, because the plot-crop is reported in more "
                f"than one `condition` in the same unit ({n_rows} candidate "
                f"rows).  The sale module records its own quantity and unit "
                f"but its shelled/unshelled column (ag_i02c) is unwired, so "
                f"there is nothing to say which harvest row the total belongs "
                f"to; attaching it to both would double-count it.  Wiring "
                f"ag_i02c is what lets these attach again.  GH #854.",
                SaleAttachmentWarning, stacklevel=2)
        else:
            tally = {'wave': t, 'sales': 0, 'rows': 0, 'value': 0.0}

        harv['Quantity_sold'] = harv['Quantity_sold'].where(single, pd.NA)
        harv['Value_sold'] = harv['Value_sold'].where(single, pd.NA)
        harv = harv.drop(columns=['_nplots', '_nrows'])
    else:
        harv['Quantity_sold'] = pd.array([pd.NA] * len(harv), dtype='Float64')
        harv['Value_sold'] = pd.array([pd.NA] * len(harv), dtype='Float64')
        tally = {'wave': t, 'sales': 0, 'rows': 0, 'value': 0.0}

    # The 2026-09-08 RESIDUAL is CLOSED by GH #854.  It read: the defensive
    # collapse at the end of this function keys on (t, i, plot, crop) -- u is
    # a COLUMN here -- so it fires when a plot-crop was harvested in TWO
    # units, and .first() skips NA per column, so it could take `u` from one
    # row and `Quantity_sold` from another, re-creating exactly the
    # mislabelling the (i, _crop_code, u) merge above removes.  (Measured then
    # on the raw modules: 32 plot-crops of 135,410, 0.024%, carry more than
    # one harvest unit.)  The collapse below now keys on the FULL declared
    # grain (t, i, plot, crop, u, condition), so it can no longer mix two
    # units -- or two states -- into one row.  It survives only as a guard
    # against a genuinely repeated line at that grain, which the groupby
    # above has already consolidated.
    harv = harv.drop(columns=['_crop_code'])
    harv['t'] = t

    # crop must be non-null for the index; rows where the code did not map
    # to a Preferred Label (only code 48/1018 "Other (Specify)" and any
    # unmapped) are dropped from the index axis but logged by row count.
    harv = harv[harv['crop'].notna()]

    # Defensive de-duplication at the DECLARED grain.  `u` and `condition`
    # stay COLUMNS (the framework promotes them, exactly as it always has for
    # `u`), so the check is on columns rather than on index levels; keeping
    # them out of the index also keeps the ~8.5k rows whose `u` is NULL,
    # which a set_index + groupby would delete on a NaN key (GH #323 3b).
    _grain = ['t', 'i', 'plot_id', 'crop', 'u', 'condition']
    if harv.duplicated(_grain).any():
        harv = harv.groupby(_grain, as_index=False, dropna=False).agg({
            'Quantity':       'sum',
            'crop_variety':   'first',
            'Quantity_sold':  'first',
            'Value_sold':     'first',
            'planting_month': 'first',
            'harvest_month':  'first',
            'intercropped':   'first',
            'perennial':      'first',
        })

    out = harv.set_index(['t', 'i', 'plot_id', 'crop'])
    # Readable by a test without parsing a warning message.  It does NOT
    # survive to_parquet (attrs are not written), which is why the warning
    # above exists as well -- the warning is what a build log sees.
    out.attrs['sale_suppressed'] = tally
    return out

# --- IHS5 crop-side conversion factor tables (GH #854) -------------------
#
# Malawi's IHS5 (2019-20) v04 release supplemented the survey with THREE
# conversion-factor files.  Only the food-side one was ever read
# (`2019-20/_/food_acquired.py:17`); the two crop-side ones sat DVC-tracked
# and unwired until #854:
#
#   Data/Cross_Sectional/ihs_seasonalcropconversion_factor_2020.dta  857 x 7
#   Data/Cross_Sectional/ihs_treeconversion_factor_2020.dta          153 x 6
#
# Raw grain, measured 2026-09-09:
#
#   seasonal  region x crop_code x unit_code x condition  (+ unit_name,
#             conversion, collectionround); 3 regions (North / Central /
#             South), 41 crop_code values, 15 unit_code values, 3 conditions
#             ('S: Shelled' / 'U: Unshelled' / 'Not Applicable'); zero nulls
#             in every column; ZERO duplicate rows on that key.
#   tree      region x crop_code x unit_code (NO condition column: a tree
#             crop has nothing to shell); 3 regions, 14 crop_code values,
#             9 unit_code values; zero nulls; ZERO duplicate rows.
#
# `collectionround` records whether the factor was measured in 2016, in 2019,
# or in both -- so the file itself already spans two survey rounds.
#
# THREE THINGS ARE NOT WHAT THEY LOOK LIKE.
#
# 1. `crop_code` is not a code, it is a VARIETY LABEL, and it is FINER than
#    our `crop`.  'MAIZE LOCAL' / 'MAIZE COMPOSITE/OPV' / 'MAIZE HYBRID' /
#    'MAIZE HYBRID RECYCLED' are the Module-G value labels for codes 1-4, and
#    `harmonize_crop` maps all four to the single Preferred Label 'Maize'.
#    Ten rice varieties collapse to 'Rice', six groundnuts to 'Groundnut'.
#    Collapsing them is therefore a REDUCTION, and where the variety rows
#    disagree there is no honest answer -- see `_collapse_varieties`.
# 2. `unit_code` is a STRING and its namespace is WIDER than the harvest
#    module's.  The harvest asks ag_g13b / ag_p09b on codes 1-13; the
#    conversion files add '14' (PAIL MEDIUM), '31A'/'31B'/'31C' (BUNDLE
#    small/medium/large), '98' (HEAP) and '8A'/'8B'/'8C' (BUNCH
#    small/medium/large).  No harvest row can carry those, so their factors
#    are unusable -- dropped and counted, never folded onto a coarser unit.
# 3. `region` matters and cannot be collapsed.  259 of 315 seasonal
#    (crop, unit, condition) triples and 53 of 56 tree (crop, unit) pairs
#    carry more than one distinct regional factor.  So the table is
#    region-keyed and the CALLER must resolve `region` onto crop_production;
#    `with_region` does that.  The file spells the south 'South' and
#    `cluster_features.Region` spells it 'Southern' (the `region` table in
#    categorical_mapping.org), so the loader translates -- otherwise the join
#    matches nothing, silently, for a third of the country.

#: The seasonal file's variety labels -> the Module G `crop_code` value the
#: wave scripts already read (2019-20 Cross_Sectional/ag_mod_g.dta value
#: labels for `crop_code`, which are identical to 2010-11's ag_g0d).  Only
#: the string->code step is new; the code->`crop` step is the SAME
#: `harmonize_crop` table `_crop_codes` uses, so the two sides cannot drift.
_IHS5_SEASONAL_CROPS = {
    'MAIZE LOCAL': 1, 'MAIZE COMPOSITE/OPV': 2, 'MAIZE HYBRID': 3,
    'MAIZE HYBRID RECYCLED': 4,
    'TOBACCO BURLEY': 5,
    'GROUNDNUT CHALIMBANA': 11, 'GROUNDNUT CG7': 12,
    'GROUNDNUT MANI-PINTAR': 13, 'GROUNDNUT MAWANGA': 14,
    'GROUNDNUT JL24': 15, 'OTHER GROUNDNUT (SPECIFY)': 16,
    'RICE LOCAL': 17, 'RICE FAYA': 18, 'RICE PUSA': 19, 'RICE TCG10': 20,
    'RICE IET4094 (SENGA)': 21, 'RICE WAMBONE': 22, 'RICE KILOMBERO': 23,
    'RICE ITA': 24, 'RICE MTUPATUPA': 25, 'OTHER RICE (SPECIFY)': 26,
    'GROUND BEAN(NZAMA)': 27, 'SWEET POTATO': 28,
    'IRISH [MALAWI] POTATO': 29, 'WHEAT': 30,
    'FINGER MILLET(MAWERE)': 31, 'SORGHUM': 32,
    'PEARL MILLET(MCHEWERE)': 33, 'BEANS': 34, 'SOYABEAN': 35,
    'PIGEONPEA(NANDOLO)': 36, 'COTTON': 37, 'SUNFLOWER': 38,
    'SUGAR CANE': 39, 'CABBAGE': 40, 'TANAPOSI': 41, 'NKHWANI': 42,
    'THERERE/OKRA': 43, 'TOMATO': 44, 'ONION': 45,
    # The file writes the plural; Module G's label for code 46 is 'PEA'.
    'PEAS': 46,
}

#: The tree file's labels -> the PERENNIAL crop code in the +1000 namespace
#: `_crop_codes(perennial=True)` and `harmonize_crop` use (Module P ag_p0c
#: codes 1-17, offset by 1000).  Note the file spells Masuku
#: 'MEXICAN APPLE(MASUKU)' where the 2019-20 module writes
#: 'MEXICAN APPLE [MASUKU]'; both are code 14 -> 1014.
_IHS5_TREE_CROPS = {
    'CASSAVA': 1001, 'TEA': 1002, 'MANGO': 1004, 'ORANGE': 1005,
    'PAWPAW/PAPAYA': 1006, 'BANANA': 1007, 'AVOCADO': 1008, 'GUAVA': 1009,
    'LEMON': 1010, 'NAARTJE (TANGERINE)': 1011, 'PEACH': 1012,
    '[CUSTADE APPLE] POZA': 1013, 'MEXICAN APPLE(MASUKU)': 1014,
    'MASAU': 1015,
}

#: The seasonal file's `condition` strings -> the canonical `condition`
#: vocabulary.  Deliberately keyed on the FILE'S OWN values rather than on a
#: code: the file ships the decoded string, and the three values map 1:1 onto
#: what `harmonize_crop_condition` gives ag_g13c codes 1/2/3, so the shipped
#: table and the harvest rows land in the same vocabulary by construction.
_IHS5_CONDITIONS = {
    'S: Shelled': 'shelled',
    'U: Unshelled': 'unshelled',
    'Not Applicable': 'shell_not_applicable',
}

#: The file's region spelling -> `cluster_features.Region` / the `region`
#: table's Preferred Label.  Only 'South' actually moves.
_IHS5_REGIONS = {'North': 'North', 'Central': 'Central', 'South': 'Southern'}

_IHS5_CF_DIR = '2019-20/Data/Cross_Sectional/'


def _ihs5_cf_path(filename):
    """Resolve an IHS5 conversion file relative to THIS module.

    The wave scripts run with cwd ``Malawi/<wave>/_``, but this loader is
    called from anywhere (a notebook, a test, another wave's script), so a
    ``../Data/...`` relative path would resolve differently per caller.  The
    path is built from this file's own location and handed to
    ``get_dataframe``, which still does the local -> DVC -> WB NADA fallback.
    """
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, '..', _IHS5_CF_DIR, filename)


def _collapse_varieties(df, keys, *, source):
    """Reduce the file's VARIETY rows to our ``crop`` grain, refusing conflicts.

    The IHS5 files key on a crop VARIETY; ``crop_production.crop`` is the
    aggregate ``harmonize_crop`` Preferred Label.  Collapsing four maize
    varieties onto 'Maize' is a reduction, and this is the loader's
    deliberate act -- ``transformations._shipped_factor_lookup`` refuses an
    ambiguous table outright and says so ("De-duplicate the table in the
    LOADER, with a stated rule").

    THE RULE: a key survives only when EVERY variety row under it reports the
    SAME factor.  Where they disagree the key is DROPPED, not averaged, not
    modal, not first -- there is no evidence here for choosing among them, and
    inventing a weight is the failure this whole layer exists to avoid
    (``CLAUDE.md`` "Grain Collapse", #854 "Never average or silently resolve a
    table lookup miss; report it").

    Measured 2026-09-09 on the two shipped files: 23 of 314 seasonal keys and
    8 of 92 tree keys disagree, so ~92% of the table survives.  The dropped
    keys are recorded on ``attrs['dropped_ambiguous']`` for reporting.

    Comparison is on the float32 value the ``.dta`` carries, exactly; two
    varieties whose factors differ in the last bit are a disagreement, which
    is the conservative direction.
    """
    # `nunique()` SKIPS NaN, so a key holding one NaN factor and one real one
    # would read nunique == 1 and `drop_duplicates` could keep the NaN row.
    # Neither shipped file has a NaN `conversion`, so this cannot fire today
    # -- it is here so the function stays correct if it is ever pointed at
    # another table (red-team latent nit, 2026-09-10).
    df = df.dropna(subset=['KgFactor'])
    g = df.groupby(keys, dropna=False)['KgFactor']
    nunique = g.nunique()
    ok = nunique[nunique == 1].index
    kept = (df.set_index(keys).loc[ok].reset_index()
            .drop_duplicates(subset=keys))
    dropped = nunique[nunique > 1]
    kept.attrs['dropped_ambiguous'] = [
        dict(zip(keys, k if isinstance(k, tuple) else (k,)))
        for k in dropped.index
    ]
    kept.attrs['source'] = source
    return kept


def crop_conversion_factors(*, by_variety=True, seasonal=True, tree=True,
                            waves=None):
    """Malawi's two SHIPPED IHS5 crop conversion tables, canonicalised.

    VINTAGE: both files are **2019-20 (IHS5) artefacts** -- the v04 release of
    catalog 3818 supplemented the survey with them, and their own
    ``collectionround`` column says each factor was measured in 2016, in 2019,
    or in both.  They are the ONLY crop-side conversion tables Malawi ships;
    IHS2 / IHS3 / IHPS-2013 / IHS4 ship none.

    NO ``t`` LEVEL BY DEFAULT, so the table applies to every wave.  That is an
    EXTRAPOLATION and it is stated rather than hidden.  Three things support
    it and one qualifies it:

    * the units are physical containers -- a 50 kg bag, an ox-cart, a pail --
      not survey instruments, and the questionnaire's own unit code list is
      unchanged across IHS3-IHS5;
    * the file already spans two rounds by its own account
      (``collectionround``), so it is not a single-year snapshot;
    * EPAR does the same and says why ("We borrow this survey's conversion
      factor files for the remaining Waves in the absence of having similar
      files for earlier versions of the survey" --
      ``Malawi IHS/Nonstandard Unit Conversion Factors/README.md``);
    * BUT nothing measured here establishes that a 2019 pail weighed what a
      2010 pail weighed.  A caller who wants the factors confined to the wave
      that shipped them passes ``waves=['2019-20']``, which adds a ``t`` level
      and fences the join to that wave.  Pass a longer list to fence it to a
      chosen subset.

    KEYED ON THE VARIETY BY DEFAULT (``by_variety=True``).  The files' crop
    key is a VARIETY -- 'MAIZE HYBRID', 'RICE FAYA', 'GROUNDNUT CG7' -- and
    ``crop_production.crop`` is the collapsed Preferred Label, so the two
    grains differ.  Keying on ``crop_variety`` (the un-collapsed column
    ``_harvest_block`` writes) is both what EPAR does -- "merge m:1 region
    crop_code_long unit condition ... //NOTE THAT WE MERGE ON CROP_CODE_LONG
    INTENTIONALLY" -- and measurably better here: it needs no de-duplication
    at all (813 rows, zero duplicate keys), it serves 6,890 rows the collapsed
    key had to refuse, and on the 66,768 rows both keys can serve it returns
    an IDENTICAL factor on every one.

    ``by_variety=False`` returns the collapsed ``crop``-keyed table instead,
    for a frame built before ``crop_variety` existed.  That path must reduce
    the varieties, and its rule is agreement-or-drop (see
    :func:`_collapse_varieties`): 23 seasonal and 8 tree keys are refused,
    which is 6,890 served rows on Groundnut, Rice and Citrus -- not noise.  It
    also cannot tell "every variety agrees" from "only one variety is in the
    file", so it hands Tobacco Burley's factor to flue-cured, NNDF, SDF and
    oriental tobacco (95 rows); the variety key withdraws those, correctly.

    Returns
    -------
    pd.DataFrame
        Columns ``KgFactor`` (kg per ONE unit of the row's ``u``, the same
        meaning ``crop_production.KgFactor`` carries) and ``Source``
        (``'IHS5 seasonal'`` or ``'IHS5 tree'``), indexed on
        ``(crop_variety, u, condition, region)`` -- or on
        ``(crop, u, condition, region)`` when ``by_variety=False`` -- plus
        ``t`` when *waves* is given.
        Every one of those is in
        :data:`~lsms_library.transformations.SHIPPED_FACTOR_JOIN_LEVELS`, so
        the frame is ready for
        ``harvest_kg(cp, shipped_factors=malawi.crop_conversion_factors())``.

        ``crop_production`` must carry ``region`` for the join to key on it;
        :func:`with_region` resolves it.  Without ``region`` the transform
        drops that key, the remaining keys go ambiguous, and it REFUSES the
        table -- loudly, which is the correct outcome, since 259 of 315
        seasonal triples vary by region.

        Diagnostics ride on ``.attrs['loader']``: per-file row counts, the
        crop / unit / condition match rates, and the keys dropped for
        variety disagreement.

    Notes
    -----
    The TREE table's ``condition`` is ``'unknown_condition'``, not NA.  A tree
    crop has nothing to shell and the file has no condition column at all; the
    perennial Module P likewise asks no condition question, so
    ``_harvest_block`` sentinels every perennial harvest row to
    ``unknown_condition``.  Writing NA here instead would normalise to the
    join's private NA sentinel and match only a NULL condition -- which no
    ``crop_production`` row has, by construction.  The table would match
    exactly zero perennial rows, silently but for the
    ``ShippedFactorWarning``.

    NOT PORTED, deliberately (#854 "What it must NOT do"): EPAR's
    interpolation ladder -- the shelled/unshelled ratio, the regional and
    national means, the tuber and cowpea borrows
    (``EPAR_UW_conversionfactors.do:399-432, 469-487``).  Those are IMPUTED
    values wearing a ``source`` tag.  Only the two raw IHS5 tables are
    ``reported``, and they are what this returns.  If we want the ladder it
    belongs in a transform, layered and counted like ``harvest_kg``'s.
    """
    from lsms_library.local_tools import get_dataframe

    cmap = _malawi_code_map('harmonize_crop')
    vmap = _malawi_code_map(_VARIETY_TABLE)
    umap = _malawi_code_map('harmonize_crop_unit')
    diag = {}
    pieces = []
    # The crop key this table is built on.  `crop_variety` needs no
    # de-duplication (the files are unique at that grain); `crop` does, and
    # `_collapse_varieties` is the stated rule for it.
    cropkey = 'crop_variety' if by_variety else 'crop'

    def _units(codes):
        """The file's string unit_code -> our `u` Preferred Label.

        Non-numeric codes ('31A', '8B') and numeric ones outside the harvest
        module's own 1-13 list ('14' PAIL MEDIUM, '98' HEAP) have no `u` and
        are dropped by the caller.  Folding '8A'/'8B'/'8C' onto code 8
        'Bunch' would be exactly the disagreeing collapse this loader refuses
        elsewhere: three sizes, three different weights, one label.
        """
        def _one(x):
            s = str(x).strip()
            return umap.get(int(s)) if s.isdigit() else None
        return codes.map(_one)

    if seasonal:
        raw = get_dataframe(_ihs5_cf_path(
            'ihs_seasonalcropconversion_factor_2020.dta'))
        code = raw['crop_code'].map(_IHS5_SEASONAL_CROPS)
        d = pd.DataFrame({
            'crop': code.map(lambda c: cmap.get(c) if pd.notna(c) else None),
            'crop_variety': code.map(
                lambda c: vmap.get(c) if pd.notna(c) else None),
            'u': _units(raw['unit_code']),
            'condition': raw['condition'].map(_IHS5_CONDITIONS),
            'region': raw['region'].map(_IHS5_REGIONS),
            'KgFactor': pd.to_numeric(raw['conversion'], errors='coerce'),
        })
        diag['seasonal'] = _match_rates(raw, d, 'crop_code', 'unit_code',
                                        'condition')
        d = d.dropna(subset=[cropkey, 'u', 'condition', 'region'])
        keys = [cropkey, 'u', 'condition', 'region']
        d = _collapse_varieties(d, keys, source='IHS5 seasonal')
        diag['seasonal']['keys_kept'] = int(len(d))
        diag['seasonal']['keys_dropped_ambiguous'] = d.attrs['dropped_ambiguous']
        d['Source'] = 'IHS5 seasonal'
        pieces.append(d)

    if tree:
        raw = get_dataframe(_ihs5_cf_path('ihs_treeconversion_factor_2020.dta'))
        code = raw['crop_code'].map(_IHS5_TREE_CROPS)
        d = pd.DataFrame({
            'crop': code.map(lambda c: cmap.get(c) if pd.notna(c) else None),
            'crop_variety': code.map(
                lambda c: vmap.get(c) if pd.notna(c) else None),
            'u': _units(raw['unit_code']),
            # See Notes: a real value, never NA -- it must meet the
            # `unknown_condition` every perennial harvest row carries.
            'condition': _CONDITION_UNKNOWN,
            'region': raw['region'].map(_IHS5_REGIONS),
            'KgFactor': pd.to_numeric(raw['conversion'], errors='coerce'),
        })
        diag['tree'] = _match_rates(raw, d, 'crop_code', 'unit_code', None)
        d = d.dropna(subset=[cropkey, 'u', 'region'])
        keys = [cropkey, 'u', 'condition', 'region']
        d = _collapse_varieties(d, keys, source='IHS5 tree')
        diag['tree']['keys_kept'] = int(len(d))
        diag['tree']['keys_dropped_ambiguous'] = d.attrs['dropped_ambiguous']
        d['Source'] = 'IHS5 tree'
        pieces.append(d)

    if not pieces:
        raise ValueError(
            "crop_conversion_factors() was asked for neither the seasonal nor "
            "the tree table (seasonal=False, tree=False); there is nothing to "
            "return.  Malawi ships exactly these two crop-side files.")

    out = pd.concat(pieces, ignore_index=True)

    # The two files are disjoint by construction -- a seasonal crop is never a
    # tree crop -- but "by construction" is what the duplicate refusal in
    # `_shipped_factor_lookup` exists to disbelieve.  Check it here, where the
    # message can name the file, rather than letting the transform raise about
    # a table the caller did not build.
    dup = out.duplicated([cropkey, 'u', 'condition', 'region'], keep=False)
    if dup.any():
        raise ValueError(
            "The IHS5 seasonal and tree conversion tables collide on "
            f"{int(dup.sum())} ({cropkey}, u, condition, region) key(s): "
            f"{out.loc[dup, [cropkey, 'u', 'condition', 'region']].head().to_dict('records')}. "
            "That is new -- the two files were disjoint on the shipped v04 "
            "release.  Resolve it here, in the loader, with a stated rule; do "
            "not let harvest_kg pick one.")

    if waves is not None:
        waves = [waves] if isinstance(waves, str) else list(waves)
        out = pd.concat([out.assign(t=w) for w in waves], ignore_index=True)
        idx = ['t', cropkey, 'u', 'condition', 'region']
    else:
        idx = [cropkey, 'u', 'condition', 'region']

    out = out.set_index(idx)[['KgFactor', 'Source']]
    out.attrs['loader'] = diag
    out.attrs['keyed_on'] = cropkey
    out.attrs['vintage'] = '2019-20 (IHS5 v04 release, catalog 3818)'
    return out


def _match_rates(raw, mapped, cropcol, unitcol, condcol):
    """Code -> label match rates for one shipped conversion file."""
    n = len(raw)
    out = {'rows': n,
           'crop_matched': int(mapped['crop'].notna().sum()),
           'variety_matched': int(mapped['crop_variety'].notna().sum()),
           'unit_matched': int(mapped['u'].notna().sum()),
           'region_matched': int(mapped['region'].notna().sum()),
           'crop_unmatched_labels': sorted(
               set(raw.loc[mapped['crop'].isna(), cropcol].astype(str))),
           'unit_unmatched_codes': sorted(
               set(raw.loc[mapped['u'].isna(), unitcol].astype(str)))}
    if condcol is not None:
        out['condition_matched'] = int(mapped['condition'].notna().sum())
        out['condition_unmatched_labels'] = sorted(
            set(raw.loc[mapped['condition'].isna(), condcol].astype(str)))
    return out


def with_region(crop_production, cluster_features=None):
    """Attach the lower-case ``region`` a shipped-factor join needs.

    ``crop_conversion_factors`` is region-keyed because it has to be: 259 of
    315 seasonal (crop, unit, condition) triples carry more than one distinct
    regional factor, so there is no honest way to collapse the axis in the
    loader.  ``crop_production`` carries the cluster ``v`` (the framework
    joins it from ``sample()``), and ``cluster_features`` carries ``Region``
    per ``(t, v)`` -- this joins the two and renames to the lower-case
    ``region`` that :data:`SHIPPED_FACTOR_JOIN_LEVELS` spells.

    The rename is not cosmetic.  ``_shipped_factor_lookup`` matches the key
    name EXACTLY and deliberately refuses to accept both spellings, because a
    silently-accepted ``Region`` would hide a mis-keyed join.

    Parameters
    ----------
    crop_production : pd.DataFrame
        ``Country('Malawi').crop_production()``.  Must carry ``v`` and ``t``.
    cluster_features : pd.DataFrame, optional
        ``Country('Malawi').cluster_features()``.  Loaded from
        ``Country('Malawi')`` when omitted -- which is why this lives here and
        not in ``transformations.py``, whose transforms never re-enter
        ``Country``.

    Returns
    -------
    pd.DataFrame
        *crop_production* with a ``region`` COLUMN added (never an index
        level: it is a property of the cluster, not part of the item grain).
        Rows whose cluster has no ``Region`` keep NA and simply match no
        shipped factor.
    """
    if cluster_features is None:
        import lsms_library as ll
        cluster_features = ll.Country('Malawi').cluster_features()

    region = (cluster_features.reset_index()[['t', 'v', 'Region']]
              .rename(columns={'Region': 'region'})
              .drop_duplicates(['t', 'v']))
    region['region'] = region['region'].astype('string')

    flat = crop_production.reset_index()
    if 'v' not in flat.columns:
        raise ValueError(
            "with_region needs the cluster level `v` on crop_production, and "
            f"this frame carries {list(crop_production.index.names)}.  `v` is "
            "joined from sample() by the framework at API time; pass the "
            "frame Country('Malawi').crop_production() returned, not a "
            "wave-level parquet.")
    merged = flat.merge(region, on=['t', 'v'], how='left')
    out = merged.set_index(list(crop_production.index.names))
    # attrs do NOT survive a merge whose sides disagree, and here the right
    # side has none at all -- see CLAUDE.md "Panel ID Transitive Chains and
    # the `attrs` Flag".  The population record and `id_converted` both ride
    # on attrs, so copy them across explicitly.
    out.attrs = dict(crop_production.attrs)
    return out


# --- non-FIES food security (GH #332) -----------------------------------
# Module H "Food Security" of the IHS3/IHS4/IHS5 household questionnaire and
# the IHPS 2013 carries two non-FIES batteries shared verbatim across all
# four buildable waves (the IHS2 2004-05 instrument is unrelated -- a 3-day
# food-consumption recall, no coping/months items -- so 2004-05 is ABSENT):
#
#   * H02 a-e: rCSI coping-strategy day counts (0-7 days in the past 7 days).
#     The 5 sub-items are in the SAME order in every wave (verified against
#     the IHS3 Household Questionnaire Module H, Page 35; the 2010-11 .dta
#     Stata labels are truncated to the question stem but the questionnaire
#     order is identical to the explicitly-labelled IHS4/IHS5/IHPS items):
#         hh_h02a = Rely on less preferred / less expensive foods
#         hh_h02b = Limit portion size at meal-times
#         hh_h02c = Reduce number of meals eaten in a day
#         hh_h02d = Restrict consumption by adults so small children can eat
#         hh_h02e = Borrow food / rely on help from a friend or relative
#   * H04 + H05: months of inadequate food provisioning in the last 12
#     months.  H04 is a Yes(1)/No(2) gate; H05 is a wide month-calendar
#     (one cell per month, 'X' marks a month the HH lacked enough food).

# Canonical rCSI Strategy names, indexed by the hh_h02 suffix order that is
# stable across every Malawi wave (a, b, c, d, e).
_MALAWI_COPING_STRATEGIES = {
    'a': 'LessPreferred',
    'b': 'LimitPortion',
    'c': 'ReduceMeals',
    'd': 'RestrictAdults',
    'e': 'BorrowFood',
}


def food_coping_for_wave(t, df, idcol, i_prefix=''):
    """Build canonical ``food_coping`` for one Malawi Module-H wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2010-11"``), used as the ``t`` index value.
    df : pd.DataFrame
        Household Module H rows, loaded with ``convert_categoricals=False``
        so the day counts stay integer-coded.  Must carry ``idcol`` plus
        ``hh_h02a`` .. ``hh_h02e``.
    idcol : str
        Household-id column (``case_id`` for the cross-sectional waves,
        ``y2_hhid`` for the 2013-14 IHPS panel).
    i_prefix : str, optional
        Prefix to prepend to ``format_id(idcol)`` so ``i`` matches the
        wave's ``household_roster`` index.  ``'cs-17-'`` for 2016-17 (its
        cross-sectional half is prefixed in the roster); empty otherwise.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, Strategy)`` with a single integer
        column ``Days`` (0-7, days in the past 7 the HH used the strategy).
        Recall period: last 7 days.
    """
    df = df.copy()
    df['i'] = i_prefix + df[idcol].apply(format_id)

    pieces = []
    for suffix, strategy in _MALAWI_COPING_STRATEGIES.items():
        col = f'hh_h02{suffix}'
        if col not in df.columns:
            continue
        days = pd.to_numeric(df[col], errors='coerce')
        # Day counts live on 0-7; anything outside (e.g. the lone 20 in the
        # 2019-20 cross-section) is a data-entry error -> NaN.
        days = days.where((days >= 0) & (days <= 7))
        piece = pd.DataFrame({'i': df['i'].values,
                              'Strategy': strategy,
                              'Days': days.values})
        pieces.append(piece)

    out = pd.concat(pieces, ignore_index=True)
    out['t'] = t
    out = out.dropna(subset=['Days'])
    out['Days'] = out['Days'].round().astype('Int64')
    # One row per (HH, strategy).  GH #637: audited cold in all four waves by
    # patching DataFrameGroupBy.first in-process and driving the wave scripts
    # with runpy -- rows in == groups out EXACTLY (61,355 / 19,994 / 62,232 /
    # 57,157), zero groups of more than one row, zero rows with a null key.
    # So this reduces nothing and cannot produce the skipna composite #637 is
    # about.  That equality is also what rules out two of the three traps:
    # `idcol` is one row per household in Module H, and `format_id` merges no
    # two distinct ids -- either failure would show up right here as a
    # duplicate (i, Strategy) group, and none exists.  The third trap needs a
    # different instrument: a WRONG-NAMESPACE key also yields 0 duplicates
    # (#637 trap 3), so `i` was checked PER WAVE against household_roster --
    # 100% overlap in all four waves.  Kept as a guard, not removed.
    out = out.groupby(['t', 'i', 'Strategy'], as_index=False).first()
    out = out.set_index(['t', 'i', 'Strategy'])
    return out


def _months_inadequate_columns(df):
    """Return the ordered list of wide month-calendar columns (hh_h05*).

    Handles both Module-H layouts: 2010-11 names its 25 month cells
    ``hh_h05a_01`` .. ``hh_h05b_15``; the later waves use single-letter
    suffixes ``hh_h05a`` .. ``hh_h05y``.
    """
    cols = [c for c in df.columns if c.lower().startswith('hh_h05')]
    # Exclude any "other specify" / cause companions defensively.
    cols = [c for c in cols if not c.lower().endswith('_os')]
    return sorted(cols)


def months_food_inadequate_for_wave(t, df, idcol, i_prefix=''):
    """Build canonical ``months_food_inadequate`` for one Malawi wave.

    Parameters
    ----------
    t, df, idcol, i_prefix
        As in :func:`food_coping_for_wave`.  ``df`` must carry the H04 gate
        (``hh_h04``: 1=Yes faced shortage, 2=No) and the wide H05 month
        calendar (cells marked ``'X'`` for months the HH lacked food).

    Returns
    -------
    pd.DataFrame indexed by ``(t, i)`` with columns ``MonthsInadequate``
        (Int64 0-12) and ``AnyInadequate`` (nullable bool).  Recall: last
        12 months.

    Notes
    -----
    The H05 calendar is **gated on H04==Yes**.  In IHS4 (2016-17) and IHS5
    (2019-20) every household that answered H04==No has ALL 25 calendar
    cells pre-filled with 'X' (a skip-pattern/template artifact -- verified:
    the count of 25-X households equals the count of H04==No households).
    Counting X marks unconditionally would therefore report 25 months for
    food-secure households.  Gating on H04==Yes yields a clean 0-12 count
    in every wave (the earlier IHS3/IHPS calendars are already blank for
    H04==No households, so the gate is a no-op there).
    """
    df = df.copy()
    df['i'] = i_prefix + df[idcol].apply(format_id)

    gate = pd.to_numeric(df['hh_h04'], errors='coerce')
    any_inadequate = pd.Series(pd.NA, index=df.index, dtype='boolean')
    any_inadequate = any_inadequate.where(gate != 1, True)
    any_inadequate = any_inadequate.where(gate != 2, False)

    month_cols = _months_inadequate_columns(df)
    marked = (df[month_cols].astype('string')
              .apply(lambda s: s.str.strip().str.upper() == 'X')
              .sum(axis=1))
    # Only H04==Yes households contribute a genuine month count; everyone
    # else (No, or unanswered) is zero months.
    months = marked.where(gate == 1, 0)
    # Plausibility clamp to the 12-month recall window.
    months = months.clip(upper=12).astype('Int64')

    out = pd.DataFrame({
        't': t,
        'i': df['i'].values,
        'MonthsInadequate': months.values,
        'AnyInadequate': any_inadequate.values,
    })
    # GH #637: same audit as food_coping_for_wave above.  Rows in == groups out
    # exactly in all four waves -- 12,271 / 4,000 / 12,447 / 11,434, which is
    # precisely each wave's Module H household count -- so `i` is unique here
    # and `.first()` is a no-op.  A `.first(skipna=False)` would change nothing
    # and is deliberately NOT used; the composite this site could in principle
    # build never arises, because there are no duplicate rows to build it from.
    # Trap 3 checked here too: `i` overlaps household_roster 100% PER WAVE, so
    # the absent duplicates are absent because the key is sound, not because it
    # is in the wrong namespace.
    out = out.groupby(['t', 'i'], as_index=False).first()
    out = out.set_index(['t', 'i'])
    return out


# --- plot_inputs (GAP 2) ------------------------------------------------
# Item-level (t, i, plot, input) feature -- one row per agricultural input
# applied to a plot.  Sources (the four IHS3+/IHPS waves):
#   * Module D (ag_mod_d) -- the plot-level input module.  Per plot it
#     records, in up to two slots, an INORGANIC FERTILIZER type + applied
#     quantity (ag_d39a/ag_d39d/ag_d39e slot 1; ag_d39f|ag_d39g type +
#     ag_d39i|ag_d39j applied-qty slot 2 -- the slot-2 column letters
#     shifted between the IHS3/IHPS2 layout and the IHS4/IHS5 layout),
#     an ORGANIC FERTILIZER used? flag (ag_d36), and up to two
#     PESTICIDE/HERBICIDE types + qty + unit (ag_d41a/b/c slot 1;
#     ag_d41d/e/f slot 2), gated by the any-agrochemical flag ag_d40.
#   * Module G (ag_mod_g) -- the seasonal plot-crop roster.  Per (plot,
#     crop) it records the SEED quantity planted + unit (ag_g04a/ag_g04b)
#     and, in IHS4/IHS5 only, an improved-seed flag (ag_g0f: 2=improved).
#
# The grain is (t, i, plot, input).  Fertilizer / pesticide rows have NO
# crop attached (the questionnaire records them at the plot, not the
# plot-crop, level); seed rows DO carry the crop (shared harmonize_food
# label axis) because Module G is a plot-crop roster.  `input` is mapped
# to the shared harmonize_input Preferred Label.  Reported item columns
# only: Quantity + native unit `u`, Purchased (bool) + Quantity_purchased
# for fertilizer/seed acquired through purchase (Module F ferts / Module H
# seeds, household-crop grain -- attached only when single-plot, like the
# crop_production sale), and Improved (bool) for seed rows.  Everything
# aggregate (seed_kg, nitrogen_kg, inorganic/organic/pesticide any-flags)
# is a transformations.py concern, NOT a stored column.

# Module D inorganic-fertilizer TYPE codes (ag_d39a / slot-2 type) ->
# harmonize_input codes 1-6 (identity: the source codes ARE the
# harmonize_input codes for fertilizer).
_MWI_FERT_TYPE_CODES = [1, 2, 3, 4, 5, 6]

# Module D agrochemical TYPE codes (ag_d41a / ag_d41d) -> harmonize_input
# codes 7-11 (identity: the source codes ARE the harmonize_input codes for
# pesticides; the source already numbers them 7..11 disjoint from fert 1-6).
_MWI_PEST_TYPE_CODES = [7, 8, 9, 10, 11]

# Synthetic harmonize_input codes for the two non-coded inputs.
_MWI_INPUT_ORGANIC = 20   # ag_d36 == Yes (organic fertilizer used on plot)
_MWI_INPUT_SEED = 30      # Module G seed planted (per plot-crop)

# Module D APPLIED-amount unit codes -> readable label.  The applied-amount
# unit value-label set (ag_d39e / ag_d39j) is the SAME bag-size coding as
# the acquired-amount unit (ag_d39c): 1=gram, 2=kilogram, 3..7 = bag sizes,
# 13=Other.  Codes 11/12 appear on the applied unit but are not in the
# acquired value-label set (free-text "tin"/"ox-cart"-style residuals);
# they map to NaN so we never fabricate a quantity basis.  We carry the
# REPORTED applied quantity with this declared unit -- the kg conversion
# (which the WB code performs, treating the applied amount as already-kg)
# is a transformation, NOT stored here.
_MWI_FERT_UNIT_LABELS = {
    1: 'Gram',
    2: 'Kilogram',
    3: '2 kg Bag',
    4: '3 kg Bag',
    5: '5 kg Bag',
    6: '10 kg Bag',
    7: '50 kg Bag',
    13: 'Other (Specify)',
}

# Module G SEED unit codes (ag_g04b) -> readable label.  Distinct bag-size
# coding from the fertilizer unit (note 5 = 3.7 kg bag here, vs 5 kg bag in
# the fertilizer set), so it gets its own map.
_MWI_SEED_UNIT_LABELS = {
    1: 'Gram',
    2: 'Kilogram',
    3: '2 kg Bag',
    4: '3 kg Bag',
    5: '3.7 kg Bag',
    6: '5 kg Bag',
    7: '10 kg Bag',
    8: '50 kg Bag',
    9: 'Other (Specify)',
}

# Module H / Module F PURCHASE unit codes (ag_h{n}6b / ag_f{n}6b) -> label.
# Same coding as the seed unit set for Module H; carried with the reported
# purchased quantity.
_MWI_PURCH_UNIT_LABELS = dict(_MWI_SEED_UNIT_LABELS)


def _int_codes(series):
    """Coerce a raw column to a nullable-Int64 code Series, rounding any
    stray fractional values (a unit/type code is conceptually an integer;
    a fractional entry is a data-entry residual).  Plain ``.astype('Int64')``
    raises on non-equivalent floats (e.g. the 0.5 that turns up in some
    IHS5 pesticide unit columns), so round first."""
    s = pd.to_numeric(series, errors='coerce')
    return s.round().astype('Int64')


def _input_labels(code_series):
    """Map a numeric harmonize_input code Series -> Preferred Label
    (nullable string), via the harmonize_input categorical table.  NaN
    where the code is absent / has no Preferred Label."""
    cmap = _malawi_code_map('harmonize_input')
    codes = _int_codes(code_series)
    out = codes.map(cmap)
    return out.astype('string').where(out.notna(), pd.NA)


def _fertilizer_block(df, *, hhid, plotkey, type1, qty1, unit1,
                      type2, qty2, unit2, t):
    """Reshape Module D's two inorganic-fertilizer slots to canonical
    long rows -- one per (plot, fertilizer-type) applied.

    All keyword args are RAW column names in ``df`` (or None when a slot
    is absent for that wave).  ``df`` must already carry an ``hhid`` string
    column and a ``plotkey`` string column.  Returns a long DataFrame with
    (i, plot, _input_code, Quantity, u) and the perennial/seed columns set
    to NA.  NO aggregation, NO kg conversion.
    """
    pieces = []
    for tcol, qcol, ucol in ((type1, qty1, unit1), (type2, qty2, unit2)):
        if tcol is None or tcol not in df.columns:
            continue
        code = _int_codes(df[tcol])
        keep = code.isin(_MWI_FERT_TYPE_CODES)
        q = (pd.to_numeric(df[qcol], errors='coerce').astype('Float64')
             if qcol and qcol in df.columns
             else pd.Series(pd.NA, index=df.index, dtype='Float64'))
        if ucol and ucol in df.columns:
            ucode = _int_codes(df[ucol])
            u = ucode.map(_MWI_FERT_UNIT_LABELS).astype('string')
            u = u.where(u.notna(), pd.NA)
        else:
            u = pd.Series(pd.NA, index=df.index, dtype='string')
        piece = pd.DataFrame({
            'i':           df['hhid'].astype('string').values,
            'plot_id':        df['plotkey'].astype('string').values,
            '_input_code': code.values,
            'crop':        pd.array([pd.NA] * len(df), dtype='string'),
            'Quantity':    q.values,
            'u':           u.values,
            'Improved':    pd.array([pd.NA] * len(df), dtype='boolean'),
        })
        pieces.append(piece[keep.values])
    if not pieces:
        return pd.DataFrame(columns=['i', 'plot_id', '_input_code', 'crop',
                                     'Quantity', 'u', 'Improved'])
    return pd.concat(pieces, ignore_index=True)


def _organic_block(df, *, hhid, plotkey, flag, t):
    """One row per plot where organic fertilizer was reported applied
    (ag_d36 == 1/Yes).  Presence is the reported item; Quantity / u are NA
    (Module D records no organic-fertilizer quantity)."""
    if flag is None or flag not in df.columns:
        return pd.DataFrame(columns=['i', 'plot_id', '_input_code', 'crop',
                                     'Quantity', 'u', 'Improved'])
    used = _int_codes(df[flag]) == 1
    out = pd.DataFrame({
        'i':           df['hhid'].astype('string').values,
        'plot_id':        df['plotkey'].astype('string').values,
        '_input_code': _MWI_INPUT_ORGANIC,
        'crop':        pd.array([pd.NA] * len(df), dtype='string'),
        'Quantity':    pd.array([pd.NA] * len(df), dtype='Float64'),
        'u':           pd.array([pd.NA] * len(df), dtype='string'),
        'Improved':    pd.array([pd.NA] * len(df), dtype='boolean'),
    })
    return out[used.values]


def _pesticide_block(df, *, hhid, plotkey, gate, slots, t):
    """Reshape Module D's agrochemical slots to canonical long rows -- one
    per (plot, pesticide-type).  ``slots`` is a list of (type, qty, unit)
    raw-column-name triples (qty/unit may be None).  ``gate`` is ag_d40
    (any agrochemical used? 1/2); rows are dropped where gate != Yes."""
    if gate is not None and gate in df.columns:
        gated = _int_codes(df[gate]) == 1
    else:
        gated = pd.Series(True, index=df.index)
    pieces = []
    for tcol, qcol, ucol in slots:
        if tcol is None or tcol not in df.columns:
            continue
        code = _int_codes(df[tcol])
        keep = code.isin(_MWI_PEST_TYPE_CODES) & gated
        q = (pd.to_numeric(df[qcol], errors='coerce').astype('Float64')
             if qcol and qcol in df.columns
             else pd.Series(pd.NA, index=df.index, dtype='Float64'))
        if ucol and ucol in df.columns:
            ucode = _int_codes(df[ucol])
            # Pesticide unit value-label set: 1=gram, 2=kilogram, 8=liter,
            # 9=milliliter (ag_d41c).  Reuse the fert label map for the
            # mass codes; add the volume codes.
            umap = dict(_MWI_FERT_UNIT_LABELS)
            umap.update({8: 'Liter', 9: 'Milliliter'})
            u = ucode.map(umap).astype('string')
            u = u.where(u.notna(), pd.NA)
        else:
            u = pd.Series(pd.NA, index=df.index, dtype='string')
        piece = pd.DataFrame({
            'i':           df['hhid'].astype('string').values,
            'plot_id':        df['plotkey'].astype('string').values,
            '_input_code': code.values,
            'crop':        pd.array([pd.NA] * len(df), dtype='string'),
            'Quantity':    q.values,
            'u':           u.values,
            'Improved':    pd.array([pd.NA] * len(df), dtype='boolean'),
        })
        pieces.append(piece[keep.values])
    if not pieces:
        return pd.DataFrame(columns=['i', 'plot_id', '_input_code', 'crop',
                                     'Quantity', 'u', 'Improved'])
    return pd.concat(pieces, ignore_index=True)


def _seed_block(df, *, hhid, plotkey, cropcode, qty, unit, improved=None,
                t=None):
    """Reshape Module G's seed-planted columns to canonical long rows --
    one per (plot, crop) seed.  ``cropcode`` is the harmonize_crop code
    column (mapped to the shared crop/food label).  Returns (i, plot,
    _input_code=SEED, crop, Quantity, u, Improved)."""
    crop_label, _crop_code = _crop_codes(df[cropcode], perennial=False)
    q = (pd.to_numeric(df[qty], errors='coerce').astype('Float64')
         if qty and qty in df.columns
         else pd.Series(pd.NA, index=df.index, dtype='Float64'))
    if unit and unit in df.columns:
        ucode = _int_codes(df[unit])
        u = ucode.map(_MWI_SEED_UNIT_LABELS).astype('string')
        u = u.where(u.notna(), pd.NA)
    else:
        u = pd.Series(pd.NA, index=df.index, dtype='string')
    if improved is not None and improved in df.columns:
        icode = _int_codes(df[improved])
        # ag_g0f: 2 = improved/hybrid, 1 = local/unimproved (WB recode).
        imp = (icode == 2).astype('boolean')
        imp = imp.where(icode.notna(), pd.NA)
    else:
        imp = pd.Series(pd.NA, index=df.index, dtype='boolean')
    out = pd.DataFrame({
        'i':           df['hhid'].astype('string').values,
        'plot_id':        df['plotkey'].astype('string').values,
        '_input_code': _MWI_INPUT_SEED,
        'crop':        crop_label.values,
        'Quantity':    q.values,
        'u':           u.values,
        'Improved':    imp.values,
    })
    # Keep a seed row only where a crop identity is present (the seed item
    # is meaningless without the crop it seeds).
    out = out[crop_label.notna().values]
    return out


def _seed_purchase_block(df, *, hhid, cropcode, qty_cols, unit_cols,
                         value_cols):
    """Reshape Module H purchased-seed columns to (i, crop) reported
    purchase totals.  ``qty_cols``/``unit_cols``/``value_cols`` are lists
    of the per-source raw column names (two purchase sources: ag_h16*,
    ag_h26*).  A purchased amount is counted only where the corresponding
    value column is non-zero (WB rule).  Returns (i, _crop_code,
    Quantity_purchased) -- household-crop grain, no plot."""
    crop_label, crop_code_int = _crop_codes(df[cropcode], perennial=False)
    cols = []
    for qcol, vcol in zip(qty_cols, value_cols):
        if qcol not in df.columns:
            continue
        q = pd.to_numeric(df[qcol], errors='coerce').astype('Float64')
        if vcol and vcol in df.columns:
            v = pd.to_numeric(df[vcol], errors='coerce').astype('Float64')
            q = q.where(v.notna() & (v != 0), pd.NA)
        cols.append(q)
    if cols:
        # Sum the per-source purchased amounts; the row total stays NA when
        # NO source recorded a (value-backed) purchase -- so a household
        # that did not buy seed of that crop gets NA, never a spurious 0.
        total = pd.concat(cols, axis=1).sum(axis=1, min_count=1).astype('Float64')
    else:
        total = pd.Series(pd.NA, index=df.index, dtype='Float64')
    out = pd.DataFrame({
        'i':            df['hhid'].astype('string').values,
        '_crop_code':   crop_code_int.values,
        'Quantity_purchased': total.values,
    })
    out = out[out['_crop_code'].notna()]
    grp = out.groupby(['i', '_crop_code'], as_index=False).agg(
        {'Quantity_purchased': lambda s: s.sum(min_count=1)})
    # Drop the (i, crop) rows where no purchase was recorded.
    grp = grp[grp['Quantity_purchased'].notna()]
    return grp


def assemble_plot_inputs(t, input_pieces, seed_purchase_pieces=None):
    """Combine reshaped input pieces into the canonical plot_inputs
    DataFrame for wave ``t``.

    Parameters
    ----------
    t : str -- wave id, used as the ``t`` index value.
    input_pieces : list[pd.DataFrame] -- outputs of the per-module blocks
        (_fertilizer_block, _organic_block, _pesticide_block, _seed_block),
        each carrying (i, plot, _input_code, crop, Quantity, u, Improved).
    seed_purchase_pieces : list[pd.DataFrame] | None -- outputs of
        _seed_purchase_block (i, _crop_code, Quantity_purchased) at the
        household-crop grain; attached to the single-plot seed row of that
        (i, crop), like crop_production's sale (no plot fabrication).

    Returns
    -------
    pd.DataFrame indexed (t, i, plot, input) with columns Quantity, u,
    Purchased, Quantity_purchased, Improved, crop.  Item-level reported
    values only.
    """
    df = pd.concat([p for p in input_pieces if p is not None and len(p)],
                   ignore_index=True)

    df['input'] = _input_labels(df['_input_code'])
    # An input row needs a resolved input label to land on the index.
    df = df[df['input'].notna()]

    # Seed-purchase merge (household-crop grain -> single-plot seed rows).
    df['Quantity_purchased'] = pd.array([pd.NA] * len(df), dtype='Float64')
    if seed_purchase_pieces:
        sp = pd.concat([p for p in seed_purchase_pieces if p is not None
                        and len(p)], ignore_index=True)
        if len(sp):
            sp = sp.groupby(['i', '_crop_code'], as_index=False).agg(
                {'Quantity_purchased': 'sum'})
            # Re-derive the crop label so we can join on (i, crop) without
            # carrying _crop_code on the seed rows.
            sp_label, _ = _crop_codes(sp['_crop_code'], perennial=False)
            sp = sp.assign(crop=sp_label.values)
            sp = sp[sp['crop'].notna()]
            seed = df['input'] == 'Seed'
            # Count plots per (i, crop) among seed rows; attach only when a
            # crop's seed is on exactly one plot (unambiguous).
            seed_rows = df[seed]
            nplots = (seed_rows.groupby(['i', 'crop'])['plot_id']
                      .nunique().rename('_nplots').reset_index())
            attach = sp.merge(nplots, on=['i', 'crop'], how='inner')
            attach = attach[attach['_nplots'] == 1][
                ['i', 'crop', 'Quantity_purchased']]
            attach = attach.rename(
                columns={'Quantity_purchased': '_qp'})
            df = df.merge(attach, on=['i', 'crop'], how='left')
            take = seed & df['_qp'].notna()
            df['Quantity_purchased'] = df['Quantity_purchased'].where(
                ~take, df['_qp'])
            df = df.drop(columns=['_qp'])

    # Purchased flag: True where a purchased quantity is attached (seed),
    # else NA (Module D fertilizer/pesticide applied-on-plot rows do not
    # record a purchase split at the plot grain).
    df['Purchased'] = pd.array([pd.NA] * len(df), dtype='boolean')
    df.loc[df['Quantity_purchased'].notna(), 'Purchased'] = True

    df = df.drop(columns=['_input_code'])
    df['t'] = t

    # Consolidate exact-duplicate reported lines: same (i, plot, input,
    # crop, u) summed on Quantity (a plot may record the same fertilizer
    # in two slots with the same unit); first-wins on the flag columns.
    # Rows in different units `u` stay distinct.  NaN crop / u participate
    # in the grouping (dropna=False) so fertilizer/pesticide rows (crop
    # NaN) are not silently dropped.
    df['crop'] = df['crop'].astype('string')
    df['u'] = df['u'].astype('string')
    grp = df.groupby(['t', 'i', 'plot_id', 'input', 'crop', 'u'],
                     as_index=False, dropna=False).agg({
        'Quantity':           'sum',
        'Quantity_purchased': 'first',
        'Purchased':          'first',
        'Improved':           'first',
    })

    # The natural per-input-line grain is (t, i, plot, input, crop, u): a
    # plot may carry several seed crops (same input='Seed', distinct crop)
    # and a fertilizer may be reported in two slots in different units
    # (same input, crop NaN, distinct u).  crop / u therefore belong in the
    # index to keep it unique; both are NaN for the inputs that don't carry
    # them (fertilizer/pesticide have NaN crop; organic-fertilizer rows have
    # NaN u as well).  dropna=False above keeps those NaN-keyed rows.
    grp = grp.set_index(['t', 'i', 'plot_id', 'input', 'crop', 'u'])
    return grp


# --- livestock (GAP 4) --------------------------------------------------
# Item-level (t, i, animal) livestock-roster feature -- one reported row per
# (household, species owned).  Source: Module R livestock roster
# (ag_mod_r1_{10,13,16,19}.dta), the SAME file the World Bank cleaning code
# reads then collapses to a single HH "engaged in livestock" binary
# (MWI_IHPS{1-4}.do: `recode ag_r00 ... gen(livestock)` then
# `collapse (max) livestock, by(<hhid>)`).  We keep the PRE-collapse roster
# at its natural grain, which is strictly richer than their binary (their
# binary = our `livestock().groupby(['t','i']).size() > 0`).
#
# One row per (household, species).  Reported item columns (only those the
# survey records; missing-in-wave -> NaN):
#   * HeadCount     -- ag_r02: head owned now (present at farm or away).
#   * HeadAcquired  -- ag_r10: head bought to raise in the last 12 months
#                      (the survey also records born/ag_r08 and gifts/ag_r09
#                      separately; HeadAcquired carries the PURCHASED count,
#                      the closest single reported "acquired" figure -- a
#                      born+gifts+bought total would be a transformation).
#   * HeadSold      -- ag_r16: head sold alive in the last 12 months.
#   * ValuePerAnimal -- ag_r04: REPORTED per-head current value ("If you sold
#                      one of the [LIVESTOCK] today, how much would you
#                      receive from the sale?").  A per-head price, so NOT
#                      additive; a herd value (ValuePerAnimal x HeadCount)
#                      and a TLU rollup are
#                      transformations, NOT stored columns.  Malawi reports
#                      no herd-total value column, so we carry the honest
#                      per-head figure rather than fabricating a total.
#
# The per-animal ownership gate is ag_r01 (Yes/No, "did you own
# [LIVESTOCK]?"); ag_r00 is the HH-level any-livestock gate the WB binary
# comes from.  We keep a roster row when the household reports owning that
# species (ag_r01 == 1, i.e. "Yes") OR records any positive HeadCount /
# HeadSold / HeadAcquired -- i.e. the species is genuinely part of the
# household's livestock holding.  Rows for species the HH was simply asked
# about and does not own (ag_r01 == No, all counts 0/NaN) are dropped, so
# the roster is the household's actual herd, not the full species checklist.
# `animal` is the harmonize_species Preferred Label (code 318 "Other
# (Specify)" -> NaN -> dropped from the index, like crop_production's
# "Other" crop).

# Module R columns are stable across all four waves EXCEPT the gifts-received
# count, which is ag_r09 in 2010-11 and ag_r09_1 from 2013-14 on.  The
# columns we read (ag_r00/r0a/r01/r02/r04/r10/r16) carry the same name in
# every wave; ag_r09* is not among them (gifts is not one of our four
# reported columns), so no per-wave column remap is needed.

def _livestock_block(df, *, hhid, animalcode, owned_flag='ag_r01',
                     headcount='ag_r02', acquired='ag_r10', sold='ag_r16',
                     value='ag_r04', t=None):
    """Reshape one wave's Module R livestock roster to canonical long rows.

    All keyword args are RAW column names in ``df`` (or None when a wave
    lacks that field).  ``df`` must already carry an ``hhid`` string column.
    Returns a long DataFrame at grain (t, i, animal) with the reported item
    columns (HeadCount, HeadAcquired, HeadSold, ValuePerAnimal) and an
    internal ``_animal_code`` used for the keep-filter / dedup.  NO
    aggregation.
    """
    species_map = _malawi_code_map('harmonize_species')
    code = pd.to_numeric(df[animalcode], errors='coerce').astype('Int64')
    animal = code.map(species_map)
    animal = animal.astype('string').where(animal.notna(), pd.NA)

    def _num(col):
        if col is None or col not in df.columns:
            return pd.Series(pd.NA, index=df.index, dtype='Float64')
        return pd.to_numeric(df[col], errors='coerce').astype('Float64')

    head = _num(headcount)
    acq = _num(acquired)
    sold_n = _num(sold)
    val = _num(value)

    # Per-species ownership gate (ag_r01 == 1 "Yes").  NaN where the wave
    # lacks the column (then the count-based keep below carries the row).
    if owned_flag is not None and owned_flag in df.columns:
        owns = pd.to_numeric(df[owned_flag], errors='coerce') == 1
    else:
        owns = pd.Series(False, index=df.index)

    out = pd.DataFrame({
        't':            t,
        'i':            df['hhid'].astype('string').values,
        'animal':       animal.values,
        '_animal_code': code.values,
        'HeadCount':    head.values,
        'HeadAcquired': acq.values,
        'HeadSold':     sold_n.values,
        'ValuePerAnimal': val.values,
        '_owns':        owns.values,
    })
    # Keep a roster row when the household genuinely holds the species:
    # owned-flag Yes, or any positive reported head movement / holding.
    holds = (out['_owns']
             | (out['HeadCount'].fillna(0) > 0)
             | (out['HeadSold'].fillna(0) > 0)
             | (out['HeadAcquired'].fillna(0) > 0))
    out = out[holds]
    # Must have a species identity for the index axis.
    out = out[out['_animal_code'].notna()]
    out = out.drop(columns=['_owns'])
    return out


def assemble_livestock(t, pieces):
    """Combine reshaped per-half livestock blocks (_livestock_block) into the
    canonical livestock DataFrame for wave ``t``.

    Parameters
    ----------
    t : str -- wave id, used as the ``t`` index value.
    pieces : list[pd.DataFrame] -- outputs of _livestock_block.

    Returns
    -------
    pd.DataFrame indexed (t, i, animal) with columns HeadCount,
    HeadAcquired, HeadSold, ValuePerAnimal.  Item-level reported values only.
    """
    cat = pd.concat(pieces, ignore_index=True)

    # Drop rows whose code did not resolve to a canonical species (code 318
    # "Other (Specify)" and any unmapped) -- they have no place on the
    # `animal` index axis, exactly like crop_production's "Other" crop.
    cat = cat[cat['animal'].notna()]

    # Collapse any duplicate (i, animal) rows.  A canonical species is
    # reached from a SINGLE code within any wave (the harmonize_species map
    # is 1:1 per wave), so a duplicate here means the household reported the
    # same species on two roster lines: sum the head movements (HeadCount /
    # HeadAcquired / HeadSold are additive counts of the same herd) and take
    # the first reported ValuePerAnimal (a per-head price is not additive).
    cat['t'] = t
    grp = cat.groupby(['t', 'i', 'animal'], as_index=False, dropna=False).agg({
        'HeadCount':    'sum',
        'HeadAcquired': 'sum',
        'HeadSold':     'sum',
        'ValuePerAnimal': 'first',
    })
    out = grp.set_index(['t', 'i', 'animal'])
    assert out.index.is_unique, f"Non-unique (t,i,animal) in livestock {t}"
    return out


# --- anthropometry (GAP 5) ------------------------------------------------
# Item-level (t, i, pid) body-measurement feature for the four IHS3+/IHPS
# waves.  Source: Module V (hh_mod_v) of the household questionnaire -- the
# SAME module the World Bank cleaning code reads then collapses to
# WHO-2006 z-scores (MWI_IHPS{1-4}.do:1213-1231:
#   gen weight = hh_v08
#   gen height = hh_v09
#   gen cage   = age*12 ; replace cage = hh_b05b if age==.
#   zscore06 ... -> haz06/waz06/whz06/bmiz06).
# We keep the RAW reported measures (Weight kg = hh_v08, Height cm = hh_v09)
# plus the child Age_months and Sex the z-score transform needs to be
# self-describing.  The z-scores / wasting / stunting are TRANSFORMS (they
# require the WHO-2006 reference population), computed at query time, NEVER
# stored here.  Module V records no MUAC (mid-upper-arm circumference) in
# any IHS/IHPS wave, so Malawi anthropometry carries no MUAC column -- we
# store only the columns the source actually records.
#
# Age_months reproduces the .do `cage`: years-of-age (hh_b05a) x 12, falling
# back to the infant months-of-age (hh_b05b) where years is missing -- both
# read from the roster (Module B), joined on the wave's native HH+person
# id keys.  Sex is the roster's hh_b03 (Female/Male), normalised to F/M.


_SEX_TO_FM = {
    'female': 'F', 'f': 'F', '2': 'F',
    'male':   'M', 'm': 'M', '1': 'M',
}


def _norm_sex(x):
    """Roster hh_b03 label -> canonical 'F'/'M' (NaN otherwise)."""
    if pd.isna(x):
        return pd.NA
    return _SEX_TO_FM.get(str(x).strip().lower(), pd.NA)


def _age_months(years, months):
    """Reproduce the .do `cage`: age-in-years*12, else infant months-of-age.

    ``years`` = roster hh_b05a (whole years); ``months`` = roster hh_b05b
    (months-of-age for under-1s).  Returns months as a float, NaN if both
    are missing.
    """
    if pd.notna(years):
        return float(years) * 12.0
    if pd.notna(months):
        return float(months)
    return np.nan


def _anthropometry_block(health, roster, *, t,
                         hh_id_health, pid_health,
                         hh_id_roster, pid_roster,
                         weight='hh_v08', height='hh_v09',
                         i_prefix=None):
    """Build one wave-half's reported (i, pid) anthropometry block.

    Parameters
    ----------
    health, roster : pd.DataFrame
        Raw Module V (anthropometry) and Module B (roster) frames for the
        SAME sample half (Cross_Sectional or Panel), read via get_dataframe
        with convert_categoricals=True.
    t : str -- wave id, used as the ``t`` index value.
    hh_id_health, pid_health : str
        HH-id and person-id column names in ``health``.
    hh_id_roster, pid_roster : str
        HH-id and person-id column names in ``roster`` -- these MUST resolve
        to the SAME (i, pid) the framework's household_roster feature emits
        (so anthropometry's grain aligns with household_roster).  Age/Sex
        are joined from the roster on (hh_id, pid).
    weight, height : str -- Module V measurement columns (default hh_v08/09).
    i_prefix : str | None
        Prepended to the formatted HH id (e.g. 'cs-17-' for the
        Cross_Sectional half of IHS4/IHS5), reproducing the roster
        feature's ``cs_i`` mapping so anthropometry's ``i`` aligns with
        household_roster.  The Age/Sex roster join happens on the *raw*
        formatted ids (the roster half is read with the matching prefix
        applied too, so the join is internally consistent), then the
        prefix is applied to the emitted ``i``.

    Returns
    -------
    pd.DataFrame with columns [i, pid, Weight, Height, Age_months, Sex]
    (Sex/Age from the roster join).  No MUAC column: Module V records no
    mid-upper-arm circumference in any IHS/IHPS wave.  Index reset; the
    caller concatenates halves and sets the index.
    """
    pre = (lambda s: (i_prefix + s)) if i_prefix else (lambda s: s)

    h = health.copy()
    h['i'] = h[hh_id_health].apply(format_id).map(pre)
    h['pid'] = h[pid_health].apply(format_id)
    h['Weight'] = pd.to_numeric(h[weight], errors='coerce')
    h['Height'] = pd.to_numeric(h[height], errors='coerce')
    # Module V records no mid-upper-arm circumference in any IHS/IHPS wave,
    # so Malawi anthropometry carries no MUAC column (only the columns the
    # source actually records).  Countries whose Module V has MUAC declare
    # and populate it themselves.
    h = h[['i', 'pid', 'Weight', 'Height']]

    # Keep only rows that actually carry a reported measure -- the rest of
    # Module V is the "was the person measured?" administrivia (hh_v05/06).
    h = h[h['Weight'].notna() | h['Height'].notna()]

    r = roster.copy()
    r['i'] = r[hh_id_roster].apply(format_id).map(pre)
    r['pid'] = r[pid_roster].apply(format_id)
    r['Sex'] = r['hh_b03'].apply(_norm_sex)
    years = r['hh_b05a'] if 'hh_b05a' in r.columns else pd.Series(pd.NA, index=r.index)
    months = r['hh_b05b'] if 'hh_b05b' in r.columns else pd.Series(pd.NA, index=r.index)
    r['Age_months'] = [
        _age_months(y, m) for y, m in zip(years, months)
    ]
    r = r[['i', 'pid', 'Sex', 'Age_months']].drop_duplicates(['i', 'pid'])

    out = h.merge(r, on=['i', 'pid'], how='left')
    out['t'] = t
    return out


def assemble_anthropometry(t, pieces):
    """Combine per-half anthropometry blocks into the canonical frame.

    Parameters
    ----------
    t : str -- wave id.
    pieces : list[pd.DataFrame] -- outputs of _anthropometry_block.

    Returns
    -------
    pd.DataFrame indexed (t, i, pid) with columns Weight, Height,
    Age_months, Sex.  Reported item-level values only.
    """
    cat = pd.concat(pieces, ignore_index=True)

    # A measured individual appears once per sample half; if the same
    # (i, pid) is reported twice (e.g. CS/Panel overlap), keep the first
    # non-null measurement -- the measures are a single physical reading,
    # not additive.
    cat = cat.groupby(['t', 'i', 'pid'], as_index=False, dropna=False).agg({
        'Weight':     'first',
        'Height':     'first',
        'Age_months': 'first',
        'Sex':        'first',
    })
    out = cat.set_index(['t', 'i', 'pid'])
    assert out.index.is_unique, f"Non-unique (t,i,pid) in anthropometry {t}"
    return out


# --- plot_labor (GAP 3, plot grain) ---------------------------------------
# Item-level (t, i, plot, source) plot-labor feature for the four IHS3+/IHPS
# waves.  Source: the Module D (ag_mod_d) plot-labor block -- the SAME
# columns the World Bank cleaning code (MWI_IHPS{1-4}.do labor-days block)
# reads then collapses to per-plot total_*_labor_days / hired_labor_value.
# We keep the PRE-collapse REPORTED person-days for each of the survey's
# three labor sources (family / hired / other), as the `source` axis of the
# (t, i, plot, source) grain -- one row per (plot, source).  The
# WB total_labor_days (sum ACROSS sources), the per-source HH-level
# rollups, and hired_labor_value (median-wage valuation) are ALL
# transformations, NEVER stored columns here.
#
# Reported columns:
#   PersonDays -- reported person-days of that source on the plot.  For
#                 family labor the survey records (days x #people) per
#                 person-slot/week block, so PersonDays sums those
#                 day*count products (the WB temp_* / total_family_labor_days
#                 numerator BEFORE the across-source sum).  For hired/other
#                 the survey records day counts directly across man/woman/
#                 child categories and two visit columns, so PersonDays sums
#                 those reported day counts.
#   Wage       -- reported cash daily wage paid to HIRED labor (the mean of
#                 the reported man/woman/child daily wages that the survey
#                 recorded for that plot); NaN for family / other sources
#                 (no wage is paid).  This is the REPORTED per-day wage, NOT
#                 the median-wage valuation the WB code derives.
#
# The hired/other "person-day" columns differ by questionnaire generation:
#   IHS3 / IHPS-2013 (2010-11, 2013-14):
#     hired man  = ag_d47a + ag_d48a   wage man  = ag_d47b, ag_d48b
#     hired woman= ag_d47c + ag_d48c   wage woman= ag_d47d, ag_d48d
#     hired child= ag_d47e + ag_d48e   wage child= ag_d47f, ag_d48f
#   IHS4 / IHS5 (2016-17, 2019-20):
#     hired man  = ag_d47a1 + ag_d48a1 wage man  = ag_d47b1, ag_d48b1
#     hired woman= ag_d47a2 + ag_d48a2 wage woman= ag_d47b2, ag_d48b2
#     hired child= ag_d47a3 + ag_d48a3 wage child= ag_d47b3, ag_d48b3
# Other (free / exchange) labor is the same in every wave:
#     other man  = ag_d52a + ag_d54a
#     other woman= ag_d52b + ag_d54b
#     other child= ag_d52c + ag_d54c
# NOTE: the IHS4/IHS5 Cross_Sectional halves drop the Module D "other"
# labor columns (ag_d52*/ag_d54* absent), so those halves emit no `other`
# rows -- reported, not faked.

def _labor_source_code_map():
    """{Preferred Label: synthetic code} for harmonize_labor_source (for
    documentation / round-trip).  The plot_labor `source` axis carries the
    Preferred Labels directly, not the codes."""
    return _malawi_code_map('harmonize_labor_source')


def _sum_products(df, pairs):
    """Row-wise Sum over a list of (days_col, count_col) pairs of
    days*count, treating absent columns / NaN as 0; the whole result is NaN
    only when EVERY contributing cell is NaN (matching Stata's
    `egen rowtotal, missing`)."""
    total = pd.Series(0.0, index=df.index)
    any_present = pd.Series(False, index=df.index)
    for dcol, ccol in pairs:
        if dcol not in df.columns or ccol not in df.columns:
            continue
        d = pd.to_numeric(df[dcol], errors='coerce')
        c = pd.to_numeric(df[ccol], errors='coerce')
        prod = d * c
        total = total + prod.fillna(0.0)
        any_present = any_present | prod.notna()
    return total.where(any_present, np.nan)


def _sum_cols(df, cols):
    """Row-wise sum over ``cols`` (absent cols skipped); NaN only when every
    present cell is NaN (Stata rowtotal-missing semantics)."""
    total = pd.Series(0.0, index=df.index)
    any_present = pd.Series(False, index=df.index)
    for col in cols:
        if col not in df.columns:
            continue
        v = pd.to_numeric(df[col], errors='coerce')
        total = total + v.fillna(0.0)
        any_present = any_present | v.notna()
    return total.where(any_present, np.nan)


def _mean_cols(df, cols):
    """Row-wise mean over the NON-null values among ``cols`` (absent cols
    skipped); NaN when every present cell is NaN.  Used for the reported
    hired daily wage across man/woman/child categories."""
    present = [c for c in cols if c in df.columns]
    if not present:
        return pd.Series(np.nan, index=df.index)
    sub = df[present].apply(lambda s: pd.to_numeric(s, errors='coerce'))
    return sub.mean(axis=1, skipna=True)


def _family_labor_pairs(df):
    """The (days, count) family-labor column pairs present in ``df``.

    Handles both questionnaire layouts:
      * IHS3/IHPS-2013: ag_d4{2,3,4}{b/c, f/g, j/k, n/o} -- four person
        slots (b*c, f*g, j*k, n*o) across the 2nd/3rd/4th week blocks.
      * IHS4/IHS5: ag_d4{2,3,4}{b}{n} * ag_d4{2,3,4}{c}{n} -- one (b,c)
        day*count pair per occurrence-suffix n (n = 1..13).
    Returns a list of (days_col, count_col) tuples, only those present.
    """
    pairs = []
    blocks = ('ag_d42', 'ag_d43', 'ag_d44')
    # IHS3/IHPS-2013 layout: 4 person-slots, bare suffixes.
    legacy_slots = [('b', 'c'), ('f', 'g'), ('j', 'k'), ('n', 'o')]
    legacy = [(blk + d, blk + c) for blk in blocks for d, c in legacy_slots
              if (blk + d) in df.columns and (blk + c) in df.columns]
    if legacy:
        pairs += legacy
    # IHS4/IHS5 layout: (b{n}, c{n}) day*count per occurrence suffix.
    for blk in blocks:
        for n in range(1, 14):
            dcol, ccol = f'{blk}b{n}', f'{blk}c{n}'
            if dcol in df.columns and ccol in df.columns:
                pairs.append((dcol, ccol))
    return pairs


def _plot_labor_block(df, *, hhid, plotkey, t, hired_suffix,
                      include_other=True):
    """Build one wave-half's reported (i, plot, source) plot-labor block.

    ``df`` is a raw Module D frame that already carries an ``hhid`` string
    column and a ``plotkey`` string column.  ``hired_suffix`` selects the
    hired-labor column naming: '' for the IHS3/IHPS-2013 a/c/e + b/d/f
    layout, '1/2/3' for the IHS4/IHS5 a1/a2/a3 + b1/b2/b3 layout.
    ``include_other`` is False for the IHS4/IHS5 Cross_Sectional halves that
    drop the ag_d52*/ag_d54* "other" columns.

    Returns a long DataFrame at grain (t, i, plot, source) with the reported
    item columns PersonDays and Wage (Wage non-null only for source=hired).
    NO across-source sum, NO per-plot HH rollup.
    """
    work = pd.DataFrame({
        'i':    df[hhid].astype('string').values,
        'plot_id': df[plotkey].astype('string').values,
    }, index=df.index)

    rows = []

    # --- family: PersonDays = Sum(days * #people) over the slot/week pairs.
    fam_days = _sum_products(df, _family_labor_pairs(df))
    fam = work.copy()
    fam['source'] = 'family'
    fam['PersonDays'] = fam_days.values
    fam['Wage'] = np.nan
    rows.append(fam)

    # --- hired: PersonDays = Sum reported man/woman/child day counts across
    # the two visit columns; Wage = mean reported daily wage across the
    # man/woman/child categories that were recorded.
    if hired_suffix == '':
        day_cols = ['ag_d47a', 'ag_d48a', 'ag_d47c', 'ag_d48c',
                    'ag_d47e', 'ag_d48e']
        wage_cols = ['ag_d47b', 'ag_d48b', 'ag_d47d', 'ag_d48d',
                     'ag_d47f', 'ag_d48f']
    else:
        day_cols = ['ag_d47a1', 'ag_d48a1', 'ag_d47a2', 'ag_d48a2',
                    'ag_d47a3', 'ag_d48a3']
        wage_cols = ['ag_d47b1', 'ag_d48b1', 'ag_d47b2', 'ag_d48b2',
                     'ag_d47b3', 'ag_d48b3']
    hired = work.copy()
    hired['source'] = 'hired'
    hired['PersonDays'] = _sum_cols(df, day_cols).values
    hired['Wage'] = _mean_cols(df, wage_cols).values
    rows.append(hired)

    # --- other (free / exchange): PersonDays = Sum reported man/woman/child
    # day counts across the two visit columns; no wage.
    if include_other:
        other_cols = ['ag_d52a', 'ag_d54a', 'ag_d52b', 'ag_d54b',
                      'ag_d52c', 'ag_d54c']
        if any(c in df.columns for c in other_cols):
            other = work.copy()
            other['source'] = 'other'
            other['PersonDays'] = _sum_cols(df, other_cols).values
            other['Wage'] = np.nan
            rows.append(other)

    out = pd.concat(rows, ignore_index=True)
    out['t'] = t
    # Drop rows with no reported person-days for that source on that plot
    # (the survey did not record that source -> not a real item row).
    out = out[out['PersonDays'].notna() & (out['PersonDays'] > 0)
              | (out['source'] == 'hired') & out['Wage'].notna()]
    return out


def assemble_plot_labor(t, pieces):
    """Combine per-half plot-labor blocks (_plot_labor_block) into the
    canonical plot_labor DataFrame for wave ``t``.

    Returns a DataFrame indexed (t, i, plot, source) with columns PersonDays
    and Wage.  A (plot, source) reached on two source rows (e.g. the same
    plot reported across CS/Panel overlap, which should not happen but is
    guarded) sums PersonDays (additive) and takes the first reported Wage.
    """
    cat = pd.concat(pieces, ignore_index=True)
    cat['t'] = t
    grp = cat.groupby(['t', 'i', 'plot_id', 'source'], as_index=False,
                      dropna=False).agg({
        'PersonDays': 'sum',
        'Wage':       'first',
    })
    out = grp.set_index(['t', 'i', 'plot_id', 'source'])
    assert out.index.is_unique, f"Non-unique (t,i,plot,source) in plot_labor {t}"
    return out


# --- people_last7days (GAP 3, individual grain) ---------------------------
# Item-level (t, i, pid) 7-day individual time-use feature, mirroring
# Uganda's people_last7days but at the individual grain GAP_RANKING.org
# specifies (Uganda's existing feature is an HH-level Men/Women/Boys/Girls
# count; the parity target is the per-individual activity record).  Source:
# the household labor module (hh_mod_e) -- the SAME module the World Bank
# cleaning code (MWI_IHPS{1-4}.do labor block) reads to build farm_work /
# SOB_work / wage_work dummies, farm_hrs / SB_hrs / wage_hrs, the six
# ind_* industry one-hots, and working_age.  We keep the per-individual
# REPORTED record (no HH rollup):
#   farm_work  (bool) -- worked on HH agric/livestock/fishing in last 7 days
#   SOB_work   (bool) -- helped/ran a non-farm HH business in last 7 days
#   wage_work  (bool) -- worked for a wage/salary OR ganyu in last 7 days
#   farm_hrs   (float)-- hours on HH agric (+livestock/fishing where split)
#   SB_hrs     (float)-- hours on the non-farm HH business
#   wage_hrs   (float)-- hours of wage work + ganyu (the WB wage_hrs sum)
#   wage_industry (str)-- main wage-job industry on the harmonize_industry
#                         Preferred Labels (Agriculture/Fishing/Mining/
#                         Manufacturing/Construction/Services/Other), binned
#                         from hh_e20b by the WB numeric ranges; NaN where
#                         no wage job / not asked.  Replaces the six WB
#                         ind_* dummies (a per-industry dummy is then a
#                         trivial transformation).
#   working_age (bool)-- the WB working_age flag.
#
# The variable names differ by questionnaire generation:
#   IHS3 / IHPS-2013 (2010-11, 2013-14):
#     farm  = hh_e07 (single agric hours col)        -> farm_work/farm_hrs
#     SOB   = hh_e08                                  -> SOB_work/SB_hrs
#     wage  = hh_e11 (wage) + hh_e10 (ganyu)          -> wage_work/wage_hrs
#     working_age = (hh_e02 != "X")
#   IHS4 / IHS5 (2016-17, 2019-20):
#     farm  = hh_e07a (+ hh_e07b livestock, hh_e07c fishing) -> farm_work/hrs
#     SOB   = hh_e08 (+ hh_e09)                        -> SOB_work/SB_hrs
#     wage  = hh_e11 (wage) + hh_e10 (ganyu)           -> wage_work/wage_hrs
#     working_age = (roster age hh_b05a >= 5), joined from the roster.
# Industry hh_e20b (2-digit ISIC) is present in every wave.

# WB hh_e20b industry-range bins (MWI_IHPS{1-4}.do): code -> label.
def _industry_label(code_series):
    """Bin the hh_e20b 2-digit industry code into the harmonize_industry
    Preferred Label, reproducing the WB numeric ranges:
      11,12 -> Agriculture; 13 -> Fishing; 29 -> Mining;
      31-42 -> Manufacturing; 50 -> Construction; 61-96 -> Services;
      any other recorded code -> Other; missing -> NaN.
    """
    code = pd.to_numeric(code_series, errors='coerce')

    def _one(c):
        if pd.isna(c):
            return pd.NA
        c = int(c)
        if c in (11, 12):
            return 'Agriculture'
        if c == 13:
            return 'Fishing'
        if c == 29:
            return 'Mining'
        if 31 <= c <= 42:
            return 'Manufacturing'
        if c == 50:
            return 'Construction'
        if 61 <= c <= 96:
            return 'Services'
        return 'Other'

    return pd.Series([_one(c) for c in code], index=code_series.index,
                     dtype='string')


def _dummy_from_hours(series):
    """Reproduce the WB `recode (0=0)(.=.)(else=1)`: >0 -> True, ==0 ->
    False, NaN -> NA (a nullable boolean)."""
    v = pd.to_numeric(series, errors='coerce')
    out = pd.Series(pd.NA, index=v.index, dtype='boolean')
    out[v == 0] = False
    out[v > 0] = True
    return out


def _people_last7days_block(labor, *, t, hhid, pid, layout,
                            i_prefix=None, roster=None,
                            roster_hhid=None, roster_pid=None):
    """Build one wave-half's reported (i, pid) 7-day time-use block.

    Parameters
    ----------
    labor : pd.DataFrame -- raw hh_mod_e labor module for this half
        (convert_categoricals=False so hh_e20b is numeric).
    t : str -- wave id.
    hhid, pid : str -- HH-id and person-id columns in ``labor`` (must resolve
        to the SAME (i, pid) household_roster emits, after ``i_prefix``).
    layout : 'legacy' | 'modern'
        'legacy' = IHS3/IHPS-2013 (hh_e07 single farm col, hh_e02 working
        age); 'modern' = IHS4/IHS5 (hh_e07a/b/c, working age from roster
        age).
    i_prefix : str | None -- prepended to the formatted HH id (e.g. 'cs-17-'
        for the IHS4 Cross_Sectional half) to match household_roster.
    roster, roster_hhid, roster_pid : optional -- the Module B roster half
        (convert_categoricals=False) used to derive working_age = age>=5 in
        the 'modern' layout.  Ignored for 'legacy'.

    Returns a DataFrame [i, pid, farm_work, SOB_work, wage_work, farm_hrs,
    SB_hrs, wage_hrs, wage_industry, working_age] (index reset).
    """
    pre = (lambda s: (i_prefix + s)) if i_prefix else (lambda s: s)
    d = labor.copy()
    out = pd.DataFrame(index=d.index)
    out['i'] = d[hhid].apply(format_id).map(pre)
    out['pid'] = d[pid].apply(format_id)

    if layout == 'legacy':
        farm_hrs = _sum_cols(d, ['hh_e07'])
        sb_hrs = _sum_cols(d, ['hh_e08'])
    else:
        farm_hrs = _sum_cols(d, ['hh_e07a', 'hh_e07b', 'hh_e07c'])
        sb_hrs = _sum_cols(d, ['hh_e08', 'hh_e09'])
    wage_hrs = _sum_cols(d, ['hh_e11', 'hh_e10'])  # wage + ganyu, both waves

    out['farm_work'] = _dummy_from_hours(farm_hrs).values
    out['SOB_work'] = _dummy_from_hours(sb_hrs).values
    # wage_work = worked for wage OR ganyu (either hours > 0).
    wage_dummy = _dummy_from_hours(pd.to_numeric(d.get('hh_e11'),
                                                 errors='coerce'))
    ganyu_dummy = _dummy_from_hours(pd.to_numeric(d.get('hh_e10'),
                                                  errors='coerce'))
    ww = pd.Series(pd.NA, index=d.index, dtype='boolean')
    ww[(wage_dummy == False) & (ganyu_dummy == False)] = False
    ww[(wage_dummy == True) | (ganyu_dummy == True)] = True
    out['wage_work'] = ww.values

    out['farm_hrs'] = farm_hrs.astype('Float64').values
    out['SB_hrs'] = sb_hrs.astype('Float64').values
    out['wage_hrs'] = wage_hrs.astype('Float64').values

    if 'hh_e20b' in d.columns:
        out['wage_industry'] = _industry_label(d['hh_e20b']).values
    else:
        out['wage_industry'] = pd.Series(pd.NA, index=d.index, dtype='string')

    # working_age
    if layout == 'legacy' and 'hh_e02' in d.columns:
        wa = d['hh_e02'].astype('string').str.strip().str.upper() != 'X'
        out['working_age'] = wa.astype('boolean').values
    elif roster is not None and roster_hhid is not None:
        r = roster.copy()
        r['_i'] = r[roster_hhid].apply(format_id).map(pre)
        r['_pid'] = r[roster_pid].apply(format_id)
        age = pd.to_numeric(r.get('hh_b05a'), errors='coerce')
        r['_wa'] = (age >= 5).astype('boolean')
        key = r[['_i', '_pid', '_wa']].drop_duplicates(['_i', '_pid'])
        merged = out.merge(key, left_on=['i', 'pid'],
                           right_on=['_i', '_pid'], how='left')
        out['working_age'] = merged['_wa'].values
    else:
        out['working_age'] = pd.Series(pd.NA, index=d.index, dtype='boolean')

    out['t'] = t
    return out.reset_index(drop=True)


def assemble_people_last7days(t, pieces):
    """Combine per-half people_last7days blocks into the canonical frame.

    Returns a DataFrame indexed (t, i, pid) with the reported per-individual
    7-day columns.  A duplicate (i, pid) keeps the first reported record
    (one labor-module row per individual per half).
    """
    cat = pd.concat(pieces, ignore_index=True)
    cat = cat.groupby(['t', 'i', 'pid'], as_index=False, dropna=False).agg({
        'farm_work':     'first',
        'SOB_work':      'first',
        'wage_work':     'first',
        'farm_hrs':      'first',
        'SB_hrs':        'first',
        'wage_hrs':      'first',
        'wage_industry': 'first',
        'working_age':   'first',
    })
    out = cat.set_index(['t', 'i', 'pid'])
    assert out.index.is_unique, f"Non-unique (t,i,pid) in people_last7days {t}"
    return out


# ---- community_prices (GAP C): Community/Market Module CK reference prices --
# One REPORTED row per (cluster EA, priced item, native unit) from the
# Community questionnaire's Module CK "Reference prices of selected items"
# (codes 1-51).  v is the community EA id (== sample().v keyspace, so a
# surveyed price joins the households of that cluster), j is the item on the
# shared harmonize_price_item -> harmonize_food / harmonize_crop Preferred
# Label axis, u the native unit on the shared harmonize_price_unit -> `u`
# Preferred Label axis.  Reported columns only: the surveyed Price (MK),
# the NumberOfUnits that price refers to (the native price-per-quantity
# basis the survey gives), and Available (the in-market y/n flag).  We do
# NOT compute a per-unit price (Price / NumberOfUnits) or any cross-cluster
# median/mean -- those are transformations.  There is no household i, so
# v is NATIVE in the index (the framework's _join_v_from_sample does not
# apply).  2016-17 (IHS4) collected no Module CK -> absent for that wave.

def _price_item_code_map():
    """{int item code 1-51: Preferred Label} for community_prices `j`."""
    return _malawi_code_map('harmonize_price_item')


def _price_unit_code_map():
    """{int unit code 1-21: Preferred Label} for community_prices `u`."""
    return _malawi_code_map('harmonize_price_unit')


def _strip_ck(series):
    """2013-14 (IHPS) codes Module CK items as the string 'CK<n>' (CK1,
    CK22, ...).  Strip the 'CK' prefix to the bare integer 1-51 code so it
    maps through harmonize_price_item like the numeric IHS3/IHS5 codes."""
    s = series.astype('string').str.upper().str.replace('CK', '', regex=False)
    return pd.to_numeric(s, errors='coerce').astype('Int64')


def _price_block(df, *, ea_col, item_col, price_col, nunits_col, unit_col,
                 avail_col, t, strip_ck=False):
    """Reshape one Module CK price file to canonical (t, v, j, u) rows.

    All keyword args are RAW column names in ``df`` (loaded
    convert_categoricals=False so codes are numeric / 'CK<n>' strings).
    Returns a long DataFrame with columns [t, v, j, u, _item_code, Price,
    NumberOfUnits, Available] -- NO aggregation, one row per source row.
    """
    item_map = _price_item_code_map()
    unit_map = _price_unit_code_map()

    code = (_strip_ck(df[item_col]) if strip_ck
            else pd.to_numeric(df[item_col], errors='coerce').astype('Int64'))
    j = code.map(item_map)
    j = j.astype('string').where(j.notna(), pd.NA)

    v = df[ea_col].apply(format_id).astype('string')

    if unit_col is not None and unit_col in df.columns:
        ucode = pd.to_numeric(df[unit_col], errors='coerce').astype('Int64')
        u = ucode.map(unit_map)
        u = u.astype('string').where(u.notna(), pd.NA)
    else:
        u = pd.Series(pd.NA, index=df.index, dtype='string')

    price = (pd.to_numeric(df[price_col], errors='coerce').astype('Float64')
             if price_col in df.columns
             else pd.Series(pd.NA, index=df.index, dtype='Float64'))

    nunits = (pd.to_numeric(df[nunits_col], errors='coerce').astype('Float64')
              if nunits_col is not None and nunits_col in df.columns
              else pd.Series(pd.NA, index=df.index, dtype='Float64'))

    if avail_col is not None and avail_col in df.columns:
        av = pd.to_numeric(df[avail_col], errors='coerce').astype('Int64')
        # 1 = Yes (available for sale), 2 = No.
        available = (av == 1).astype('boolean').where(av.notna(), pd.NA)
    else:
        available = pd.Series(pd.NA, index=df.index, dtype='boolean')

    out = pd.DataFrame({
        't':             t,
        'v':             v.values,
        'j':             j.values,
        'u':             u.values,
        '_item_code':    code.values,
        'Price':         price.values,
        'NumberOfUnits': nunits.values,
        'Available':     available.values,
    })
    # Drop rows with no item identity (item code missing) -- not a priced
    # item.  Keep rows where the item exists but Price is NaN (a reported
    # "not available"/refused price is still a legitimate item row).
    out = out[out['j'].notna() | out['_item_code'].notna()]
    return out


def assemble_community_prices(t, pieces):
    """Combine reshaped Module CK price blocks (_price_block outputs) into
    the canonical community_prices DataFrame for wave ``t``.

    Returns a DataFrame indexed (t, v, j, u) with reported columns Price,
    NumberOfUnits, Available.  Drops rows whose item failed to resolve to a
    Preferred Label (j NaN) -- those cannot sit on the shared j axis.  Where
    a unit failed to resolve (u NaN, rare 2013-14 data-entry artifacts) the
    row is kept with u = <NA> in the index (the price is still reported).
    """
    cat = pd.concat(pieces, ignore_index=True)
    cat = cat[cat['j'].notna()].drop(columns=['_item_code'])

    # Collapse any exact (v, j, u) duplicates (none expected -- (ea, item)
    # is unique per wave -- but guard the index).  first() keeps the single
    # reported record.
    cat = cat.groupby(['t', 'v', 'j', 'u'], as_index=False, dropna=False).agg({
        'Price':         'first',
        'NumberOfUnits': 'first',
        'Available':     'first',
    })

    # community_prices is a CLUSTER table -- there is no household i, the
    # natural grain is (t, v, j, u).  But the framework's
    # local_tools.map_index() (run on EVERY read path) unconditionally
    # swaps j -> i whenever a `j` index level is present and an `i` level
    # is NOT (a Malawi-legacy food_acquired inversion).  That swap would
    # rename our item level `j` to `i`, drop `j`, and then collapse the
    # table to (t, v, u).  To keep `j` intact WITHOUT touching the
    # framework, we carry a redundant `i` level (set equal to the cluster
    # v) positioned BEFORE `j`: map_index then sees i present and j after
    # i, so it does NOT swap, and the framework's _normalize_dataframe_index
    # drops the undeclared `i` level (data_scheme declares (t, v, j, u)),
    # leaving the canonical (t, v, j, u) grain.  v already present in the
    # index means _join_v_from_sample is skipped; the spurious i==v never
    # reaches the API.
    cat['i'] = cat['v']
    out = cat.set_index(['t', 'v', 'i', 'j', 'u'])
    out = out[['Price', 'NumberOfUnits', 'Available']]

    chk = out.reset_index()
    assert not chk.duplicated(['t', 'v', 'j', 'u']).any(), \
        f"Non-unique (t,v,j,u) in community_prices {t}"
    assert len(out) > 0, f"community_prices {t} produced no rows"
    return out
