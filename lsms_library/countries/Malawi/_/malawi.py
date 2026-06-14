#!/usr/bin/env python
"""Malawi-specific helpers for wave-level food_acquired.py scripts.

The live surface is three functions used by the four IHS3+ wave scripts
(2010-11, 2013-14, 2016-17, 2019-20) to apply Malawi's region-keyed
unit-conversion CSV and to handle "300 grams"-style free-text units.
Other helpers (roster decomposition, get_other_features, etc.) were
removed in 2026-05-05 alongside the shadowed
food_prices_quantities_and_expenditures.py — see GH #218.
"""

import pandas as pd
import numpy as np
import re
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import conversion_table_matching_global, format_id


def _extract_kg_conversion(series):
    """Extract kilogram conversion factors from a unit-detail string series.

    Parses patterns like '300 grams', '1kg', '2 kilo' and returns
    a Series of conversion factors in kilograms.
    """
    grams = r'(\d+)\s*g(?:\s+|r)'
    kgs = r'(\d+)\s*k(?:g|ilo)'

    lower = series.str.lower()
    conv = pd.concat([lower.str.extract(grams).astype(float) * 0.01,
                      lower.str.extract(kgs).astype(float)], axis=0).dropna()
    return conv


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
    """Convert unusual unit descriptions to kg-based quantities.

    Parameters
    ----------
    df : DataFrame
    suffixes : list[str], optional
        Column suffixes to process (e.g. ``['consumed', 'bought']``).
        For each suffix, expects columns ``unitsdetail_{suffix}``,
        ``cfactor_{suffix}``, ``quantity_{suffix}``, and ``units_{suffix}``.
        Defaults to ``['consumed', 'bought']`` for backward compatibility.
    """
    if suffixes is None:
        suffixes = ['consumed', 'bought']

    for suffix in suffixes:
        detail_col = f'unitsdetail_{suffix}'
        cfactor_col = f'cfactor_{suffix}'
        quantity_col = f'quantity_{suffix}'
        units_col = f'units_{suffix}'
        u_col = f'u_{suffix}'

        if detail_col not in df.columns:
            continue

        conv_kg = _extract_kg_conversion(df[detail_col])  # parse "300 grams" first

        # Tidy the other-specify free text for use as a `u` label (after the
        # kg parse above): "1 Basket" -> "Basket", "1/4" -> NA (#223 Layer 2).
        df[detail_col] = df[detail_col].map(_clean_freetext_unit)

        df[cfactor_col] = df.apply(lambda x, c=cfactor_col: x[c] or conv_kg, axis=1)
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
            # exact Quantity_kg in _make, and the inline grams/kgs regex
            # already captured any metric magnitude into cfactor -- converting
            # again here would double-count (GH #378 / Malawi migration).
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
                         omit (or absent) -> Tenure NaN (2016-17/2019-20)

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares, float), ``AreaUnit`` (str, always 'acres'),
        ``Tenure`` (str), ``TenureSystem`` (str), ``SoilType`` (str),
        and ``Irrigated`` (nullable bool).  Latitude / Longitude are
        deferred (Malawi plot GPS is offset / redacted; GH #167).
    """
    c = colmap

    # C<->D merge on (hhid, plotkey).  Left-join keeps every area row;
    # D attributes are NaN where the plot is absent from Module D.
    df = df_c.copy()
    if df_d is not None and not df_d.empty:
        d_cols = ['hhid', 'plotkey'] + [
            df_d_col for df_d_col in (c.get('soil_type'),
                                      c.get('water_source'),
                                      c.get('acquire'))
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

    est_col = c.get('area_est')
    unit_col = c.get('area_unit')
    if est_col and est_col in df.columns:
        est = pd.to_numeric(df[est_col], errors='coerce').astype('Float64')
        unit = (pd.to_numeric(df[unit_col], errors='coerce').astype('Int64')
                if unit_col and unit_col in df.columns
                else pd.Series(pd.NA, index=df.index, dtype='Int64'))
        # acre -> ha, hectare -> ha, sq metre -> ha; OTHER (4) / 0 -> NaN
        est_ha = pd.Series(pd.NA, index=df.index, dtype='Float64')
        est_ha = est_ha.where(unit != 1, est * ACRES_TO_HECTARES)
        est_ha = est_ha.where(unit != 2, est)
        est_ha = est_ha.where(unit != 3, est / 10000.0)
        # Clamp implausible estimates too (>1000 ha)
        est_ha = est_ha.where((est_ha <= 1000) | est_ha.isna(), pd.NA)
        area_ha = area_ha.where(area_ha.notna(), est_ha)

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

    # Tenure / TenureSystem from the acquire code (ag_d03), present in
    # 2010-11 & 2013-14 only.  Absent -> all NaN (2016-17 / 2019-20).
    tenure = pd.Series(pd.NA, index=df.index, dtype='string')
    tenure_system = pd.Series(pd.NA, index=df.index, dtype='string')
    acq_col = c.get('acquire')
    if acq_col and acq_col in df.columns:
        acode = pd.to_numeric(df[acq_col], errors='coerce').astype('Int64')
        tenure = _map_codes(acode, _malawi_code_map('harmonize_tenure'))
        # Leasehold acquire code (6) -> TenureSystem 'leasehold'.
        tenure_system = pd.Series(pd.NA, index=df.index, dtype='string')
        tenure_system = tenure_system.where(acode != 6, 'leasehold')

    out = pd.DataFrame({
        't':            t,
        'i':            idx_i.values,
        'plot_id':      plot_id.values,
        'Area':         area_ha.values,
        'AreaUnit':     area_unit.values,
        'Tenure':       tenure.values,
        'TenureSystem': tenure_system.values,
        'SoilType':     soil_type.values,
        'Irrigated':    irrigated.values,
    })
    # Collapse any duplicate (hhid, plotkey) area rows defensively
    # (Module C should be one row per plot; first-wins keeps it unique).
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


def _harvest_block(df, *, hhid, plotkey, cropcode, qty, unit, condition=None,
                   plant_m=None, plant_y=None, harv_m=None, intercrop=None,
                   perennial=False, t=None):
    """Reshape one harvest module (G or P) to canonical long rows.

    All keyword args are RAW column names in ``df`` (or None when the
    wave/module lacks that field).  ``df`` must already carry an ``hhid``
    string column.  Returns a long DataFrame with the canonical columns
    (i, plot, crop, crop_code, Quantity, u, planting_month, harvest_month,
    intercropped, perennial) -- NO aggregation.
    """
    unit_map = _malawi_code_map('harmonize_crop_unit')

    crop_label, crop_code_int = _crop_codes(df[cropcode], perennial=perennial)
    plot = df[plotkey].apply(format_id).astype('string')

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
        'plot':           plot.values,
        'crop':           crop_label.values,
        '_crop_code':     crop_code_int.values,
        'Quantity':       quantity.values,
        'u':              u.values,
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
                perennial=False):
    """Reshape one sale module (I or Q) to (i, _crop_code) reported sale.

    Returns a DataFrame with columns [i, _crop_code, Quantity_sold,
    Value_sold] at the household-crop grain (no plot).  Summed within
    (i, _crop_code) because a household may report several sale rows for
    the same crop -- this is the REPORTED total the household sold of that
    crop, not a derived aggregate over plots.
    """
    crop_label, crop_code_int = _crop_codes(df[cropcode], perennial=perennial)

    qs = (pd.to_numeric(df[qty_sold], errors='coerce').astype('Float64')
          if qty_sold is not None and qty_sold in df.columns
          else pd.Series(pd.NA, index=df.index, dtype='Float64'))
    vs = (pd.to_numeric(df[value_sold], errors='coerce').astype('Float64')
          if value_sold is not None and value_sold in df.columns
          else pd.Series(pd.NA, index=df.index, dtype='Float64'))

    out = pd.DataFrame({
        'i':            df['hhid'].astype('string').values,
        '_crop_code':   crop_code_int.values,
        'Quantity_sold': qs.values,
        'Value_sold':   vs.values,
    })
    out = out[out['_crop_code'].notna()]
    grp = out.groupby(['i', '_crop_code'], as_index=False).agg(
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
    Quantity_sold, Value_sold, planting_month, harvest_month,
    intercropped, perennial.  Item-level reported values only.
    """
    harv = pd.concat(harvest_pieces, ignore_index=True)

    # Collapse exact duplicate (i, plot, crop, u) harvest rows by summing
    # reported quantity (a plot-crop may be split across several recorded
    # lines in the same unit); keep the first non-null date/flag.  This is
    # a reported-line consolidation, NOT a cross-unit aggregation: rows in
    # different units `u` stay distinct.
    harv['u'] = harv['u'].astype('string')
    harv = harv.groupby(['i', 'plot', 'crop', 'u', '_crop_code'],
                        as_index=False, dropna=False).agg({
        'Quantity':       'sum',
        'planting_month': 'first',
        'harvest_month':  'first',
        'intercropped':   'first',
        'perennial':      'first',
    })

    if sale_pieces:
        sale = pd.concat(sale_pieces, ignore_index=True)
        sale = sale.groupby(['i', '_crop_code'], as_index=False).agg(
            {'Quantity_sold': 'sum', 'Value_sold': 'sum'})
        # Attach sale ONLY where the (i, crop) is grown on exactly one
        # plot, so the household-crop reported figure unambiguously
        # belongs to that single plot-crop.  Multi-plot crops keep NaN.
        nplots = (harv.groupby(['i', '_crop_code'])['plot']
                  .nunique().rename('_nplots').reset_index())
        harv = harv.merge(nplots, on=['i', '_crop_code'], how='left')
        harv = harv.merge(sale, on=['i', '_crop_code'], how='left')
        single = harv['_nplots'] == 1
        harv['Quantity_sold'] = harv['Quantity_sold'].where(single, pd.NA)
        harv['Value_sold'] = harv['Value_sold'].where(single, pd.NA)
        harv = harv.drop(columns=['_nplots'])
    else:
        harv['Quantity_sold'] = pd.array([pd.NA] * len(harv), dtype='Float64')
        harv['Value_sold'] = pd.array([pd.NA] * len(harv), dtype='Float64')

    harv = harv.drop(columns=['_crop_code'])
    harv['t'] = t

    # crop must be non-null for the index; rows where the code did not map
    # to a Preferred Label (only code 48/1018 "Other (Specify)" and any
    # unmapped) are dropped from the index axis but logged by row count.
    harv = harv[harv['crop'].notna()]

    out = harv.set_index(['t', 'i', 'plot', 'crop'])
    # Defensive: collapse any residual duplicate index tuples (same
    # plot-crop reported in two units would survive above; sum Quantity).
    if not out.index.is_unique:
        num = out.groupby(level=['t', 'i', 'plot', 'crop']).agg({
            'Quantity':       'sum',
            'u':              'first',
            'Quantity_sold':  'first',
            'Value_sold':     'first',
            'planting_month': 'first',
            'harvest_month':  'first',
            'intercropped':   'first',
            'perennial':      'first',
        })
        out = num
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
    # One row per (HH, strategy); first-wins guards against duplicate HH rows.
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
            'plot':        df['plotkey'].astype('string').values,
            '_input_code': code.values,
            'crop':        pd.array([pd.NA] * len(df), dtype='string'),
            'Quantity':    q.values,
            'u':           u.values,
            'Improved':    pd.array([pd.NA] * len(df), dtype='boolean'),
        })
        pieces.append(piece[keep.values])
    if not pieces:
        return pd.DataFrame(columns=['i', 'plot', '_input_code', 'crop',
                                     'Quantity', 'u', 'Improved'])
    return pd.concat(pieces, ignore_index=True)


def _organic_block(df, *, hhid, plotkey, flag, t):
    """One row per plot where organic fertilizer was reported applied
    (ag_d36 == 1/Yes).  Presence is the reported item; Quantity / u are NA
    (Module D records no organic-fertilizer quantity)."""
    if flag is None or flag not in df.columns:
        return pd.DataFrame(columns=['i', 'plot', '_input_code', 'crop',
                                     'Quantity', 'u', 'Improved'])
    used = _int_codes(df[flag]) == 1
    out = pd.DataFrame({
        'i':           df['hhid'].astype('string').values,
        'plot':        df['plotkey'].astype('string').values,
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
            'plot':        df['plotkey'].astype('string').values,
            '_input_code': code.values,
            'crop':        pd.array([pd.NA] * len(df), dtype='string'),
            'Quantity':    q.values,
            'u':           u.values,
            'Improved':    pd.array([pd.NA] * len(df), dtype='boolean'),
        })
        pieces.append(piece[keep.values])
    if not pieces:
        return pd.DataFrame(columns=['i', 'plot', '_input_code', 'crop',
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
        'plot':        df['plotkey'].astype('string').values,
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
            nplots = (seed_rows.groupby(['i', 'crop'])['plot']
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
    grp = df.groupby(['t', 'i', 'plot', 'input', 'crop', 'u'],
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
    grp = grp.set_index(['t', 'i', 'plot', 'input', 'crop', 'u'])
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
#   * Value         -- ag_r04: REPORTED per-head current value ("if you sold
#                      one [LIVESTOCK] today, how much would you receive?").
#                      This is the per-head value the survey records; a herd
#                      value (Value x HeadCount) and a TLU rollup are
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
    columns (HeadCount, HeadAcquired, HeadSold, Value) and an internal
    ``_animal_code`` used for the keep-filter / dedup.  NO aggregation.
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
        'Value':        val.values,
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
    HeadAcquired, HeadSold, Value.  Item-level reported values only.
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
    # the first reported per-head Value (a per-head price is not additive).
    cat['t'] = t
    grp = cat.groupby(['t', 'i', 'animal'], as_index=False, dropna=False).agg({
        'HeadCount':    'sum',
        'HeadAcquired': 'sum',
        'HeadSold':     'sum',
        'Value':        'first',
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
