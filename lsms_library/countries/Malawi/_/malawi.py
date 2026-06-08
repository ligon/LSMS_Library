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
                q = pd.to_numeric(work[qcol], errors='coerce')
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
