"""Shared machinery for GhanaLSS ``community_prices`` (GH #562, phase 3a).

Every GLSS round fielded a dedicated market/community PRICE questionnaire,
separate from the household instrument (BID §2.3 for GLSS1/2; the GLSS3-7
price forms).  Each wave's ``<wave>/_/community_prices.py`` reads that wave's
own price file and calls :func:`assemble` here to produce the canonical table:

    index   (t, v, j, u, obs)
    columns Price, NumberOfUnits, Description

* ``v``   the price survey's own cluster id (``CLUST`` / ``clust``), the SAME
          keyspace as ``sample().v`` -- NATIVE in the index (there is no
          household ``i``, so the framework's ``_join_v_from_sample`` does not
          apply).  It is what every GLSS price form is keyed on: the 1987-88
          cover reads "LOCALITY / CLUSTER", GLSS3-6 "REGION / DISTRICT / NAME
          OF LOCALITY / EA (/ MARKET NUMBER)", GLSS7 "REGION / DISTRICT /
          CLUSTER / MARKET NAME".
* ``j``   the priced item, on the wave's ``harmonize_food`` Preferred-Label
          axis where the item is a food ``food_acquired`` carries; non-food
          items (and foods the wave's axis does not name) carry their OWN label
          on the same axis.  Decoded per wave through the ``harmonize_price_item``
          table in ``<wave>/_/categorical_mapping.org`` -- every wave's price
          list is its OWN code scheme (none coincides with the consumption
          ``Code_9b`` scheme), so the decode is by name, wave by wave.
* ``u``   the unit the price refers to, on the shared ``u`` axis of
          ``_/unit_labels.org`` (Preferred Label).
* ``obs`` the vendor observation.  EVERY GLSS price form records up to THREE
          observations per item ("1ST / 2ND / 3RD OBSERVATION"; GLSS7 a/b/c;
          BID §2.3 "prices from up to three vendors").  ``obs`` 1-3 is the
          form's slot.  Values above 3 arise ONLY where the source holds more
          than one record for the same (cluster, item, unit) -- a repeat visit
          (1988-89 clusters 2305/2310), several brand lines of one item
          (2016-17), or a mis-keyed sibling cluster -- and are enumerated in a
          deterministic order (the wave script's sort keys, then the slot).
          Nothing is averaged and nothing is dropped except rows with no
          reported price at all.

Reported columns ONLY:

* ``Price``          the surveyed price, in the wave's native cedi (pre-2007
                     GHC; 2012-13 and 2016-17 in redenominated GHS).  No
                     conversion, no deflation (the 1980s files' ``PRICEnU`` and
                     ``PRICE`` are CALCULATED fields -- BID §6.2 -- and are not
                     stored).
* ``NumberOfUnits``  the quantity, in ``u``, that ``Price`` refers to: the
                     weighed/measured quantity where the file carries one
                     (1987-88/1988-89 ``QUANn``, 2012-13 ``s1stkg`` etc.,
                     2016-17 ``quantity{a,b,c}``), otherwise the form's stated
                     basis for the item (``Basis`` column of
                     ``harmonize_price_item``: 1 for a weighed kg, 10 for "10
                     tablets", 6 for "6 yards", 0.170 for the evaporated-milk
                     tin).  GLSS3/GLSS4 distribute only the per-unit value
                     ``p`` = PRICE/KG (the weighed KG is not shipped), so
                     ``NumberOfUnits`` there is the form's basis.
* ``Description``    the free text the survey attaches to the row beyond
                     ``j``/``u``: the form's own item label (so two items the
                     ``harmonize_food`` axis folds together -- "Goat (fresh)"
                     and "Fresh mutton" -> ``Goat`` -- stay distinguishable),
                     the 2012-13 non-food basis (``s2desc``, "20 PIECES"), the
                     2016-17 brand (``itname``) and other-unit text.

A per-unit price (Price / NumberOfUnits), kg-standardisation, cross-cluster
medians and the price->quantity imputation of phase 3b are TRANSFORMATIONS,
never stored here.

Config paths are resolved through ``lsms_library.paths.countries_root()`` so
``LSMS_COUNTRIES_ROOT`` is honoured (GH #436 / #753; ``CONTENTS.org`` Trap 6
-- the wave ``mapping.py`` modules' ``files('lsms_library')`` pattern silently
reads the installed package's config tree).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from lsms_library.local_tools import df_from_orgfile, format_id, get_dataframe
from lsms_library.paths import countries_root

COUNTRY = 'GhanaLSS'

INDEX = ['t', 'v', 'j', 'u', 'obs']
COLUMNS = ['Price', 'NumberOfUnits', 'Description']


def wave_dir(wave: str) -> Path:
    """``<countries_root>/GhanaLSS/<wave>/_`` -- honours LSMS_COUNTRIES_ROOT."""
    return countries_root() / COUNTRY / wave / '_'


def _strip_table(t: pd.DataFrame) -> pd.DataFrame:
    t = t.copy()
    t.columns = [str(c).strip() for c in t.columns]
    return t.map(lambda x: str(x).strip() if pd.notna(x) else '')


def price_item_table(wave: str) -> pd.DataFrame:
    """The wave's ``harmonize_price_item`` org table, indexed by integer ``Code``.

    Columns: ``Label`` (the form / value-label item name), ``Preferred Label``
    (the ``j`` axis), ``Food`` (``yes`` = on the wave's ``harmonize_food``
    axis; ``own`` = a food the wave's axis does not name, own label;
    ``no`` = non-food), ``Unit`` (the ``u`` the form fixes for the item, blank
    where the file records a unit per row), ``Basis`` (the number of ``Unit``
    the form's price refers to, blank where the file records a quantity per
    row), ``Note``.
    """
    path = wave_dir(wave) / 'categorical_mapping.org'
    t = _strip_table(df_from_orgfile(path, name='harmonize_price_item',
                                     to_numeric=False))
    for col in ('Code', 'Label', 'Preferred Label', 'Food', 'Unit', 'Basis'):
        if col not in t.columns:
            raise KeyError(f'{path}: harmonize_price_item lacks column {col!r}')
    t = t[t['Code'] != '']
    t['Code'] = pd.to_numeric(t['Code'], errors='raise').astype(int)
    t['Basis'] = pd.to_numeric(t['Basis'].replace('', pd.NA), errors='raise')
    # A decode that silently yields {} is this country's most expensive
    # recurring defect (CONTENTS.org Trap 1).  Assert rather than discover it
    # as an empty table downstream.
    assert len(t) > 0, f'{path}: harmonize_price_item decoded to ZERO rows'
    assert t['Code'].is_unique, f'{path}: duplicate Code in harmonize_price_item'
    assert (t['Preferred Label'] != '').all(), \
        f'{path}: blank Preferred Label in harmonize_price_item'
    return t.set_index('Code')


def unit_label_map() -> dict[str, str]:
    """``{native unit spelling: Preferred Label}`` from ``_/unit_labels.org``."""
    path = countries_root() / COUNTRY / '_' / 'unit_labels.org'
    t = _strip_table(df_from_orgfile(path, name='unit_label', to_numeric=False))
    m = dict(zip(t['u'], t['Preferred Label']))
    m = {k: v for k, v in m.items() if k and v}
    assert m, f'{path}: unit_label decoded to an EMPTY dict'
    return m


def other_unit_map(wave: str) -> dict[str, str]:
    """``{lower-cased free-text spelling: Preferred Label}`` from the wave's
    ``harmonize_price_unit`` org table (the reader's "other unit" text);
    ``{}`` if the wave has no such table."""
    path = wave_dir(wave) / 'categorical_mapping.org'
    try:
        t = _strip_table(df_from_orgfile(path, name='harmonize_price_unit',
                                         to_numeric=False))
    except KeyError:
        return {}
    m = {k.lower(): v for k, v in zip(t['Text'], t['Preferred Label']) if k and v}
    assert m, f'{path}: harmonize_price_unit decoded to an EMPTY dict'
    return m


def canon_unit(label, umap: dict[str, str] | None = None,
               extra: dict[str, str] | None = None):
    """Native unit spelling -> Preferred Label on the shared ``u`` axis.

    Exact match first, then case-insensitive, then the wave's free-text
    spelling table ``extra`` (lower-cased keys); a spelling no table knows is
    returned unchanged (title-cased) so it is visible as NEW rather than
    silently dropped.  ``None``/blank -> ``pd.NA``.
    """
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return pd.NA
    s = str(label).strip()
    if s == '' or s.lower() in ('nan', '<na>'):
        return pd.NA
    umap = unit_label_map() if umap is None else umap
    if s in umap:
        return umap[s]
    low = {k.lower(): v for k, v in umap.items()}
    if s.lower() in low:
        return low[s.lower()]
    if extra and s.lower() in extra:
        return extra[s.lower()]
    return s.title()


def v_from_clust(series: pd.Series) -> pd.Series:
    """Cluster id -> ``v`` string on ``sample().v``'s keyspace (``format_id``)."""
    return series.apply(lambda x: format_id(x) if pd.notna(x) else pd.NA)


def assemble(t: str, rows: pd.DataFrame, sort_keys: list[str] | None = None
             ) -> pd.DataFrame:
    """Canonicalise one wave's long price rows.

    ``rows`` must carry columns ``v, j, u, slot, Price, NumberOfUnits,
    Description`` -- one row per (source record, observation slot), ``slot``
    being the form's 1/2/3.  ``sort_keys`` are extra columns (e.g. the survey
    month) that order several source records of the same (v, j, u) before the
    slot; they are dropped from the output.

    Rows with no reported ``Price`` are dropped (an empty slot is not a
    reported price); everything else is kept and enumerated under ``obs``.
    Returns a DataFrame indexed (t, v, j, u, obs) with the reported columns.
    """
    need = {'v', 'j', 'u', 'slot', 'Price', 'NumberOfUnits', 'Description'}
    missing = need - set(rows.columns)
    assert not missing, f'assemble({t}): rows lack {sorted(missing)}'
    df = rows.copy()
    df['t'] = t
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['NumberOfUnits'] = pd.to_numeric(df['NumberOfUnits'], errors='coerce')
    df['v'] = df['v'].astype('string')
    df['j'] = df['j'].astype('string')
    df['u'] = df['u'].astype('string')
    df['Description'] = df['Description'].astype('string')

    n0 = len(df)
    df = df[df['Price'].notna() & df['v'].notna() & df['j'].notna()]
    dropped = n0 - len(df)

    keys = ['t', 'v', 'j', 'u']
    order = keys + list(sort_keys or []) + ['slot']
    df = df.sort_values(order, kind='mergesort', na_position='last')
    # Deterministic enumeration within (t, v, j, u): the form's slot for a
    # single source record; continues 4.. where the source holds more records.
    df['obs'] = df.groupby(keys, dropna=False, sort=False).cumcount() + 1
    out = df.set_index(INDEX)[COLUMNS]
    assert out.index.is_unique, f'community_prices {t}: (t,v,j,u,obs) not unique'
    assert len(out) > 0, f'community_prices {t}: no rows'
    out.attrs['rows_dropped_no_price'] = dropped
    return out


def melt_observations(df: pd.DataFrame, slots: list[tuple],
                      base_cols: dict[str, str]) -> pd.DataFrame:
    """Wide (1st/2nd/3rd observation columns) -> long, one row per slot.

    ``slots`` is a list of ``(slot_number, {'Price': col, 'NumberOfUnits': col
    or None, 'u': col or None, 'unit_other': col or None})``; ``base_cols``
    maps output column -> source column for the per-record fields (v, j, ...).
    Missing source columns yield ``pd.NA``.
    """
    pieces = []
    for slot, cols in slots:
        part = pd.DataFrame(index=df.index)
        for out, src in base_cols.items():
            part[out] = df[src].values
        for out in ('Price', 'NumberOfUnits', 'u', 'unit_other'):
            src = cols.get(out)
            if src is not None and src in df.columns:
                part[out] = df[src].values
            elif out not in part.columns:
                # Not a per-slot field for this source; keep a per-record
                # value from base_cols if one was given, else NA.
                part[out] = pd.NA
        part['slot'] = slot
        pieces.append(part)
    return pd.concat(pieces, ignore_index=True)


# ---------------------------------------------------------------------------
# Shared wave builders.  Two pairs of waves share an instrument and a file
# layout (GLSS1/GLSS2; GLSS3/GLSS4); the wave scripts call these with their
# own wave id and source path.  2012-13 and 2016-17 have their own builders
# in their wave scripts.
# ---------------------------------------------------------------------------

def build_glss12(wave: str, src: str = '../Data/PRICE.DAT') -> pd.DataFrame:
    """GLSS1/GLSS2 ``PRICE.DAT``: one record per (CLUST, ITEMNO) with three
    vendor observations ``QUANn``/``PRICEn``; ``PRICEnU``/``DEFL``/``PRICE``
    are calculated fields (BID §6.2) and are not stored.  ``NumberOfUnits`` =
    QUANn x the form's lot size (Basis) where the description is a multi-unit
    lot (6 yards of cloth); repeat records are ordered by interview month."""
    raw = get_dataframe(src)
    items = price_item_table(wave)
    code = pd.to_numeric(raw['ITEMNO'], errors='coerce').astype('Int64')
    known = code.isin(items.index)
    assert known.all(), f'{wave}: unknown ITEMNO {sorted(code[~known].dropna().unique())}'
    rec = pd.DataFrame({
        'v': v_from_clust(raw['CLUST']),
        'j': code.map(items['Preferred Label']),
        'u': code.map(items['Unit']),
        'Description': code.map(items['Label']),
        'lot': code.map(items['Basis']).astype('Float64').fillna(1.0),
        'yr': pd.to_numeric(raw['YRINT'], errors='coerce'),
        'mo': pd.to_numeric(raw['MOINT'], errors='coerce'),
    })
    for n in (1, 2, 3):
        rec[f'Price{n}'] = pd.to_numeric(raw[f'PRICE{n}'], errors='coerce')
        rec[f'Quan{n}'] = pd.to_numeric(raw[f'QUAN{n}'], errors='coerce') * rec['lot']
    long = melt_observations(
        rec,
        slots=[(n, {'Price': f'Price{n}', 'NumberOfUnits': f'Quan{n}'}) for n in (1, 2, 3)],
        base_cols={'v': 'v', 'j': 'j', 'u': 'u', 'Description': 'Description',
                   'yr': 'yr', 'mo': 'mo'})
    return assemble(wave, long, sort_keys=['yr', 'mo'])


def build_glss34(wave: str, src: str, item_col: str = 'item', price_col: str = 'p',
                 time_col: str | None = 'time') -> pd.DataFrame:
    """GLSS3/GLSS4 ``G3PRICE``/``G4PRICE``: already long, one row per
    observation, with only the per-unit value (PRICE/KG) -- the weighed KG is
    not distributed, so ``NumberOfUnits`` is the form's basis per item.  The
    form's slot is not recorded: rows are enumerated in file order within
    (cluster, item, survey month)."""
    raw = get_dataframe(src, convert_categoricals=False)
    items = price_item_table(wave)
    code = pd.to_numeric(raw[item_col], errors='coerce').astype('Int64')
    known = code.isin(items.index)
    assert known.all(), f'{wave}: unknown item codes {sorted(code[~known].dropna().unique())}'
    rows = pd.DataFrame({
        'v': v_from_clust(raw['clust']),
        'j': code.map(items['Preferred Label']),
        'u': code.map(items['Unit']),
        'Description': code.map(items['Label']),
        'Price': pd.to_numeric(raw[price_col], errors='coerce'),
        'NumberOfUnits': code.map(items['Basis']).astype('Float64'),
        'month': (pd.to_numeric(raw[time_col], errors='coerce')
                  if time_col and time_col in raw.columns else 0),
    })
    rows['slot'] = rows.groupby(['v', 'j', 'u', 'month'], dropna=False).cumcount() + 1
    return assemble(wave, rows, sort_keys=['month'])
