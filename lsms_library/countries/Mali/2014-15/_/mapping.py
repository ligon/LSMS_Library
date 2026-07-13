import os
import numpy as np
import pandas as pd
from lsms_library.local_tools import all_dfs_from_orgfile

# GH #380 (Phase 3 audit, 2026-06-07): the EACI 2014-15 Section 13
# consumption module records quantity PER ACQUISITION SOURCE, not as a
# single total.  Confirmed from the .dta variable labels:
#   s13q02   "A consommé le produit au cours des 7 derniers jours"   -> Oui/Non gate (value labels {1:Oui,2:Non}); NOT a quantity
#   s13q03a  "Quantité totale ... consommé ... acheté"               -> purchased quantity  (unit s13q03b)
#   s13q03c  "Valeur de cette quantité ... acheté"                   -> purchased value     (Expenditure)
#   s13q04a  "Quantité totale ... consommé ... propre production"    -> produced quantity   (unit s13q04b)
#   s13q05a  "Quantité totale ... consommé ... reçu en cadeau,... troc" -> in-kind quantity (unit s13q05b)
# Empirically s13q03a is purchased-only (in 307/2664 co-occurring rows
# purchased < produced, impossible for a total), so the EHCVM
# food_acquired_to_canonical (purchased = total - produced) does NOT
# apply.  Instead, food_acquired(df) below melts the three streams
# directly onto the canonical s axis {purchased, produced, inkind}.


def _unit_canonicalizer():
    '''Build a Code -> Preferred Label dict from the Mali country-level
    `u` table (categorical_mapping.org, the canonical
    Code|Preferred Label|<per-wave> unit table), mirroring the by-name
    auto-application of that table the EHCVM waves get on their `u`
    index level.  `Code` is the first non-Preferred-Label column (the
    raw source label).  Best-effort: returns an identity (empty) map if
    the org file or table is unavailable, so the melt never crashes on
    it.  Unmatched unit labels pass through unchanged, exactly as
    `.replace()` does.
    '''
    org = os.path.join(os.path.dirname(__file__), '..', '..', '_',
                       'categorical_mapping.org')
    try:
        tables = all_dfs_from_orgfile(org, to_numeric=False)
    except (OSError, ValueError):
        return {}
    tbl = tables.get('u')
    if tbl is None:
        tbl = tables.get('unit')
    if tbl is None or 'Preferred Label' not in tbl.columns:
        return {}
    src = [c for c in tbl.columns if c != 'Preferred Label']
    if not src:
        return {}
    return (tbl.set_index(src[0])['Preferred Label']
            .astype(str).str.strip().to_dict())


def food_acquired(df):
    '''Melt the three EACI 2014-15 acquisition streams into canonical
    long form (GH #380).

    Input (post-grab, post-concat of EACIALI_p1/p2): index
    ``(t, v, visit, i, j)`` with columns ``QuantityPurchased`` /
    ``UnitPurchased`` / ``ExpenditurePurchased`` /
    ``QuantityProduced`` / ``UnitProduced`` / ``QuantityInkind`` /
    ``UnitInkind``.

    Output: index ``(t, v, i, j, u, s)`` with columns
    ``Quantity`` and ``Expenditure``; ``s`` in
    ``{purchased, produced, inkind}``.  Only the purchased stream carries
    an Expenditure (value); produced / in-kind are quantity-only (NaN
    Expenditure), matching the EHCVM canonical contract.

    ``visit`` (passage) is dropped because the canonical food_acquired index
    (lsms_library/data_info.yml) is ``(t, v, i, j, u, s)`` -- it has no
    ``visit`` level -- so the two passages are AGGREGATED into the wave total.

    GH #323 -- correcting the rationale that used to sit here.  This docstring
    previously claimed passage "is a sample split, not a repeated measure, so it
    must not enter the canonical index".  That is FALSE for the EACI waves and
    the claim is worth killing explicitly, because it is the same false premise
    that made ``interview_date`` throw away one date per household:

        EACIALI_p1: 479,304 rows, passage=1, 3,804 households
        EACIALI_p2: 479,304 rows, passage=2, 3,804 households
        households in p1 only: 0 | p2 only: 0 | in BOTH: 3,804  (100% overlap)

    The two passages are a genuine REPEATED MEASURE: the same households
    revisited a median 124 days apart (post-planting and post-harvest), each
    visit carrying its own 7-day consumption recall ("au cours des 7 derniers
    jours").

    Dropping `visit` is nevertheless SAFE here -- and, unlike interview_date,
    it loses nothing -- because food_acquired is an additive-measure table
    (``_ADDITIVE_MEASURE_COLUMNS`` in feature.py, GH #501):
    ``_normalize_dataframe_index`` SUMS Quantity and Expenditure over the
    resulting duplicate (t, v, i, j, u, s) tuples and re-derives Price from the
    summed totals.  It does NOT ``first()`` them.  So the wave figure is the
    total acquired across the survey's two recall weeks, and summing does not
    double-count: the two 7-day windows are disjoint for 3,738 of 3,804
    households (the remaining 66 have visits <=7 days apart -- a source-data
    fieldwork artefact, some with negative gaps -- where the windows may
    overlap; that is a pre-existing wrinkle in the .dta, not an artefact of this
    reshape).

    NOTE the consequence for cross-wave comparison: 2014-15 (and 2017-18) are
    two-recall-week totals, while the EHCVM waves (2018-19, 2021-22) are single
    7-day recalls.  Consumers comparing levels ACROSS waves must account for
    that; it is a property of the instrument, not of this code.
    '''
    work = df.reset_index()
    if 'visit' in work.columns:
        work = work.drop(columns=['visit'])

    base = ['t', 'v', 'i', 'j']  # 'i' expands to (grappe, menage) already flattened
    base = [c for c in base if c in work.columns]

    unit_map = _unit_canonicalizer()

    def _stream(source, qty_col, unit_col, exp_col=None):
        cols = base + [qty_col, unit_col] + ([exp_col] if exp_col else [])
        sub = work[cols].copy()
        sub = sub.rename(columns={qty_col: 'Quantity', unit_col: 'u'})
        if exp_col:
            sub = sub.rename(columns={exp_col: 'Expenditure'})
        else:
            sub['Expenditure'] = np.nan
        sub['s'] = source
        # Canonicalize unit labels (Kg, Litre, ...) via the country `unit`
        # table; unmatched labels (e.g. Gramme, Centilitre, Sac large)
        # pass through unchanged.
        sub['u'] = sub['u'].astype('string').str.strip()
        if unit_map:
            sub['u'] = sub['u'].replace(unit_map)
        # Drop empty rows: keep a stream row only if it carries a positive
        # quantity OR (purchased only) a positive expenditure.
        qpos = pd.to_numeric(sub['Quantity'], errors='coerce').fillna(0) > 0
        epos = pd.to_numeric(sub['Expenditure'], errors='coerce').fillna(0) > 0
        return sub[qpos | epos]

    purchased = _stream('purchased', 'QuantityPurchased', 'UnitPurchased',
                        'ExpenditurePurchased')
    produced = _stream('produced', 'QuantityProduced', 'UnitProduced')
    inkind = _stream('inkind', 'QuantityInkind', 'UnitInkind')

    out = pd.concat([purchased, produced, inkind], ignore_index=True)
    out['Quantity'] = pd.to_numeric(out['Quantity'], errors='coerce')
    out['Expenditure'] = pd.to_numeric(out['Expenditure'], errors='coerce')
    out = out.set_index(base + ['u', 's'])[['Quantity', 'Expenditure']]
    return out


def Int_t(value):
    '''
    Formatting interview date
    ''' 
    # date = f'{value[0]}-{value[1]}-{value[2]}'
    date = f'{int(value.iloc[0])}-{int(value.iloc[1])}-{int(value.iloc[2])}'
    return pd.to_datetime(date, format='%Y-%m-%d', errors='coerce').date()

def interview_date(df):
    """Parse the interview date.

    GH #323: ``visit`` now comes from the source ``passage`` column (declared in
    ``_/data_info.yml`` idxvars), NOT from ranking ``Int_t`` within household as
    this hook used to do::

        df['visit'] = df.groupby(level='i')['Int_t'].rank(method='first')...

    That rank was a positional GUESS at the visit number, and it is provably
    wrong for the 66 households whose passage-2 interview is dated BEFORE their
    passage-1 interview (the inter-visit gap runs from -10 to +203 days): the
    rank would label their passage 2 as visit "1".  ``passage`` is ground truth
    and needs no inference.  (It also ranked a level the declared index then
    dropped, so the whole synthesis was discarded downstream anyway.)
    """
    df['Int_t'] = pd.to_datetime(df['Int_t'])
    return df
