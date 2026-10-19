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
    index level.  The key column comes from the library's one resolver
    (`country._categorical_key_column`, GH #928): the table's `#+key:`
    if it declares one, else the first column that is neither
    `Preferred Label` nor another reserved curation column -- today
    `Code` (the raw source label).  Best-effort: returns an identity
    (empty) map if the org file or table is unavailable, so the melt
    never crashes on it -- but a malformed `#+key:` declaration is NOT
    unavailability and raises.  Unmatched unit labels pass through
    unchanged, exactly as `.replace()` does.
    '''
    from lsms_library.country import _categorical_key_column

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
    key = _categorical_key_column(tbl)
    if key is None:
        return {}
    return (tbl.set_index(key)['Preferred Label']
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

    ``visit`` (passage) is dropped, and that SUMS the two passages.
    Unlike the EHCVM ``vague`` it is NOT a sample split: the same 3,804
    households are in EACIALI_p1 and EACIALI_p2, each asked the 7-day
    module, so the two passages collide on one canonical tuple and the
    additive collapse adds them -- a served 2014-15 row covers 2 x 7 = 14
    days.  ``Mali/_/recall.yml`` records ``n_asks: 2`` for this wave (GH
    #851); see ``Mali/_/CONTENTS.org``.
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


def interview_date(df):
    # Int_t arrives already parsed (GH #1045: `datetime:` spec in data_info.yml).
    df['visit'] = df.groupby(level='i')['Int_t'].rank(method='first').astype(int).astype(str)
    df = df.set_index('visit', append=True)
    return df
