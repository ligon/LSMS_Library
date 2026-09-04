#!/usr/bin/env python
"""GhanaLSS household nutrient intake from the West African Food Composition
Table 2019 (GH #563).

Output: ``../var/nutrition.parquet`` -- index ``(i, t)``, one column per
nutrient ``Preferred Label``, values the ABSOLUTE amount of each nutrient in
the food the household ACQUIRED over the wave's recall period.  That is the
grain and unit basis of ``Uganda/_/nutrition.py`` and
``Ethiopia/_/nutrition.py``, and ``Feature('nutrition')`` concatenates the
three, so it is matched exactly.

Inputs, all resolved through ``paths.countries_root()`` (GH #753 -- never
``files('lsms_library')``, and never the precedents' ``sys.path.append(
'../../_/')``, which reaches into a sibling country's directory to import a
module that is not on any import path):

* ``GhanaLSS/_/fct_west_africa.org``   the FCT, per 100 g edible portion
* ``GhanaLSS/_/food_items.org``        food labels + their ``FCT Code``
* ``Ethiopia/_/nutrient_labels.org``   the canonical nutrient axis (read-only;
                                       the same cross-country read Ethiopia
                                       itself does on Tanzania's demands.org)

----------------------------------------------------------------------------
WHY THIS SCRIPT DOES NOT CALL ``Country.food_quantities()``
----------------------------------------------------------------------------

The precedents do ``q = c.food_quantities()`` and keep the rows tagged
``u == 'kg'``.  Doing that here would put CEDIS into the nutrient matrix.

``GhanaLSS`` elicits expenditure rather than quantity before 2016-17
(``GhanaLSS/_/CONTENTS.org``), so ``food_acquired`` carries ``u='Value'``
rows whose ``Quantity`` *is* the cedi amount.  ``transformations.
_get_kg_factors`` finishes by inferring kg-per-unit factors from price
ratios; for ``u='Value'`` the ratio is degenerate (quantity and expenditure
are the same number) and it infers a factor of **0.49139 kg per cedi**.  The
conversion then relabels those rows ``u='kg'``.

Measured on this corpus: 1987-88 has 71,605 ``u='Value'`` rows and
``food_quantities(units='kgs')`` emits exactly 71,605 ``u='kg'`` rows -- the
whole wave.  Median household "kg" runs 2,228 (1987-88) to 251,592 (2005-06),
against 85 for 2016-17, tracking the cedi rather than any mass.

This contradicts ``food_quantities_from_acquired``'s own docstring, which
names ``u='Value'`` as the canonical *carry-through* case.  It is a defect in
``lsms_library/transformations.py``, not in this country's config, and it is
NOT fixed here -- see ``slurm_logs/ghana_audit/ISSUE_value_kg_factor.org``.

The work-around is local and small: drop the non-physical units from
``food_acquired`` *before* deriving, then call the same public derivation the
API would have called.  Filtering afterwards is impossible -- the kg relabel
destroys the native ``u``.

Of the units dropped, only ``value`` actually receives an inferred factor
today; ``all`` and ``none`` have none and are already carried through
unconverted.  They are dropped with it because none of the three is a
physical quantity, so a future inference pass must not pick them up either.

----------------------------------------------------------------------------
WHAT THIS MEANS FOR EACH WAVE -- read before using the output
----------------------------------------------------------------------------

* **1987-88, 1988-89**: 100% of rows are ``u='Value'``.  After the filter
  there is no physical quantity at all, so these waves produce **no rows**.
  That is by design, and is what the pre-existing intent recorded in #563
  expected; the defect above would have made them non-empty and wrong.
* **1991-92, 1998-99, 2005-06, 2012-13**: the surviving kg rows are
  **100% own production** (``s='produced'``); purchases were elicited by
  value.  Nutrition for these waves therefore describes the OWN-PRODUCTION
  basket only -- a biased subset (rural, staple-heavy), not a scaled-down
  version of total intake.  Do not read it as household intake.
* **2016-17**: the only wave carrying physical quantities for both purchases
  and own production.  This is the wave to use.

Quantities for the value-only waves are the subject of GH #562 phase 3b
(inverse price -> quantity imputation).  That is separate from the defect
above, which would corrupt 3b's output too.

----------------------------------------------------------------------------
UNIT BASIS
----------------------------------------------------------------------------

``fct_west_africa.org`` stores values per 100 g **edible portion**, as
published.  They are multiplied by 10 here to reach a per-kg basis, which is
the convention ``Ethiopia/_/fct_tools.py::fct_filter`` established ("Convert
serving size to Kgs instead of hectograms").  The edible-portion coefficient
``EDIBLE1`` is deliberately NOT applied: ``food_quantities`` is the quantity
acquired, and both precedents multiply acquired mass by per-EP densities.
Applying it here alone would make GhanaLSS silently incomparable with the
other two countries in ``Feature('nutrition')``.  See
``fct_west_africa.org`` for the full statement of that decision.

No FoodData Central fallback is used: the WAFCT covers the GhanaLSS basket
(measured coverage is reported by ``--report``).  No API key is read, and
none is embedded -- the precedents' literal key is not copied.
"""
import numpy as np
import pandas as pd

from lsms_library.local_tools import df_from_orgfile, to_parquet
from lsms_library.paths import countries_root

#: Units whose ``Quantity`` is not a physical amount.  See the module
#: docstring: dropping these before the kg derivation is what keeps cedis out
#: of the nutrient matrix.
NON_PHYSICAL_UNITS = {'value', 'all', 'none'}

#: per 100 g EP -> per kg.
HECTOGRAMS_PER_KG = 10


def _load_fct(root):
    """The WAFCT, indexed by FCT Code, per KILOGRAM, nutrient columns only."""
    fct = df_from_orgfile(root / 'GhanaLSS' / '_' / 'fct_west_africa.org',
                          name='fct_west_africa')
    n_labels = df_from_orgfile(root / 'Ethiopia' / '_' / 'nutrient_labels.org')
    canon = [s.strip() for s in n_labels['Preferred Label']]

    nutrients = [c for c in fct.columns if c in canon]
    # No fourth nutrient vocabulary (GH #563): every column we keep must be on
    # the canonical axis.  Vitamin K is absent from the WAFCT and is therefore
    # simply not among them -- it is NOT zero-filled, which would assert that
    # these foods contain none.
    assert set(nutrients) <= set(canon), sorted(set(nutrients) - set(canon))

    out = fct.set_index('FCT Code')[nutrients]
    out = out.apply(lambda s: pd.to_numeric(s, errors='coerce'))
    out = out * HECTOGRAMS_PER_KG
    out.index.name = 'FCT Code'
    out.columns.name = 'Nutrient'
    return out.fillna(0)


def _load_food_codes(root):
    """(wave, native label) -> FCT Code, plus Preferred Label -> FCT Code."""
    lab = df_from_orgfile(root / 'GhanaLSS' / '_' / 'food_items.org',
                          name='food_label')
    lab['Preferred Label'] = lab['Preferred Label'].astype(str).str.strip()
    lab['FCT Code'] = lab['FCT Code'].astype(str).str.strip()
    waves = [c for c in lab.columns if c not in ('Preferred Label', 'FCT Code')]

    rows = []
    for w in waves:
        native = lab[w].astype(str).str.strip()
        for nat, pl, code in zip(native, lab['Preferred Label'], lab['FCT Code']):
            if nat:
                rows.append((w, nat, pl, code))
    by_wave = pd.DataFrame(rows, columns=['t', 'j', 'Preferred Label', 'FCT Code'])
    by_label = dict(zip(lab['Preferred Label'], lab['FCT Code']))
    return by_wave, by_label


def _quantities(root):
    """Household food quantities in kg, with non-physical units removed.

    Returns a Series indexed (i, t, j) where j is the wave's own food label.
    """
    import lsms_library as ll
    from lsms_library.transformations import food_quantities_from_acquired

    fa = ll.Country('GhanaLSS').food_acquired()

    u = fa.index.get_level_values('u').astype(str).str.strip().str.lower()
    dropped = int(u.isin(NON_PHYSICAL_UNITS).sum())
    if dropped:
        print(f'nutrition: dropped {dropped} of {len(fa)} food_acquired rows '
              f'whose unit is non-physical {sorted(NON_PHYSICAL_UNITS)} '
              '-- see this module\'s docstring')
    fa = fa[~u.isin(NON_PHYSICAL_UNITS)]

    q = food_quantities_from_acquired(fa, units='kgs')
    kg = q.index.get_level_values('u').astype(str).str.lower() == 'kg'
    q = q[kg]
    q = q['Quantity'] if isinstance(q, pd.DataFrame) else q
    return q.groupby(['i', 't', 'j']).sum()


def build(report=False):
    root = countries_root()
    fct = _load_fct(root)
    by_wave, by_label = _load_food_codes(root)
    q = _quantities(root)

    d = q.rename('Quantity').reset_index()
    d = d.merge(by_wave[['t', 'j', 'FCT Code']], on=['t', 'j'], how='left')
    # A j that is already a Preferred Label (waves whose scripts emit the
    # harmonised label directly) resolves through the label map instead.
    fallback = d['j'].map(by_label)
    d['FCT Code'] = d['FCT Code'].where(d['FCT Code'].notna(), fallback)
    d['FCT Code'] = d['FCT Code'].fillna('')

    if report:
        _report(d, fct)

    mapped = d[d['FCT Code'].isin(fct.index)]
    # Sum to (i, t) x FCT Code: several GhanaLSS labels legitimately share one
    # FCT row (e.g. every smoked fish -> 09_053), so this is an addition over
    # distinct foods, not a duplicate-index collapse.
    m = mapped.groupby(['i', 't', 'FCT Code'])['Quantity'].sum().unstack('FCT Code')
    m = m.reindex(columns=fct.index).fillna(0)

    n = m @ fct
    n.columns.name = None
    n = n.astype('float64')
    return n


def _report(d, fct):
    tot = d['Quantity'].sum()
    ok = d['FCT Code'].isin(fct.index)
    print('\n--- FCT coverage of GhanaLSS food quantities (kg) ---')
    print(f'overall: {100 * d.loc[ok, "Quantity"].sum() / tot:.2f}% of '
          f'{tot:,.0f} kg maps to an FCT row')
    per = d.assign(ok=ok).groupby('t').apply(
        lambda g: pd.Series({
            'kg': g['Quantity'].sum(),
            'mapped_share': g.loc[g['ok'], 'Quantity'].sum() / g['Quantity'].sum(),
        }), include_groups=False)
    print(per.to_string())
    print('\ntop-10 unmapped j by kg:')
    print(d[~ok].groupby('j')['Quantity'].sum().sort_values(ascending=False)
          .head(10).to_string())


if __name__ == '__main__':
    import sys
    n = build(report='--report' in sys.argv)
    print(f'\nnutrition: {n.shape[0]} rows x {n.shape[1]} nutrients; '
          f'index {list(n.index.names)}')
    print(n.groupby('t').size().to_frame('rows').to_string())
    to_parquet(n, '../var/nutrition.parquet')
