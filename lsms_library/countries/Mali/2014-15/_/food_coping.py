"""Build food_coping (rCSI coping-strategies day-counts) for Mali EACI 2014-15.

Source: EACIMEN_p2.dta, household questionnaire Section 17 ("Sécurité
alimentaire").  s17q02a..s17q02e are the standard reduced Coping
Strategies Index (rCSI) battery — "Au cours des 7 derniers jours, nombre
de jours à ..." — i.e. the number of days (0-7) in the last 7 the
household used each strategy.  The five items appear in the canonical
CSI order:

    s17q02a  consommer des aliments moins chers          -> LessPreferred
    s17q02b  Réduire les quantités consommées (portions) -> LimitPortion
    s17q02c  Réduire le nombre de repas par jour         -> ReduceMeals
    s17q02d  Réduire les quantités (adultes, p/ enfants) -> RestrictAdults
    s17q02e  Emprunter de la nourriture / compter sur l'aide -> BorrowFood

Loaded with convert_categoricals=False so the day-counts arrive as
integers; the residual code 9 is the survey's "Manquant" (missing)
sentinel and is dropped to NaN.  Recall window: last 7 days.

Output is long-form: one row per (t, i, Strategy) with column Days
(Int64, 0-7).  ``i`` is the EACI composite household id built with
Mali's i() formatter so it matches household_roster().i for this wave.
``v`` is NOT baked in — the framework joins it from sample().
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i

WAVE = '2014-15'

# Canonical CSI-order -> Strategy name.
STRATEGY = {
    's17q02a': 'LessPreferred',
    's17q02b': 'LimitPortion',
    's17q02c': 'ReduceMeals',
    's17q02d': 'RestrictAdults',
    's17q02e': 'BorrowFood',
}

src = get_dataframe('../Data/EACIMEN_p2.dta', convert_categoricals=False)

src = src.copy()
src['i'] = src.apply(lambda r: mali_i(pd.Series([r['grappe'], r['menage']])),
                     axis=1)

cols = list(STRATEGY)
long = src.melt(id_vars=['i'], value_vars=cols,
                var_name='Strategy', value_name='Days')
long['Strategy'] = long['Strategy'].map(STRATEGY)

# 9 == "Manquant" (missing); valid day-counts are 0-7.
long['Days'] = pd.to_numeric(long['Days'], errors='coerce')
long.loc[(long['Days'] < 0) | (long['Days'] > 7), 'Days'] = pd.NA
long = long.dropna(subset=['Days'])
long['Days'] = long['Days'].astype('Int64')

long['t'] = WAVE
df = long.set_index(['t', 'i', 'Strategy'])[['Days']].sort_index()

assert df.index.is_unique, "Non-unique (t, i, Strategy) index in food_coping 2014-15"
assert len(df) > 0, "food_coping 2014-15 produced no rows"

to_parquet(df, 'food_coping.parquet')
