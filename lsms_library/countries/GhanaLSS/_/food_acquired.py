"""Concatenate wave-level food_acquired data for GhanaLSS.

Each wave-level script (``GhanaLSS/<wave>/_/food_acquired.py``) now emits
a canonical long-form parquet with index ``[t, i, j, u, s, visit]`` and
columns ``[Quantity, Expenditure(, Price)]`` (Phase 1 of GH #109; see
``slurm_logs/DESIGN_ghanalss_food_acquired_2026-06-15.org``).  ``i`` is the
household id, ``j`` the harmonized food item, ``u`` the unit, ``s`` the
source (``{'purchased', 'produced'}``), and ``visit`` the GhanaLSS-local
repeated-visit recall level.

This script just concatenates the wave parquets and applies GhanaLSS's
cross-wave ``id_walk`` (the ``change_id`` mechanism in ``ghanalss.py``,
driven by the ``Waves`` linkage-file dict).  Only the GLSS1<->GLSS2 panel
(1987-88 <-> 1988-89) is linked, via ``PANELC.DAT``; all later waves are
cross-sectional and ``change_id`` only normalizes the id dtype for them.

``change_id`` operates on the index level named ``'j'`` (the household id
under the legacy schema).  Under the canonical schema the household id is
``'i'`` and ``'j'`` is the food item, so we transiently swap the two level
names around the ``id_walk`` call: rename ``i`` -> ``j`` (and the food item
``j`` -> a private name) so ``change_id`` rewrites the household id, then
rename back.  This preserves the real panel logic without touching
``ghanalss.py``.

``food_expenditures``/``food_prices``/``food_quantities`` are no longer
built here -- they are auto-derived at runtime by the framework's
``_FOOD_DERIVED`` from this canonical ``food_acquired`` (cf. Uganda
post-#245).
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from ghanalss import change_id, Waves


def id_walk(df, wave, waves):
    use_waves = list(waves.keys())
    T = use_waves.index(wave)
    for t in use_waves[T::-1]:
        if len(waves[t]):
            df = change_id(df, '../%s/Data/%s' % (t, waves[t][0]), *waves[t][1:])
        else:
            df = change_id(df)
    return df


dfs = []
for t in Waves.keys():
    df = get_dataframe('../' + t + '/_/food_acquired.parquet').squeeze()
    # Apply the cross-wave panel id_walk.  change_id rewrites the index
    # level named 'j'; under the canonical schema that is the household id
    # 'i', so swap the level names around the call and swap back.
    df = df.rename_axis(index={'j': '__jfood', 'i': 'j'})
    df = id_walk(df, t, Waves)
    df = df.rename_axis(index={'j': 'i', '__jfood': 'j'})
    dfs.append(df)

p = pd.concat(dfs)

to_parquet(p, '../var/food_acquired.parquet')
