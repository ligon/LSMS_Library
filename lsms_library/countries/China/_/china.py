# Formatting functions for China (CHNS).
#
# China is otherwise fully YAML-driven; this module exists for the
# auto-wired (by table name) `plot_features` df_edit hook below.

import pandas as pd


def plot_features(df):
    """Resolve (i, plot_id) collisions in China CHNS 1995-97 land roster (GH #513).

    S05B.DTA carries a few duplicate (hid, s05bpn) plot rows for two
    households (hid 30132, 30137; documented in the wave data_info.yml as
    a raw-source quirk, previously left as-is).  Most are exact-duplicate
    rows (benign redundancy); a couple differ in recorded plot Area for the
    same plot_id.  Under the downstream ``groupby().first()`` the divergent
    ones silently drop a row.

    Drop the exact-duplicate rows (lossless), then cumcount-suffix any
    residual genuinely-divergent (i, plot_id) collisions -- Albania
    precedent (albania.py:287-295) -- so each survives rather than being
    dropped.  No canonical-index change (suffixes are just ``_2`` on the
    few real collisions).
    """
    flat = df.reset_index().drop_duplicates()
    key = [c for c in ('i', 'plot_id') if c in flat.columns]
    if key and flat.duplicated(key, keep=False).any():
        if 'Area' in flat.columns:
            flat = flat.sort_values('Area', ascending=False, na_position='last')
        n = flat.groupby(key, dropna=False).cumcount()
        extra = n > 0
        flat.loc[extra, 'plot_id'] = (
            flat.loc[extra, 'plot_id'].astype('string')
            + '_' + (n[extra] + 1).astype('string'))
    idx = [c for c in ('t', 'i', 'plot_id') if c in flat.columns]
    return flat.set_index(idx)
