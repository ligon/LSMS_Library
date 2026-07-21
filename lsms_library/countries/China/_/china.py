# Formatting functions for China (CHNS).
#
# China is otherwise fully YAML-driven; this module exists for the
# auto-wired (by table name) `plot_features` and `individual_education`
# df_edit hooks below.

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


def _years_to_education_level(y):
    """Bin years-of-schooling -> canonical Educational Attainment label (#495).

    CLSS 1995-97 has NO categorical attainment variable; education exists only
    as continuous years (S02.DTA s0201).  Cutoffs follow China's 6+3+3+4 system
    (primary 6 / junior-secondary 3 / senior-secondary 3 / bachelor 4) mapped
    onto canonical_education_labels.org; complete/incomplete is inferred from
    cycle length (the standard approach when only years are recorded).  99 is
    the survey's missing sentinel -> NaN.
    """
    if pd.isna(y):
        return pd.NA
    try:
        y = int(round(float(y)))
    except (TypeError, ValueError):
        return pd.NA
    if y == 99 or y < 0:
        return pd.NA                       # missing sentinel
    if y == 0:
        return 'None'
    if y <= 5:
        return 'Primary incomplete'
    if y == 6:
        return 'Primary complete'
    if y <= 8:
        return 'Lower secondary'
    if y == 9:
        return 'Lower secondary complete'
    if y <= 11:
        return 'Upper secondary'
    if y == 12:
        return 'Upper secondary complete'
    if y <= 15:
        return 'Tertiary certificate/diploma'   # da-zhuan / non-degree college
    if y == 16:
        return 'Bachelor'                        # 12 + 4-yr degree
    return 'Postgraduate'                        # >=17 (none observed this wave)


def individual_education(df):
    """df_edit hook (auto-wired by table name): derive canonical Educational
    Attainment from S02 years-of-schooling for China CLSS 1995-97 (#495).

    The wave data_info.yml extracts the raw years (s0201) into the
    ``Educational Attainment`` column; here we bin it to the canonical ordinal
    vocabulary.  Index (t, i, pid) is preserved.  Replaces the prior wiring to
    S01B s01b10 (the *mother's occupation* code) -- a silent data-correctness bug.
    """
    out = df.copy()
    out['Educational Attainment'] = (
        out['Educational Attainment'].map(_years_to_education_level).astype('string'))
    return out
