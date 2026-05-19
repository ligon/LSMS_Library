# Formatting functions for EthiopiaRHS (ERHS).
#
# Formatting helpers ONLY -- intentionally NO `waves`/`Waves` dict, so
# Country.waves falls through to SOURCE.org auto-discovery
# (country.py:1054-1073). See _/CONTENTS.org and GH #271.
#
# ERHS `hhid` (HH no. for this survey) is unique only WITHIN a peasant
# association, so household identity is the composite (village, hhid)
# -- the EHCVM (grappe, menage) pattern. Pure YAML cannot express a
# composite key; the framework needs a named `i` formatter for the
# list-valued idxvar (see Mali/_/mali.py for the analogous helper).

import numpy as np
import pandas as pd

import lsms_library.local_tools as tools


def _norm_village(v):
    """Canonicalize a peasant-association name across source files.

    Village appears as 'Haresaw' in the demographics file (demo123)
    but 'HARESAW' in the food file (food1).  Title-case both so the
    composite household id joins across modules.  '.title()' leaves
    the already-title 'Haresaw' unchanged.
    """
    return str(v).strip().title()


def i(value):
    """Composite household id: (peasant association, within-village HH no.).

    `value` is a row Series [village, hh_no].  ERHS `hhid`/`q5` is
    unique only within a village, so identity is the pair.
    """
    return (tools.format_id(_norm_village(value.iloc[0]))
            + '_' + tools.format_id(value.iloc[1]))


def food_acquired(df):
    """Melt ERHS wide per-source food columns into canonical long form.

    ERHS records, per (household, item), separate quantities + units
    for three acquisition sources.  This is a new pattern: the
    framework helper (transformations.food_acquired_to_canonical)
    assumes a single unit with purchased = Total - Produced and no
    in-kind source, which ERHS does not satisfy.

    Input (df_edit hook, pre-`t`): index (i, j); columns the source
    triplets named q_/u_/e_ for purchased, produced, inkind.

    Output: index (i, j, u, s), columns [Quantity, Expenditure];
    s in {purchased, produced, inkind}.  The framework prepends `t`
    (check_adding_t) and joins `v` from sample() at API time.
    """
    w = df.reset_index()
    specs = [('purchased', 'q_purch', 'u_purch', 'e_purch'),
             ('produced',  'q_prod',  'u_prod',  None),
             ('inkind',    'q_inkind', 'u_inkind', None)]
    frames = []
    for s, q, u, e in specs:
        sub = pd.DataFrame({
            'i': w['i'].values,
            'j': w['j'].values,
            'u': w[u].astype('string').str.strip(),
            's': s,
            'Quantity': pd.to_numeric(w[q], errors='coerce'),
            'Expenditure': (pd.to_numeric(w[e], errors='coerce')
                            if e else np.nan),
        })
        # Keep a row only if it carries a measurement.
        sub = sub[(sub['Quantity'].fillna(0) > 0)
                  | (sub['Expenditure'].fillna(0) > 0)]
        frames.append(sub)
    out = pd.concat(frames, ignore_index=True)
    # A unit is required to place the quantity on the (.. u ..) axis.
    out = out[out['u'].notna() & (out['u'] != '')]
    out = out.set_index(['i', 'j', 'u', 's'])
    return out
