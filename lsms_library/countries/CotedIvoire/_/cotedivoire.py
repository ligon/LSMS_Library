# Formatting Functions for CotedIvoire
import warnings

import numpy as np
import pandas as pd
import lsms_library.local_tools as tools


def _cluster_majority(s):
    """Reduce a cluster-constant attribute to the value its households agree on.

    Returns the strict-majority value, or <NA> when there is no strict majority:
    a tie has no defensible winner, and emitting <NA> (class-2, loudly missing)
    is strictly safer than letting source row order pick one (class-1, silently
    wrong) -- which is exactly what the framework's groupby().first() did.
    ``cluster_features`` warns about every disagreement before this runs, so an
    <NA> here is never silent.
    """
    vals = s.dropna()
    if vals.empty:
        return pd.NA
    counts = vals.value_counts()
    if len(counts) > 1 and counts.iloc[0] == counts.iloc[1]:
        return pd.NA
    return counts.index[0]


def cluster_features(df):
    """Project the household-grain source onto the declared cluster grain (t, v).

    GH #323.  EVERY CotedIvoire wave reads cluster_features from a HOUSEHOLD-
    grain file -- WEIGHT{85,86,87,88}.DAT in 1985-89 and the EHCVM cover page
    Menage/s00_me_CIV2018.dta in 2018-19, i.e. literally the same files that
    ``sample`` reads with i = [CLUST, NH] / [grappe, menage].  The YAML declares
    only ``v``, so the extraction emitted one row per HOUSEHOLD into a table
    whose grain is one row per CLUSTER: 1588/1600/1600/1600/12992 rows for only
    100/100/100/100/1084 clusters.  The framework then silently collapsed the
    17,896 surplus rows with groupby().first().

    (t, v) IS the correct grain -- cluster_features owns ``v``, and adding ``i``
    would stop it being cluster_features.  The surplus rows are redundant
    household repeats of a cluster-constant attribute; the extraction simply
    never projected.  Do that here, deliberately, so the table is one row per
    cluster BY CONSTRUCTION and the framework's duplicate-collapse can never
    fire on it again.

    ``first`` is a row-order lottery wherever the households of a cluster
    DISAGREE about a cluster attribute.  They nearly always agree (Region is
    constant in all 100 clusters x 4 waves, and s00q01 in all 1,084 grappes), so
    first() landed on the right value by luck -- but not everywhere:

        grappe 648 (CAVALLY, 2018-19) has 11 households coded Rural and 1 coded
        Urbain.  These are distinct raw labels, not a case variant (the YAML
        already folds Rural/rural/RURAL and Urbain/urbain/URBAIN).  first()
        happened to return Rural only because the Urbain household sorts last.

    Take the strict majority (-> Rural) and WARN, naming the cluster and the
    competing values; on a tie emit <NA> rather than let row order decide.  Every
    disagreement is surfaced, so a future conflict fails loudly instead of being
    first()-ed away in silence.
    """
    if df.empty:
        return df

    levels = [lvl for lvl in df.index.names if lvl is not None]
    cols = list(df.columns)
    if not levels or not cols:
        return df

    flat = df.reset_index()

    # Surface every cluster whose households disagree on a cluster-constant
    # attribute BEFORE reducing, so nothing is ever resolved quietly.
    for col in cols:
        nun = flat.groupby(levels, observed=True)[col].nunique(dropna=True)
        for key in nun[nun > 1].index:
            keyt = key if isinstance(key, tuple) else (key,)
            mask = np.ones(len(flat), dtype=bool)
            for lvl, kval in zip(levels, keyt):
                mask &= (flat[lvl] == kval).to_numpy()
            counts = flat.loc[mask, col].value_counts(dropna=True)
            resolved = _cluster_majority(flat.loc[mask, col])
            tie_note = ("" if pd.notna(resolved)
                        else " (no strict majority -- emitted <NA> rather than guess)")
            warnings.warn(
                f"CotedIvoire/cluster_features: cluster {dict(zip(levels, keyt))} "
                f"has households disagreeing on the cluster-constant column "
                f"'{col}': {counts.to_dict()}. Resolved by strict majority to "
                f"{resolved!r}{tie_note}. (GH #323)",
                RuntimeWarning,
            )

    out = flat.groupby(levels, observed=True, sort=True)[cols].agg(_cluster_majority)

    assert out.index.is_unique, (
        "CotedIvoire/cluster_features: index "
        f"{levels} still not unique after projection to cluster grain"
    )
    return out


def i(value):
    '''
    Formatting household id: concatenate grappe + zero-padded menage.
    Compound key [grappe, menage] -> string like "1001001".
    '''
    return tools.format_id(value.iloc[0]) + tools.format_id(value.iloc[1], zeropadding=3)


# Coping strategy labels (EHCVM standard for CotedIvoire)
_COPING_LABELS = {
    1: "Utilisation de son épargne",
    2: "Aide de parents ou d'amis",
    3: "Aide du gouvernement/l'Etat",
    4: "Aide d'organisations religieuses ou d'ONG",
    5: "Marier les enfants",
    6: "Changement des habitudes de consommation",
    7: "Achat d'aliments moins chers",
    8: "Membres actifs ont pris des emplois supplémentaires",
    9: "Membres adultes inactifs/chômeurs ont pris des emplois",
    10: "Enfants de moins de 15 ans amenés à travailler",
    11: "Les enfants ont été déscolarisés",
    12: "Migration de membres du ménage",
    13: "Réduction des dépenses de santé/d'éducation",
    14: "Obtention d'un crédit",
    15: "Vente des actifs agricoles",
    16: "Vente des biens durables du ménage",
    17: "Vente de terrain/immeubles/Maisons",
    18: "Louer/mettre ses terres en gages",
    19: "Vente du stock de vivres",
    20: "Pratique plus importante des activités de pêche",
    21: "Vente de bétail",
    22: "Confiage des enfants à d'autres ménages",
    23: "Engagé dans des activités spirituelles",
    24: "Pratique de la culture de contre saison",
    25: "Autre stratégie",
    26: "Aucune stratégie",
}


def shocks(df):
    '''
    Post-process shocks DataFrame: collapse Cope1..Cope26 binary columns into
    HowCoped0, HowCoped1, HowCoped2 (top-3 coping strategies per shock-HH row).

    HowCoped2 is omitted from the output when no household used 3+ strategies
    (all-null column), since it would fail structural sanity checks and provides
    no information.  It is declared optional in data_scheme.yml.
    '''
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped: dict[int, list] = {0: [], 1: [], 2: []}
    for _, row in df[cope_cols].iterrows():
        found = []
        for c in cope_cols:
            num = int(c.replace('Cope', ''))
            val = row[c]
            try:
                val = float(val)
            except (ValueError, TypeError):
                continue
            if val >= 1:
                found.append(_COPING_LABELS.get(num, f'Strategy {num}'))
            if len(found) == 3:
                break
        for k in range(3):
            how_coped[k].append(found[k] if k < len(found) else pd.NA)

    df['HowCoped0'] = pd.array(how_coped[0], dtype='string')
    df['HowCoped1'] = pd.array(how_coped[1], dtype='string')
    hc2 = pd.array(how_coped[2], dtype='string')
    if hc2.isna().all():
        # No household used 3+ strategies — omit rather than write an all-null column.
        pass
    else:
        df['HowCoped2'] = hc2
    df = df.drop(columns=cope_cols)
    return df


def interview_date(df):
    """Melt EHCVM per-visit interview start/end timestamps onto a `visit`
    index. q23/q24/q25 a/b = visit 1/2/3 start/end -> int_start/int_end[_v2/_v3].
    Delegates to local_tools.melt_visit_intervals -> 'Interview start' /
    'Interview end'; collapsing `visit` with `first` reproduces the legacy
    single-date table."""
    return tools.melt_visit_intervals(df)
