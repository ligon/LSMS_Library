# Formatting  Functions for Ghana 1987-88
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files("lsms_library")/'countries'/'GhanaLSS'/'1987-88'
_dirs = [f'{path}/_/', f'{path}/../_', f'{path}/../../_']

# A bare tools.get_categorical_mapping(tablename=...) returns an EMPTY dict --
# it passes no value column, so df_data_grabber drops the Label column and the
# squeeze yields {}.  Every lookup then misses and silently produces pd.NA,
# with nothing raised.  That is what left Region/Birthplace/Relationship 100%
# NULL in this wave (and, downstream, the Generation/Distance/Affinity kinship
# columns that _expand_kinship derives from Relationship).
# tools.code_label_map passes Label='Label' and keys the result by BOTH the
# string and the int form of each code.  See GH #372/#377/#348.
region_dict = tools.code_label_map('region', _dirs)
relationship_dict = tools.code_label_map('relationship', _dirs)

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value)

def Sex(value):
    '''
    Formatting sex veriable
    '''
    return (lambda s: 'MF'[s-1])(value)

def Age(value):
    '''
    Formatting age variable.

    Dtype-agnostic on purpose.  This used to be
    ``int(value) if value.isdigit() else pd.NA``, which requires a *string*
    and raises ``AttributeError`` on any number -- it only ever worked
    because AGEY happened to contain Stata's '.' missing marker and so
    arrived as a str column.  Since GH #704 the .DAT reader honours that
    marker, AGEY arrives as float64, and the whole wave's
    ``household_roster`` build died.  (The sibling 1988-89 wave already
    used the number-tolerant ``int(value)``.)
    '''
    try:
        return pd.NA if pd.isna(value) else int(float(value))
    except (TypeError, ValueError):
        return pd.NA

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    #needs mapping
    return region_dict.get(str(value), pd.NA)

def Relationship(value):
    '''
    Formatting relationship variable
    '''
    # GLSS1 uses the 14-code scheme the country-level `relationship` table
    # documents (it cites this wave's own data dictionary: catalog 2313,
    # y01a).  Verified against the raw data: REL takes codes 1..14, and code 1
    # occurs 3136 times = the household count.
    return relationship_dict.get(value, pd.NA)

def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)

def Region(value):
    '''
    Formatting region variable
    '''

    return region_dict.get(str(value), pd.NA)


def cluster_features(df):
    """Attach Appendix I's cluster attributes to this wave's cluster universe.

    GLSS1/GLSS2 have NO cluster-location variable in the microdata.  This used
    to infer the cluster's region as the modal birth region of its members
    under 12 -- a documented approximation that agreed with the Appendix on
    172/176 clusters and was wrong on the rest, always naming an
    ADJACENT region (the border-migration failure the under-12 filter cannot
    dodge).  Appendix I is authoritative; the inference is retired.

    The cluster universe comes from the DATA (176 clusters), not from the
    Appendix, which lists sampling areas.  A cluster absent from the Appendix
    keeps `Region` NA rather than being dropped or guessed.

    `Rural` now comes from the Appendix too.  The Appendix is three-way --
    U / R / SU -- and `SU` is folded onto `Rural` by decision (@ligon,
    2026-08-21); canonical `Rural` stays the binary {Urban, Rural}.  That fold
    is a judgement call and the survey offers no evidence either way (the GLSS2
    7-way `rural` table once cited as precedent is editorial, is not on the
    questionnaire, and decodes a non-resident child's place of residence -- see
    GH #692; do not cite it).  The U/R/SU distinction IS lost here; the raw
    three-way survives in `_/appendix_i_clusters.org`.
    """
    # Load the country module BY PATH: `countries/GhanaLSS/_/` is not an
    # importable package, and a path built from countries_root() honours
    # LSMS_COUNTRIES_ROOT.
    import importlib.util as _ilu
    from lsms_library.paths import countries_root
    _p = countries_root()/'GhanaLSS'/'_'/'ghanalss.py'
    _spec = _ilu.spec_from_file_location('_ghanalss_country', _p)
    _c = _ilu.module_from_spec(_spec); _spec.loader.exec_module(_c)
    attrs = _c.appendix_i_cluster_attributes('yr1', 1000)

    out = df.reset_index()[['t', 'v']].drop_duplicates()
    out['v'] = out['v'].astype(str)
    out = out.set_index(['t', 'v'])

    joined = out.join(attrs, how='left')
    return joined[['Region', 'Rural', 'Ecological_zone']]

def Int_t(value):
    '''
    Build interview date from first-visit (DAY1, MO1, YR1).  YR1 is a
    2-digit year (e.g. 87, 88) -> 1987, 1988.  .DAT columns may arrive
    as strings or ints.
    '''
    def _to_int(x):
        if pd.isna(x):
            return None
        try:
            return int(float(x))
        except (TypeError, ValueError):
            return None
    d, m, y = _to_int(value.iloc[0]), _to_int(value.iloc[1]), _to_int(value.iloc[2])
    if d is None or m is None or y is None or m < 1 or m > 12 or d < 1 or d > 31:
        return pd.NaT
    if y < 100:
        y += 1900
    return pd.to_datetime(f"{y}-{m}-{d}", format='%Y-%m-%d', errors='coerce')

Visits = range(1,7)


def sample(df):
    """Broadcast the cluster's Appendix I `Rural` onto its households.

    GLSS1/GLSS2 ship no household-level urban/rural variable -- a sweep of all
    170 dictionaries finds none.  The classification that DOES exist is of the
    cluster (BID Appendix I, U / R / SU), and every household in an enumeration
    area shares its EA's settlement class by construction, so broadcasting is
    the meaning of the variable rather than an imputation.

    The Appendix's `SU` (semi-urban) is folded onto `Rural` by decision
    (@ligon, 2026-08-21) -- canonical `Rural` is the binary {Urban, Rural}.
    The fold is @ligon's call, and it IS supported by the survey: the joint
    BID Sec 3.1 defines the tiers by 1984 Census locality population -- urban
    >5,000, semi-urban 1,500-5,000, rural <1,500 -- so the whole SU band sits
    below the survey's own urban threshold.  See `_/appendix_i_clusters.org`
    and GH #692.  (An earlier version of this docstring said "no survey
    evidence either way"; that predates the verification.)  The distinction is lost in this
    column and preserved raw in that org file.

    Households whose cluster is absent from the Appendix keep `Rural` NA rather
    than being guessed.  GH #685.
    """
    import importlib.util as _ilu
    from lsms_library.paths import countries_root
    _p = countries_root()/'GhanaLSS'/'_'/'ghanalss.py'
    _spec = _ilu.spec_from_file_location('_ghanalss_country', _p)
    _c = _ilu.module_from_spec(_spec); _spec.loader.exec_module(_c)
    attrs = _c.appendix_i_cluster_attributes('yr1', 1000)

    out = df.copy()
    out['Rural'] = out['v'].astype(str).map(attrs['Rural'])

    # ---- Sampling weights (GH #713; @ligon, 2026-08-22) -------------------
    # GLSS1 ships NO weight column of any kind -- a sweep of every
    # readable source file in this wave found only anthropometric body weight
    # (Y16A `WTA` beside `HTA` height; ZSCORE `WEIGHT` beside HAZ/WAZ).
    #
    # THE 1.0 BELOW IS OURS, NOT GSS's.  That is the sharp difference from
    # GLSS3, where GSS shipped an explicit 1.0 and stated in writing that the
    # sample "can be considered as being self-weighting".  Here we ASSERT a
    # design weight from the documented design rather than relaying a producer
    # value.  Evidence tier: (a) STATED.
    #
    # GLSS1 is the stronger case: report Appendix 1 states the self-weighting
    # property for round 1 directly, anchored to the 3,200-household draw.
    #
    # THE QUALIFIER THAT MATTERS: self-weighting is with respect to the *1984
    # Census* population -- the frame the master sample was drawn from.  It is
    # NOT self-weighting for the population at interview time, and no updated
    # population figures exist here to adjust for the drift.
    #
    # Contrast GLSS4, which used the SAME 1984 figures for PPS selection but
    # then assigned weights ex post from EA population change between the 1984
    # and 2000 censuses.  So GLSS4's weights pull toward a ~2000 population
    # while GLSS1-GLSS3 represent 1984 as drawn.  After normalisation the
    # SCALES match; the POPULATIONS REPRESENTED still do not.  Anything
    # comparing weighted levels across GLSS3 and GLSS4 must say which it means.
    #
    # `panel_weight` mirrors `weight` per the convention in `_/CONTENTS.org`.
    # For GLSS2 that is thin: the retained panel half is not distinguished from
    # the fresh half here, so `panel_weight` does no real longitudinal work.
    # `PANELC.DAT` carries the GLSS1->GLSS2 linkage if a genuine panel weight
    # is ever needed.
    #
    # Acquisition target that would settle GLSS2 (cited in a footnote, not
    # held): "Sample Design and Implementation in the First Three Rounds of
    # the GLSS".
    out['weight'] = 1.0
    out['panel_weight'] = 1.0
    return out
