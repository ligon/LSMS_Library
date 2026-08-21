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
    Formatting age variable
    '''
    return int(value) if value.isdigit() else pd.NA

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

    `Rural` stays NA here: the Appendix is three-way (U/R/SU) against a
    canonical vocabulary of {Urban, Rural} -- see GH #685.
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
    joined['Rural'] = pd.NA
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
