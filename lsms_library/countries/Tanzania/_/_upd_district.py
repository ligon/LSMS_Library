"""District names for NPS rounds 1-2, from the district code (GH #1441).

Used by ``2008-15/_/cluster_features.py`` and named by ``_/derivations.yml``.  It
is its own module because ``tests/test_gh905_tanzania_signed_sums.py`` pins the
digest of the whole of ``tanzania.py``, so that file must not change for this.
"""
import sys
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import format_id, get_categorical_mapping, get_dataframe
from lsms_library.paths import countries_root

# Loaded by importlib from an absolute path when derivations.yml resolves it, so
# a relative sys.path entry does not find the country module.  Resolve it from
# __file__, as the wave `mapping.py` modules do.
_COUNTRY = str(Path(__file__).resolve().parent)
if _COUNTRY not in sys.path:
    sys.path.insert(0, _COUNTRY)
import tanzania


_REGION_CODE_BY_NAME = {tanzania.normalize_place(name): code
                        for code, name in tanzania.TZ_REGION_BY_CODE.items()}


def upd_district_labels(df, region='Region', district_code='_district_code',
                        v='v', codes=None):
    """District NAME for rows of NPS rounds 1-2, from the district CODE (GH #1441).

    The UPD leaves the name ``ha_02_2`` blank in rounds 1-2 and keeps the code
    ``ha_02_1``.  ``2008-15/_/categorical_mapping.org`` table
    ``upd_district_code`` holds each code's label as round 3's residents write
    it; see that file for the source of every row and why it stops at round 3.

    A row is labelled only where its own report is the one its cluster's geocode
    carries: reported region = the id's region and reported code = the id's
    district digit.  Every other row stays NA.  So a cluster stays NA only with
    ZERO confirming households, and one household is enough to label it: the gate
    is thin (7 labelled 2010-11 clusters have under half confirming).  Measured:
    2010-11 cluster ``4115002`` reports code 2 (KOROGWE) throughout, while its
    households in round 3 report code 2 and write LUSHOTO, which is what its own
    id's digit 1 names.

    Args:
        df: household-grain frame with ``v`` in the index (or columns).
        region: name of the reported-region column.
        district_code: name of the reported ``ha_02_1`` column ('1.0' or 1).
        v: name of the cluster-id level/column.
        codes: ``{Code: District}``; default reads the table, cwd-relative, as
            every wave script does.

    Returns:
        A Series on ``df.index``: the label, or NA.
    """
    if codes is None:
        codes = get_categorical_mapping(tablename='upd_district_code',
                                        idxvars='Code',
                                        **{'District': 'District'})
    codes = {int(k): val for k, val in codes.items()}
    flat = df.reset_index() if v not in df.columns else df

    def _cell(region_code, digit):
        if pd.isna(region_code) or pd.isna(digit):
            return pd.NA
        return int(region_code) * 10 + int(str(digit).split('.')[0])

    own = [_cell(_REGION_CODE_BY_NAME.get(tanzania.normalize_place(r), pd.NA), d)
           for r, d in zip(flat[region], flat[district_code])]
    geo = [_cell(*tanzania._geocode_fields(x, 'nps')) for x in flat[v]]
    labels = [codes.get(g, pd.NA) if (not pd.isna(g) and not pd.isna(o) and o == g)
              else pd.NA for o, g in zip(own, geo)]
    return pd.Series(labels, index=df.index, dtype=object)


UPD_DISTRICT_DERIVATION = 'Tanzania::cluster_features::upd-district-name-from-round-3'


def derive_upd_district_name(df, codes=None):
    """Rounds 1-2 ``District`` from the code, with its registry key (GH #1441).

    Only blank names in 2008-09 and 2010-11 are filled: rounds 3-4 carry their
    own answers, and round 4 numbers districts differently.  ``Derivation`` is set
    on the rows this fills and on no other.  The frame stays at household grain;
    every row of a cluster-round carries the same value, so the framework's
    ``(t, v)`` projection keeps it.

    Args:
        df: household-grain frame with ``t`` and ``v`` in its index, carrying
            ``District`` and the columns :func:`upd_district_labels` reads.
        codes: forwarded to :func:`upd_district_labels`.

    Returns:
        A copy of ``df`` with ``District`` filled and ``Derivation`` added.
    """
    out = df.copy()
    no_name = (out.index.get_level_values('t').isin(['2008-09', '2010-11'])
               & out['District'].isna().to_numpy())
    out['District'] = out['District'].where(
        ~no_name, upd_district_labels(out, codes=codes))
    out['Derivation'] = pd.Series(pd.NA, index=out.index, dtype='string')
    out.loc[no_name & out['District'].notna().to_numpy(),
            'Derivation'] = UPD_DISTRICT_DERIVATION
    return out


def inputs_upd_district_name():
    """The raw cover answers behind the name: rounds 1-3 at ``(t, v, r_hhid)``,
    one row per ``(r_hhid, round, clusterid)``.  Takes no ``wave``, because the
    names live in round 3 and the entry is scoped to rounds 1-2."""
    rounds = {1: '2008-09', 2: '2010-11', 3: '2012-13'}
    raw = get_dataframe(countries_root() / 'Tanzania' / '2008-15' / 'Data' / 'upd4_hh_a.dta')
    raw = raw[raw['round'].isin(rounds)].drop_duplicates(['r_hhid', 'round', 'clusterid'])
    out = raw[['r_hhid', 'round', 'clusterid', 'ha_01_1', 'ha_02_1', 'ha_02_2']].assign(
        t=raw['round'].map(rounds), v=raw['clusterid'].map(format_id))
    return out.set_index(['t', 'v', 'r_hhid'])
