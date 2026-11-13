"""Independent raw-cover oracle for the approved GH #1120 D1 contract.

File declarations are shared with the country; identity, membership and weight
expectations are constructed from recorded answers, never from its builders.
"""
from functools import lru_cache

import numpy as np
import pandas as pd

from lsms_library.local_tools import get_dataframe, format_id
from lsms_library.paths import countries_root
from lsms_library.countries.Nigeria._ import nigeria as ng


def _key(row):
    if pd.notna(row.get('ea')) and row['ea'] == 0:
        return 'moved-' + format_id(row['hhid'])
    if any(pd.isna(row.get(c)) for c in ('state', 'lga', 'ea')):
        return 'unlocated-' + format_id(row['hhid'])
    return '/'.join(format_id(row[c]) for c in ('state', 'lga', 'ea'))


@lru_cache(maxsize=10)
def recorded_round(t):
    """Own-round cover > supplement > roster, with native (unscaled) weights."""
    folder = ng.wave_folder_map[t]
    spec = ng.ROUND_COVERS[folder]
    rnd = 'pp' if t == ng.PP_QUARTER[folder] else 'ph'

    def read(role, labels=False):
        df = get_dataframe(str(countries_root() / 'Nigeria' / folder /
                               'Data' / spec[role]),
                           convert_categoricals=labels).copy()
        df.index = pd.Index(df.hhid.map(format_id), name='i')
        return df

    raw = read(rnd)
    assert raw.index.is_unique
    out = pd.DataFrame({'v': raw.apply(_key, axis=1),
                        'weight': raw[spec['weight']].astype(float),
                        'panel_weight': raw[spec['panel_weight']].astype(float)})
    if rnd == 'ph' and folder in ('2018-19', '2023-24'):
        # Owner ruling #1120 comment6380186741: code AND labelled meaning.
        field, code, labels = (
            ('hhsmlc', 1, {'YES', '1. YES'}) if folder == '2018-19' else
            ('hh_moved', 2, {'NO, IN ORIGINAL DWELLING',
                             '2. NO, IN ORIGINAL DWELLING'}))
        pp, dec = read('pp'), read('ph', labels=True)
        for i in raw.index[(raw.ea == 0) & (raw[field] == code)]:
            meaning = ' '.join(str(dec.loc[i, field]).upper().split())
            if meaning in labels and i in pp.index:
                candidate = _key(pp.loc[i])
                if not candidate.startswith(('moved-', 'unlocated-')):
                    out.loc[i, 'v'] = candidate
    for role in (rnd + '_supplement', rnd + '_roster'):
        if role not in spec:
            continue
        extra = read(role)
        extra = extra[~extra.index.isin(out.index)]
        for i, group in extra.groupby(level='i'):
            keys = group.apply(_key, axis=1)
            assert keys.nunique() == 1
            out.loc[i] = [keys.iloc[0], np.nan, np.nan]
    return out.sort_index()


def recorded_geo(t):
    folder = ng.wave_folder_map[t]
    spec = ng.ROUND_COVERS[folder]
    expected = recorded_round(t)[['v']].copy()
    if spec['geo'] is None:
        return expected.assign(Latitude=np.nan, Longitude=np.nan)
    fn, lat, lon = spec['geo']
    geo = get_dataframe(str(countries_root() / 'Nigeria' / folder / 'Data' / fn))
    geo = geo.set_index(geo.hhid.map(format_id))
    assert geo.index.is_unique
    return expected.join(geo[[lat, lon]].rename(columns={lat: 'Latitude',
                                                        lon: 'Longitude'}))


def assert_numeric_exact(actual, expected):
    """Nullable exact equality plus explicit NumPy equality (GH #1206)."""
    a = pd.Series(actual).reset_index(drop=True).astype('Float64')
    b = pd.Series(expected).reset_index(drop=True).astype('Float64')
    assert a.isna().equals(b.isna())
    assert a.array.equals(b.array)
    np.testing.assert_array_equal(a.dropna().to_numpy(dtype=float),
                                  b.dropna().to_numpy(dtype=float))
