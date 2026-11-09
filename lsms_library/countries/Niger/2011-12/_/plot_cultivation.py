"""Native ECVMA3B cultivation/seed/use answers, without a parcel/season join.

Every numeric-reader row and column survives, including No, unknown and missing
answers. Recorded household/slot identity is not a physical parcel or input
allocation. See ledger1529-niger-native3b3d and NigerCONTENTS.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import i as niger_i, _ecvma_input_observations


def build_native(raw):
    """Add only standard coordinates; keep native columns/dtypes/NA/order.

    The source helper reads its declared sidecar, not household data. Household
    panel walking remains the ordinary Country finalizer's responsibility.
    """
    if any(name in raw.columns for name in ('t', 'i', 'cultivation_obs')):
        raise ValueError('Niger plot_cultivation source contains a reserved coordinate')
    households = raw['hid'].map(
        lambda value: niger_i(value) if pd.notna(value) else pd.NA)
    observations = _ecvma_input_observations(
        households, raw['as03bqa'],
        '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3b_p1.dta',
        script_path=__file__)
    result = raw.copy(deep=True)
    result.index = pd.MultiIndex.from_arrays(
        [pd.array(['2011-12'] * len(raw), dtype='string'),
         households.array, observations.array],
        names=['t', 'i', 'cultivation_obs'])
    if not result.index.is_unique:
        raise ValueError('Niger plot_cultivation repeats a native observation')
    return result


def main():
    raw = get_dataframe(
        '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3b_p1.dta',
        convert_categoricals=False)
    to_parquet(build_native(raw), 'plot_cultivation.parquet')


if __name__ in ('__main__', '<run_path>'):
    main()
