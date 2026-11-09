"""Native ECVMA3D payment count and mean FCFA/payment, never their product.

Retain all numeric-reader rows/columns, including missing charge codes and No
answers. Constructed release positions are not payment/transaction IDs. No
Cost, Quantity, plot allocation or annualization is inferred. See
ledger1529-niger-native3b3d and NigerCONTENTS.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import i as niger_i, _crop_row_observations


def build_native(raw):
    """Add only standard coordinates; keep native columns/dtypes/NA/order.

    The source helper reads its declared sidecar, not household data. Household
    panel walking remains the ordinary Country finalizer's responsibility.
    """
    if any(name in raw.columns for name in ('t', 'i', 'cost_obs')):
        raise ValueError('Niger farm_cost_operands source contains a reserved coordinate')
    households = raw['hid'].map(
        lambda value: niger_i(value) if pd.notna(value) else pd.NA)
    observations = _crop_row_observations(
        len(raw),
        '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3d_p1.dta',
        script_path=__file__)
    result = raw.copy(deep=True)
    result.index = pd.MultiIndex.from_arrays(
        [pd.array(['2011-12'] * len(raw), dtype='string'),
         households.array, observations.array],
        names=['t', 'i', 'cost_obs'])
    if not result.index.is_unique:
        raise ValueError('Niger farm_cost_operands repeats a native observation')
    return result


def main():
    raw = get_dataframe(
        '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3d_p1.dta',
        convert_categoricals=False)
    to_parquet(build_native(raw), 'farm_cost_operands.parquet')


if __name__ in ('__main__', '<run_path>'):
    main()
