#!/usr/bin/env python
"""Malawi 2024 food: native acquisitions plus registered IHS5 factor borrowing.

815 refreshment copies are omitted from the Panel half; source quantities,
expenditure and native units follow the recovered 83a producer. Factors are a
cross-wave modelling choice approved 2026-11-07, not measurements in this wave.
See food_borrowing.py, the derivation registry and Malawi/_/CONTENTS.org.
"""
from lsms_library.local_tools import to_parquet
from food_borrowing import read_food_wide, load_ihs5_factors, derive_borrowed_food

# Declared raw inputs for scan_script_data_refs / Wave._input_hash. The helper
# constructs absolute paths; these literals bind the same sidecars to this wave.
SOURCE_FILES = (
    '../Data/Cross_Sectional/hh_mod_g1.dta',
    '../Data/Panel/hh_mod_g1_24.dta',
    '../Data/Cross_Sectional/hh_mod_a_filt.dta',
    '../Data/Panel/hh_mod_a_filt_24.dta',
    '../../2019-20/Data/Cross_Sectional/ihs_foodconversion_factor_2020.dta',
)

if __name__ == '__main__':
    out = derive_borrowed_food(read_food_wide(), load_ihs5_factors())
    to_parquet(out, 'food_acquired.parquet')
