"""Reported household inputs from ECVMA2011-12 sections2C and3C.

2C covers rainy-season inputs;3C asks the2010/11 dry window. Each has its
OWN application gate, roster slots, crop and native unit. Transport reported
amounts verbatim under2011-12; no cycle scaling, parcel allocation or annual
comparability is inferred. In3C,05b is the unit and08b is TOTALFCFA, never a
unit/factor. See ledger1529-niger-native3c §§2–5 and NigerCONTENTS.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import (i as niger_i, _input_maps, _input_labels, _crop_maps,
                   _crop_labels, _unit_labels, _finish_plot_inputs,
                   _ecvma_input_observations)


# Own source names bind the numeric prefix; never borrow another module's
# slot/unit meanings or give one source's sidecar reference to the other.
_INPUT_SOURCES = {
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta': 'as02cq',
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3c_p1.dta': 'as03cq',
}


def _build_input_source(src, srcn, *, source_path, input_map, unit_map, crop_map):
    """Map one source-local labeled/numeric pair with existing native helpers.

    No source getter, map loader or writer runs here. The observation helper
    reads the declared sidecar only. Callers retain full original raw inputs;
    this output follows the existing explicit-Yes observed-row eligibility.
    Input frames are not modified. Mapping categorical labels is unchanged;
    category-card dictionaries alone do not prove reader decode fidelity.
    """
    p = _INPUT_SOURCES[source_path]
    if not src.index.equals(srcn.index):
        raise ValueError('Niger input labeled/numeric source rows must align')
    applied = srcn[p + '03'] == 1
    src = src[applied.values]
    srcn = srcn[applied.values]
    hh = srcn['hid'].apply(lambda x: niger_i(x) if pd.notna(x) else pd.NA)
    purchased = srcn[p + '07'].map({1: True, 2: False})
    input_obs = _ecvma_input_observations(
        hh, srcn[p + '02'], source_path, script_path=__file__)
    df = pd.DataFrame({
        'input_obs': input_obs.values,
        'i': hh.values,
        'input': _input_labels(src[p + '02'], input_map).values,
        'crop': _crop_labels(srcn[p + '04'], src[p + '04'], crop_map).values,
        'u': _unit_labels(src[p + '05b'], unit_map).values,
        'Quantity': pd.to_numeric(srcn[p + '05a'], errors='coerce').values,
        'Purchased': purchased.values,
        'Quantity_purchased': pd.to_numeric(srcn[p + '08a'], errors='coerce').values,
    })
    return _finish_plot_inputs(df, '2011-12')


def main():
    # Literal getter paths keep each new declared sidecar visible to source
    # discovery/fingerprinting. Reuse the current sanctioned reader unchanged.
    rainy = get_dataframe(
        '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta',
        convert_categoricals=True)
    rainy_n = get_dataframe(
        '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta',
        convert_categoricals=False)
    dry = get_dataframe(
        '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3c_p1.dta',
        convert_categoricals=True)
    dry_n = get_dataframe(
        '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3c_p1.dta',
        convert_categoricals=False)
    input_map, unit_map, _ = _input_maps()
    crop_map, _ = _crop_maps()
    sources = [
        _build_input_source(rainy, rainy_n,
            source_path='../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta',
            input_map=input_map, unit_map=unit_map, crop_map=crop_map),
        _build_input_source(dry, dry_n,
            source_path='../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3c_p1.dta',
            input_map=input_map, unit_map=unit_map, crop_map=crop_map),
    ]
    # Empty native frames can carry different index dtypes. Preserve the sole
    # contributing component exactly; both sources were already read/built.
    nonempty = [frame for frame in sources if len(frame) > 0]
    df = pd.concat(nonempty) if len(nonempty) > 1 else (nonempty or sources)[0]
    assert df.index.is_unique, 'plot_inputs has repeated native input observations'
    assert len(df) > 0, 'plot_inputs 2011-12 produced no rows'
    to_parquet(df, 'plot_inputs.parquet')


if __name__ in ('__main__', '<run_path>'):
    main()
