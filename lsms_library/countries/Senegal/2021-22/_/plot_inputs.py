"""Own2021 household s16b driver (GH #1483; native/serving UNRUN).

Keep sanctioned writer inference in this wave. Public projection retains
full native unresolved/compound seed descriptions and missing-unit rows;
complete unfiltered raw transport lives in s16b_rows.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))

from lsms_library.local_tools import get_dataframe, get_categorical_mapping, to_parquet
from senegal import i
from s16b_2021 import s16b_plot_inputs


def build():
    maps = {name: get_categorical_mapping(
        tablename=name, idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
        for name in ('harmonize_input', 'harmonize_seed_crop', 'u')}
    raw = get_dataframe('../Data/s16b_me_sen2021.dta', convert_categoricals=False)
    frame = s16b_plot_inputs(raw, household_id=i,
                            input_map=maps['harmonize_input'],
                            seed_crop_map=maps['harmonize_seed_crop'], unit_map=maps['u'])
    to_parquet(frame, 'plot_inputs.parquet')


if __name__ == '__main__':
    build()
