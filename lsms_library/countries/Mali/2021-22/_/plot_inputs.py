"""Own EHCVM 2021 household inputs; namespaced raw IDs and separate amounts."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import ehcvm21_i as household_id
from s16b_header_adapter import s16b_plot_inputs_for_wave


def build():
    raw = get_dataframe('../Data/s16b_me_mli2021.dta', convert_categoricals=False)
    frame = s16b_plot_inputs_for_wave(raw, '2021-22', household_id=household_id)
    to_parquet(frame, 'plot_inputs.parquet')


if __name__ == '__main__':
    build()
