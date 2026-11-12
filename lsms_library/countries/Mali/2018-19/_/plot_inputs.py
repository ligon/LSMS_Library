"""Own EHCVM 2018 household inputs; native use=Oui, never parcel allocated."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as household_id
from s16b_header_adapter import s16b_plot_inputs_for_wave


def build():
    raw = get_dataframe('../Data/s16b_me_mli2018.dta', convert_categoricals=False)
    frame = s16b_plot_inputs_for_wave(raw, '2018-19', household_id=household_id)
    to_parquet(frame, 'plot_inputs.parquet')


if __name__ == '__main__':
    build()
