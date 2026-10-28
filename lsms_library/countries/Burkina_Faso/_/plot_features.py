"""Concatenate wave-level plot_features for Burkina Faso (GH #167; EHCVM cluster).

Each EHCVM wave's ``Burkina_Faso/<wave>/_/plot_features.py`` writes a
parquet indexed by ``(t, i, plot_id)`` with the canonical plot_features
columns.  This script concatenates the waves that have one (2018-19,
2021-22; the pre-EHCVM 2014 EMC wave uses a different instrument and is
out of scope).  ``v`` is NOT baked in — the framework joins it from
``sample()`` at API time.

This concatenator preserves each wave's native household ids. The framework
applies its exactly-once panel rename (GH #1245); this script does not apply
``id_walk``. The default ``load_from_waves`` route walks ids before writing
its country parquet, whereas this standalone script writes native ids.

Both listed waves declare a builder, so a missing output is a failed or
unrun build, never an unwired wave (GH #1287). Refuse it before writing a
partial country table; other reader errors propagate unchanged. Like the
Niger refusal, this checks absence, not the freshness of an existing output.
"""
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from lsms_library.paths import data_root


WAVES = ['2018-19', '2021-22']

# Reuse the Niger source-script premise and data-root lookup (ledger #1287 §2).
country_dir = Path(__file__).resolve().parent.parent
unwired = [t for t in WAVES
           if not (country_dir / t / '_' / 'plot_features.py').is_file()]
assert not unwired, f'Burkina_Faso plot_features: WAVES lists {unwired} with no wave script'

pieces = []
for t in WAVES:
    fn = data_root('Burkina_Faso') / t / '_' / 'plot_features.parquet'
    if not fn.exists():
        raise FileNotFoundError(
            f'Burkina_Faso plot_features: wave {t} declares plot_features '
            f'but its parquet was not built; refusing to concatenate the '
            f'other waves without it (GH #1287). Run that wave script and '
            f'read its error.')
    pieces.append(get_dataframe(fn))

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot_id) after concat"

to_parquet(p, '../var/plot_features.parquet')
