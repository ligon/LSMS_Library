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

Only waves whose own metadata declares this table contribute. A retired or
unwired wave is skipped before reading; a declared native or YAML output
must exist and be readable (GH #1287). This checks absence, not the freshness
of an existing output.
"""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet
from lsms_library.paths import data_root


country = Country('Burkina_Faso')
pieces = []
for t in country.waves:
    wave = country[t]
    # Wave.data_scheme includes both native scripts and YAML declarations;
    # Country resolves logical waves through its declared folder mapping.
    # A parquet's presence is never a table declaration (ledger §7).
    if 'plot_features' not in wave.data_scheme:
        continue
    fn = data_root(country.name) / wave.wave_folder / '_' / 'plot_features.parquet'
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
