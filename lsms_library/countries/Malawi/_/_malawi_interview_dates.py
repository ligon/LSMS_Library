"""Build the earlier Malawi interview dates without losing visit provenance."""

import numpy as np
import pandas as pd

from lsms_library.datetime_parse import parse_datetime
from lsms_library.derivations import COLUMN
from lsms_library.local_tools import melt_visit_intervals


def panel_date_source(_row):
    """Mark a row read from the panel file, even if its ID is blank."""
    return True


def parsed_interview_dates(frame, wave, *, components=False, panel_visit=False, sentinels=()):
    """Parse the existing visit fields before the usual single-date melt.

    The two split-file covers use distinct runtime source names. The YAML
    maps them to common columns and supplies an explicit panel-only source flag
    for Visit 2. A structural cross-section non-visit is not a missing value.
    """
    wide = frame[[]].copy()
    keys = {}
    for ordinal, target in enumerate(('int_t', 'int_t_v2'), 1):
        if components:
            raw_columns = {part: f'{target}_{part}' for part in ('y', 'm', 'd')}
            if all(c not in frame for c in raw_columns.values()):
                continue
            operands = frame[list(raw_columns.values())].rename(
                columns={c: p for p, c in raw_columns.items()})
            options = {}
        else:
            if target not in frame:
                continue
            operands = frame[target]
            options = {'format': '%Y-%m-%d'}
            if ordinal == 2:
                options['sentinels'] = sentinels

        mask = np.ones(len(frame), dtype=bool)
        if ordinal == 2 and panel_visit:
            mask = frame['date_panel_role'].eq(True).to_numpy(dtype=bool, na_value=False)
            outside = operands.iloc[~mask].astype('string').str.strip()
            if outside.notna().any() and outside.fillna('').ne('').any():
                raise ValueError(f'Malawi/{wave}: visit 2 outside the panel cover')

        result = parse_datetime(operands.iloc[mask], calendar='gregorian',
                                resolution='day', country='Malawi',
                                table='interview_date', wave=wave, wave_folder=wave,
                                column=target, **options)
        dates = np.full(len(frame), np.datetime64('NaT'), dtype='datetime64[us]')
        dates[mask] = result.values.to_numpy(dtype='datetime64[us]')
        wide[target] = dates
        if result.derivation.notna().any():
            keyed = np.full(len(frame), pd.NA, dtype=object)
            keyed[mask] = result.derivation.to_numpy()
            keys[ordinal] = keyed

    if keys:
        # Transport per-source-row, per-visit provenance through the shared
        # melt, also when a raw household identifier is repeated.
        wide = wide.set_index(pd.RangeIndex(len(wide), name='_date_source_row'), append=True)
    out = melt_visit_intervals(wide, start_base='int_t', out_start='Int_t')
    if keys:
        positions = out.index.get_level_values('_date_source_row').to_numpy()
        labels = np.full(len(out), pd.NA, dtype=object)
        for visit, values in keys.items():
            select = out.index.get_level_values('visit') == visit
            labels[select] = values[positions[select]]
        out[COLUMN] = labels
        out = out.droplevel('_date_source_row')
    return out
