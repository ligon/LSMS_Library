#!/usr/bin/env python3
"""2010–11 bindings for the existing plot-input deletion (issue 1174)."""


def derive_nonseed_group_no_payload(plot_inputs):
    """Select existing 2010–11 nonseed groups removed by all-payload-NA.

    This documents the finalizer's deletion; it neither fills an amount nor
    interprets an unanswered use question. A recorded zero or False is payload.
    The separately registered seed deletion keeps its existing predicate.
    """
    index = plot_inputs.index
    return (plot_inputs.isna().all(axis=1)
            & (index.get_level_values('t') == '2010-11')
            & (index.get_level_values('input') != 'Seed'))


def inputs_nonseed_group_no_payload(wave):
    """Original Module D lines contributing to deleted groups, at input grain.

    Reuse the producer's three projections and reducer (ledger 1174 §§2/5).
    A second assembly with release positions as temporary household IDs retains
    each raw line's membership without changing the producer or its row keys.
    Multiple slots or raw lines in one group remain retrievable, once per line.
    """
    if wave != '2010-11':
        raise ValueError(f'The nonseed deletion is registered for 2010-11, not {wave!r}')

    from types import SimpleNamespace
    import numpy as np
    import pandas as pd
    from lsms_library.derivations import resolve_callable
    from lsms_library.local_tools import get_dataframe, format_id
    from lsms_library.paths import countries_root

    prefix = 'lsms_library.countries.Malawi._.malawi:'
    names = ['_fertilizer_block', '_organic_block', '_pesticide_block',
             'assemble_plot_inputs', 'drop_repeated_records', '_seed_source_reference']
    m = SimpleNamespace(**{name: resolve_callable(prefix + name) for name in names})
    source = 'Data/Full_Sample/Agriculture/ag_mod_d.dta'
    path = countries_root() / 'Malawi' / wave / source
    raw = get_dataframe(str(path), convert_categoricals=False)
    if {'t', 'i', 'input_obs', 'source'} & set(raw.columns):
        raise ValueError('Module D raw columns conflict with input-coordinate names')

    # Positions precede the same full-record repeat policy as plot_inputs.py.
    positions = np.arange(len(raw))[~raw.duplicated().to_numpy()]
    d = m.drop_repeated_records(raw).copy()
    d['hhid'] = positions.astype(str)
    d['plotkey'] = d['ag_d00'].apply(format_id)
    household = pd.Series(raw['case_id'].apply(format_id).to_numpy(),
                          index=np.arange(len(raw)))

    fertilizer = m._fertilizer_block(
        d, hhid='hhid', plotkey='plotkey',
        type1='ag_d39a', qty1='ag_d39d', unit1=None,
        type2='ag_d39f', qty2='ag_d39i', unit2=None,
        t=wave, gate='ag_d38')
    fertilizer['u'] = pd.Series('Kilogramme', index=fertilizer.index, dtype='string')
    pieces = [
        fertilizer,
        m._organic_block(d, hhid='hhid', plotkey='plotkey', flag='ag_d36', t=wave),
        m._pesticide_block(d, hhid='hhid', plotkey='plotkey', gate='ag_d40',
                          slots=[('ag_d41a', 'ag_d41b', 'ag_d41c'),
                                 ('ag_d41d', 'ag_d41e', 'ag_d41f')], t=wave),
    ]
    selected = []
    if any(len(piece) for piece in pieces):
        real = [piece.assign(i=piece['i'].astype(int).map(household)) for piece in pieces]
        assembled = m.assemble_plot_inputs(wave, real)
        gone = assembled.loc[derive_nonseed_group_no_payload(assembled)].reset_index()

        by_line = m.assemble_plot_inputs(wave, pieces).reset_index()
        by_line['_release_row'] = by_line['i'].astype(int)
        by_line['i'] = by_line['_release_row'].map(household)
        keys = ['t', 'i', 'plot_id', 'input', 'crop', 'u']
        contributing = by_line.merge(gone[keys], on=keys, how='inner', validate='many_to_one')
        selected = sorted(contributing['_release_row'].unique())
    out = raw.iloc[selected].copy()
    reference = m._seed_source_reference(path)
    out['t'] = wave
    out['i'] = household.loc[selected].to_numpy()
    out['input_obs'] = [f'row:{reference}:{position}' for position in selected]
    out['source'] = source
    return out.set_index(['t', 'i', 'input_obs'])
