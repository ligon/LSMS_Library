"""Own2021 household-input adapter candidate (GH #1483).

Pure numeric-frame adapter; no providers, package imports or top-level IO.
Bindings and limits: ledger 1483-burkina_faso-post-header-adapter-20261110,
sections 2–6. Cards are transcribed in s16b_2021_cards.org. In particular,
q09a names BOTH quantity and value; its own q09b is Unité and the separately
authenticated q09c is Montant. That source structure binds purchased quantity
to q09a, not q09c, under the existing #1188 wide-column contract. All raw
admitted answers and the combined wording remain available. No2018 edit.
"""
import pandas as pd


SOURCE = '../Data/s16b_me_bfa2021.dta'
CARD_FILE = 's16b_2021_cards.org'
UNIT_FIELDS = ('s16bq03b', 's16bq07b', 's16bq09b')
CARD_FIELDS = ('s16bq01', 's16bq02', 's16bq05', 's16bq08') + UNIT_FIELDS
RAW_FIELDS = ('grappe', 'menage', 'vague', 's16bq01', 's16bq02',
              's16bq03a', 's16bq03b', 's16bq05', 's16bq07a', 's16bq07b',
              's16bq08', 's16bq09a', 's16bq09b', 's16bq09c')
INDEX = ['t', 'i', 'input', 'crop', 'u']
CROP_NA = '(not crop-specific)'


class S16b2021Error(Exception):
    """Refuse loss or an unsupported source layout, without printing keys."""


def _codes(series, name):
    """No rounding, coercion-to-NA, or guessed sentinel decoding."""
    try:
        return pd.to_numeric(series, errors='raise').astype('Int64')
    except (TypeError, ValueError, OverflowError) as exc:
        raise S16b2021Error(f'{name}: expected integral numeric source codes') from exc


def _amount(series, name):
    try:
        return pd.to_numeric(series, errors='raise').astype('Float64')
    except (TypeError, ValueError, OverflowError) as exc:
        raise S16b2021Error(f'{name}: expected numeric source amounts') from exc


def _flag(series, name):
    codes = _codes(series, name)
    bad = codes.notna() & ~codes.isin([1, 2])
    if bad.any():
        raise S16b2021Error(f'{name}: {int(bad.sum())} unknown flag code(s)')
    return codes.map({1: True, 2: False}).astype('boolean')


def _labels(codes, field, card):
    """An unlabelled code retains its own source-qualified identity."""
    return codes.map(
        lambda code: (pd.NA if pd.isna(code) else card.get(
            str(code), f'Unknown [Burkina_Faso 2021-22 {field}={code}]'))
    ).astype('string')


def _curated(labels, mapping):
    # Exact own labels only: never look up the country map by numeric code.
    return labels.map(lambda x: mapping.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def adapt_s16b_2021(source, *, cards, input_map, seed_crop_map, unit_map,
                    id_fn):
    """Return independently unit-tagged used, gifted and purchased amounts.

    Only explicit q02=2 rows are excluded, counted in attrs. Missing/unknown
    use answers refuse the whole candidate instead of silently dropping rows.
    Acquisition answers do not gate eligibility and never fill one another.
    The caller supplies current country i; no id_walk here.
    """
    missing = set(RAW_FIELDS).difference(source.columns)
    if missing:
        raise S16b2021Error('missing source columns: ' + ', '.join(sorted(missing)))
    if source.attrs.get('id_converted'):
        raise S16b2021Error('expected native source, not already walked ids')
    for field in CARD_FIELDS:
        if field not in cards or not cards[field]:
            raise S16b2021Error(f'{field}: own card unavailable')
    for field in ('s16bq02', 's16bq05', 's16bq08'):
        if cards[field] != {'1': 'Oui', '2': 'Non'}:
            raise S16b2021Error(f'{field}: unexpected own flag card')

    raw = source.loc[:, list(RAW_FIELDS)].reset_index(drop=True).copy()
    for field in ('grappe', 'menage', 'vague', 's16bq01', 's16bq02') + UNIT_FIELDS:
        raw[field] = _codes(raw[field], field)
    native_key = ['grappe', 'menage', 's16bq01']
    if raw[native_key].isna().any(axis=None):
        raise S16b2021Error('missing native household/input identity')
    if raw.duplicated(native_key).any():
        raise S16b2021Error('duplicate native household/input identity')
    use = _flag(raw['s16bq02'], 's16bq02')
    if use.isna().any():
        raise S16b2021Error('missing usage answer; eligibility unresolved')
    excluded = int((~use).sum())
    raw = raw.loc[use].reset_index(drop=True)

    hh = pd.Series([id_fn(row) for _, row in raw[['grappe', 'menage']].iterrows()],
                   index=raw.index, dtype='string')
    if hh.isna().any():
        raise S16b2021Error('household formatter returned a missing identity')
    labels = _labels(raw['s16bq01'], 's16bq01', cards['s16bq01'])
    inputs = _curated(labels, input_map)
    crops = labels.map(lambda x: seed_crop_map.get(x, pd.NA)).astype('string')
    # Own card's exact spelling extends an already approved identity; no2018
    # tail code is consulted. Rubber/teak remains its literal combined label.
    groundnut = labels.eq("Semences d'arichide")
    inputs.loc[groundnut] = 'Seed'
    crops.loc[groundnut] = 'Arachide'
    units = {field: _curated(_labels(raw[field], field, cards[field]), unit_map)
             for field in UNIT_FIELDS}

    out = pd.DataFrame({
        't': pd.Series('2021-22', index=raw.index, dtype='string'),
        'i': hh, 'input': inputs, 'crop': crops.fillna(CROP_NA),
        'u': units['s16bq03b'].fillna('Manquant'),
        'Quantity': _amount(raw['s16bq03a'], 's16bq03a'),
        'Purchased': _flag(raw['s16bq08'], 's16bq08'),
        'Quantity_purchased': _amount(raw['s16bq09a'], 's16bq09a'),
        'Unit_purchased': units['s16bq09b'],
        'Received_as_gift': _flag(raw['s16bq05'], 's16bq05'),
        'Quantity_received_as_gift': _amount(raw['s16bq07a'], 's16bq07a'),
        'Unit_received_as_gift': units['s16bq07b'],
    })
    # Preserve all admitted raw answers, including unclassified numeric codes
    # and q09a's combined-worded answer. q09c is retained as a native amount;
    # no canonical expenditure, unit price or monetary derivation is added.
    for field in RAW_FIELDS:
        out[field] = source[field].reset_index(drop=True).loc[use].reset_index(drop=True)
    out = out.set_index(INDEX)
    if not out.index.is_unique:
        raise S16b2021Error('distinct reported input identities collide on declared keys')
    if out.index.to_frame(index=False).isna().any(axis=None):
        raise S16b2021Error('missing declared identity after mapping')
    out.attrs = dict(source.attrs)
    out.attrs['s16b_2021_candidate'] = {
        'excluded_explicit_nonuse': excluded,
        'purchase_quantity': 's16bq09a; own q09b Unité / q09c Montant',
        'component': 'panel source only; no Cross_Section coverage claim',
    }
    return out


def build_s16b_candidate(*, read, write, get_map, id_fn, format_id):
    """Minimal injected driver; only explicit caller-provided IO leaves run.

    The own2021 wave wrapper supplies sanctioned leaves. Pure controls supply
    invented frames and spies; no providers are reachable from the adapter.
    """
    cards = {field: get_map(fn=CARD_FILE, dirs=['../../_/'],
                           tablename='bfa2021_' + field, idxvars='Code',
                           **{'Label': 'Label'}) for field in CARD_FIELDS}
    # Org keys must have the same canonical spelling as native numeric codes.
    cards = {field: {format_id(code): label for code, label in card.items()}
             for field, card in cards.items()}
    maps = {name: get_map(tablename=name, idxvars='Original Label',
                          **{'Preferred Label': 'Preferred Label'})
            for name in ('harmonize_input', 'harmonize_seed_crop', 'u')}
    source = read(SOURCE, convert_categoricals=False)
    out = adapt_s16b_2021(source, cards=cards,
                         input_map=maps['harmonize_input'],
                         seed_crop_map=maps['harmonize_seed_crop'],
                         unit_map=maps['u'], id_fn=id_fn)
    write(out, 'plot_inputs.parquet')
    return out
