"""Senegal own2021 household s16b transport and guarded public projection.

Ledger: 1483-senegal-own2021-adapter §§2–6. Literal cards below are from the
released zero-record own header (SHA256 883e269e...48c80f7), managed source
cc408db6055a1a78506e30260c73f9c3 / 5,044,514B. No providers or top-level IO.
Keep raw answers separately from the applied-input projection. In particular,
used code1 is Kg, whereas gift/purchase code1 is Gramme. No2018 behavior,
parcel allocation, nutrient grade, factor or monetary derivation is changed.
"""
from copy import deepcopy

import pandas as pd


SOURCE = '../Data/s16b_me_sen2021.dta'
SOURCE_MD5 = 'cc408db6055a1a78506e30260c73f9c3'
HEADER_SHA256 = '883e269e3ee2a47c473ad347efc10ff648fb2599650277d634019e36b48c80f7'
RAW_FIELDS = (
    'grappe', 'menage', 'vague', 's16bq01', 's16bq02', 's16bq03a',
    's16bq03b', 's16bq03b_autre', 's16bq04', 's16bq04_autre', 's16bq05',
    's16bq06', 's16bq06_autre', 's16bq07a', 's16bq07b', 's16bq08',
    's16bq09a', 's16bq09b', 's16bq09c',
)
INPUT_CARD = dict(enumerate((
    "Engrais organiques - déchets d'animaux",
    'Engrais organiques - Ordures ménagères',
    'Engrais inorganiques - Urée', 'Engrais inorganiques - Phosphates',
    'Engrais inorganiques - NPK/Formule unique',
    'DAP et autres engrais inorganiques',
    'Produits phytosanitaires - Pesticides',
    'Produits phytosanitaires - Herbicides',
    'Produits phytosanitaires - Fongicides', 'Autres produits phytosanitaires',
    'Semences de petit mil', 'Semences de sorgho', 'Semences de maïs',
    'Semences de riz', "Semences d'autres céréales", 'Semences de coton',
    'Semences de césame', 'Semences de haricots/niébé', "Semences d'arachide",
    'Plants/boutures de tubercules', "Plants d'hévéa, de teck",
    "Semences d'ognon", 'Semences de Carotte', "Semences d'aubergine",
    'Semences de Combo', 'Semences de Piment', 'Semences de Pastéque',
    'Semences de poivron', 'Semences de courge/courgette', 'Semences de fonio',
    'Semences de Oseille/bissap', 'Semences de Tomate', 'Semences de chou',
    'Semences de Conconbre', 'Semences de Laitue/Salade', 'Autre semences',
), 1))
# Separate dictionaries even where two cards happen to have the same words.
USED_UNITS = {1: 'Kg', 2: 'Sac Gros', 3: 'Sac moyen', 4: 'Sac Petit',
              5: 'Charrette asine', 6: 'Charrette bovine', 7: 'Seau',
              8: 'Panier', 9: 'Camion benne', 10: 'Autre (à spécifier)'}
GIFT_UNITS = {1: 'Gramme', 2: 'Kilogramme', 3: 'Tonne', 4: 'Litre', 5: 'Charrettée'}
PURCHASE_UNITS = {1: 'Gramme', 2: 'Kilogramme', 3: 'Tonne', 4: 'Litre', 5: 'Charrettée'}
CARDS = {'s16bq01': INPUT_CARD, 's16bq02': {1: True, 2: False},
         's16bq03b': USED_UNITS, 's16bq05': {1: True, 2: False},
         's16bq07b': GIFT_UNITS, 's16bq08': {1: True, 2: False},
         's16bq09b': PURCHASE_UNITS}
ALIASES = {'s16bq01': 'Input_native', 's16bq02': 'Used', 's16bq03b': 'u',
           's16bq05': 'Received_as_gift', 's16bq07b': 'Unit_received_as_gift',
           's16bq08': 'Purchased', 's16bq09b': 'Unit_purchased'}
AMOUNTS = {'s16bq03a': 'Quantity', 's16bq07a': 'Quantity_received_as_gift',
           's16bq09a': 'Quantity_purchased'}
INDEX = ['t', 'i', 'input', 'crop', 'u']
# Exact OWN planting-material descriptions with no established Preferred crop
# correspondence. Keep the full recorded category, including compounds and
# the singular "Autre semences"; these are valid identities, not missing keys.
NATIVE_SEED_CROPS = frozenset((
    "Plants d'hévéa, de teck", "Semences d'ognon", 'Semences de Carotte',
    "Semences d'aubergine", 'Semences de Combo', 'Semences de Piment',
    'Semences de Pastéque', 'Semences de poivron', 'Semences de courge/courgette',
    'Semences de fonio', 'Semences de Oseille/bissap', 'Semences de Tomate',
    'Semences de chou', 'Semences de Conconbre', 'Semences de Laitue/Salade',
    'Autre semences',
))


class S16b2021Error(Exception):
    """Refuse unsupported public identity or loss before the wave write."""


def s16b_use_classes(raw):
    """Partition own q02 without treating an amount as application evidence."""
    use = raw['s16bq02']
    return pd.DataFrame({
        'yes': use.eq(1).fillna(False).astype(bool),
        'no': use.eq(2).fillna(False).astype(bool),
        'unanswered': use.isna(),
        'unlisted': (use.notna() & ~use.isin([1, 2])).astype(bool),
    }, index=raw.index)


def s16b_rows(raw):
    """Lossless all-row transport, with independent own-card role aliases.

    Original columns, index, order, dtypes, masks and typed attrs are retained;
    selecting the original columns in order is the exact inverse. Missing or
    repeated native keys do not destroy this transport. Public key validation
    belongs after application selection (ledger §4; Mali1483 ordering repair).
    """
    if not raw.columns.is_unique:
        raise S16b2021Error('source column names must be unique')
    missing = set(RAW_FIELDS) - set(raw.columns)
    if missing:
        raise S16b2021Error('missing own source columns: ' + ', '.join(sorted(missing)))
    reserved = set(ALIASES.values()) | set(AMOUNTS.values()) | set(INDEX)
    reserved.add('input_code_native')
    reserved |= {alias + '_unlisted' for alias in ALIASES.values()}
    if set(raw.columns) & reserved:
        raise S16b2021Error('source columns collide with adapter aliases')
    for name in ('grappe', 'menage') + tuple(CARDS) + tuple(AMOUNTS):
        if (not pd.api.types.is_numeric_dtype(raw[name].dtype)
                or pd.api.types.is_bool_dtype(raw[name].dtype)):
            raise S16b2021Error('expected numeric native field: ' + name)
    out = raw.copy(deep=True)
    for name, alias in AMOUNTS.items():
        out[alias] = out[name].copy()
    for name, alias in ALIASES.items():
        card = CARDS[name]
        dtype = 'boolean' if name in ('s16bq02', 's16bq05', 's16bq08') else 'string'
        out[alias] = out[name].map(card).astype(dtype)
        out[alias + '_unlisted'] = (out[name].notna() & ~out[name].isin(card)).astype(bool)
    out.attrs = deepcopy(raw.attrs)
    return out


def s16b_plot_inputs(raw, *, household_id, input_map, seed_crop_map, unit_map):
    """Project explicit own q02=1 onto the existing household public grain.

    Reuse exact existing OWN label correspondences, retaining uncurated seed
    descriptions literally, including compound and unspecified categories.
    Missing used units use the existing Unknown sentinel; the raw answer stays
    missing. Never infer a crop, reduce collisions, fill an amount/flag, or
    promote the full DAP catch-all to a pure fertilizer.
    """
    selected = raw['s16bq02'].eq(1).fillna(False).to_numpy(dtype=bool)
    out = s16b_rows(raw).loc[selected].reset_index(drop=True)
    if raw.attrs.get('id_converted'):
        raise S16b2021Error('expected native source before household ID walk')
    native = out[['grappe', 'menage', 's16bq01']]
    for name in native:
        valid = native[name].gt(0) & native[name].mod(1).eq(0)
        if not valid.fillna(False).all():
            raise S16b2021Error('selected native keys require nonmissing positive integers')
    if native.duplicated().any():
        raise S16b2021Error('duplicate selected native household/input identity')
    unlisted = [alias + '_unlisted' for alias in ALIASES.values()]
    if out[unlisted].any(axis=None):
        raise S16b2021Error('unlisted selected input/unit/acquisition code')
    ids = [household_id(pd.Series(pair, dtype=object))
           for pair in out[['grappe', 'menage']].itertuples(index=False, name=None)]
    if any(not isinstance(value, str) or not value or not value.isdecimal() for value in ids):
        raise S16b2021Error('own household formatter must return bare decimal IDs')
    out['i'] = pd.Series(ids, dtype='string')
    # Verify household injectivity independently of input slot or payload.
    households = set(out[['grappe', 'menage']].itertuples(index=False, name=None))
    if len(set(ids)) != len(households):
        raise S16b2021Error('distinct native households collide after formatting')
    out['t'] = pd.Series('2021-22', index=out.index, dtype='string')
    out['input'] = out['Input_native'].map(input_map).astype('string')
    out['crop'] = out['Input_native'].map(seed_crop_map).astype('string')
    native_seed = out['Input_native'].isin(NATIVE_SEED_CROPS)
    out.loc[native_seed, 'input'] = 'Seed'
    out.loc[native_seed, 'crop'] = out.loc[native_seed, 'Input_native']
    out['input_code_native'] = out['s16bq01'].astype('Int64')
    catchall = out['s16bq01'].eq(6)
    out.loc[catchall, 'input'] = out.loc[catchall, 'Input_native']
    seeds = out['input'].eq('Seed')
    if out['input'].isna().any() or out.loc[seeds, 'crop'].isna().any():
        raise S16b2021Error('existing own input/crop correspondence unavailable')
    out.loc[~seeds, 'crop'] = '(not crop-specific)'
    # Only exact labels from each independent unit card enter the old u map.
    for name in ('u', 'Unit_received_as_gift', 'Unit_purchased'):
        out[name] = out[name].map(
            lambda label: unit_map.get(label, label) if pd.notna(label) else pd.NA
        ).astype('string')
    out['u'] = out['u'].fillna('Unknown')
    if out[INDEX].isna().any(axis=None):
        raise S16b2021Error('missing declared key after own mapping')
    out = out.set_index(INDEX)
    if not out.index.is_unique:
        raise S16b2021Error('distinct selected inputs collide on declared keys')
    out.attrs = deepcopy(raw.attrs)
    return out
