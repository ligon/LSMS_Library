"""Nonactivated Mali s16b adapter, grounded in own authenticated header cards.

Ledger: .coder/ledger/1483-mali-post-header-adapter-20261110.md §§2–6.
This is an input-grain review operand, NOT a plot_inputs producer. It keeps
all native rows and columns, with literal-card decoded aliases. No provider,
writer, categorical curation, eligibility filter or EAC-I reducer is reachable.
Callers supply mali.i (2018) or mali.ehcvm21_i (2021); no ID walk occurs here.
The constants below transcribe each own HEADER.json, not a sibling codebook.
"""
from copy import deepcopy

import pandas as pd


def _card_alias(frame, wave, raw_name, alias, *, flag=False):
    """Decode one OWN card; keep raw unknowns plus an explicit unlisted mask."""
    card = CARDS[wave][raw_name]
    raw = frame[raw_name]
    if flag:
        # Header-bound codes, not truthiness: missing/unlisted is never False.
        values = raw.map({1: True, 2: False}).astype('boolean')
    else:
        values = raw.map(card).astype('string')
    frame[alias] = values
    frame[alias + '_unlisted'] = raw.notna() & ~raw.isin(card)


def s16b_use_classes(raw):
    """Partition the native use answers without choosing serving eligibility.

    This labels answers, not observed application or inclusion. No/unknown
    rows with amounts remain in the review operand (ledger §6).
    """
    use = raw['s16bq02']
    return pd.DataFrame({
        'yes': use.eq(1).fillna(False).astype(bool),
        'no': use.eq(2).fillna(False).astype(bool),
        'unanswered': use.isna(),
        'unlisted': use.notna() & ~use.isin([1, 2]),
    }, index=raw.index)


def s16b_applied_candidate(raw, wave, *, household_id):
    """Nonactivated Tier B projection: native use code 1, without amount gates.

    Own q02=Oui plus #1483's unserved-APPLIED scope and the existing EHCVM
    drivers settle this predicate. No/blank/unlisted answers remain available
    through s16b_rows_for_wave; they are not applications merely because they
    have a gift, purchase or used amount. This still has the review-operand
    schema: public input/crop classification and serving wiring are separate.
    """
    rows = s16b_rows_for_wave(raw, wave, household_id=household_id)
    selected = raw['s16bq02'].eq(1).fillna(False).to_numpy(dtype=bool)
    out = rows.loc[selected].copy()
    out.attrs = deepcopy(raw.attrs)
    return out


def s16b_rows_for_wave(raw, wave, *, household_id):
    """Copy a complete numeric own-source frame into a lossless review operand.

    ``household_id`` MUST be existing mali.i / mali.ehcvm21_i for the wave.
    The explicit dependency makes this helper usable without importing the
    country module's provider-capable namespace. Its source is AST-extracted
    with the actual local_tools.format_id in the frozen pure controls.

    Index is (t, i, s16bq01), NOT the public plot_inputs schema. s16bq01 also
    stays in the payload so every original column can be recovered exactly.
    Repeated/missing native keys REFUSE before any summation/selection/write;
    this does not claim that actual sources are unique. Extra source columns
    are retained. Unknown card codes retain raw values and explicit masks.
    Numeric aliases keep raw dtypes, masks and amounts: no sentinel cleaning.
    """
    if wave not in SOURCE_BINDINGS:
        raise ValueError('Mali s16b adapter supports only 2018-19 and 2021-22')
    if not raw.columns.is_unique:
        raise ValueError('Mali s16b requires unique source column names')
    missing = set(SOURCE_BINDINGS[wave]['columns']) - set(raw.columns)
    if missing:
        raise ValueError('Mali s16b missing required own-source columns: '
                         + ', '.join(sorted(missing)))
    numeric = ('grappe', 'menage', 's16bq01', 's16bq02', 's16bq03a',
               's16bq03b', 's16bq05', 's16bq07a', 's16bq07b',
               's16bq08', 's16bq09a', 's16bq09b', 's16bq09c')
    for name in numeric:
        if (not pd.api.types.is_numeric_dtype(raw[name].dtype)
                or pd.api.types.is_bool_dtype(raw[name].dtype)):
            raise ValueError('Mali s16b requires numeric native field: ' + name)
    keys = raw[['grappe', 'menage', 's16bq01']]
    for name in keys:
        valid = keys[name].gt(0) & keys[name].mod(1).eq(0)
        if not valid.fillna(False).all():
            raise ValueError('Mali s16b requires nonmissing positive integer keys')
    if keys.duplicated().any():
        raise ValueError('Mali s16b repeats a native household/input-slot key')

    # Positional assembly: arbitrary/repeated pandas row indexes are not IDs.
    out = raw.copy(deep=True).reset_index(drop=True)
    aliases = ('Input_native', 'Used', 'u', 'Received_as_gift',
               'Unit_received_as_gift', 'Purchased', 'Unit_purchased')
    reserved = {'t', 'i', 'Quantity', 'Quantity_received_as_gift',
                'Quantity_purchased'} | set(aliases)
    reserved |= {name + '_unlisted' for name in aliases}
    if set(out.columns) & reserved:
        raise ValueError('Mali s16b source column collides with adapter aliases')
    ids = [household_id(pd.Series(pair, dtype=object))
           for pair in out[['grappe', 'menage']].itertuples(index=False, name=None)]
    if any(not isinstance(value, str) or not value for value in ids):
        raise ValueError('Mali s16b household helper must return nonempty strings')
    if wave == '2021-22' and any(not value.startswith('EHCVM21_') for value in ids):
        raise ValueError('Mali s16b 2021 raw households require EHCVM21_ namespace')
    if wave == '2018-19' and any(not value.isdecimal() for value in ids):
        raise ValueError('Mali s16b 2018 raw households require bare numeric IDs')
    out['t'] = pd.Series(wave, index=out.index, dtype='string')
    out['i'] = pd.Series(ids, dtype='string')
    if out[['i', 's16bq01']].duplicated().any():
        raise ValueError('Mali s16b formatted household/input-slot keys collide')

    # Mechanical country-only acquisition bindings: Burkina #1188 (§2/§5).
    # q09c remains under its raw name; it is not a price or a used-input cost.
    for alias, name in (('Quantity', 's16bq03a'),
                        ('Quantity_received_as_gift', 's16bq07a'),
                        ('Quantity_purchased', 's16bq09a')):
        out[alias] = out[name].copy()
    for name, alias, flag in (
        ('s16bq01', 'Input_native', False),
        ('s16bq02', 'Used', True),
        ('s16bq03b', 'u', False),
        ('s16bq05', 'Received_as_gift', True),
        ('s16bq07b', 'Unit_received_as_gift', False),
        ('s16bq08', 'Purchased', True),
        ('s16bq09b', 'Unit_purchased', False),
    ):
        _card_alias(out, wave, name, alias, flag=flag)
    out = out.set_index(['t', 'i', 's16bq01'], drop=False).drop(columns=['t', 'i'])
    # Copy typed attrs explicitly; metadata summaries are not a serving claim.
    out.attrs = deepcopy(raw.attrs)
    return out


SOURCE_BINDINGS = {'2018-19': {'columns': ('vague',
                         'grappe',
                         'menage',
                         's16aq09a',
                         's16aq09b',
                         's16bq01',
                         's16bq02',
                         's16bq03a',
                         's16bq03b',
                         's16bq04',
                         's16bq05',
                         's16bq06',
                         's16bq07a',
                         's16bq07b',
                         's16bq08',
                         's16bq09a',
                         's16bq09b',
                         's16bq09c',
                         's16bq00'),
             'header_sha256': '701c60fe2b2e988d116da23d92d8cc5e260592a4d292ecfb34e7e0c870ee5eb5',
             'source': 'Mali/2018-19/Data/s16b_me_mli2018.dta',
             'source_md5': '4c5a9f33e93b9f7655fdee856cbe8fc7',
             'source_sha256': '8ee5defcfae05769a7ded322e6930d13b17d64415e039c80f2827c22791f1e5b'},
 '2021-22': {'columns': ('grappe',
                         'menage',
                         'vague',
                         's16bq01',
                         's16bq02',
                         's16bq03a',
                         's16bq03b',
                         's16bq04',
                         's16bq05',
                         's16bq06',
                         's16bq06_autre',
                         's16bq07a',
                         's16bq07b',
                         's16bq08',
                         's16bq09a',
                         's16bq09b',
                         's16bq09c'),
             'header_sha256': '3f467ae181bb4d3f1d537c535ce2cf94d8cf8bc8cfe1ded5067adcf6577aca35',
             'source': 'Mali/2021-22/Data/s16b_me_mli2021.dta',
             'source_md5': '673cc63eb5afdf3dc45931d81e867538',
             'source_sha256': 'c74f064eded6d0928e88f15c1f8c0395edb19b0b2c28a05100b2d54da86bba32'}}


CARDS = {'2018-19': {'s16bq01': {1: "Engrais organiques - déchets d'animaux",
                         2: 'Engrais organiques - Ordures ménagères',
                         3: 'Engrais inorganiques - Urée',
                         4: 'Engrais inorganiques - Phosphates',
                         5: 'Engrais inorganiques - NPK/Formule unique',
                         6: 'DAP et autres engrais inorganiques',
                         7: 'Produits phytosanitaires - Pesticides',
                         8: 'Produits phytosanitaires - Herbicides',
                         9: 'Produits phytosanitaires -Fongicides',
                         10: 'Autres produits phytosanitaires',
                         11: 'Semences de petit mil',
                         12: 'Semences de sorgho',
                         13: 'Semences de maïs',
                         14: 'Semences de riz',
                         15: "Semences d'autres céréales",
                         16: 'Semences de coton',
                         17: 'Semences de césame',
                         18: 'Semences de haricots/niébé',
                         19: 'Plants/boutures de tubercules',
                         20: 'Autres semences'},
             's16bq02': {1: 'Oui', 2: 'Non'},
             's16bq03b': {1: 'Gramme',
                          2: 'Kilogramme',
                          3: 'Tonne',
                          4: 'Litre',
                          5: 'Charrette'},
             's16bq05': {1: 'Oui', 2: 'Non'},
             's16bq07b': {1: 'Gramme',
                          2: 'Kilogramme',
                          3: 'Tonne',
                          4: 'Litre',
                          5: 'Charrette'},
             's16bq08': {1: 'Oui', 2: 'Non'},
             's16bq09b': {1: 'Gramme',
                          2: 'Kilogramme',
                          3: 'Tonne',
                          4: 'Litre',
                          5: 'Charrette'}},
 '2021-22': {'s16bq01': {1: "Engrais organiques - déchets d'animaux",
                         2: 'Engrais organiques - Ordures ménagères',
                         3: 'Engrais inorganiques - Urée',
                         4: 'Engrais inorganiques - Phosphates',
                         5: 'Engrais inorganiques - NPK/Formule unique',
                         6: 'DAP et autres engrais inorganiques',
                         7: 'Produits phytosanitaires - Pesticides',
                         8: 'Produits phytosanitaires - Herbicides',
                         9: 'Produits phytosanitaires -Fongicides',
                         10: 'Autres produits phytosanitaires',
                         11: 'Semences de petit mil',
                         12: 'Semences de sorgho',
                         13: 'Semences de maïs',
                         14: 'Semences de riz',
                         15: "Semences d'autres céréales",
                         16: 'Semences de coton',
                         17: 'Semences de césame',
                         18: 'Semences de haricots/niébé',
                         19: "Semences d'arichide",
                         20: 'Plants/boutures de tubercules',
                         21: "Plants d'hévéa, de teck",
                         22: 'Autres semences'},
             's16bq02': {1: 'Oui', 2: 'Non'},
             's16bq03b': {1: 'Gramme',
                          2: 'Kilogramme',
                          3: 'Tonne',
                          4: 'Litre',
                          5: 'Charrette'},
             's16bq05': {1: 'Oui', 2: 'Non'},
             's16bq07b': {1: 'Gramme',
                          2: 'Kilogramme',
                          3: 'Tonne',
                          4: 'Litre',
                          5: 'Charrette'},
             's16bq08': {1: 'Oui', 2: 'Non'},
             's16bq09b': {1: 'Gramme',
                          2: 'Kilogramme',
                          3: 'Tonne',
                          4: 'Litre',
                          5: 'Charrette'}}}
