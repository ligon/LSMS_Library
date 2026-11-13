"""PP C2 community-price adapters for GH587, and the served PP rows.

Two layers.  The pure REPORT adapter (``pp_community_price_report``) keeps
every native row/column of a PP C2 source and attaches its operand roles and
own-card labels; its dictionaries are captured from the six admitted headers,
not copied from PH or a conflicting print card (ledger
``.coder/ledger/587-nigeria-pp-price-foundation.md`` §§2–6).  The SERVING
layer below it (``pp_community_prices_for_folder``, GH #587 rulings
2026-11-12) puts those reports on Nigeria's shared labels -- ``j`` the
``harmonize_food`` Preferred Label, ``u`` the size-agnostic base unit PH
serves, the native size in ``Size`` -- and computes ``Price`` per each wave's
own PP form.  The item/unit crosswalk lives HERE, not in a categorical
mapping table, so nothing auto-applies it to another Nigeria table (ledger
``.coder/ledger/587-nigeria-pp-mapping.md``; proposals and judgements in
``587-nigeria-pp-mapping/MAPPING.org``).  The PH builder
(``nigeria.community_prices_for_wave``) is unchanged and separate.

Imports follow the wave-module convention (this directory on ``sys.path``,
plain ``nigeria``), so the wave scripts, the registry's by-path resolution and
an ``LSMS_COUNTRIES_ROOT`` override all bind ONE ``nigeria`` module from the
same config tree.
"""
from copy import deepcopy
from dataclasses import dataclass
from collections.abc import Mapping
import os
import re
import sys
import warnings

import numpy as np
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.append(_HERE)

import nigeria as _ng  # noqa: E402
from nigeria import (  # noqa: E402
    PP_QUARTER, C8_NO_VARIETY, cluster_id, format_id,
)

from pp_community_price_sources import SOURCES as _SOURCES  # noqa: E402
_PUBLIC_INDEX = ('t', 'v', 'j', 'u', 'variety', 'obs')
_NATIVE_COLUMNS = (
    'NativeItemCode', 'NativeItemLabel', 'NativeAvailabilityCode',
    'NativeAvailabilityLabel', 'ReportedPrice', 'NativeUnitCode',
    'NativeUnitLabel', 'NativeSizeCode', 'NativeSizeLabel',
    'ReportedNumberOfUnits', 'ReportedQuantity', 'ReportedConversionFactor',
    'ReportedOtherUnit', 'PriceReportSource',
)
_ADDED_COLUMNS = ('t', 'v', 'variety', 'obs') + _NATIVE_COLUMNS


@dataclass(frozen=True, eq=False)
class PPCommunityPriceReport:
    """One reversible source frame plus separately retained label provenance.

    ``frame`` has the original columns first, unchanged index/order/dtypes/attrs,
    followed by report aliases and eventual public identity components. It has
    no invented canonical j/u or standardized Price. The caller's frame is not
    mutated. ``source`` includes the exact native report dictionaries, including
    known code/label conflicts. This object is not a new public table/schema.
    """
    source_key: str
    frame: pd.DataFrame
    source_columns: tuple
    source: dict

    def native_frame(self):
        """Invert the adaptation without decoding, casting or row selection."""
        result = self.frame.loc[:, list(self.source_columns)].copy(deep=True)
        result.attrs = deepcopy(self.frame.attrs)
        return result


def pp_source(source_key):
    """Return a private copy of the actual source's captured specification."""
    if source_key not in _SOURCES:
        raise ValueError('unknown Nigeria PP community-price source')
    return deepcopy(_SOURCES[source_key])


def _native_labels(values, column, source):
    """Decode only a native attached card; unlisted/missing codes stay missing.

    Raw codes are a separate column, so an unlabelled31 cannot be mistaken for
    missing source data or silently replaced by the printed card's semantics.
    """
    association = source['variable_value_label_associations'].get(column, '')
    card = source['report_value_label_sets'].get(association, [])
    labels = {row['code']: row['label'] for row in card}
    return values.map(labels).astype('string')


def pp_community_price_report(raw, source_key):
    """Preserve every native row/column and attach exact PP operand roles.

    Invalid or repeated complete native keys refuse the whole adapter; they are
    never repaired or selected away. No missing-availability, nonpositive-price,
    missing-unit/quantity or OtherUnit row is filtered. Source labels decorate
    the numeric inputs rather than replacing them. All optional numeric/text
    operands remain distinct; W3 OtherUnit is never SIZE.
    """
    source = pp_source(source_key)
    if not raw.columns.is_unique:
        raise ValueError('PP price source has repeated column names')
    required = set(source['variable_order'])
    if not required.issubset(raw.columns):
        raise ValueError('PP price source lacks captured native columns')
    if set(_ADDED_COLUMNS).intersection(raw.columns):
        raise ValueError('PP price report aliases would overwrite native columns')
    keys = source['native_keys']
    for key in keys:
        if any(isinstance(value, (bool, np.bool_)) for value in raw[key]):
            raise ValueError('PP price source has boolean native keys')
        values = pd.to_numeric(raw[key], errors='coerce')
        valid = values.notna() & np.isfinite(values)
        valid &= values.eq(np.floor(values))
        if not valid.all():
            raise ValueError('PP price source has missing or nonintegral native keys')
    if raw.duplicated(keys, keep=False).any():
        raise ValueError('PP price source repeats a complete native key')

    frame = raw.copy(deep=True)
    frame.attrs = deepcopy(raw.attrs)
    frame['t'] = PP_QUARTER[source['folder']]
    clusters = [cluster_id(state, lga, ea) for state, lga, ea in
                zip(raw['state'], raw['lga'], raw['ea'], strict=True)]
    if any(value is None for value in clusters):
        raise ValueError('PP price source cannot supply a sampling-cluster key')
    frame['v'] = pd.array(clusters, dtype='string')
    frame['variety'] = (raw['item_cd'].map(format_id).astype('string').array
                        if source['folder'] in ('2010-11', '2012-13') else
                        pd.array([C8_NO_VARIETY] * len(raw), dtype='string'))
    frame['obs'] = np.full(len(raw), source['obs'], dtype=np.int64)
    frame['NativeItemCode'] = raw['item_cd'].array.copy()
    frame['NativeItemLabel'] = _native_labels(raw['item_cd'], 'item_cd', source).array
    operands = source['operand_columns']
    aliases = {
        'availability': 'NativeAvailabilityCode', 'price': 'ReportedPrice',
        'unit': 'NativeUnitCode', 'size': 'NativeSizeCode',
        'factor': 'ReportedConversionFactor', 'other': 'ReportedOtherUnit',
        'quantity': ('ReportedNumberOfUnits' if source_key == 'W1' else
                     'ReportedQuantity'),
    }
    for alias in _NATIVE_COLUMNS:
        if alias not in frame:
            frame[alias] = pd.array([pd.NA] * len(raw), dtype='object')
    for meaning, column in operands.items():
        frame[aliases[meaning]] = raw[column].array.copy()
    for meaning, alias in [('availability', 'NativeAvailabilityLabel'),
                           ('unit', 'NativeUnitLabel'), ('size', 'NativeSizeLabel')]:
        if meaning in operands:
            frame[alias] = _native_labels(raw[operands[meaning]],
                                         operands[meaning], source).array
    frame['PriceReportSource'] = source_key
    return PPCommunityPriceReport(source_key, frame, tuple(raw.columns), source)


def pp_community_price_projection(report, *, item_map, unit_map):
    """Prototype pending the raw-report/NA-Price representation decision.

    This function is not called by any production script/API. Explicit maps
    must be admitted for THIS source's codes; no PH/print map is a default.
    Incomplete labels or map-induced key collisions refuse rather than drop or
    aggregate native reports. Price is missing throughout: no normalization
    basis is chosen by this foundation. Raw price/quantities/factor survive.
    """
    if not isinstance(item_map, Mapping) or not isinstance(unit_map, Mapping):
        raise TypeError('PP projection requires explicit source-qualified maps')
    frame = report.frame
    items = frame['NativeItemCode'].map(item_map).astype('string')
    units = frame['NativeUnitCode'].map(unit_map).astype('string')
    if items.isna().any() or units.isna().any():
        raise ValueError('PP public identity has unresolved item/unit labels')
    if items.str.strip().eq('').any() or units.str.strip().eq('').any():
        raise ValueError('PP public identity has empty item/unit labels')
    out = frame.loc[:, ['t', 'v', 'variety', 'obs', *_NATIVE_COLUMNS]].copy(deep=True)
    out['j'] = items.array
    out['u'] = units.array
    out['Price'] = pd.array([pd.NA] * len(out), dtype='Float64')
    out = out.set_index(list(_PUBLIC_INDEX))
    if not out.index.is_unique:
        raise ValueError('PP item/unit mapping collides on the public native-slot key')
    out.attrs = deepcopy(frame.attrs)
    return out


# ===========================================================================
# GH #587 SERVING: PP C2 rows on Nigeria's shared labels (rulings 2026-11-12)
# ===========================================================================
#
# The rulings (``.sucoder/isa-decisions.org``, the 2026-11-12 GH587 entries):
#   * labels: j = Nigeria's shared harmonize_food Preferred Label, u = the
#     shared base unit label (the data_scheme.yml j/u promise);
#   * unit axis: the PH size-agnostic base u, the native size in its own
#     column (``Size``);
#   * W1: Price of one unit = sc2q3a / sc2q3b where Number of Units > 1, a
#     registered derivation; N = 1 as recorded; no count -> unserved,
#     registered.  W1 groups held, each registered on its own: g / ml with
#     N = 1 (package prices, ~144x the household unit value) and Kg / l with
#     N > 50 (the count reads as a size) ("Hold both groups"); then, after
#     the red team's N-band test ("GH587 W1 division after red team"), Kg
#     with N 6-50 (divided 0.16-0.33x on three references), non-metric units
#     with N > 50 (divided 0.015x, undivided 1.0x: N reads as a size) and
#     g / ml with N 2-5 (~9x household).  The division is kept, under its
#     OWN key marking it unvalidated, on non-metric N 2-50, Kg N 2-5 and
#     every Unknown-unit row; g / ml N >= 6 and l N 2-50 keep the original
#     division key;
#   * W2-W4: the printed "PLEASE USE A QUANTITY OF ONE (1)" -- as recorded;
#   * W5: "accept quantity of one"; the c2q2a Quantity is disclosed (below);
#   * W4/W5 coded metric SIZE: the PH c8-metric-size division, for PP --
#     including W5's Bottle/Can rows whose c2q2a is not 1 ("Serve the 63
#     via c8", "GH587 label judgements and two follow-ups");
#   * metric UNITS with no recorded package size ("Serve W3 kg/l and W5
#     g/cl", 2026-11-12): W3 Kg / l served as recorded (per-kg / per-litre
#     prices, 1.07x / 1.00x the household unit value); W5 g / cl that record
#     c2q2a served as c2q3 / c2q2a, a registered derivation; W2 g / cl, W3
#     g / cl and W5 g / cl with no c2q2a stay held, registered;
#   * items ("Add a Semovita row"): W2 code 15 is served as harmonize_food's
#     'Semovita'; W2 code 0 (the "No more option" sentinel) is not served.
#
# Every unserved, priced row is in exactly one registered omission (a
# ``rows: 0`` entry in ``_/derivations.yml``) whose ``inputs`` callable below
# returns it, with its original variables, under the served-index labels it
# would carry.

PP_FOLDER_SOURCES = {
    '2010-11': ('W1',),
    '2012-13': ('W2',),
    '2015-16': ('W3-first', 'W3-second'),
    '2018-19': ('W4',),
    '2023-24': ('W5',),
}

PP_W1_UNITS_KEY = 'Nigeria::community_prices::pp-w1-number-of-units'
PP_METRIC_SIZE_KEY = 'Nigeria::community_prices::pp-c2-metric-size'
PP_W1_NO_COUNT_KEY = 'Nigeria::community_prices::pp-w1-no-count-hold'
PP_UNRECORDED_SIZE_KEY = 'Nigeria::community_prices::pp-unrecorded-size-hold'
PP_ITEM_HOLD_KEY = 'Nigeria::community_prices::pp-unharmonized-item-hold'
PP_W5_QUANTITY_KEY = 'Nigeria::community_prices::pp-w5-quantity-not-one-hold'
PP_W1_METRIC_PACKAGE_KEY = 'Nigeria::community_prices::pp-w1-metric-package-hold'
PP_W1_COUNT_AS_SIZE_KEY = 'Nigeria::community_prices::pp-w1-count-as-size-hold'
PP_W5_METRIC_QUANTITY_KEY = 'Nigeria::community_prices::pp-w5-metric-unit-quantity'
PP_W1_UNITS_UNVALIDATED_KEY = 'Nigeria::community_prices::pp-w1-number-of-units-unvalidated'
PP_W1_METRIC_SMALL_COUNT_KEY = 'Nigeria::community_prices::pp-w1-metric-small-count-hold'
PP_W1_KG_MID_COUNT_KEY = 'Nigeria::community_prices::pp-w1-kg-mid-count-hold'
PP_W1_NONMETRIC_COUNT_AS_SIZE_KEY = 'Nigeria::community_prices::pp-w1-nonmetric-count-as-size-hold'

#: W1's metric units, decoded from its own card (101-104).  Every other
#: placed unit is "non-metric"; ``Unknown`` (no unit recorded) is neither.
PP_W1_METRIC_UNITS = ('Kg', 'g', 'l', 'ml')

#: The W1 Number-of-Units bands the red team measured and the maintainer
#: ruled on ("GH587 W1 division after red team"): 2-5 (N <= 5), 6-50
#: (5 < N <= 50) and above 50.  Above 50 a Kg / l or non-metric count reads
#: as a size ("Hold both groups"; red team F1).
PP_W1_SMALL_COUNT_MAX = 5
PP_W1_COUNT_AS_SIZE_ABOVE = 50

#: The disposition of every native PP row, in the order they are decided.
#: ``served`` / ``w1-divided`` / ``w1-divided-unvalidated`` / ``metric-size``
#: / ``w5-metric-quantity`` are served; ``unpriced`` (no positive Naira --
#: not available, or not priced) is excluded exactly as PH excludes it;
#: every other one is a registered omission.
PP_DISPOSITIONS = ('unpriced', 'item-hold', 'unit-unplaced', 'w1-no-count',
                   'w1-metric-package-hold', 'w1-count-as-size-hold',
                   'w1-metric-small-count-hold', 'w1-kg-mid-count-hold',
                   'w1-nonmetric-count-as-size-hold',
                   'w5-metric-quantity', 'unrecorded-size-hold',
                   'w5-quantity-hold', 'metric-size', 'w1-divided-unvalidated',
                   'w1-divided', 'served')
PP_SERVED_DISPOSITIONS = ('served', 'w1-divided', 'w1-divided-unvalidated',
                          'metric-size', 'w5-metric-quantity')
PP_OMISSION_KEYS = {
    'item-hold': PP_ITEM_HOLD_KEY,
    'w1-no-count': PP_W1_NO_COUNT_KEY,
    'w1-metric-package-hold': PP_W1_METRIC_PACKAGE_KEY,
    'w1-count-as-size-hold': PP_W1_COUNT_AS_SIZE_KEY,
    'w1-metric-small-count-hold': PP_W1_METRIC_SMALL_COUNT_KEY,
    'w1-kg-mid-count-hold': PP_W1_KG_MID_COUNT_KEY,
    'w1-nonmetric-count-as-size-hold': PP_W1_NONMETRIC_COUNT_AS_SIZE_KEY,
    'unrecorded-size-hold': PP_UNRECORDED_SIZE_KEY,
    'w5-quantity-hold': PP_W5_QUANTITY_KEY,
}

# --------------------------------------------------------------------- j ---
#: The household food quarter whose item codebook each PP C2 card IS
#: (MAPPING.org: W1/W2 match the PP-quarter consumption card, W3/W4 the
#: printed PP and PH cards code for code).  W5's item card is byte-identical
#: to W4's (pinned by test), and the food codebook has no 2023Q3 column (the
#: quarter resolver raises for it on purpose), so W5 reads W4's.  For W3-W5
#: this equals the no-argument Code contract PH C8 serves on every carded
#: code (pinned by test); for W1/W2 the Code contract is wrong on 7 / 6 codes
#: (GH #1144: food codes collide across quarters).
PP_ITEM_QUARTER = {'W1': '2010Q3', 'W2': '2012Q3', 'W3-first': '2015Q3',
                   'W3-second': '2015Q3', 'W4': '2018Q3', 'W5': '2018Q3'}

#: Carded PP item codes with NO harmonize_food Preferred Label -- held, not
#: guessed.  Any other carded code the quarter resolver cannot place is a
#: defect, and the map refuses it.  W2 code 15 (Semovita) used to be held
#: here; since the 2026-11-12 ruling "Add a Semovita row" harmonize_food's
#: 2012Q3 column names it and a label-only 'Semovita' row carries its
#: Preferred and Aggregate Labels, so the quarter resolver serves it (the
#: wave-blind Code contract would still say "Bread", W1's code 15).
PP_ITEM_HELD = {
    ('W2', 0): 'questionnaire sentinel "No more option", not a food item',
}


def _group(source_key):
    return 'W3' if source_key.startswith('W3') else source_key


def _card(spec, operand):
    """``{code: label}`` of the source's own native card for an operand."""
    column = spec['operand_columns'][operand]
    association = spec['variable_value_label_associations'].get(column, '')
    return {int(r['code']): r['label']
            for r in spec['report_value_label_sets'].get(association, [])}


def pp_item_map(source_key):
    """``{native item code: j}`` for every code on the source's item card.

    j is the quarter resolver's Preferred Label (``PP_ITEM_QUARTER``); the
    codes in ``PP_ITEM_HELD`` are left out.  Refuses a carded code the
    resolver cannot place that is not held, so a codebook edit cannot drop
    PP rows silently.
    """
    spec = pp_source(source_key)
    card = _card_item(spec)
    labels = _ng.harmonized_food_labels(t=PP_ITEM_QUARTER[source_key])
    out, missing = {}, []
    for code in card:
        if (_group(source_key), code) in PP_ITEM_HELD:
            continue
        label = labels.get(code)
        if label is None or str(label).strip() == '':
            missing.append(code)
            continue
        out[code] = str(label).strip()
    if missing:
        raise ValueError(f'Nigeria PP {source_key}: carded item codes {missing} '
                         'have no harmonize_food label and are not held')
    return out


def _card_item(spec):
    association = spec['variable_value_label_associations']['item_cd']
    return {int(r['code']): r['label']
            for r in spec['report_value_label_sets'][association]}


# --------------------------------------------------------------------- u ---
#: Where the community decoder (``nigeria._canon_community_unit``, the one PH
#: C8 uses) is wrong or silent on a PP label.  Accepted as proposed in
#: MAPPING.org (ruling 2026-11-12, "Accept all").  The three W1 spellings
#: are ``u`` Preferred Labels (codes 1020-1022, "Register tokens, keep
#: Unknown").  Keyed ``(source group, native code)``.
PP_UNIT_OVERRIDES = {
    # W1's own card: no u row means a tomato cup.  A weightless spelling
    # (u code 1022); the label's stated 0.087 kg stays out of u (GH #1035).
    ('W1', 1): 'Tomatoes cup',
    # One W1 code names TWO units, 'congo/mudu (1.5 kg)'; W2-W5 code congo
    # and mudu apart.  The decoder returns 'Congo' (first substring hit).
    # A combined spelling, u code 1020.
    ('W1', 6): 'Congo/Mudu',
    # 'millilitre' hits 'litre' -> 'l', a 1000x error.  'ml' (u code 1021)
    # is also the spelling food_acquired W1 serves (u table 2010-11, code 4).
    ('W1', 104): 'ml',
    # 'MEDIUM DERCA' (typo): the decoder returns nothing.
    ('W2', 10): 'Derica',
    # 31 / 42 carry no label on the W3 PP native card, but the data use them
    # (707+53 / 66+1 priced rows); the printed PP card and u[2015-16] say
    # Mudu Large / Derica Large.  (The PH W3 printed card numbers them
    # differently -- a third layout -- so it is not used.)
    ('W3', 31): 'Mudu',
    ('W3', 42): 'Derica',
}

#: The native SIZE where W1-W3 code it INTO the unit (W4/W5 have their own
#: size field, served verbatim).  Base u drops it (the unit-axis ruling), so
#: it rides in ``Size``.  Spelled as the card spells it; W1's stated weights
#: are not copied (they stay in the native label, GH #1035).
_W1_SIZE = {
    4: 'small', 5: 'big',
    11: 'small', 12: 'medium', 13: 'big/large', 14: 'extra large',
    21: 'small', 22: 'medium', 23: 'big', 24: 'extra large',
    31: 'small', 32: 'medium', 33: 'big/large', 34: 'extra large',
    41: 'small', 42: 'medium', 43: 'big',
    51: 'small', 52: 'medium', 53: 'big/large',
    61: 'small', 62: 'medium', 63: 'big',
    71: 'small', 72: 'medium', 73: 'big/large', 74: 'extra large',
    81: 'small', 82: 'medium', 83: 'big',
    91: 'small', 92: 'medium', 93: 'big', 94: 'large', 95: 'drum',
}
_W2_SIZE = {9: 'LARGE', 10: 'MEDIUM', 11: 'SMALL'}
_W3_SIZE_RE = re.compile(r'\s(SMALL|MEDIUM|VERY LARGE|LARGE)\s*$')
_W3_UNLABELLED_SIZE = {31: 'LARGE', 42: 'LARGE'}


def pp_unit_map(source_key):
    """``{native unit code: base u}`` for the source's unit card (+ W3 31/42).

    The PH community decoder, then ``PP_UNIT_OVERRIDES``.  A carded label
    neither places is kept VERBATIM, code prefix included -- the GH #1095
    ruling for PH (no factor, no shared Unknown); W4/W5 '210. STEMS' and
    '260. SEEDLING/PLANT' are the only such labels, on 0 priced rows.
    """
    spec = pp_source(source_key)
    group = _group(source_key)
    out = {}
    for code, label in _card(spec, 'unit').items():
        if (group, code) in PP_UNIT_OVERRIDES:
            out[code] = PP_UNIT_OVERRIDES[(group, code)]
            continue
        unit = _ng._canon_community_unit(label)
        out[code] = str(label).strip() if pd.isna(unit) else str(unit)
    for (g, code), unit in PP_UNIT_OVERRIDES.items():
        if g == group:
            out.setdefault(code, unit)
    return out


def pp_unplaced_unit_codes(source_key):
    """Carded unit codes served under their verbatim label (GH #1095)."""
    spec = pp_source(source_key)
    group = _group(source_key)
    return sorted(code for code, label in _card(spec, 'unit').items()
                  if (group, code) not in PP_UNIT_OVERRIDES
                  and pd.isna(_ng._canon_community_unit(label)))


def pp_size_map(source_key):
    """``{native unit code: native size}`` where W1-W3 code the size into the unit."""
    spec = pp_source(source_key)
    group = _group(source_key)
    if group == 'W1':
        return dict(_W1_SIZE)
    if group == 'W2':
        return dict(_W2_SIZE)
    if group == 'W3':
        out = dict(_W3_UNLABELLED_SIZE)
        for code, label in _card(spec, 'unit').items():
            m = _W3_SIZE_RE.search(str(label))
            if m:
                out[code] = m.group(1)
        return out
    return {}


# ----------------------------------------------------------- dispositions ---

def derive_pp_w1_unit_price(price, number_of_units):
    """W1 PP: the price of ONE unit when the community priced several.

    The W1 PP form (Section 2, pp.5-8) records "What is the price for the
    item?" in three columns -- Naira | Number of Units | Unit Codes -- and,
    unlike every later wave, prints no "quantity of one" instruction.  Read
    as the price of ``number_of_units`` units together, the price of one is
    ``price / number_of_units``.  Vectorised; NaN where the count is missing
    or not positive (the build serves no such row).  The build applies it
    only where the count exceeds 1; a count of 1 is the identity.
    """
    p = pd.to_numeric(pd.Series(price), errors='coerce')
    n = pd.to_numeric(pd.Series(number_of_units), errors='coerce')
    return (p / n.where(n > 0)).to_numpy()


def derive_pp_w5_metric_unit_price(price, quantity):
    """W5 PP: the price of ONE gram / centilitre on a metric-unit row with no size.

    W5's C2 export records the unit (``c2q2b``), the SIZE (``c2q2c``), a
    ``c2q2a`` "Quantity" the printed form does not carry, and the Naira
    price ``c2q3``.  On a row whose unit is '2. GRAMS (G)' or
    '4. CENTLITRES (CL)' and whose SIZE is blank, ``c2q2a`` is the number of
    grams / centilitres the price is for (the evidence is in the registry
    entry ``pp-w5-metric-unit-quantity``), so one gram / centilitre costs
    ``price / quantity`` (ruling 2026-11-12, "Serve W3 kg/l and W5 g/cl").
    Vectorised; NaN where the quantity is missing or not positive (the build
    serves no such row under this key).
    """
    p = pd.to_numeric(pd.Series(price), errors='coerce')
    q = pd.to_numeric(pd.Series(quantity), errors='coerce')
    return (p / q.where(q > 0)).to_numpy()


def pp_dispositions(raw, source_key):
    """Every native row of one PP C2 source, decided.

    Returns a frame aligned with ``raw`` (same index and order) holding the
    served identity ``t, v, j, u, variety, obs``, ``Price``, ``Derivation``,
    ``Size`` and ``Disposition`` (one of :data:`PP_DISPOSITIONS`).  ``j`` /
    ``u`` are what the row is, or would be, served under: a held row keeps
    them so its registry ``inputs`` join the served grain.  Pure; no IO.
    """
    report = pp_community_price_report(raw, source_key)
    frame = report.frame
    group = _group(source_key)
    index = frame.index
    reported = pd.to_numeric(frame['ReportedPrice'], errors='coerce').astype('float64')
    item_code = pd.to_numeric(frame['NativeItemCode'], errors='coerce')
    unit_code = pd.to_numeric(frame['NativeUnitCode'], errors='coerce')

    items = pp_item_map(source_key)
    held_labels = {c: _card_item(report.source).get(c)
                   for (g, c) in PP_ITEM_HELD if g == group}
    j = item_code.map(items).astype('string')
    item_held = j.isna() & item_code.notna()
    j = j.where(~item_held, item_code.map(held_labels).astype('string'))

    units = pp_unit_map(source_key)
    u = unit_code.map(units).astype('string')
    unit_unplaced = u.isna() & unit_code.notna()      # an off-card, unlabelled code
    u = u.where(unit_code.notna(), 'Unknown')         # no unit recorded (data_info.yml)

    size = unit_code.map(pp_size_map(source_key)).astype('string')
    metric_qty = pd.Series(np.nan, index=index, dtype='float64')
    is_litre = pd.Series(False, index=index)
    one_size = pd.Series(False, index=index)
    if 'size' in report.source['operand_columns']:
        size_label = frame['NativeSizeLabel'].astype('string')
        size = size_label
        metric_qty = size_label.map(
            lambda s: np.nan if pd.isna(s) else (_ng._c8_size_metric_grams(s) or np.nan)
        ).astype('float64')
        is_litre = size_label.str.contains(r'(?i)\bCL\b|LITRE', na=False)
        one_size = size_label.str.match(_ng._C8_SIZE_ONE_SIZE_ONLY, na=False)
    metric_size = metric_qty.gt(0)

    priced = reported.gt(0)
    disp = pd.Series('served', index=index, dtype='object')
    price = reported.copy()
    derivation = pd.Series(pd.NA, index=index, dtype='string')

    # Metric UNIT with no recorded package size (ruled 2026-11-12, "Serve
    # W3 kg/l and W5 g/cl"): W3's c2q2b is free text and blank on every
    # metric-unit row (0 parse); its Kg / l measure as per-kilogram /
    # per-litre prices (1.07x / 1.00x the household unit value) and are
    # SERVED as recorded -- unlike PH W3, which still drops its Kg / l for
    # the same blank field.  g / cl with no size stay held in W2 and W3
    # (package prices: W3 150x / 57x; W2 g 2.2x, mixed).  W5 g / cl are
    # divided by c2q2a where it is recorded (below), held where it is not.
    # W4 records no g/cl unit.  '99. ONE SIZE ONLY' is the questionnaire's
    # own one-unit statement, exempt as in PH.
    metric_units = {'W2': ('g', 'cl'), 'W3': ('g', 'cl'),
                    'W5': ('g', 'cl')}.get(group, ())
    no_size = u.isin(metric_units) & ~metric_size & ~one_size

    # W5: c2q2a is "Quantity" on 350 priced rows, 19 of them 1.  On a g / cl
    # row with no size it is the number of grams / centilitres priced (208
    # rows), so the price of one is c2q3 / c2q2a -- a registered derivation.
    # Elsewhere the ruled basis is a quantity of one, so a row recording
    # another quantity is contradicted by its own export -- not served
    # (measured: on CUBE rows the price scales with c2q2a), and registered --
    # except a Bottle/Can row with a coded metric size, which the PH size
    # division serves whatever its c2q2a ("Serve the 63 via c8", 2026-11-12).
    if source_key == 'W5':
        q = pd.to_numeric(frame['ReportedQuantity'], errors='coerce')
        w5_quantity = q.notna() & q.ne(1) & ~(u.eq('Bottle/Can') & metric_size)
        w5_metric_quantity = no_size & q.gt(0)
    else:
        q = None
        w5_quantity = w5_metric_quantity = pd.Series(False, index=index)

    false = pd.Series(False, index=index)
    if source_key == 'W1':
        n = pd.to_numeric(frame['ReportedNumberOfUnits'], errors='coerce')
        no_count = n.isna() | n.lt(1)
        several = n.gt(1)
        small = several & n.le(PP_W1_SMALL_COUNT_MAX)
        mid = n.gt(PP_W1_SMALL_COUNT_MAX) & n.le(PP_W1_COUNT_AS_SIZE_ABOVE)
        large = n.gt(PP_W1_COUNT_AS_SIZE_ABOVE)
        unknown = u.eq('Unknown')
        nonmetric = ~u.isin(PP_W1_METRIC_UNITS) & ~unknown
        # "Hold both groups" (2026-11-12): a single g / ml is a package
        # price (144x the household per-gram price), and a Kg / l count
        # above 50 reads as a size (divided: 50-1000x below household).
        w1_metric_package = u.isin(('g', 'ml')) & n.eq(1)
        w1_count_as_size = u.isin(('Kg', 'l')) & large
        # "GH587 W1 division after red team" (2026-11-12): held -- g / ml
        # with N 2-5 (divided ~9x household), Kg with N 6-50 (divided
        # 0.16-0.33x on three references) and non-metric units with N > 50
        # (divided 0.015x, undivided 1.0x their own N = 1 price).
        w1_metric_small_count = u.isin(('g', 'ml')) & small
        w1_kg_mid_count = u.eq('Kg') & mid
        w1_nonmetric_count_as_size = nonmetric & large
        # ... and still divided, under a separate key marking the division
        # unvalidated: non-metric N 2-50, Kg N 2-5, every Unknown-unit row
        # (the held bands above are decided first).  What is left of N > 1
        # -- g / ml N >= 6 and l N 2-50 -- keeps the original key.
        divide_unvalidated = several & (nonmetric | unknown | u.eq('Kg'))
        divide = several
    else:
        n = None
        no_count = divide = divide_unvalidated = false
        w1_metric_package = w1_count_as_size = false
        w1_metric_small_count = w1_kg_mid_count = w1_nonmetric_count_as_size = false

    decided = pd.Series(False, index=index)
    for name, mask in (('unpriced', ~priced),
                       ('item-hold', item_held),
                       ('unit-unplaced', unit_unplaced),
                       ('w1-no-count', no_count),
                       ('w1-metric-package-hold', w1_metric_package),
                       ('w1-count-as-size-hold', w1_count_as_size),
                       ('w1-metric-small-count-hold', w1_metric_small_count),
                       ('w1-kg-mid-count-hold', w1_kg_mid_count),
                       ('w1-nonmetric-count-as-size-hold', w1_nonmetric_count_as_size),
                       ('w5-metric-quantity', w5_metric_quantity),
                       ('unrecorded-size-hold', no_size),
                       ('w5-quantity-hold', w5_quantity),
                       ('metric-size', metric_size),
                       ('w1-divided-unvalidated', divide_unvalidated),
                       ('w1-divided', divide)):
        take = mask.fillna(False).astype(bool) & ~decided
        disp[take] = name
        decided |= take

    take = disp.eq('metric-size')
    price[take] = reported[take] / metric_qty[take]
    u[take & is_litre] = 'cl'
    u[take & ~is_litre] = 'g'
    derivation[take] = PP_METRIC_SIZE_KEY
    take = disp.eq('w5-metric-quantity')
    if take.any():
        price[take] = derive_pp_w5_metric_unit_price(reported[take], q[take])
        derivation[take] = PP_W5_METRIC_QUANTITY_KEY
    for name, key in (('w1-divided', PP_W1_UNITS_KEY),
                      ('w1-divided-unvalidated', PP_W1_UNITS_UNVALIDATED_KEY)):
        take = disp.eq(name)
        if take.any():
            price[take] = derive_pp_w1_unit_price(reported[take], n[take])
            derivation[take] = key

    unplaced = set(pp_unplaced_unit_codes(source_key))
    verbatim = disp.isin(PP_SERVED_DISPOSITIONS) & unit_code.isin(unplaced)
    if verbatim.any():
        counts = frame.loc[verbatim, 'NativeUnitLabel'].value_counts().to_dict()
        warnings.warn(f'Nigeria community_prices {PP_QUARTER[report.source["folder"]]} '
                      f'(PP {source_key}): {int(verbatim.sum())} row(s) are served '
                      f'under an unplaced recorded unit label, verbatim: {counts}',
                      _ng.UnplacedUnitLabelWarning, stacklevel=2)
    if disp.eq('unit-unplaced').any():
        warnings.warn(f'Nigeria community_prices PP {source_key}: '
                      f'{int(disp.eq("unit-unplaced").sum())} priced row(s) carry an '
                      'unlabelled unit code off the native card and are not served',
                      _ng.UnplacedUnitLabelWarning, stacklevel=2)

    out = pd.DataFrame({
        't': frame['t'].astype('string').array, 'v': frame['v'].astype('string').array,
        'j': j.array, 'u': u.astype('string').array,
        'variety': frame['variety'].astype('string').array,
        'obs': frame['obs'].astype('int64').array,
        'Price': price.astype('float64').array, 'Derivation': derivation.array,
        'Size': size.astype('string').array, 'Disposition': disp.array,
    }, index=index)
    return out


_INDEX = ['t', 'v', 'j', 'u', 'variety', 'obs']


def pp_community_prices_for_source(raw, source_key):
    """The served PP rows of one source, in the PH builder's layout.

    Index ``(t, v, i, j, u, variety, obs)`` with the ``i == v`` shim
    ``community_prices_for_wave`` carries (map_index); columns ``Price``,
    ``Derivation``, ``Size``.  A repeated served key is an error, never a
    selection (GH #840 / #637).
    """
    d = pp_dispositions(raw, source_key)
    d = d[d['Disposition'].isin(PP_SERVED_DISPOSITIONS)].drop(columns='Disposition')
    dup = d.duplicated(_INDEX, keep=False)
    if dup.any():
        raise ValueError(f'Nigeria community_prices PP {source_key}: {int(dup.sum())} '
                         f'rows share a {tuple(_INDEX)} key')
    d['i'] = d['v']
    out = d.set_index(['t', 'v', 'i', 'j', 'u', 'variety', 'obs']).sort_index()
    out.attrs = {}
    return out[['Price', 'Derivation', 'Size']]


def pp_source_relpath(source_key):
    """The source path as a wave script (cwd ``<wave>/_``) spells it."""
    spec = pp_source(source_key)
    prefix = spec['folder'] + '/'
    assert spec['source_path'].startswith(prefix)
    return '../' + spec['source_path'][len(prefix):]


def pp_community_prices_for_folder(folder, reader, declared):
    """Served PP rows for one wave folder; ``reader(path)`` -> raw frame.

    The wave script passes ``lambda f: get_dataframe(f,
    convert_categoricals=False)`` and ``declared``, its PP source paths as
    STRING LITERALS: ``Wave._input_hash`` folds the DVC sidecar of every data
    path a wave script spells literally (``scan_script_data_refs``), so a
    re-released PP source invalidates the cache.  A computed path alone would
    be invisible to it (CLAUDE.md cache limitation (a)).  The declaration must
    equal the sealed source paths, in order.
    """
    expected = tuple(pp_source_relpath(key) for key in PP_FOLDER_SOURCES[folder])
    if tuple(declared) != expected:
        raise ValueError(f'Nigeria {folder} community_prices declares PP sources '
                         f'{tuple(declared)}, but the PP adapter reads {expected}')
    pieces = [pp_community_prices_for_source(reader(path), key)
              for key, path in zip(PP_FOLDER_SOURCES[folder], expected)]
    out = pd.concat(pieces).sort_index()
    out.attrs = {}
    return out


def combine_rounds(ph, pp):
    """One wave folder's PH and PP frames, distinct ``t`` (pp-ph skill)."""
    shared = set(ph.index.get_level_values('t')) & set(pp.index.get_level_values('t'))
    if shared:
        raise ValueError(f'PH and PP community_prices share t {sorted(shared)}')
    out = pd.concat([ph, pp]).sort_index()
    for column in ('Derivation', 'Size'):
        if column not in out:
            out[column] = pd.Series(pd.NA, index=out.index, dtype='string')
        out[column] = out[column].astype('string')
    out['Price'] = out['Price'].astype('float64')
    out.attrs = {}
    return out[['Price', 'Derivation', 'Size']]


# -------------------------------------------------------- registry inputs ---

_RAW_VARIABLES = {
    'W1': ['state', 'lga', 'ea', 'item_cd', 'sc2q2', 'sc2q3a', 'sc2q3b', 'sc2q3c'],
    'W2': ['state', 'lga', 'ea', 'item_cd', 'c2q1', 'c2q2', 'c2q3'],
    'W3-first': ['state', 'lga', 'ea', 'item_cd', 'c2q1', 'c2q2', 'c2q2b', 'c2q3'],
    'W3-second': ['state', 'lga', 'ea', 'item_cd', 'c2q1', 'c2q2', 'c2q2b', 'c2q3'],
    'W4': ['state', 'lga', 'ea', 'item_cd', 'c2q1', 'c2q2', 'c2q2b', 'c2q2c', 'c2q3'],
    'W5': ['state', 'lga', 'ea', 'item_cd', 'c2q1', 'c2q2a', 'c2q2b', 'c2q2b_os',
           'c2q2c', 'c2q2_cvn', 'c2q3'],
}


def _folder_of(wave):
    if wave in PP_FOLDER_SOURCES:
        return wave
    for folder, quarter in PP_QUARTER.items():
        if wave == quarter:
            return folder
    raise ValueError(f'not a Nigeria PP wave folder or quarter: {wave!r}')


def pp_inputs(wave, dispositions):
    """Raw PP answers of the rows in ``dispositions``, at the served grain.

    Re-reads each source through ``get_dataframe`` (the countries root, so
    any cwd works), decides every row exactly as the build does, and returns
    the ORIGINAL variables of the selected rows indexed by the served
    ``(t, v, j, u, variety, obs)`` labels, plus ``Disposition``.
    """
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root
    root = countries_root() / 'Nigeria'
    pieces = []
    for key in PP_FOLDER_SOURCES[_folder_of(wave)]:
        raw = get_dataframe(str(root / pp_source(key)['source_path']),
                            convert_categoricals=False)
        d = pp_dispositions(raw, key)
        keep = d['Disposition'].isin(dispositions)
        cols = [c for c in _RAW_VARIABLES[key] if c in raw.columns]
        piece = raw.loc[keep, cols].copy()
        piece['Disposition'] = d.loc[keep, 'Disposition'].values
        piece.index = pd.MultiIndex.from_frame(d.loc[keep, _INDEX])
        pieces.append(piece)
    out = pd.concat(pieces) if pieces else pd.DataFrame()
    out.attrs = {}
    return out


def inputs_pp_w1_number_of_units(wave='2010-11'):
    """Registry inputs: W1 PP rows whose Price is sc2q3a / sc2q3b, on the
    bands the evidence supports (g / ml N >= 6, l N 2-50)."""
    return pp_inputs(wave, ('w1-divided',))


def inputs_pp_w1_number_of_units_unvalidated(wave='2010-11'):
    """Registry inputs: W1 PP rows whose Price is sc2q3a / sc2q3b, on the
    bands no comparator validates (non-metric N 2-50, Kg N 2-5, Unknown)."""
    return pp_inputs(wave, ('w1-divided-unvalidated',))


def inputs_pp_c2_metric_size(wave):
    """Registry inputs: W4/W5 PP rows divided by their coded metric SIZE."""
    return pp_inputs(wave, ('metric-size',))


def inputs_pp_w5_metric_unit_quantity(wave='2023-24'):
    """Registry inputs: W5 PP g / cl rows with no size whose Price is c2q3 / c2q2a."""
    return pp_inputs(wave, ('w5-metric-quantity',))


def inputs_pp_w1_no_count_hold(wave='2010-11'):
    """Registry inputs (deletion): W1 PP priced rows with no Number of Units."""
    return pp_inputs(wave, ('w1-no-count',))


def inputs_pp_w1_metric_package_hold(wave='2010-11'):
    """Registry inputs (deletion): W1 PP g / ml rows with Number of Units = 1."""
    return pp_inputs(wave, ('w1-metric-package-hold',))


def inputs_pp_w1_count_as_size_hold(wave='2010-11'):
    """Registry inputs (deletion): W1 PP Kg / l rows with Number of Units > 50."""
    return pp_inputs(wave, ('w1-count-as-size-hold',))


def inputs_pp_w1_metric_small_count_hold(wave='2010-11'):
    """Registry inputs (deletion): W1 PP g / ml rows with Number of Units 2-5."""
    return pp_inputs(wave, ('w1-metric-small-count-hold',))


def inputs_pp_w1_kg_mid_count_hold(wave='2010-11'):
    """Registry inputs (deletion): W1 PP Kg rows with Number of Units 6-50."""
    return pp_inputs(wave, ('w1-kg-mid-count-hold',))


def inputs_pp_w1_nonmetric_count_as_size_hold(wave='2010-11'):
    """Registry inputs (deletion): W1 PP non-metric rows with Number of Units > 50."""
    return pp_inputs(wave, ('w1-nonmetric-count-as-size-hold',))


def inputs_pp_unrecorded_size_hold(wave):
    """Registry inputs (deletion): PP metric-unit rows with no package size."""
    return pp_inputs(wave, ('unrecorded-size-hold',))


def inputs_pp_unharmonized_item_hold(wave='2012-13'):
    """Registry inputs (deletion): PP priced rows on an item with no j."""
    return pp_inputs(wave, ('item-hold',))


def inputs_pp_w5_quantity_not_one_hold(wave='2023-24'):
    """Registry inputs (deletion): W5 PP rows recording a c2q2a other than 1."""
    return pp_inputs(wave, ('w5-quantity-hold',))
