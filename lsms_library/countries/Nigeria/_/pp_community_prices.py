"""Pure PP C2 report adapters for GH587; not wired into the served table yet.

The current PH builder is deliberately separate. This module neither reads
native files nor normalizes prices. Its own-source dictionaries are captured
from the six admitted headers, not copied from PH or a conflicting print card.
See .coder/ledger/587-nigeria-pp-price-foundation.md §§2–6.
"""
from copy import deepcopy
from dataclasses import dataclass
from collections.abc import Mapping

import numpy as np
import pandas as pd

from lsms_library.countries.Nigeria._.nigeria import (
    PP_QUARTER, C8_NO_VARIETY, cluster_id, format_id,
)

from lsms_library.countries.Nigeria._.pp_community_price_sources import SOURCES as _SOURCES
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
