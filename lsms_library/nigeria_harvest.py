"""Opt-in, source-native Nigeria W3 harvest kilograms (GH #1518).

Pure inputs only: no reader, Country, cache, default wiring or yield adapter.
Callers must admit the numeric A3i/conversion frames and their OWN complete
unit cards against the named releases. Provenance is recorded, not authenticated
by a DataFrame API. National versus own-zone factors is an explicit caller
decision. Published container weights remain candidates, not weighed masses.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import json
import re
import warnings

import numpy as np
import pandas as pd

from . import transformations as _tr
from .local_tools import format_id, id_walk
from .countries.Nigeria._ import nigeria as _ng


_ZONES = {1: 'conv_NC_1', 2: 'conv_NE_2', 3: 'conv_NW_3',
          4: 'conv_SE_4', 5: 'conv_SS_5', 6: 'conv_SW_6'}
_SOURCE = 'Nigeria/2015-16/Data/ag_conv_w3.dta'
_TIME = '2016Q1'
_PREFIX = 'W3_'
_FLUID = r'\b(?:LITRE|LITRES|CENTILITRE|CENTILITRES)\b'
_KEYS = ['t', 'i', 'plot_id', 'crop', 'production_obs']


class HarvestFactorHoldWarning(UserWarning):
    """Unresolved source unit identity or fluid output basis was withheld."""


@dataclass(frozen=True)
class NativeHarvestAudit:
    """Full typed source frames with separate derived fields; never an operand.

    ``native_lines`` retains the original harvest index/columns/dtypes and adds
    W3_-prefixed lineage/status fields, one per release position. Likewise
    ``conversion_rows`` retains every source factor row, including holds.
    Each frame can be serialized independently; neither stores a frame in attrs.
    Exact roundtrip fidelity is limited by the caller's original raw codec
    (for example, Arrow may lose numeric/unused categorical types/categories).
    Raw fields are never cast to conceal those underlying carrier limitations.
    Metadata contains only provenance/cards/counts and no inferred source truth.
    """

    native_lines: pd.DataFrame
    conversion_rows: pd.DataFrame
    metadata: dict


def _codes(values):
    return pd.to_numeric(values, errors='raise').astype('Int64')


def _card_body(code, card):
    """Strip ONLY this numeric code's own enumeration; keep numeric bodies."""
    if pd.isna(code):
        return pd.NA
    label = card.get(int(code))
    match = re.fullmatch(r'(\d+)\.\s+(.+)', str(label).strip())
    if match is None or int(match[1]) != code:
        return pd.NA
    return match[2]


def _bodies(codes, card):
    return codes.map(lambda c: _card_body(c, card)).astype('string')


def _lookup_units(codes, bodies):
    # Only exact OWN metric definitions, after enumeration proof, are aliases.
    result = bodies.copy()
    for code, body, canonical in ((1, 'KILOGRAM (KG)', 'kg'),
                                  (2, 'GRAMS (G)', 'g')):
        is_definition = (codes.eq(code).fillna(False).to_numpy(dtype=bool)
                         & bodies.eq(body).fillna(False).to_numpy(dtype=bool))
        result = result.mask(is_definition, canonical)
    return result


def _validate_frame(frame, required, name):
    if not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique:
        raise ValueError(f'{name} must have unique columns')
    missing = set(required) - set(frame.columns)
    if missing:
        raise ValueError(f'{name} lacks required source dimensions: {sorted(missing)}')
    if any(str(c).startswith(_PREFIX) for c in frame.columns):
        raise ValueError(f'{name} uses reserved W3_ audit fields')


def _factor_inputs(conversion, card, geography, provenance):
    """Recovered50be transcription with source-bound, size-preserving keys."""
    columns = {None: 'conv_national'} if geography == 'national' else _ZONES
    _validate_frame(conversion, ['crop_cd', 'unit_cd', 'crop_name', 'unit_name',
                                'note', *columns.values()], 'conversion')
    raw = conversion.reset_index(drop=True).copy(deep=True)
    crop, unit = _codes(raw.crop_cd), _codes(raw.unit_cd)
    if crop.isna().any() or unit.isna().any():
        raise ValueError('conversion has missing native crop/unit keys')
    if pd.DataFrame({'crop': crop, 'unit': unit}).duplicated().any():
        raise ValueError('conversion has duplicate native crop/unit keys; no averaging')
    body = _bodies(unit, card)
    if body.isna().any():
        raise ValueError('conversion own unit card is missing or mis-keyed')
    conflict = raw.unit_name.astype('string').str.strip().ne(body).fillna(True)
    fluid = body.str.contains(_FLUID, case=False, regex=True).fillna(False)
    held = conflict | fluid
    values = raw[list(columns.values())].apply(pd.to_numeric, errors='raise')
    # Validation view ONLY: nullable unknowns must fail just like float NaN.
    # Do not change raw/audit/admitted values or let pandas skip unknown checks.
    validation = values.to_numpy(dtype=float, na_value=np.nan)
    if not (np.isfinite(validation).all() and (validation > 0).all()):
        raise ValueError('conversion factors must be finite and positive; no clipping')
    for code, expected in ((1, 1.), (2, .001)):
        own_definition = 'KILOGRAM (KG)' if code == 1 else 'GRAMS (G)'
        if not body.loc[unit == code].eq(own_definition).all():
            raise ValueError(f'W3 own metric definition failed for native unit {code}')
        if not np.isclose(values.loc[unit == code], expected, rtol=1e-6, atol=0).all():
            raise ValueError(f'W3 metric control failed for native unit {code}')
    labels = _lookup_units(unit, body)
    base = pd.DataFrame({'country': 'Nigeria', 't': _TIME,
                         'crop_variety': crop.astype('string'), 'u': labels})
    pieces = []
    for zone, column in columns.items():
        piece = base.loc[~held].copy()
        if zone is not None:
            piece['region'] = str(zone)
        piece['KgFactor'] = values.loc[~held, column].to_numpy(dtype=float)
        piece['Source'] = (_SOURCE + ' [dvc:' + provenance['conversion_md5']
                           + ']:' + column)
        pieces.append(piece)
    factors = pd.concat(pieces, ignore_index=True)
    keys = ['country', 't', 'crop_variety', 'u'] + (['region'] if geography == 'zone' else [])
    # Validate with exactly the shared join's normalization, not a second alias.
    normalized = pd.DataFrame({k: _tr._normalise_join_key(factors[k]) for k in keys})
    if normalized.duplicated().any():
        raise ValueError('own card decoding creates ambiguous normalized factor keys')
    factors = factors.set_index(keys).sort_index()
    audit = conversion.copy(deep=True)
    audit.attrs = {}
    for name, value in {'SourceRow': np.arange(len(raw)), 'UnitLabel': unit.map(card).astype('string'),
                        'UnitBody': body, 'LookupUnit': labels,
                        'UnitIdentityHeld': conflict, 'FluidBasisHeld': fluid,
                        'Held': held}.items():
        audit[_PREFIX + name] = value.array if isinstance(value, pd.Series) else value
    audit[_PREFIX + 'SourceMD5'] = provenance['conversion_md5']
    audit[_PREFIX + 'Country'] = 'Nigeria'
    audit[_PREFIX + 'Time'] = _TIME
    counts = {'native_rows': len(raw), 'unit_identity_held': int(conflict.sum()),
              'fluid_basis_held': int(fluid.sum()), 'held_union': int(held.sum()),
              'native_rows_returned': int((~held).sum()), 'geography': geography}
    if held.any():
        warnings.warn(f'Nigeria W3 harvest factors HELD: {counts}; source rulings required',
                      HarvestFactorHoldWarning, stacklevel=4)
    held_pairs = set(zip(crop[conflict].astype(int), unit[conflict].astype(int)))
    fluid_codes = set(unit[fluid].astype(int))
    return factors, audit, counts, held_pairs, fluid_codes


def _native_w3_inputs(harvest, conversion, *, harvest_unit_card,
                      conversion_unit_card, crop_labels, updated_ids,
                      provenance, geography):
    """Private full-row bridge; no public generic/default conversion operand."""
    if geography not in ('national', 'zone'):
        raise ValueError("geography must explicitly be 'national' or 'zone'")
    if not isinstance(updated_ids, dict) or not isinstance(updated_ids.get(_TIME), dict):
        raise ValueError('updated_ids must explicitly supply the committed PH2016Q1 map')
    try:
        json.dumps(provenance, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError('provenance must be plain JSON-serializable admission metadata') from exc
    for name, length in (('harvest_md5', 32), ('conversion_md5', 32),
                         ('updated_ids_sha256', 64), ('crop_labels_sha256', 64)):
        if not re.fullmatch('[0-9a-f]{' + str(length) + '}', str(provenance.get(name, ''))):
            raise ValueError(f'provenance must bind {name}')
    _validate_frame(harvest, ['hhid', 'plotid', 'cropcode', 'cropid', 'zone',
                            'sa3iq6i', 'sa3iq6ii'], 'harvest')
    factors, factor_audit, counts, held_pairs, fluid_codes = _factor_inputs(
        conversion, conversion_unit_card, geography, provenance)
    # Reset the working index only. Caller/audit raw positions and dtypes survive.
    raw = harvest.reset_index(drop=True).copy(deep=True)
    raw.attrs = {}
    crop, unit, zone = (_codes(raw[c]) for c in ('cropcode', 'sa3iq6ii', 'zone'))
    if not zone.dropna().isin(_ZONES).all():
        raise ValueError('W3 zone must be a native code 1–6 or missing')
    body = _bodies(unit, harvest_unit_card)
    conversion_body = _bodies(unit, conversion_unit_card)
    card_conflict = (body.notna() & conversion_body.notna()
                     & body.ne(conversion_body)).fillna(False)
    invalid_card = unit.notna() & body.isna()
    # Unknown/malformed cards may not acquire a guessed metric label.
    dec = raw.copy(deep=True)
    dec['sa3iq6ii'] = unit.map(harvest_unit_card).astype('string').where(body.notna())
    fr = dict(df=raw, dec=dec, hhid='hhid', plot='plotid', crop='cropcode',
              line='cropid', qty='sa3iq6i', unit='sa3iq6ii',
              value_harvested='sa3iq6a', harv_m='sa3iq6c1', harv_y='sa3iq6c2',
              perennial=False)
    # The real producer, including its eligibility/line-id/duplicate refusals.
    footprint = _ng.crop_production_for_wave(_TIME, [fr], crop_labels, hh_sales=None)
    i = raw.hhid.apply(format_id)
    plot = raw.plotid.apply(format_id)
    labels = crop.map(crop_labels).astype('string')
    obs = _ng._crop_line_observations(raw, 'cropcode', line='cropid')
    keys = pd.DataFrame({'t': _TIME, 'i': i, 'plot_id': plot, 'crop': labels,
                         'production_obs': obs})
    # Assign release positions via a validated one-to-one match to its footprint.
    eligible = pd.MultiIndex.from_frame(keys).isin(footprint.index)
    expected = keys.loc[eligible].set_index(_KEYS)
    if len(expected) != len(footprint) or not expected.index.difference(footprint.index).empty:
        raise ValueError('native row witness does not match complete producer footprint')
    positions = pd.DataFrame({'W3_SourceRow': np.flatnonzero(eligible)}, index=expected.index)
    positions = id_walk(positions, updated_ids, state='raw')
    if not positions.index.is_unique:
        raise ValueError('committed panel mapping creates ambiguous serving keys')
    # Also refuse two distinct native households merged by an inert target.
    native_ids = keys.loc[eligible, 'i'].to_numpy()
    walked_i = pd.Series(pd.NA, index=raw.index, dtype='string')
    walked_i.iloc[positions.W3_SourceRow.to_numpy()] = positions.index.get_level_values('i').to_numpy()
    relation = pd.DataFrame({'raw': native_ids, 'canonical': walked_i.loc[eligible].to_numpy()}).drop_duplicates()
    if relation.canonical.duplicated().any():
        raise ValueError('committed panel mapping merges distinct native households')
    pair_hold = pd.Series([(int(c), int(u)) in held_pairs if pd.notna(c) and pd.notna(u) else False
                           for c, u in zip(crop, unit)], index=raw.index)
    fluid = (body.str.contains(_FLUID, case=False, regex=True).fillna(False)
             | unit.isin(fluid_codes))
    held = pair_hold | fluid | card_conflict | invalid_card
    delegated = eligible & ~held.to_numpy()
    lookup = _lookup_units(unit, body)
    operand = keys.loc[delegated].copy()
    operand['i'] = walked_i.loc[delegated].to_numpy()
    operand.insert(0, 'country', 'Nigeria')
    operand['crop_variety'] = crop.loc[delegated].astype('string').to_numpy()
    operand['u'] = lookup.loc[delegated].fillna(_tr.U_UNKNOWN).to_numpy()
    if geography == 'zone':
        operand['region'] = zone.loc[delegated].astype('string').to_numpy()
    # Known native fields ONLY: never arbitrary mass/price columns or attrs.
    operand['Quantity'] = pd.to_numeric(raw.loc[delegated, 'sa3iq6i'], errors='coerce').to_numpy()
    operand = operand.set_index(['country', *_KEYS])
    operand.attrs = {'id_converted': True}
    audit = harvest.copy(deep=True)
    audit.attrs = {}  # Never transport arbitrary caller mass attrs.
    fields = {'SourceRow': np.arange(len(raw)), 'RawFormattedI': i,
              'CanonicalI': walked_i, 'PlotID': plot, 'Crop': labels,
              'ProductionObs': obs, 'UnitLabel': unit.map(harvest_unit_card).astype('string'),
              'UnitBody': body, 'LookupUnit': lookup, 'Region': zone.astype('string'),
              'Eligible': eligible, 'UnitIdentityHeld': pair_hold,
              'FluidBasisHeld': fluid, 'CardConflict': card_conflict,
              'InvalidCard': invalid_card, 'Held': held, 'Delegated': delegated}
    for name, value in fields.items():
        audit[_PREFIX + name] = value.array if isinstance(value, pd.Series) else value
    audit[_PREFIX + 'SourceMD5'] = provenance['harvest_md5']
    audit[_PREFIX + 'Country'] = 'Nigeria'
    audit[_PREFIX + 'Time'] = _TIME
    # The current coarse served unit is a witness, never the lookup key.
    served_u = pd.Series(pd.NA, index=raw.index, dtype='string')
    if eligible.any():
        served_u.loc[eligible] = footprint.reindex(expected.index)['u'].to_numpy()
    audit[_PREFIX + 'ServedUnit'] = served_u.array
    audit[_PREFIX + 'ServingStatus'] = np.where(eligible, 'eligible',
        np.where(i.isna(), 'missing-household', 'unlabelled-crop'))
    audit[_PREFIX + 'MassStatus'] = np.where(~eligible, 'unserved',
        np.where(held, 'held', 'delegated'))
    return operand, factors, audit, factor_audit, counts, delegated


def native_w3_harvest_kg(harvest, conversion, *, harvest_unit_card,
                          conversion_unit_card, crop_labels, updated_ids,
                          provenance, geography):
    """Return ``(Harvest_kg aggregate, NativeHarvestAudit)`` from admitted W3 inputs.

    ``geography`` is mandatory national|zone; no averaging or missing-zone fill.
    ``provenance`` names harvest_md5, conversion_md5, updated_ids_sha256 and
    crop_labels_sha256. These are caller admission claims, not authentication.
    Original raw frames/cards/maps are not mutated. An audit row survives even
    when current serving excludes it or conversion is held/unresolved. No held
    row reaches ANY kernel layer; the aggregate is over delegated rows only.

    Kernel counts describe delegated rows, not the full native universe.
    Actual final Country/API correspondence is a separate, unperformed gate.
    This function is not automatically used by any Country/Feature/default API.
    """
    operand, factors, audit, factor_audit, counts, delegated = _native_w3_inputs(
        harvest, conversion, harvest_unit_card=harvest_unit_card,
        conversion_unit_card=conversion_unit_card, crop_labels=crop_labels,
        updated_ids=updated_ids, provenance=provenance, geography=geography)
    unit_kg = {'kg': 1., 'g': .001}
    diagnostics = _tr.harvest_kg_factors(operand, shipped_factors=factors,
                                        unit_kg=unit_kg, volume_as_mass=False)
    result = _tr.harvest_kg(operand, shipped_factors=factors,
                            unit_kg=unit_kg, volume_as_mass=False)
    for name in diagnostics.columns:
        dtype = ('boolean' if pd.api.types.is_bool_dtype(diagnostics[name].dtype)
                 else 'Float64' if pd.api.types.is_numeric_dtype(diagnostics[name].dtype)
                 else 'string')
        values = pd.Series(pd.NA, index=np.arange(len(audit)), dtype=dtype)
        values.loc[delegated] = diagnostics[name].to_numpy()
        audit[_PREFIX + name] = values.array
    metadata = {'provenance': deepcopy(provenance), 'geography': geography,
                'harvest_unit_card': {str(k): str(v) for k, v in harvest_unit_card.items()},
                'conversion_unit_card': {str(k): str(v) for k, v in conversion_unit_card.items()},
                'factor_inventory': counts, 'native_rows': len(audit),
                'eligible_rows': int(audit.W3_Eligible.sum()),
                'delegated_rows': int(delegated.sum()),
                'held_eligible_rows': int((audit.W3_Eligible & audit.W3_Held).sum()),
                'unserved_rows': int((~audit.W3_Eligible).sum()),
                'kernel_counts_scope': 'delegated rows only', 'id_converted': True}
    result.attrs['nigeria_w3_native_audit'] = {k: metadata[k] for k in
        ('provenance', 'geography', 'native_rows', 'eligible_rows', 'delegated_rows',
         'held_eligible_rows', 'unserved_rows', 'kernel_counts_scope', 'id_converted')}
    return result, NativeHarvestAudit(audit, factor_audit, metadata)
