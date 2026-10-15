"""GH #838 -- a stated unit size must beat the price-ratio inference, and a
contradiction between the two must be LOUD.

Two defects under test:

1. ``KNOWN_METRIC`` / the explicit-metric parser lacked labels the corpus
   actually mints (``milligramme``, ``centilitre``, ``quintal``, ``tonne``),
   so their rows were served an invented weight -- Mali's ``Centilitre`` at
   0.984 kg against a stated 0.01.

2. Where a label STATES its metric content (``Sac moyen (50 kg)``) the stated
   factor must win over the inference (it already did), AND a disagreement
   beyond :data:`STATED_VS_INFERRED_TOLERANCE` must warn -- that disagreement
   is evidence about the unit vocabulary, previously thrown away.  Mali's
   ``Sac moyen`` was inferred at 26.2 kg against a stated 50.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    KNOWN_METRIC,
    STATED_VS_INFERRED_TOLERANCE,
    StatedVsInferredKgWarning,
    _get_kg_factors,
    _parse_explicit_metric,
    food_kg_factors,
)

IDX = ['t', 'i', 'j', 'u']


def _rows(item, unit, n, qty, price, t='2020', start=0):
    """*n* households buying *item* in *unit*, *qty* units at *price* each."""
    return [(t, f'h{k}', item, unit, qty, qty * price) for k in range(start, start + n)]


def _frame(rows):
    idx = pd.MultiIndex.from_tuples([r[:4] for r in rows], names=IDX)
    return pd.DataFrame({'Quantity': [r[4] for r in rows],
                         'Expenditure': [r[5] for r in rows]}, index=idx)


def _mali_like_frame(*, unit_label='Sac moyen (50 kg)', n=30,
                     price_per_kg=100.0, price_per_unit=2620.0):
    """*n* households EACH buying the item by the kilogram at a coherent
    price (the baseline) AND in a container label whose inference (26.2 kg at
    these prices) contradicts the label's stated 50 kg -- Mali's
    ``Sac moyen``, compressed.  One household must report both units or the
    inference has nothing to ratio against."""
    rows = []
    for k in range(n):
        rows.append(('2020', f'h{k}', 'rice', 'kg', 1.0, price_per_kg))
        rows.append(('2020', f'h{k}', 'rice', unit_label, 1.0, price_per_unit))
    return _frame(rows)


# ----- the new vocabulary ---------------------------------------------------

def test_known_metric_new_spellings():
    for u in ('milligramme', 'milligrammes'):
        assert KNOWN_METRIC[u] == pytest.approx(1e-6)
    for u in ('centilitre', 'centilitres'):
        assert KNOWN_METRIC[u] == pytest.approx(1 / 100)
    # GH #919: `quintal` was here and is now in `categorical_mapping/u.org`'s
    # `u_kg` table instead -- it is country-qualified (100 kg in Ethiopia,
    # 100 lb in Central America) and a KNOWN_METRIC entry is unoverridable.
    for u in ('quintal', 'quintals'):
        assert u not in KNOWN_METRIC
    for u in ('tonne', 'tonnes'):
        assert KNOWN_METRIC[u] == 1000
    # The GH #838 audit behind the issue: gramme/grammes were already in.
    assert KNOWN_METRIC['gramme'] == pytest.approx(1 / 1000)
    assert KNOWN_METRIC['grammes'] == pytest.approx(1 / 1000)


def test_parse_explicit_metric_new_spellings():
    assert _parse_explicit_metric('Boite (500 milligrammes)') == pytest.approx(0.5e-3)
    assert _parse_explicit_metric('Bouteille (75 centilitres)') == pytest.approx(0.75)
    assert _parse_explicit_metric('2 quintaux') is None  # not a corpus label
    # GH #919: the parser is a SECOND, independent seed of the same factor, so
    # it had to lose `quintal` too -- otherwise a label like '1 quintal' would
    # re-introduce the unoverridable 100 kg the dict entry was removed for.
    assert _parse_explicit_metric('1 quintal') is None
    assert _parse_explicit_metric('0.5 tonne') == 500.0
    # Volumes stay gated.
    assert _parse_explicit_metric('Bouteille (75 centilitres)',
                                  volume_as_mass=False) is None


def test_centilitre_served_as_stated_not_inferred():
    """Mali's ``Centilitre``: inferred 0.984 kg, stated 0.01."""
    df = _mali_like_frame(unit_label='Centilitre', price_per_unit=98.4)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', StatedVsInferredKgWarning)
        out = food_kg_factors(df)
    rows = out[out.index.get_level_values('u') == 'Centilitre']
    assert len(rows) == 30
    assert (rows['KgFactorSource'] == 'metric').all()
    assert rows['kg_per_unit'].iloc[0] == pytest.approx(0.01)


# ----- the stated-vs-inferred disagreement ----------------------------------

def test_stated_beats_inferred_and_warns():
    """``Sac moyen (50 kg)``: stated 50 served, inferred 26.2 reported."""
    df = _mali_like_frame()
    with pytest.warns(StatedVsInferredKgWarning,
                      match=r'sac moyen \(50 kg\).*stated 50'):
        factors = _get_kg_factors(df)
    assert factors['sac moyen (50 kg)'] == 50.0

    with pytest.warns(StatedVsInferredKgWarning):
        out = food_kg_factors(df)
    served = out[out['KgFactorSource'] == 'metric']
    assert set(served.index.get_level_values('u')) == {'kg', 'Sac moyen (50 kg)'}
    sac = served[served.index.get_level_values('u') == 'Sac moyen (50 kg)']
    assert sac['kg_per_unit'].iloc[0] == 50.0


def test_agreement_within_tolerance_is_silent():
    """An inference within the tolerance is agreement, not news."""
    df = _mali_like_frame(price_per_unit=3900.0)  # inferred 39 vs stated 50 (1.28x)
    with warnings.catch_warnings():
        warnings.simplefilter('error', StatedVsInferredKgWarning)
        factors = _get_kg_factors(df)
        food_kg_factors(df)
    assert factors['sac moyen (50 kg)'] == 50.0
    assert STATED_VS_INFERRED_TOLERANCE == 1.5


def test_label_without_metric_content_is_unreported():
    """An unparseable label (``Sac moyen``) is served the inferred factor,
    exactly as before, with no stated-vs-inferred comparison to make."""
    df = _mali_like_frame(unit_label='Sac moyen')
    with warnings.catch_warnings():
        warnings.simplefilter('error', StatedVsInferredKgWarning)
        factors = _get_kg_factors(df)
        out = food_kg_factors(df)
    assert factors['sac moyen'] == pytest.approx(26.2)
    assert (out['KgFactorSource'] == 'item_unit').sum() == 30
