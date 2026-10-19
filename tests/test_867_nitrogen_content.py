"""GH #867: ``_NITROGEN_CONTENT`` for NPS, D Compound and organic inputs.

Ruling (@ligon, 2026-10-18, on the issue): *"NPS and Malawi D Compound get
CITED N shares (source in the code comment and the test). Organic inputs
(manure, compost, organic fertilizer) get a documented refusal: they stay
unmatched and loud (NutrientCoverageWarning), with no nominal share."*

The citations, repeated here because the ruling asks for them in the test:

* **NPS = 0.19 kg N / kg** -- grade 19-38-0+7S.  IFDC & AFAP for AGRA,
  *Assessment of Fertilizer Distribution Systems and Opportunities for
  Developing Fertilizer Blends: Ethiopia* (June 2018), Table 1 "Fertilizers
  available in Ethiopia and volumes sold for the 2016-17 season", report
  p. 3: ``NP 19 38 0 + 7S  NPS``.  Agrees with the WB harmonised panel
  (``ETH_ESS3.do:896`` / ``ETH_ESS4.do:918`` / ``ETH_ESS5.do:984``,
  ``NPS_kg*0.19``).
* **D Compound = 0.08 kg N / kg** -- IFDC with DARS / Ministry of
  Agriculture, for the Government of Malawi, *Proposed Fertilizer
  Regulations, 2003* (a draft; WIPO Lex MW022, "Other Texts"), Schedule B,
  A. "Investigational Allowances -- Penalty Rates", Examples of Penalty
  Compilation, example 2, printed p. 31 (PDF p. 23): 'A "D" compound
  (8-14-7-6.5S) fertilizer was guaranteed as 8% N'; IFDC & AFAP for AGRA,
  *Assessment ... Malawi* (June 2018), Table 1 "Main fertilizer straights,
  compounds and blends available in Malawi", report p. 5:
  ``8:18:15+6S+0.1B  Compound D/Super D``, whose next row is ``NPK
  10:24:20 + 6S + 0.1B  Super D`` (a different product, ~10% N); and,
  independent of IFDC, the Ministry of Agriculture's *Guide to
  Agriculture Production and Natural Resources Management in Malawi*
  (revised 2020), Table 54, printed p. 232 (PDF p. 254):
  ``Compound D 8 18 15 4 0.1``.  The WB panel's 0.07 (``MWI_IHPS1.do:804``)
  is uncited and is NOT what this map serves; it matches the 7-14-7 that
  FAO & GoM, *Soil nutrient loss assessment in Malawi* (2018), p. IX,
  gives with no source (Zimbabwe's Compound D grade).  EPAR's 0.10 is
  Zambia's 10-20-10.
* **Organic: no share.**  Refused, by ruling; the measured reasons are in
  the comment above ``_NITROGEN_CONTENT``.

Synthetic tests fail on the base (``200e504a3``: no ``nps`` / ``d compound``
key, no ``_ORGANIC_INPUT_PATTERN``, no ``refused_organic_*`` tally, and
Ethiopia's ``Other Organic`` was never named in the warning) and pass here.
The warm tests skip cleanly where the corpus is not warm.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library import transformations as T
from lsms_library.transformations import (
    NutrientCoverageWarning,
    fertilizer_rate,
    nitrogen_kg,
)


def _plot_inputs(rows):
    df = pd.DataFrame(
        rows, columns=['t', 'i', 'plot_id', 'input', 'j', 'u', 'Quantity'])
    return df.set_index(['t', 'i', 'plot_id', 'input', 'j', 'u'])


def _run(rows):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = nitrogen_kg(_plot_inputs(rows))
    cov = [str(w.message) for w in caught
           if issubclass(w.category, NutrientCoverageWarning)]
    return out, cov


# ---------------------------------------------------------------------------
# The two cited blends
# ---------------------------------------------------------------------------

def test_nps_carries_the_cited_19_percent_share():
    """Ethiopia ESS ``NPS`` (W3 2015-16 onward): 19-38-0+7S, IFDC/AFAP for
    AGRA 2018 Ethiopia assessment, Table 1, report p. 3."""
    assert T._NITROGEN_CONTENT['nps'] == 0.19
    out, _ = _run([('2018-19', 'hh1', 'p1', 'NPS', 'Whole Plot', 'Kg',
                    100.0)])
    assert out.loc[('2018-19', 'hh1', 'p1'),
                   'Nitrogen_kg'] == pytest.approx(19.0)
    tally = out.attrs['nitrogen_input_match']
    assert tally['matched_labels'] == ['NPS']
    assert tally['unmatched_rows'] == 0


def test_d_compound_carries_the_cited_8_percent_share():
    """Malawi ``D Compound Fertilizer`` (Module D type code 5): 8% N per
    IFDC's *Proposed* Fertilizer Regulations 2003 for the Government of
    Malawi, Schedule B penalty example 2 (printed p. 31), IFDC/AFAP for AGRA
    2018 Malawi assessment, Table 1 (p. 5), and the Ministry of
    Agriculture's Guide (revised 2020), Table 54 (printed p. 232).  Reached
    through the strip rule (``... fertilizer`` -> ``d compound``).  NOT the
    WB panel's uncited 0.07 nor EPAR's 0.10."""
    assert T._NITROGEN_CONTENT['d compound'] == 0.08
    out, _ = _run([('2019-20', 'hh1', 'p1', 'D Compound Fertilizer', 'x',
                    'Kg', 100.0)])
    assert out.loc[('2019-20', 'hh1', 'p1'),
                   'Nitrogen_kg'] == pytest.approx(8.0)
    assert out.attrs['nitrogen_input_match']['matched_labels'] == [
        'D Compound Fertilizer']


def test_the_blends_sum_with_the_existing_products_on_one_plot():
    out, _ = _run([
        ('2019-20', 'hh1', 'p1', 'NPS', 'x', 'Kg', 100.0),
        ('2019-20', 'hh1', 'p1', 'D Compound Fertilizer', 'x', 'Kg', 100.0),
        ('2019-20', 'hh1', 'p1', 'Urea', 'x', 'Kg', 10.0),
    ])
    # 19 + 8 + 4.6
    assert out.loc[('2019-20', 'hh1', 'p1'),
                   'Nitrogen_kg'] == pytest.approx(31.6)


@pytest.mark.parametrize('label', ['Super D Compound Fertilizer', 'NPSB',
                                   'NPS with added Zn', 'Compound D'])
def test_a_near_miss_of_a_blend_stays_unmatched(label):
    """Matching stays EXACT: a key is a claim about one product's grade, so
    a label that merely CONTAINS ``nps`` / ``d compound`` gets no share."""
    out, _ = _run([('2019-20', 'hh1', 'p1', label, 'x', 'Kg', 100.0),
                   ('2019-20', 'hh1', 'p1', 'Urea', 'x', 'Kg', 10.0)])
    tally = out.attrs['nitrogen_input_match']
    assert label in tally['unmatched_labels']
    assert out['Nitrogen_kg'].sum() == pytest.approx(4.6)


def test_fertilizer_rate_product_basis_counts_the_blends():
    """``nutrient='product'`` reuses the map's keys at share 1.0, so the
    blends now count as fertilizer PRODUCT; organic inputs still do not."""
    pi = _plot_inputs([
        ('2019-20', 'hh1', 'p1', 'NPS', 'x', 'Kg', 100.0),
        ('2019-20', 'hh1', 'p1', 'D Compound Fertilizer', 'x', 'Kg', 50.0),
        ('2019-20', 'hh1', 'p1', 'Organic manure', 'x', 'Kg', 900.0),
    ])
    pf = pd.DataFrame([('2019-20', 'hh1', 'p1', 2.0)],
                      columns=['t', 'i', 'plot_id', 'Area']
                      ).set_index(['t', 'i', 'plot_id'])
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', NutrientCoverageWarning)
        prod = fertilizer_rate(pi, pf, nutrient='product', on='plot')
        n = fertilizer_rate(pi, pf, on='plot')
    assert prod['Fertilizer_kg_per_ha'].iloc[0] == pytest.approx(75.0)
    assert n['Nitrogen_kg_per_ha'].iloc[0] == pytest.approx((19 + 4) / 2)


# ---------------------------------------------------------------------------
# The organic refusal
# ---------------------------------------------------------------------------

#: Every organic ``plot_inputs.input`` label in the 14 warm countries
#: (census 2026-10-19, ``slurm_logs/gh867_nitrogen/``), with the country
#: that writes it.
CORPUS_ORGANIC_LABELS = [
    'Organic Fertilizer',                   # Malawi, Nigeria, Tanzania, Uganda
    'Organic manure',                       # the six EHCVM countries
    'Organic compost',                      # the six EHCVM countries
    'Manure',                               # Ethiopia, Mali, GhanaLSS
    'Compost',                              # Ethiopia, Mali
    'Other Organic',                        # Ethiopia
    'Other organic',                        # Mali
    'Adubos orgânicos - estrume animal',    # Guinea-Bissau
    'Adubos orgânicos - lixo doméstico',    # Guinea-Bissau
]

#: Inorganic labels that share the substring ``organ`` and must NOT be
#: classed as an organic refusal.
INORGANIC_LOOKALIKES = ['Other inorganic', 'Other Inorganic Fertilizer',
                        'Adubos inorgânicos - ureia',
                        'DAP e outros adubos inorgânicos']


def test_no_organic_key_may_enter_the_default_map():
    """The refusal is enforced, not just written down: a key such as
    ``'manure': 0.005`` would be an invented number in a published total."""
    organic_keys = [k for k in T._NITROGEN_CONTENT
                    if T._ORGANIC_INPUT_PATTERN.search(k)]
    assert organic_keys == []


@pytest.mark.parametrize('label', CORPUS_ORGANIC_LABELS)
def test_an_organic_label_is_refused_counted_and_named(label):
    keys = [k for k in T._normalise_input_label(label)
            if k in T._NITROGEN_CONTENT]
    assert keys == [], f'{label!r} resolves to {keys}'
    out, cov = _run([('2019-20', 'hh1', 'p1', label, 'x', 'Kg', 900.0),
                     ('2019-20', 'hh1', 'p1', 'Urea', 'x', 'Kg', 10.0)])
    tally = out.attrs['nitrogen_input_match']
    # unmatched, and counted as the documented refusal
    assert label in tally['unmatched_labels']
    assert tally['refused_organic_labels'] == {label: 1}
    assert tally['refused_organic_rows'] == 1
    # contributes nothing: 900 kg of it moves no number
    assert out['Nitrogen_kg'].sum() == pytest.approx(4.6)
    # loud, and named as a refusal rather than as a gap
    assert len(cov) == 1
    assert repr(label) in cov[0]
    assert 'refused a share by policy (GH #867)' in cov[0]
    assert "no share keyed (a gap): none" in cov[0]


@pytest.mark.parametrize('label', INORGANIC_LOOKALIKES)
def test_an_inorganic_label_is_not_classed_organic(label):
    out, cov = _run([('2019-20', 'hh1', 'p1', label, 'x', 'Kg', 100.0),
                     ('2019-20', 'hh1', 'p1', 'Urea', 'x', 'Kg', 10.0)])
    tally = out.attrs['nitrogen_input_match']
    assert label in tally['unmatched_labels']
    assert tally['refused_organic_labels'] == {}
    # still named -- as a gap, which is what it is
    assert len(cov) == 1 and repr(label) in cov[0]
    assert 'organic, refused a share by policy (GH #867): none' in cov[0]


def test_other_organic_is_loud():
    """Ethiopia's ``Other Organic`` (2,651 rows) carries no stem the base's
    ``_FERTILIZER_LABEL_HINTS`` knew, so it was unmatched and never named --
    the one organic label the ruling's "loud" did not reach."""
    out, cov = _run([('2018-19', 'hh1', 'p1', 'Other Organic', 'Whole Plot',
                      'Kg', 1.0),
                     ('2018-19', 'hh1', 'p1', 'Urea', 'Whole Plot', 'Kg',
                      10.0)])
    assert cov and "'Other Organic'" in cov[0]


def test_a_caller_can_still_pass_an_organic_share():
    """The refusal is the DEFAULT map's, not a block: a caller with a share
    for their own sample passes it explicitly, and then it is a match."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', NutrientCoverageWarning)
        out = nitrogen_kg(_plot_inputs([
            ('2019-20', 'hh1', 'p1', 'Organic manure', 'x', 'Kg', 1000.0)]),
            nitrogen_content={'organic manure': 0.01})
    assert out['Nitrogen_kg'].sum() == pytest.approx(10.0)
    tally = out.attrs['nitrogen_input_match']
    assert tally['matched_labels'] == ['Organic manure']
    assert tally['refused_organic_labels'] == {}


# ---------------------------------------------------------------------------
# Warm corpus (skips where the cache is not warm)
# ---------------------------------------------------------------------------

def _warm(country, table):
    """Load a table that is ALREADY warm, or skip (see the twin helper in
    ``tests/test_863_transforms.py`` for why ``assume_cache_fresh``)."""
    try:
        import lsms_library as ll
        from lsms_library.local_tools import data_root
    except Exception as exc:                      # pragma: no cover
        pytest.skip(f"lsms_library unavailable: {exc}")
    root = data_root() / country
    warm = (root / 'var' / f'{table}.parquet').exists() or any(
        root.glob(f'*/_/{table}.parquet'))
    if not warm:
        pytest.skip(f"{country} {table} is not warm here ({root})")
    try:
        df = getattr(ll.Country(country, assume_cache_fresh=True), table)()
    except Exception as exc:
        pytest.skip(f"{country} {table} unavailable here: {exc}")
    if df is None or df.empty:
        pytest.skip(f"{country} {table} empty -- no warm cache here")
    return df


def _label_n(pi, label):
    """kg N contributed by one ``input`` label alone."""
    mask = pi.index.get_level_values('input').astype(str) == label
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', NutrientCoverageWarning)
        return float(nitrogen_kg(pi[mask])['Nitrogen_kg'].sum())


@pytest.mark.requires_s3
def test_ethiopia_warm_nps_is_matched_and_organics_refused():
    """Measured 2026-10-19: NPS 5,264 rows unmatched -> matched; Ethiopia
    total 445,471.0 -> 500,847.2 kg N (NPS: +55,376.2 over 5,261 plots).  Its 34,207 organic rows carry no
    quantity at all (``Applied`` dummy only), so the refusal costs Ethiopia
    nothing numerically -- it is the label, not the number, that is named."""
    pi = _warm('Ethiopia', 'plot_inputs')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', NutrientCoverageWarning)
        out = nitrogen_kg(pi)
    tally = out.attrs['nitrogen_input_match']
    assert 'NPS' in tally['matched_labels']
    assert 'NPS' not in tally['unmatched_labels']
    assert _label_n(pi, 'NPS') > 0
    assert {'Manure', 'Compost', 'Other Organic'} <= set(
        tally['refused_organic_labels'])
    assert set(tally['refused_organic_labels']) <= set(
        tally['unmatched_labels'])


@pytest.mark.requires_s3
def test_malawi_warm_d_compound_is_matched_at_8_percent():
    """Measured 2026-10-19: D Compound 726 rows unmatched -> matched.
    ``u`` is ``Kg`` or a bag size here, so the share is checked against the
    label's own product kilograms rather than a pinned total."""
    pi = _warm('Malawi', 'plot_inputs')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', NutrientCoverageWarning)
        out = nitrogen_kg(pi)
    tally = out.attrs['nitrogen_input_match']
    assert 'D Compound Fertilizer' in tally['matched_labels']
    assert 'Organic Fertilizer' in tally['refused_organic_labels']
    mask = pi.index.get_level_values('input').astype(str) == \
        'D Compound Fertilizer'
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', NutrientCoverageWarning)
        product = float(nitrogen_kg(
            pi[mask], nitrogen_content={'d compound': 1.0}
        )['Nitrogen_kg'].sum())
    assert product > 0
    assert _label_n(pi, 'D Compound Fertilizer') == pytest.approx(
        0.08 * product)
