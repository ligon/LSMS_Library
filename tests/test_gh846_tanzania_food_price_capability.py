"""Source-only review of the scoped C4 record (ledger #846 §§2–4).

Instrument findings and coverage verdicts are deliberately separate. The
explicit-path loader must keep every partly reviewed coverage cell open.
No Country construction, source reader, parquet read, or provider is needed.
"""
from pathlib import Path

import pytest

from lsms_library.capability import (
    CATALOG_ONLY,
    SeriesCapability,
    capability,
    proposed_absent_verdicts,
)
from lsms_library.coverage_matrix import (
    _absent_tier,
    _dropped_tier,
    feature_vocabulary,
    load_verdicts,
)


ROOT = Path(__file__).resolve().parents[1]
RECORD = ROOT / 'lsms_library/countries/Tanzania/_/capabilities/food_price_absent_verdicts.csv'
CONCEPTS = ('reported_purchase_price', 'own_consumption_valuation')
WAVES = ('2008-09', '2010-11', '2012-13', '2014-15', '2019-20', '2020-21')
PAGES = {
    '2008-09': '87;88;95;96',
    '2010-11': '87;88;93;94',
    '2012-13': '87;88;91;92',
    '2014-15': '87;88;89;90',
    '2019-20': '53;54;55;56;57',
    '2020-21': '34;35;36;37;38;39;40;41;42',
}


def test_record_separates_twelve_module_findings_from_open_coverage():
    import csv

    with RECORD.open(newline='') as stream:
        serialized = list(csv.DictReader(stream))
    rows = load_verdicts(RECORD)
    assert len(serialized) == len(rows) == 12  # duplicates must not be hidden
    assert set(rows) == {
        ('Tanzania', concept, wave) for concept in CONCEPTS for wave in WAVES
    }
    for row in rows.values():
        assert row['verdict'] == 'unsure'
        assert row['instrument_finding'] == 'not-asked-within-module'
        assert row['closes'] == 'False'
        assert row['checks_run'] == 'C2;C4'
        assert row['outstanding_checks'] == 'C1;C3'
        assert 'not shipped-source census' in row['completed_methods']
        assert 'no raster/OCR' in row['completed_methods']
        assert row['pdf_pages'] == PAGES[row['wave']]
        assert 'Section' in row['module_scope']
        assert len(row['instrument_sha256']) == 64
        assert row['review_sha256'] == (
            '370da7f8c8cce9bc1e2d817697a31642328caca4b2f07098ad970e4f6fbf9c64'
        )
        assert 'Coverage unsure:' in row['evidence']
        if row['wave'] in WAVES[:4]:
            assert 'nps_upq_questionnaire.pdf' in row['instrument_file']
            assert 'reconstructed original R' in row['original_round_provenance']
            assert 'PDF5-6,19,29' in row['original_round_provenance']
        elif row['wave'] == '2019-20':
            assert 'nps_sdd_household_qx_final_english_.pdf' in row['instrument_file']
        else:
            assert 'nps_household_qx_y5_final_.pdf' in row['instrument_file']
            assert 'released J1A' in row['module_scope']
            assert 'j1_selection' in row['original_round_provenance']


@pytest.mark.parametrize('concept', CONCEPTS)
def test_existing_loader_leaves_all_reviewed_cells_open(concept):
    rows = load_verdicts(RECORD)
    for wave in WAVES:
        tier, detail = _absent_tier('Tanzania', concept, wave, rows)
        assert tier == 'absent', (concept, wave)
        assert detail.startswith('unsure:') and 'Coverage unsure:' in detail
        tier, _ = _absent_tier(
            'Tanzania', concept, wave, rows, default_tier='undeclared',
        )
        assert tier == 'undeclared'
    for wave in ('', '2008-15', '2021-22'):
        tier, _ = _absent_tier('Tanzania', concept, wave, rows)
        assert tier == 'absent', (concept, wave)
    # Existing absent-verdict semantics must not disguise a declared build drop.
    tier, detail = _dropped_tier(
        'Tanzania', concept, '2019-20', rows, default_detail='synthetic drop',
    )
    assert tier == 'dropped'
    assert 'INERT' in detail


def test_evidence_does_not_close_whole_food_features_or_section_f():
    rows = load_verdicts(RECORD)
    for feature in ('food_acquired', 'food_prices', 'food_expenditures',
                    'community_prices', 'outside_household_food_value'):
        for wave in WAVES:
            tier, _ = _absent_tier('Tanzania', feature, wave, rows)
            assert tier == 'absent', (feature, wave)
    # This staging file is not a new canonical feature registry.
    assert set(CONCEPTS).isdisjoint(feature_vocabulary())


@pytest.mark.parametrize('unrelated_capability', [False, True])
def test_staged_evidence_does_not_install_live_or_series_verdicts(
        monkeypatch, unrelated_capability):
    # An explicit-path loader check is not a live matrix integration (§4/§6).
    # A legitimate capability about another feature must remain possible.
    if unrelated_capability:
        import importlib

        registry = importlib.import_module('lsms_library.capability')._SERIES_CAPABILITY
        monkeypatch.setitem(registry, ('Tanzania', 'NPS-SDD'), SeriesCapability(
            country='Tanzania', series='NPS-SDD',
            provides=('household_roster',), lacks=('housing',),
            validation=CATALOG_ONLY, evidence='Synthetic unrelated capability',
        ))
        assert proposed_absent_verdicts('Tanzania', 'NPS-SDD', ['2019-20'])[0][
            'feature'] == 'housing'
    staged = load_verdicts(RECORD)
    assert len(staged) == 12
    live = load_verdicts()
    assert set(staged).isdisjoint(live)
    waves = ['2008-09', '2010-11', '2012-13', '2014-15', '2019-20', '2020-21']
    for concept in CONCEPTS:
        for wave in waves:
            assert _absent_tier('Tanzania', concept, wave, live)[0] == 'absent'
    for series in ('NPS', 'NPS-SDD', 'NPS-R1', 'NPS-R2', 'NPS-R3',
                   'NPS-R4', 'NPS-R5', 'NPS-UPD'):
        record = capability('Tanzania', series)
        if record is not None:
            assert all(record.verdict_for(concept) is None for concept in CONCEPTS)
        proposed = proposed_absent_verdicts('Tanzania', series, waves)
        assert not any(row['feature'] in CONCEPTS for row in proposed)


def test_evidence_gate_still_rejects_an_unsupported_close(tmp_path):
    import csv

    rows = list(load_verdicts(RECORD).values())
    # Module evidence is not closing authority. Preserve the existing gate
    # against an evidence-free future closing edit (ledger §§3–5).
    rows[0] = {**rows[0], 'verdict': 'not-asked', 'evidence': ''}
    path = tmp_path / 'unsupported.csv'
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with pytest.warns(UserWarning, match='REQUIRES'):
        accepted = load_verdicts(path)
    assert ('Tanzania', rows[0]['feature'], rows[0]['wave']) not in accepted
    assert len(accepted) == 11
