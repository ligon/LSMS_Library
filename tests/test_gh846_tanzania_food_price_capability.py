"""Source-only review of the scoped C4 record (ledger #846 §§2–4).

These are source-concept verdicts, deliberately not a live matrix integration.
No Country construction, source reader, parquet read, or provider is needed.
"""
from pathlib import Path

import pytest

from lsms_library.capability import (
    QUESTIONNAIRE_VALIDATED,
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


def test_record_is_exactly_two_c4_concepts_in_the_attested_wave():
    rows = load_verdicts(RECORD)
    assert set(rows) == {('Tanzania', concept, '2019-20') for concept in CONCEPTS}
    for row in rows.values():
        assert row['verdict'] == 'not-asked'
        assert row['checks_run'] == 'C4'
        assert 'REPORT.org' in row['evidence']
        assert 'nps_sdd_household_qx_final_english_.pdf' in row['evidence']
        assert 'Section J' in row['evidence']
        # Reuse the existing validation-to-verdict rule, not the row generator
        # whose automatic C1;C4 would overstate our evidence (ledger §2).
        cap = SeriesCapability(
            country='Tanzania', series='NPS-SDD', provides=(),
            lacks=(row['feature'],), validation=QUESTIONNAIRE_VALIDATED,
            evidence=row['evidence'],
        )
        assert cap.verdict_for(row['feature']) == row['verdict']


@pytest.mark.parametrize('concept', CONCEPTS)
def test_existing_loader_closes_only_the_attested_absent_concept(concept):
    rows = load_verdicts(RECORD)
    tier, detail = _absent_tier('Tanzania', concept, '2019-20', rows)
    assert tier == 'not-asked'
    assert 'Section J' in detail
    for wave in ('2008-09', '2010-11', '2012-13', '2014-15', '2020-21', ''):
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
        tier, _ = _absent_tier('Tanzania', feature, '2019-20', rows)
        assert tier == 'absent', feature
    # This staging file is not a new canonical feature registry.
    assert set(CONCEPTS).isdisjoint(feature_vocabulary())


def test_staged_evidence_does_not_install_live_or_series_verdicts():
    # An explicit-path loader check is not a live matrix integration (§4/§6).
    staged = load_verdicts(RECORD)
    assert len(staged) == 2
    assert set(staged).isdisjoint(load_verdicts())
    for series in ('NPS', 'NPS-SDD', 'NPS-R1', 'NPS-R2', 'NPS-R3',
                   'NPS-R4', 'NPS-R5', 'NPS-UPD'):
        assert capability('Tanzania', series) is None
        assert proposed_absent_verdicts('Tanzania', series, ['2019-20']) == []


def test_evidence_gate_still_rejects_an_unsupported_close(tmp_path):
    import csv

    rows = list(load_verdicts(RECORD).values())
    rows[0] = {**rows[0], 'evidence': ''}
    path = tmp_path / 'unsupported.csv'
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with pytest.warns(UserWarning, match='REQUIRES'):
        accepted = load_verdicts(path)
    assert ('Tanzania', rows[0]['feature'], '2019-20') not in accepted
    assert len(accepted) == 1
