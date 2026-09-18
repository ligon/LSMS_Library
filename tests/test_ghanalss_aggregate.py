"""GhanaLSS's published Aggregate partition and its food-table contract.

Configuration checks run without microdata. The delivered checks use the
shared requires_s3 gate and cover every wave on purchased and total recorded
expenditure bases. Aggregate food_acquired must only rename j; expenditures
must sum levels while retaining households, sources and missing cluster keys.

The selection specification is the independent partition oracle. Historical
wave Aggregate columns and the nutritional crosswalk do not define this map.

The partition is keyed on the COUNTRY Preferred Label -- the served ``j``
since PR #935 applied the wave->country crosswalk
(``ghanalss.to_country_food_labels``).  A wave label is not necessarily a
country label (``Potato/Sweet Potato``, ``Sorghum``, ``Guinea Corn`` are
wave-only), so nothing here asserts wave labels against the country axis;
the wave<->country bijection is pinned once, in
``test_ghanalss_food_label_canonical.py``.
"""
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library import Country
from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root


COUNTRY = 'GhanaLSS'
WAVES = ['1987-88', '1988-89', '1991-92', '1998-99', '2005-06',
         '2012-13', '2016-17']
SELECTION = (Path(__file__).resolve().parents[1] / 'SkunkWorks' / 'aggregation'
             / 'ghanalss' / 'selected_groups.yml')
INVENTORY = SELECTION.with_name('item_review.csv')


def _labels(table):
    return {label for label in table['Preferred Label'].dropna()
            if isinstance(label, str) and label.strip()}


@pytest.fixture(scope='module')
def selected_groups():
    with SELECTION.open() as source:
        selection = yaml.safe_load(source)
    assert selection['country'] == COUNTRY
    return selection['groups']


@pytest.fixture(scope='module')
def country_food_table():
    return df_from_orgfile(
        countries_root() / COUNTRY / '_' / 'categorical_mapping.org',
        name='harmonize_food')


@pytest.fixture(scope='module')
def wave_food_labels():
    return {
        wave: _labels(df_from_orgfile(
            countries_root() / COUNTRY / wave / '_' / 'categorical_mapping.org',
            name='harmonize_food'))
        for wave in WAVES
    }


@pytest.fixture(scope='module')
def partition(country_food_table, selected_groups):
    """Unselected country labels stay distinct, including FCT-only identities."""
    mapping = {label: label for label in _labels(country_food_table)}
    for group in selected_groups:
        mapping.update({member: group['label'] for member in group['members']})
    return mapping


class TestConfiguration:
    def test_selection_has_exact_disjoint_members(
            self, selected_groups, country_food_table):
        declared = _labels(country_food_table)
        members = [member for group in selected_groups for member in group['members']]
        targets = [group['label'] for group in selected_groups]
        assert selected_groups
        assert all(len(group['members']) >= 2 for group in selected_groups)
        assert len(members) == len(set(members)), 'Selected groups overlap'
        assert set(members) <= declared, sorted(set(members) - declared)
        assert len(targets) == len(set(targets)), 'Distinct groups share an output label'
        singletons = _labels(country_food_table) - set(members)
        assert not set(targets) & singletons, 'A group would absorb an unselected singleton'

    def test_publication_universe_is_the_country_axis(self, country_food_table):
        """The inventory's delivered labels and the country table coincide.

        Post-#935 the served ``j`` is a bijection onto the country Preferred
        Labels, so ``publish_ghana`` must append no rows: a delivered label
        absent from the table would mean the crosswalk leaked a wave label,
        which is the defect #937's 47 appended rows were built on.
        """
        delivered = set(pd.read_csv(INVENTORY)['Preferred Label'])
        country = _labels(country_food_table)
        assert delivered == country, (
            f'delivered-only {sorted(delivered - country)[:10]}; '
            f'country-only {sorted(country - delivered)[:10]}')

    @pytest.mark.parametrize('wave', WAVES)
    def test_every_wave_label_has_a_country_row(
            self, country_food_table, wave_food_labels, wave):
        """Each wave Preferred Label appears in that wave's country column.

        This is the totality half of the crosswalk (the bijection proper is
        pinned in test_ghanalss_food_label_canonical.py); it is what makes
        the country-keyed partition reach every served row.
        """
        column = {str(v).strip() for v in country_food_table[wave].dropna()
                  if str(v).strip() and str(v).strip() != 'nan'}
        missing = wave_food_labels[wave] - column
        assert not missing, f'{wave}: no country row names {sorted(missing)}'

    def test_aggregate_mapping_is_complete_unambiguous_and_matches_selection(
            self, country_food_table, partition):
        assert 'Aggregate Label' in country_food_table.columns
        pairs = country_food_table[['Preferred Label', 'Aggregate Label']]
        for column in pairs:
            assert pairs[column].notna().all(), f'Missing {column}'
            assert pairs[column].map(
                lambda value: isinstance(value, str) and bool(value.strip())).all()
        target_counts = pairs.groupby('Preferred Label')['Aggregate Label'].nunique()
        assert target_counts.eq(1).all(), target_counts[target_counts != 1].to_dict()
        actual = pairs.set_index('Preferred Label')['Aggregate Label'].to_dict()
        assert actual == partition


@pytest.fixture(scope='module')
def country():
    return Country(COUNTRY)


@pytest.fixture(scope='class')
def preferred_acquired(country):
    # No broad exception-to-skip: credentials are gated by the test class,
    # while malformed configuration and real build failures must remain failures.
    # Read the all-wave cache once, and release it before expenditure tests.
    result = country.food_acquired(labels='Preferred')
    assert not result.empty, 'Empty food_acquired result'
    assert set(result.index.get_level_values('t')) == set(WAVES)
    return result


@pytest.mark.requires_s3
@pytest.mark.slow
class TestAcquired:
    def test_default_retains_preferred_labels(
            self, country, preferred_acquired, country_food_table):
        default = country.food_acquired()
        pd.testing.assert_frame_equal(default, preferred_acquired, check_exact=True)
        del default
        country_labels = _labels(country_food_table)
        for wave in WAVES:
            fine = preferred_acquired.xs(wave, level='t')
            assert set(fine.index.get_level_values('j')) <= country_labels

    def test_food_acquired_only_renames_j(
            self, country, preferred_acquired, partition):
        aggregate = country.food_acquired(labels='Aggregate')
        assert len(aggregate) == len(preferred_acquired)
        assert set(aggregate.index.get_level_values('t')) == set(WAVES)
        for wave in WAVES:
            fine = preferred_acquired.xs(wave, level='t', drop_level=False)
            renamed = fine.rename(index=partition, level='j')
            # Compare all columns and row multiplicity, copying only one wave:
            # prices, quantities, units, sources and visits may not be reduced.
            pd.testing.assert_frame_equal(
                aggregate.xs(wave, level='t', drop_level=False), renamed,
                check_exact=True)
            del fine, renamed


@pytest.mark.requires_s3
@pytest.mark.slow
class TestExpenditures:
    @pytest.mark.parametrize('basis', ['purchased', 'total'])
    def test_expenditures_sum_levels_without_losing_households_or_sources(
            self, country, basis, partition):
        preferred = country.food_expenditures(basis=basis, labels='Preferred')
        default = country.food_expenditures(basis=basis)
        pd.testing.assert_frame_equal(default, preferred, check_exact=True)
        del default
        assert not preferred.empty, f'{basis}: empty expenditures'
        assert set(preferred.index.get_level_values('t')) == set(WAVES)

        aggregate = country.food_expenditures(basis=basis, labels='Aggregate')
        assert set(aggregate.index.get_level_values('t')) == set(WAVES)
        assert aggregate.index.is_unique
        if basis == 'purchased':
            assert set(aggregate.index.get_level_values('s')) == {'purchased'}
        for wave in WAVES:
            fine = preferred.xs(wave, level='t', drop_level=False)
            grouped = aggregate.xs(wave, level='t', drop_level=False)
            renamed = fine.rename(index=partition, level='j')
            summed = renamed.groupby(
                level=renamed.index.names, dropna=False).sum(min_count=1)
            pd.testing.assert_frame_equal(
                grouped.sort_index(), summed.sort_index(), rtol=1e-12, atol=1e-8)

            # Early-wave households outside sample have missing v keys. Keep
            # them in each household/source total, on both expenditure bases.
            other_levels = [level for level in fine.index.names if level != 'j']
            before = fine.groupby(level=other_levels, dropna=False).sum(min_count=1)
            after = grouped.groupby(level=other_levels, dropna=False).sum(min_count=1)
            pd.testing.assert_frame_equal(before, after, rtol=1e-12, atol=1e-8)
