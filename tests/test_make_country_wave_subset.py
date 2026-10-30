"""Direct country builders must honor wave requests (ledger make-country-wave-subset).

Real Make/script execution, synthetic payload only; GH1039 owns the fixture.
"""
import pickle

import numpy as np
import pandas as pd
import pytest

from tests.test_gh1039_make_backend_no_country_rule import (
    fakeland, _SCHEME, _PREAMBLE, _COUNTRY_SCRIPT,
)

WAVES = ['2001', '2002', '2003']
MAKEFILE = _PREAMBLE + '''\
$(VAR_DIR)/build_me.parquet: build_me.py
\tpython build_me.py
'''
SCRIPT = _COUNTRY_SCRIPT[:_COUNTRY_SCRIPT.index('pd.DataFrame(')] + '''\
f = pd.DataFrame({
    't': ['2002', '2001', '2001', None, 'unlisted'],
    'i': ['b', 'a', 'a_missing', 'null', 'other'],
    'x': pd.array([2.000000000000001, 1.000000000000001, None, 8., 9.], dtype='Float64'),
}).set_index(['t', 'i'])
f.attrs = {'id_converted': True, 'marker': 'preserved'}
f.to_parquet(out)
'''


def exact(actual, expected):
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert pickle.dumps(actual.attrs) == pickle.dumps(expected.attrs)
    # GH1206: pandas 3 check_exact alone misses nullable float discrepancies.
    np.testing.assert_array_equal(actual.x.isna(), expected.x.isna())
    np.testing.assert_array_equal(actual.x.dropna().to_numpy(), expected.x.dropna().to_numpy())


@pytest.fixture
def direct(fakeland):
    (fakeland.country_dir / '_' / 'data_scheme.yml').write_text(
        _SCHEME.replace("Waves: ['2001']", f'Waves: {WAVES!r}'))
    return fakeland, fakeland.build(makefile=MAKEFILE, country_script=SCRIPT)


def test_country_builder_alternating_singletons_and_empty_supported_subset(direct):
    fake, country = direct
    full = country.build_me()
    before = fake.var.read_bytes()
    native = pd.read_parquet(fake.var)
    for wave, count in [('2001', 1), ('2002', 1), ('2001', 1), ('2003', 0)]:
        # The finalizer drops the all-null payload row, not the wave filter.
        actual = country.build_me(waves=[wave])
        # Finalization attaches wave-scoped metadata and null-key audits.
        # A slice of the FINAL full result has stale full-frame attrs.
        expected = country._finalize_result(
            native.loc[native.index.get_level_values('t') == wave],
            country._materialization_entry('build_me'), 'build_me')
        assert len(actual) == count
        exact(actual, expected)
        pd.testing.assert_frame_equal(
            actual, full.loc[full.index.get_level_values('t') == wave],
            check_exact=True)
        assert actual.attrs['marker'] == 'preserved'
        assert actual.attrs['id_converted'] is True
        assert fake.var.read_bytes() == before, 'selection rewrote country output'
    exact(country.build_me(), full)


@pytest.mark.parametrize('selection', [None, WAVES, list(reversed(WAVES))])
def test_full_country_keeps_null_and_unlisted_t(direct, selection):
    fake, country = direct
    actual = country.build_me(waves=selection)
    assert len(actual) == 4
    assert actual.index.get_level_values('t').isna().sum() == 1
    assert 'unlisted' in actual.index.get_level_values('t')
    expected = country._finalize_result(
        pd.read_parquet(fake.var), country._materialization_entry('build_me'), 'build_me')
    exact(actual, expected)


def test_unknown_wave_still_refused_before_builder(direct):
    fake, country = direct
    with pytest.raises(KeyError, match='2099 is not a valid wave'):
        country.build_me(waves=['2099'])
    assert not fake.var.exists()


def test_no_t_country_result_keeps_existing_passthrough(direct):
    fake, country = direct
    script = SCRIPT.replace("set_index(['t', 'i'])", "drop(columns='t').set_index('i')")
    (fake.country_dir / '_' / 'build_me.py').write_text(script)
    full = country.build_me()
    exact(country.build_me(waves=['2001']), full)


def test_empty_explicit_selection_returns_no_rows(direct):
    _, country = direct
    assert country.build_me(waves=[]).empty


def test_subset_does_not_recover_failed_country_recipe(direct):
    fake, country = direct
    country.build_me()  # stale target present, real recipe failure must win
    (fake.country_dir / '_' / 'Makefile').write_text(
        _PREAMBLE + '''\
.PHONY: force
$(VAR_DIR)/build_me.parquet: force
\t@echo deliberate-subset-failure >&2
\t@exit 7
''')
    with pytest.raises(RuntimeError, match='Make build failed'):
        country.build_me(waves=['2001'])


def test_success_without_native_output_still_refuses_subset(direct):
    fake, country = direct
    # No readable target: filtering must never manufacture an empty success.
    (fake.country_dir / '_' / 'build_me.py').write_text('pass\n')
    with pytest.raises(RuntimeError, match='Could not materialize'):
        country.build_me(waves=['2001'])
    assert not fake.var.exists()
