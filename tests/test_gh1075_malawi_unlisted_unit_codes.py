"""Malawi 2010-11 food_acquired: suffixed unit codes on no list (GH #1075).

Ruling (@ligon, 2026-11-03): the 13 rows under 2010-11 unit codes 16b / 16a /
12a / 21b / 12b / 03a -- on no instrument list, conversion-factor report or
`u` table entry -- fall under the standing "code on no instrument list ->
Unknown" rule; they are NOT decoded by analogy with the base code.  IHS3 ships
many suffixed SIZE sub-codes that the `u` table does decode (10C, 9B, 6A, ...),
so the rule is this named list, not "anything but 1-23".  The IHS2 '90 kg bag'
stays as is.  The data-gated count is in tests/test_gh1042_missing_unit_spellings.py.
"""
from lsms_library.paths import countries_root

MALAWI = countries_root() / 'Malawi'
DRIVER = MALAWI / '2010-11' / '_' / 'food_acquired.py'


def test_the_six_codes_are_sent_to_unknown():
    src = DRIVER.read_text()
    assert "_IHS3_UNLISTED_CODES = ['16A', '16B', '12A', '12B', '21B', '3A']" in src
    assert '.isin(_IHS3_UNLISTED_CODES)' in src


def test_decoded_size_subcodes_are_not_on_the_list():
    # the u table decodes these suffixed IHS3 sub-codes; the rule must not
    # touch them
    src = DRIVER.read_text()
    listed = src.split('_IHS3_UNLISTED_CODES = ')[1].split('\n')[0]
    for code in ('10C', '9B', '6A', '4A', '8B', '22B'):
        assert f"'{code}'" not in listed
    u_table = (MALAWI / '_' / 'categorical_mapping.org').read_text()
    for code in ('16a', '16b', '12a', '12b', '21b', '03a'):
        assert f'| {code} |' not in u_table, code


def test_ihs2_90_kg_bag_is_left_alone():
    src = (MALAWI / '2004-05' / '_' / 'food_acquired.py').read_text()
    assert '_IHS3_UNLISTED_CODES' not in src
