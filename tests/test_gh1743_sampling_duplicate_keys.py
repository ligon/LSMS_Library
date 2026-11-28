"""A sampling declaration must not erase an earlier one on load."""
from textwrap import dedent

import pytest

from lsms_library import Country, sampling


@pytest.fixture
def load(tmp_path, monkeypatch):
    path = tmp_path / "sampling.yml"
    monkeypatch.setattr(sampling, "_sampling_path", lambda country: path)
    sampling.sampling_records.cache_clear()

    def read(body, getter=lambda: sampling.sampling_records("Example")):
        path.write_text(dedent(body).lstrip("\n"), encoding="utf-8")
        return getter()

    yield read
    sampling.sampling_records.cache_clear()


@pytest.mark.parametrize("body,key,first,second", [
    ("""
     Sampling: {2007: {panel: {source_type: derived-from-data, confidence: high}}}
     Sampling: {2008: {panel: {source_type: derived-from-data, confidence: high}}}
     """, "Sampling", 1, 2),
    ("""
     Sampling:
       '2021-22': {panel: {role: baseline, source_type: derived-from-data, confidence: high}}
       2021-22: {served_strata: {holds: absent, source_type: derived-from-data, confidence: high, locator: proposal}}
     """, "2021-22", 2, 3),
    ("""
     Sampling:
       2021-22:
         served_strata: {holds: absent, source_type: derived-from-data,
                         confidence: high, locator: first}
         served_strata: {holds: absent, source_type: derived-from-data,
                         confidence: high, locator: second}
     """, "served_strata", 3, 5),
    ("""
     Sampling:
       2021-22: {panel: {role: baseline, role: baseline, source_type: derived-from-data, confidence: high}}
     """, "role", 2, 2),
    ("""
     Sampling:
       2021-22:
         <<:
           panel: {role: baseline, source_type: derived-from-data, confidence: high}
           panel: {role: panel, source_type: derived-from-data, confidence: high}
     """, "panel", 4, 5),
    ("""
     Sampling:
       2021-22:
         <<: {panel: {role: baseline, source_type: derived-from-data, confidence: high}}
         <<: {panel: {role: panel, source_type: derived-from-data, confidence: high}}
     """, "<<", 3, 4),
])
def test_repeated_explicit_key_names_both_lines_and_the_file(load, body, key, first, second):
    with pytest.raises(ValueError) as exc:
        load(body)
    assert f"sampling.yml: line {second}: duplicate key {key!r}" in str(exc.value)
    assert f"first declared at line {first}" in str(exc.value)


@pytest.mark.parametrize("older,newer", [("2008", "'2008'"), ("'2008'", "2008")])
def test_typed_wave_keys_that_resolve_to_the_same_public_wave_raise(load, older, newer):
    with pytest.raises(ValueError, match=r"sampling.yml: duplicate wave '2008'"):
        load(f"""
        Sampling:
          {older}: {{panel: {{role: baseline, source_type: derived-from-data, confidence: high}}}}
          {newer}: {{panel: {{role: panel, source_type: derived-from-data, confidence: high}}}}
        """)


def test_a_valid_merge_can_inherit_a_panel_and_override_an_inherited_key(load):
    records = load("""
    Sampling:
      2007: &baseline
        design_tag: two-stage-stratified-cluster
        source_type: local-documentation
        confidence: high
        panel: {role: baseline}
        notes: earlier
      '2008':
        <<: [*baseline, {notes: later fallback}]
        notes: current
      '2009':
        <<: *baseline
    """)
    assert list(records) == ["2007", "2008", "2009"]
    assert records["2008"].panel == {"role": "baseline"}
    assert records["2008"].notes == "current"
    assert records["2009"].notes == "earlier"


def test_invalid_yaml_still_warns_and_yields_no_records(load):
    with pytest.warns(UserWarning, match="could not read"):
        assert load("Sampling: [") == {}



def test_country_sampling_refuses_a_duplicate_instead_of_serving_the_last(load):
    # The first normalized record may be replaced by a strata-only record.
    # The metadata property must fail on the same duplicate as the loader.
    with pytest.raises(ValueError, match="duplicate key 'Sampling'"):
        load("Sampling: {}\nSampling: {}\n",
             lambda: Country("Ethiopia", preload_panel_ids=False).sampling)


def test_a_merge_cannot_alias_two_distinct_yaml_keys_to_one_public_wave(load):
    with pytest.raises(ValueError, match=r"sampling.yml: duplicate wave '2008'"):
        load("""
        Sampling:
          <<: {2008: {panel: {source_type: derived-from-data, confidence: high}}}
          '2008': {panel: {source_type: derived-from-data, confidence: low}}
        """)
