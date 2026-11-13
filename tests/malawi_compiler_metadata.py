"""Invented Malawi metadata trees for country-compiler controls (no data IO)."""
import json
from pathlib import Path

WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']
VARIANTS = ['base', 'native_only', 'yaml_only', 'unwired', 'retired_wave',
            'missing_wave_dir', 'mapped', 'extra_wave',
            'yaml_removed_native_retained', 'none_declared', 'country_retired']


def write_metadata(root, table, variant):
    root = Path(root)
    country = root / 'Malawi'
    own = country / '_'
    own.mkdir(mode=0o700, parents=True)
    registered = WAVES.copy()
    if variant == 'retired_wave':
        registered.remove('2013-14')
    if variant == 'extra_wave':
        registered.append('2022-23')
    mapping = {'2016-17': 'native-2016'} if variant == 'mapped' else {}
    schema = {'Waves': registered, 'wave_folder_map': mapping,
              'Data Scheme': {} if variant == 'country_retired' else
              {table: {'materialize': 'make'}}}
    # JSON is a subset of YAML; actual Country/Wave YAML loaders read these.
    (own / 'data_scheme.yml').write_text(json.dumps(schema) + '\n')
    for wave in WAVES + (['2022-23'] if variant == 'extra_wave' else []):
        wd = country / mapping.get(wave, wave) / '_'
        if variant == 'missing_wave_dir' and wave == '2013-14':
            continue
        wd.mkdir(mode=0o700, parents=True)
        active = not (variant == 'none_declared' or
                      variant == 'unwired' and wave == '2013-14' or
                      variant == 'current_three_wave' and wave == '2016-17')
        native = active and variant != 'yaml_only'
        yaml = active and variant != 'native_only' and not (
            variant == 'yaml_removed_native_retained' and wave == '2013-14')
        (wd / 'data_info.yml').write_text(json.dumps({table: None} if yaml else {}) + '\n')
        if native:
            (wd / f'{table}.py').write_text('# Invented native declaration marker; never execute.\n')
    return root
