"""Installed 0.16.0 configuration discovery only, outside all checkouts."""
import importlib.metadata as metadata
import json
import os
from pathlib import Path
import socket
import sys
import urllib.request

target = Path(os.environ['PACKAGING_TARGET']).resolve()
private = Path(os.environ['LSMS_DATA_DIR']).resolve()
out = Path(os.environ['ARTIFACT_OUT']).resolve()
assert Path.cwd().resolve() == out / 'installed-cwd'
assert not any((p / '.git').exists() for p in [Path.cwd(), *Path.cwd().parents])
assert target == out / 'installed-target' and private == out / 'installed-data'
assert 'LSMS_COUNTRIES_ROOT' not in os.environ
assert os.environ['PYTHONPATH'] == str(target)


def forbidden(*args, **kwargs):
    raise AssertionError('Installed configuration smoke forbids network and survey data builds')


socket.create_connection = forbidden
socket.socket.connect = forbidden
socket.socket.connect_ex = forbidden
socket.getaddrinfo = forbidden
urllib.request.urlopen = forbidden


def audit(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo'}:
        forbidden()


sys.addaudithook(audit)
import lsms_library as ll
from lsms_library.paths import countries_root, data_root
from lsms_library import data_access
assert Path(ll.__file__).resolve() == target / 'lsms_library/__init__.py'
assert countries_root().resolve() == target / 'lsms_library/countries'
assert data_root().resolve() == private
assert ll.__version__ == metadata.version('LSMS_Library') == '0.16.0'
data_access.get_data_file = forbidden
ll.Country._aggregate_wave_data = forbidden
ll.Country.grab_data = forbidden
ll.Feature.__call__ = forbidden
uga = ll.Country('Uganda', preload_panel_ids=False)
assert len(uga.waves) == 8
assert {'sample', 'food_acquired', 'nutrition', 'shocks'}.issubset(uga.data_scheme)
feature = ll.Feature('household_roster')
assert feature.table_name == 'household_roster' and 'Uganda' in feature.countries
assert set(feature.countries).issubset(ll.countries())
assert isinstance(feature.columns, list)
assert not list(private.rglob('*.parquet'))
result = {'package_file': str(Path(ll.__file__).resolve()), 'version': ll.__version__,
          'countries_root': str(countries_root()), 'data_root': str(data_root()),
          'registered_countries': len(ll.countries()), 'uganda_waves': uga.waves,
          'uganda_declared_tables': len(uga.data_scheme),
          'roster_declaring_countries': len(feature.countries),
          'network_forbidden': True, 'data_builds_forbidden': True,
          'no_parquet_produced': True,
          'scope': 'Installed configuration smoke only; existing image dependencies; no dependency-resolution or installed microdata claim'}
(out / 'installed-smoke.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
