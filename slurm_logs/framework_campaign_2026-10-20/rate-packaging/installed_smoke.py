"""Installed-wheel configuration smoke: no Country data method or network."""
import importlib.metadata as metadata
import json
import os
from pathlib import Path
import socket
import sys
import urllib.request

package_root=Path(os.environ['PACKAGING_TARGET']).resolve()
private=Path(os.environ['LSMS_DATA_DIR']).resolve()
assert Path.cwd().resolve()==Path('/local/job40255479/control/framework-campaign/rate-packaging/installed-cwd')
assert package_root not in Path.cwd().parents
assert 'LSMS_COUNTRIES_ROOT' not in os.environ

def forbidden(*args,**kwargs):
    raise AssertionError('Installed packaging smoke forbids network and data builds')
socket.create_connection=forbidden
socket.socket.connect=forbidden
urllib.request.urlopen=forbidden
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from lsms_library import data_access
assert Path(ll.__file__).resolve()==package_root/'lsms_library/__init__.py'
assert countries_root().resolve()==package_root/'lsms_library/countries'
assert data_root().resolve()==private
assert str(private).startswith('/local/job40255479/control/framework-campaign/rate-packaging/')
assert ll.__version__==metadata.version('LSMS_Library')
assert ll.__version__!='0.0.0'
data_access.get_data_file=forbidden
ll.Country._aggregate_wave_data=forbidden
uga=ll.Country('Uganda',preload_panel_ids=False)
assert len(uga.waves)==8
assert {'sample','food_acquired','nutrition','shocks'}.issubset(uga.data_scheme)
feature=ll.Feature('household_roster')
assert feature.table_name=='household_roster'
assert 'Uganda' in feature.countries
assert set(feature.countries).issubset(ll.countries())
assert isinstance(feature.columns,list)
assert not list(private.rglob('*.parquet'))
result={'package_file':str(Path(ll.__file__).resolve()),'version':ll.__version__,
        'countries_root':str(countries_root()),'data_root':str(data_root()),
        'registered_countries':len(ll.countries()),'uganda_waves':uga.waves,
        'uganda_declared_table_count':len(uga.data_scheme),
        'roster_declaring_countries':len(feature.countries),'roster_required_columns':feature.columns,
        'network_forbidden':True,'data_builds_forbidden':True,'no_parquet_produced':True,
        'scope':'Installed wheel config discovery only; no installed data build'}
Path(sys.argv[1]).write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
print(json.dumps(result,sort_keys=True))
