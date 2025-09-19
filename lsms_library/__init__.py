from . import country
from . country import Country, Wave
from . import local_tools as tools
from .categorical_mapping import ai_agent
from . import transformations
from .dvc_permissions import authenticate

from pathlib import Path
gpg_path = Path(__file__).resolve().parent / 'countries' / '.dvc'
creds_file = gpg_path / 's3_creds'
if not creds_file.exists():
    authenticate()
else:
    print("Already authenticated for s3 access.")

