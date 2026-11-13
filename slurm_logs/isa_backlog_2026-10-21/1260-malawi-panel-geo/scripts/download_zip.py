"""Download one WB Stata zip into a dest dir using the library's API key (never printed)."""
import sys, shutil, urllib.request, hashlib
from pathlib import Path
from lsms_library import config, data_access

idno, dest_dir = sys.argv[1], Path(sys.argv[2])
base = 'https://microdata.worldbank.org/index.php'
key = config.microdata_api_key()
url, fname = data_access._find_stata_zip_url(base, idno, key)
dest = dest_dir / fname
req = urllib.request.Request(url)
req.add_header("X-API-KEY", key)
with urllib.request.urlopen(req, timeout=900) as resp, open(dest, 'wb') as out:
    shutil.copyfileobj(resp, out, 1 << 20)
h = hashlib.md5(dest.read_bytes()).hexdigest()
print(dest, dest.stat().st_size, 'md5', h)
