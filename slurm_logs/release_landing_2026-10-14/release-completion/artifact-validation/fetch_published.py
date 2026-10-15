"""Download published artifacts and verify version, digests and tagged source."""
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile
import argparse
import email
import hashlib
import json
import subprocess
import tarfile

parser = argparse.ArgumentParser()
parser.add_argument('--expected-sha', required=True)
args = parser.parse_args()
out = Path(__file__).resolve().parent
repo = Path('/local/job39919092/mirrors/LSMS_Library')
actual = subprocess.check_output(['git', 'rev-parse', 'v0.15.0^{commit}'], cwd=repo, text=True).strip()
assert actual == args.expected_sha
with urlopen('https://pypi.org/pypi/LSMS_Library/0.15.0/json', timeout=60) as response:
    metadata = json.load(response)
assert metadata['info']['version'] == '0.15.0'
dist = out / 'dist'
dist.mkdir(exist_ok=False)
records = []
for item in metadata['urls']:
    assert item['packagetype'] in {'bdist_wheel', 'sdist'}
    assert item['url'].startswith('https://files.pythonhosted.org/')
    assert not item['yanked']
    target = dist / item['filename']
    assert target.name == item['filename'] and target.parent == dist
    with urlopen(item['url'], timeout=60) as response:
        payload = response.read()
    digest = hashlib.sha256(payload).hexdigest()
    assert digest == item['digests']['sha256'] and len(payload) == item['size']
    target.write_bytes(payload)
    records.append({'name': target.name, 'bytes': len(payload), 'sha256': digest,
                    'url': item['url'], 'packagetype': item['packagetype']})
assert sorted(r['packagetype'] for r in records) == ['bdist_wheel', 'sdist']
wheel = next(dist.glob('*.whl'))
source = next(dist.glob('*.tar.gz'))
checked = [
    'lsms_library/__init__.py', 'lsms_library/country.py',
    'lsms_library/local_tools.py', 'lsms_library/transformations.py',
    'lsms_library/catalog_relations.yml', 'lsms_library/data_info.yml',
    'lsms_library/countries/Uganda/_/assets.py',
    'lsms_library/countries/Uganda/_/kgs_per_other_units.json',
    'lsms_library/countries/Mali/_/CONTENTS.org',
    'lsms_library/countries/Tanzania/_/CONTENTS.org',
    'lsms_library/countries/Liberia/_/CONTENTS.org',
]
with ZipFile(wheel) as archive:
    names = archive.namelist()
    info = email.message_from_bytes(archive.read(next(n for n in names if n.endswith('.dist-info/METADATA'))))
    assert info['Version'] == '0.15.0'
    assert not any(n.endswith(('.parquet', '/s3_creds', '/s3_write_creds', 'config.local')) for n in names)
    for name in checked:
        expected = subprocess.check_output(['git', 'show', args.expected_sha + ':' + name], cwd=repo)
        assert archive.read(name) == expected, name
    tree = subprocess.check_output(['git', 'ls-tree', '-rz', args.expected_sha, '--', 'lsms_library'], cwd=repo)
    tracked = {}
    for entry in tree.split(b'\0'):
        if not entry:
            continue
        metadata, path = entry.split(b'\t', 1)
        mode, kind, oid = metadata.decode().split()
        assert kind == 'blob' and mode in {'100644', '100755'}, (mode, kind, path)
        tracked[path.decode()] = oid
    packaged = {n for n in names if n.startswith('lsms_library/') and not n.endswith('/')}
    assert packaged == set(tracked), {'missing': sorted(set(tracked)-packaged), 'extra': sorted(packaged-set(tracked))}
    for name, oid in tracked.items():
        payload = archive.read(name)
        assert hashlib.sha1(b'blob ' + str(len(payload)).encode() + b'\0' + payload).hexdigest() == oid, name
    (out / 'wheel-METADATA.txt').write_bytes(info.as_bytes())
unpacked = out / 'sdist-unpacked'
unpacked.mkdir(exist_ok=False)
with tarfile.open(source) as archive:
    members = archive.getmembers()
    assert not any(m.name.endswith(('.parquet', '/s3_creds', '/s3_write_creds', 'config.local')) for m in members)
    info_member = next(m for m in members if m.name.endswith('/PKG-INFO') and m.name.count('/') == 1)
    info = email.message_from_bytes(archive.extractfile(info_member).read())
    assert info['Version'] == '0.15.0'
    archive.extractall(unpacked, filter='data')
result = {'version': '0.15.0', 'tag_commit': actual, 'artifacts': records,
          'selected_tagged_source_members_equal': checked,
          'all_package_members_match_tagged_git_blobs': True,
          'package_member_count': len(tracked), 'missing_or_extra_package_members': [], 'no_credential_or_parquet_filenames_in_wheel_and_sdist': True}
(out / 'published-artifacts.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
