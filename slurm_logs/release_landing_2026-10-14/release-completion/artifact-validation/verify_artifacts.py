"""Compare frozen-head artifacts and installed files; no source mutation."""
from hashlib import sha256
from pathlib import Path
from zipfile import ZipFile
import json

OUT = Path(__file__).resolve().parent
INSTALLED = Path('/local/job39919092/release-work/published-install-env/lib/python3.11/site-packages')
original = next((OUT / 'dist').glob('*.whl'))
rebuilt = next((OUT / 'sdist-rebuild-dist').glob('*.whl'))
with ZipFile(original) as a, ZipFile(rebuilt) as b:
    an, bn = set(a.namelist()), set(b.namelist())
    result = {
        'original_wheel': str(original), 'rebuilt_wheel': str(rebuilt),
        'rebuilt_sha256': sha256(rebuilt.read_bytes()).hexdigest(),
        'only_original': sorted(an - bn), 'only_rebuilt': sorted(bn - an),
        'different_members': [p for p in sorted(an & bn) if a.read(p) != b.read(p)],
        'installed_source_content_differences': [
            p for p in sorted(an) if p.startswith('lsms_library/')
            and not p.endswith('/') and (INSTALLED / p).read_bytes() != a.read(p)
        ],
        'installed_extra_files_excluding_bytecode': sorted(
            str(p.relative_to(INSTALLED))
            for p in (INSTALLED / 'lsms_library').rglob('*')
            if p.is_file() and '__pycache__' not in p.parts
            and str(p.relative_to(INSTALLED)) not in an
        ),
    }
for field in ('only_original', 'only_rebuilt', 'different_members',
              'installed_source_content_differences',
              'installed_extra_files_excluding_bytecode'):
    assert not result[field], (field, result[field])
assert original.read_bytes() == rebuilt.read_bytes()
(OUT / 'artifact-comparison.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
