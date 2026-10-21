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
    metadata_fields = ('date_time', 'compress_type', 'comment', 'extra',
                       'create_system', 'create_version', 'extract_version',
                       'reserved', 'flag_bits', 'volume', 'internal_attr',
                       'external_attr', 'CRC', 'file_size')
    result = {
        'original_wheel': str(original), 'rebuilt_wheel': str(rebuilt),
        'original_sha256': sha256(original.read_bytes()).hexdigest(),
        'rebuilt_sha256': sha256(rebuilt.read_bytes()).hexdigest(),
        'original_bytes': original.stat().st_size,
        'rebuilt_bytes': rebuilt.stat().st_size,
        'archive_bytes_equal': original.read_bytes() == rebuilt.read_bytes(),
        'member_count': len(an),
        'member_order_equal': a.namelist() == b.namelist(),
        'archive_comments_equal': a.comment == b.comment,
        'compared_metadata_fields': metadata_fields,
        'different_member_metadata': [
            {'member': p, 'field': field}
            for p in sorted(an & bn) for field in metadata_fields
            if getattr(a.getinfo(p), field) != getattr(b.getinfo(p), field)
        ],
        'compressed_size_differences': sum(
            a.getinfo(p).compress_size != b.getinfo(p).compress_size
            for p in an & bn
        ),
        'original_zip_overhead_bytes': original.stat().st_size - sum(i.compress_size for i in a.infolist()),
        'rebuilt_zip_overhead_bytes': rebuilt.stat().st_size - sum(i.compress_size for i in b.infolist()),
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
              'different_member_metadata',
              'installed_source_content_differences',
              'installed_extra_files_excluding_bytecode'):
    assert not result[field], (field, result[field])
assert result['member_order_equal'] and result['archive_comments_equal']
assert result['original_zip_overhead_bytes'] == result['rebuilt_zip_overhead_bytes']
result['interpretation'] = ('All member contents, inventory, ordering and compared ZIP metadata match exactly. '
                            'The compressed archive bytes differ; independent ZIP review attributes the '
                            'entire size difference to DEFLATE payload encoding. No archive-byte equality is claimed.')
(OUT / 'artifact-comparison.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
