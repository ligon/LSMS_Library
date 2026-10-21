"""Read PyPI 0.16.0 artifacts, compare exact tagged payload, offline install/config smoke.

Run only after the parent has tagged and published. Never builds, installs
dependencies, changes source/Git, or calls a Country/Feature data method.
"""
import argparse
import configparser
import email
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import urllib.parse
import urllib.request
import zipfile


def sha(data):
    return hashlib.sha256(data).hexdigest()


def fetch(url):
    parsed = urllib.parse.urlparse(url)
    assert parsed.scheme == 'https' and parsed.hostname in {'pypi.org', 'files.pythonhosted.org'}
    with urllib.request.urlopen(url, timeout=60) as response:
        final = urllib.parse.urlparse(response.url)
        assert final.scheme == 'https' and final.hostname in {'pypi.org', 'files.pythonhosted.org'}
        return response.read()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--expected-sha', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    repo, out = args.repo.resolve(), args.out.resolve()
    assert len(args.expected_sha) == 40
    assert out.is_relative_to(Path('/local/job40255479/control/release-v016'))
    assert not out.exists(), 'Use a new private output/installation directory'
    assert Path(sys.executable).resolve() == Path('/local/job40255479/venv_img/bin/python').resolve()
    def git(*argv):
        return subprocess.check_output(['git', '-C', str(repo), *argv])
    assert git('rev-parse', 'HEAD').decode().strip() == args.expected_sha
    assert git('rev-parse', 'v0.16.0^{commit}').decode().strip() == args.expected_sha
    assert not git('diff', 'HEAD', '--name-only').strip()
    out.mkdir(mode=0o700, parents=True)
    (out / 'dist').mkdir()
    payload = fetch('https://pypi.org/pypi/LSMS_Library/0.16.0/json')
    (out / 'pypi-response.json').write_bytes(payload)
    release = json.loads(payload)
    assert release['info']['version'] == '0.16.0'
    wheels = [r for r in release['urls'] if r['packagetype'] == 'bdist_wheel']
    sdists = [r for r in release['urls'] if r['packagetype'] == 'sdist']
    assert len(wheels) == len(sdists) == 1
    files = {}
    downloads = []
    for record in wheels + sdists:
        name = record['filename']
        assert Path(name).name == name and not record['yanked']
        content = fetch(record['url'])
        assert len(content) == record['size']
        assert sha(content) == record['digests']['sha256']
        path = out / 'dist' / name
        path.write_bytes(content)
        files[record['packagetype']] = path
        downloads.append({'filename': name, 'bytes': len(content), 'sha256': sha(content),
                          'matches_pypi_digest': True, 'url': record['url']})
    tracked = git('ls-tree', '-r', '--name-only', 'v0.16.0', '--', 'lsms_library').decode().splitlines()
    assert len(tracked) == 11753, 'Package-file count moved: review instead of silently widening'
    tree = git('ls-tree', '-r', 'v0.16.0', '--', 'lsms_library').decode().splitlines()
    assert not any(line.startswith('120000 ') for line in tree), 'Review new package symlinks explicitly'
    # Read final-tag bytes from Git, independent of filesystem substitutions.
    with zipfile.ZipFile(files['bdist_wheel']) as wheel, tarfile.open(files['sdist']) as sdist:
        names = set(wheel.namelist())
        tops = {m.name.split('/')[0] for m in sdist.getmembers()}
        assert len(tops) == 1
        top = tops.pop()
        snames = {m.name[len(top) + 1:] for m in sdist.getmembers() if m.isfile()}
        assert set(tracked) <= names and set(tracked) <= snames
        assert {name for name in names if name.startswith('lsms_library/') and not name.endswith('/')} == set(tracked)
        assert {name for name in snames if name.startswith('lsms_library/')} == set(tracked)
        for name in tracked:
            source = git('show', f'v0.16.0:{name}')
            assert wheel.read(name) == source, name
            assert sdist.extractfile(top + '/' + name).read() == source, name
        wm = email.message_from_bytes(wheel.read(next(n for n in names if n.endswith('.dist-info/METADATA'))))
        sm = email.message_from_bytes(sdist.extractfile(top + '/PKG-INFO').read())
        assert wm['Version'] == sm['Version'] == '0.16.0'
        assert wm['Name'].lower().replace('-', '_') == sm['Name'].lower().replace('-', '_') == 'lsms_library'
        ep = configparser.ConfigParser()
        ep.read_string(wheel.read(next(n for n in names if n.endswith('.dist-info/entry_points.txt'))).decode())
        assert ep['console_scripts']['lsms-library'] == 'lsms_library.cli:main'
        forbidden = {'.dta', '.sav', '.parquet', '.pkl'}
        assert not any(Path(n).suffix.lower() in forbidden for n in names | snames)
    target = out / 'installed-target'
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONNOUSERSITE='1',
               PIP_NO_INDEX='1', PIP_DISABLE_PIP_VERSION_CHECK='1', PIP_CACHE_DIR=str(out / 'pip-cache'))
    env.pop('PYTHONPATH', None)
    with (out / 'install-private.log').open('w') as log:
        subprocess.run([sys.executable, '-m', 'pip', 'install', '--no-index', '--no-deps',
                        '--no-compile', '--target', str(target), str(files['bdist_wheel'])],
                       env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    (out / 'installed-cwd').mkdir()
    (out / 'installed-data').mkdir()
    env.update(PYTHONPATH=str(target), PACKAGING_TARGET=str(target), ARTIFACT_OUT=str(out),
               LSMS_DATA_DIR=str(out / 'installed-data'), LSMS_SKIP_AUTH='1',
               XDG_CACHE_HOME=str(out / 'xdg-cache'), OPENBLAS_NUM_THREADS='1',
               OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', LSMS_BUILD_WORKERS='1', LSMS_MAKE_JOBS='1')
    env.pop('LSMS_COUNTRIES_ROOT', None)
    env.pop('LSMS_EXPECTED_ROOT', None)
    with (out / 'smoke-private.log').open('w') as log:
        subprocess.run([sys.executable, str(Path(__file__).with_name('installed_smoke.py'))],
                       cwd=out / 'installed-cwd', env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    assert git('rev-parse', 'HEAD').decode().strip() == args.expected_sha
    assert not git('diff', 'HEAD', '--name-only').strip()
    result = {'source_sha': args.expected_sha, 'tag': 'v0.16.0', 'version': '0.16.0',
              'pypi_json_sha256': sha(payload), 'downloads': downloads,
              'tracked_package_files': len(tracked), 'missing_or_changed_package_files': 0,
              'metadata_versions_exact': True, 'console_entrypoint_exact': True,
              'source_unchanged': True, 'offline_no_deps_install': True,
              'installed_smoke': json.loads((out / 'installed-smoke.json').read_text()),
              'limits': ['Existing image dependencies reused; no fresh dependency-resolution claim',
                         'Configuration smoke only; no installed microdata build',
                         'Archives compared independently; wheel not rebuilt from downloaded sdist']}
    (out / 'verification.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
