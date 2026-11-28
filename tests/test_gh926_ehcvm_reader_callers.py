"""Run the actual curated producer lookups without survey inputs."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX C text locale required')


def _module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run_routes():
    import pandas as pd

    import lsms_library
    from lsms_library.paths import countries_root, data_root

    assert Path(lsms_library.__file__).resolve() == REPO / 'lsms_library/__init__.py'
    assert data_root().resolve() == Path(os.environ['LSMS_DATA_DIR']).resolve()
    assert not any(p.is_file() for p in data_root().rglob('*'))
    with open(__file__) as f:
        assert f.encoding.lower() == (
            'utf-8' if sys.flags.utf8_mode else 'ansi_x3.4-1968')

    root = countries_root()
    mali14dir = root / 'Mali/2014-15/_'
    mali18dir = root / 'Mali/2018-19/_'
    bfa18dir = root / 'Burkina_Faso/2018-19/_'
    niger18dir = root / 'Niger/2018-19/_'
    m14 = _module(mali14dir / 'mapping.py', 'codec_mali14')
    m18 = _module(mali18dir / 'mapping.py', 'codec_mali18')
    mali = _module(root / 'Mali/_/mali.py', 'codec_mali')
    bfa = _module(bfa18dir / 'mapping.py', 'codec_bfa18')
    niger = _module(root / 'Niger/_/niger.py', 'codec_niger')

    os.chdir(mali14dir)
    units = m14._unit_canonicalizer()
    assert units['Bo\u00eete'] == 'Boite'
    assert units['Kilogramme'] == 'Kg'
    crops = mali._crop_labels(pd.Series(['Ma\u00efs en \u00e9pi', 'Riz', 'not-in-table']))
    country_units = mali._unit_labels(pd.Series(['Bo\u00eete', 'Kilogramme', 'not-in-table']))
    assert crops.iloc[0] == 'Ma\u00efs' and pd.isna(crops.iloc[2])
    assert list(country_units.iloc[:2]) == ['Boite', 'Kg'] and pd.isna(country_units.iloc[2])

    codes = pd.DataFrame({
        'grappe': [12, 12, 13], 'vague': [1, 1, 2], 'Existe': [1, 1, 1],
        's05q01': [5, 5, 6], 's05q02': [1071, 1071, 1433],
        's05q03': [100.125, 103, 0], 's05q04': [200, 202, -1],
    })
    labels = codes.assign(
        s05q01=['Ma\u00efs en \u00e9pi', 'Ma\u00efs en \u00e9pi', 'Ma\u00efs en grain'],
        s05q02=['Boite - Petit', 'Boite - Petit', 'Tas - Grand'])
    os.chdir(mali18dir)
    mprice = m18.community_prices_s05(codes, labels)
    assert mprice.index.is_unique and len(mprice) == 6
    assert mprice.Quantity.isna().all()
    assert sorted(mprice.Price.tolist()) == [-1, 0, 100.125, 103, 200, 202]

    community = {'s05q01': {5: 'Invented five', 6: 'Invented six'},
                 's05q02': {1071: 'Boite - petit', 1433: 'Tas - grand'}}
    hh = {'s07bq01': {5: 'Viande de b\u00c5\u0093uf', 6: 'Ma\u00efs en grain'},
          's07bq03b': {107: 'Boite', 143: 'Tas'}, 's07bq03c': {1: 'Petit', 3: 'Grand'}}
    os.chdir(bfa18dir)
    bprice = bfa.community_prices_s05(codes, community, hh)
    assert bprice.index.is_unique and len(bprice) == 6
    assert sorted(bprice.Price.tolist()) == [-1, 0, 100.125, 103, 200, 202]
    assert set(bprice.index.get_level_values('variety')) == {
        's05q01=5;s05q02=1071', 's05q01=6;s05q02=1433'}

    os.chdir(niger18dir)
    maps = niger._community_prices_maps()
    assert niger._community_prices_maps([niger18dir, root / 'Niger/_']) == maps
    assert maps[0]['Arachide grill\u00c3\u00a9e'] == 'Arachide grill\u00e9e'
    assert maps[1]
    assert not any(p.is_file() for p in data_root().rglob('*'))

    def frame(df):
        return [df.reset_index().to_json(orient='table'), df.attrs,
                list(df.index.names), [str(d) for d in df.dtypes]]

    return {
        'Mali food units': list(units.items()),
        'Mali crop labels': crops.to_json(), 'Mali unit labels': country_units.to_json(),
        'Mali s05': frame(mprice), 'Burkina s05': frame(bprice),
        'Niger maps': [list(m.items()) for m in maps],
    }


def _run_unshipped_mali_file():
    import lsms_library
    from lsms_library.paths import countries_root

    assert Path(lsms_library.__file__).resolve() == REPO / 'lsms_library/__init__.py'
    mali = countries_root() / 'Mali'
    custom = Path(os.environ['LSMS_DATA_DIR']).parent / 'custom-mali'
    c14 = custom / '2014-15/_'
    c14.mkdir(parents=True)
    target = custom / '_/categorical_mapping.org'
    target.parent.mkdir()
    shutil.copyfile(mali / '_/mali.py', target.parent / 'mali.py')
    target.write_bytes((mali / '_/categorical_mapping.org').read_bytes()
                       + b'\n# custom override\n')
    source = c14 / 'mapping.py'
    shutil.copyfile(mali / '2014-15/_/mapping.py', source)
    return _module(source, 'unshipped_mali_food')._unit_canonicalizer()


def _call_in_c(tmp_path, utf8, route='stock'):
    root = tmp_path / ('utf8' if utf8 else 'ascii')
    env = dict(PATH=os.defpath, LC_ALL='C', PYTHONCOERCECLOCALE='0',
               PYTHONUTF8='1' if utf8 else '0', LSMS_SKIP_AUTH='1',
               PYTHONPATH=str(REPO), PYTHONDONTWRITEBYTECODE='1',
               AWS_EC2_METADATA_DISABLED='true',
               OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', LSMS_BUILD_WORKERS='1')
    for key in ('HOME', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'LSMS_CONFIG_DIR', 'LSMS_DATA_DIR'):
        folder = root / key.lower()
        folder.mkdir(parents=True, exist_ok=True)
        env[key] = str(folder)
    child = subprocess.run([sys.executable, str(Path(__file__).resolve()), route],
                           cwd=REPO, env=env, capture_output=True, timeout=35)
    assert child.returncode == 0, child.stderr.decode('utf-8', errors='replace')
    return json.loads(child.stdout.decode('ascii').splitlines()[-1])


def test_ehcvm_proved_callers_match_stock_utf8_in_ascii(tmp_path):
    assert _call_in_c(tmp_path, False) == _call_in_c(tmp_path, True)


def test_mali_direct_file_does_not_pin_a_changed_copy(tmp_path):
    assert _call_in_c(tmp_path, False, 'custom') == {}
    assert _call_in_c(tmp_path, True, 'custom')['Bo\u00eete'] == 'Boite'


if __name__ == '__main__':
    print(json.dumps(_run_routes() if sys.argv[1] == 'stock' else _run_unshipped_mali_file(),
                     ensure_ascii=True))
