"""The GH #1500 test's pinned source digests, recomputed on a clone.  usage: digests.py <clone> <out.json>"""
import ast, hashlib, json, sys
from pathlib import Path
clone, out = Path(sys.argv[1]), Path(sys.argv[2])
C = clone / 'lsms_library' / 'countries' / 'Senegal'
R = {rel: hashlib.sha256((C / rel).read_bytes()).hexdigest() for rel in (
    '2018-19/_/data_info.yml', '2021-22/_/data_info.yml', '2021-22/_/mapping.py',
    '_/data_scheme.yml', '_/categorical_mapping.org')}
tree = ast.parse((C / '2018-19' / '_' / 'mapping.py').read_text(encoding='utf-8'))
body = [n for n in tree.body
        if not (isinstance(n, ast.FunctionDef) and n.name == 'food_acquired')
        and not (isinstance(n, ast.ImportFrom) and n.module == 'lsms_library.transformations'
                 and [(a.name, a.asname) for a in n.names] == [('U_UNKNOWN', None)])]
R['ast'] = hashlib.sha256(ast.dump(ast.Module(body=body, type_ignores=[]), include_attributes=False).encode()).hexdigest()
R['python'] = sys.version
out.write_text(json.dumps(R, indent=1)); print(json.dumps(R, indent=1))
