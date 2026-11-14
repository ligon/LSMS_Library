"""Red team: the PARTS (qualname=source) behind framework_imports_fingerprint, per country,
and behind build_transforms_fingerprint(None), for one checkout.

usage: python fwimp_parts.py <clone> <out.json>

Replicates _build_registry.framework_imports_fingerprint's target discovery and folds
_closure_parts the same way, but returns the part NAMES with a short digest of each
part, so two checkouts can be diffed by name.
"""
import ast
import hashlib
import importlib
import json
import os
import sys

clone, out = sys.argv[1], sys.argv[2]
import lsms_library  # noqa: E402
from lsms_library import paths  # noqa: E402
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(clone) + os.sep)
dr = os.path.realpath(str(paths.data_root()))
assert dr.startswith('/local/job') or '/isa-sm-20261113/private/' in dr, dr
from lsms_library import _build_registry as B  # noqa: E402


def parts_of(paths_):
    seen, parts = set(), []
    targets_all = []
    for path in paths_:
        try:
            tree = ast.parse(open(path, encoding='utf-8').read())
        except Exception:  # noqa: BLE001
            continue
        targets, aliases = [], {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or '').startswith('lsms_library'):
                for a in node.names:
                    targets.append((node.module, a.name))
            elif isinstance(node, ast.Import):
                for a in node.names:
                    if a.name.startswith('lsms_library'):
                        aliases[a.asname or a.name] = a.name
        if aliases:
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) \
                        and node.value.id in aliases:
                    targets.append((aliases[node.value.id], node.attr))
        for modname, attr in sorted(set(targets)):
            try:
                obj = getattr(importlib.import_module(modname), attr, None)
            except Exception:  # noqa: BLE001
                obj = None
            build = B._is_build_callable(obj)
            targets_all.append([os.path.basename(path), modname, attr, bool(build)])
            if build:
                parts += B._closure_parts(obj, seen)
    named = {}
    for p in set(parts):
        k, _, v = p.partition('=')
        named[k] = hashlib.sha256(v.encode()).hexdigest()[:16]
    digest = hashlib.sha256('\x1f'.join(sorted(set(parts))).encode()).hexdigest() if parts else ''
    return named, digest, targets_all


root = paths.countries_root()
res = {'fwimp': {}, 'targets': {}}
for ds in sorted(root.glob('*/_/data_scheme.yml')):
    name = ds.parent.parent.name
    pys = tuple(sorted(str(p) for p in ds.parent.glob('*.py')))
    named, digest, targets = parts_of(pys)
    assert digest == B.framework_imports_fingerprint(pys), name  # replication is faithful
    res['fwimp'][name] = named
    res['targets'][name] = targets

# build_transforms_fingerprint(None): every tagged transform's closure
seen, parts = set(), []
for _qn, (fn, tables) in sorted(B._BUILD_TRANSFORMS.items()):
    parts += B._closure_parts(fn, seen)
res['btf_all'] = sorted({p.partition('=')[0] for p in parts})
res['excluded'] = sorted(str(x) for x in getattr(B, '_EXCLUDED_CALLABLES', ()))
json.dump(res, open(out, 'w'), indent=1, sort_keys=True)
print('FWIMP PARTS DONE', len(res['fwimp']), flush=True)
