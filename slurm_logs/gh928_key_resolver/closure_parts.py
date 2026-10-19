"""Dump every build_transform closure part as {name: sha} so two trees can be
diffed to attribute a fingerprint move to the exact callables/constants."""
import hashlib, json, sys, warnings
warnings.filterwarnings('ignore')
import lsms_library.country  # noqa: F401  registers the tagged transforms
from lsms_library._build_registry import _BUILD_TRANSFORMS, _closure_parts
seen, parts = set(), []
for _q, (fn, _t) in sorted(_BUILD_TRANSFORMS.items()):
    parts += _closure_parts(fn, seen)
out = {}
for p in set(parts):
    name, _, body = p.partition('=')
    out[name] = hashlib.sha256(body.encode()).hexdigest()[:16]
json.dump(out, open(sys.argv[1], 'w'), indent=0, sort_keys=True)
print(len(out), 'closure parts ->', sys.argv[1])
