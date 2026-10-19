"""P3 residual evidence: column ENUMERATIONS over a mapping table (comprehension
or for-loop over <tainted>.columns) -- the sites a Status/Notes column would leak into."""
import ast, sys
sys.path.insert(0, 'tests')
import test_categorical_key_resolver as T
hits = []
for path, rel in T._package_sources():
    tree = ast.parse(path.read_text(encoding='utf-8'))
    for scope, name in T._scopes(tree):
        tainted = T._mapping_tainted(scope)
        if not tainted:
            continue
        for n in T._walk_scope(scope):
            it = None
            if isinstance(n, (ast.ListComp, ast.GeneratorExp, ast.SetComp, ast.DictComp)):
                it = n.generators[0].iter
            elif isinstance(n, ast.For):
                it = n.iter
            if it is None:
                continue
            for m in ast.walk(it):
                if isinstance(m, ast.Attribute) and m.attr == 'columns' and isinstance(m.value, ast.Name) and m.value.id in tainted:
                    hits.append((rel, n.lineno, name))
                    break
for h in sorted(set(hits)):
    print(h)
