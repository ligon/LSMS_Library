"""Insert a no-op statement as the first body statement (after any docstring) of
named functions in a file, in a PRIVATE clone only.

usage: noop_patch.py <file> <func> [<func> ...]
"""
import ast
import sys

path, funcs = sys.argv[1], set(sys.argv[2:])
src = open(path, encoding='utf-8').read()
tree = ast.parse(src)
lines = src.splitlines(keepends=True)
inserts = []
for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in funcs:
        body = node.body
        first = body[0]
        if (isinstance(first, ast.Expr) and isinstance(getattr(first, 'value', None), ast.Constant)
                and isinstance(first.value.value, str) and len(body) > 1):
            first = body[1]
        indent = ' ' * first.col_offset
        inserts.append((first.lineno - 1, f'{indent}_noop_probe_1560 = None\n'))
        funcs.discard(node.name)
if funcs:
    sys.exit(f'not found: {sorted(funcs)}')
for lineno, text in sorted(inserts, reverse=True):
    lines.insert(lineno, text)
open(path, 'w', encoding='utf-8').write(''.join(lines))
ast.parse(''.join(lines))
print('patched', path, len(inserts))
