"""Run the r1 guard over historical trees: the branch head before F1, and the base."""
import subprocess, sys
sys.path.insert(0, 'tests')
import test_categorical_key_resolver as T
def show(rev, path):
    return subprocess.check_output(['git', 'show', f'{rev}:{path}'], text=True)
def files(rev):
    out = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', rev, 'lsms_library/'], text=True).splitlines()
    return [f for f in out if f.endswith('.py')]
for rev in sys.argv[1:]:
    hits = []
    fs = files(rev)
    for f in fs:
        try:
            hits += T._positional_key_picks(show(rev, f), f.replace('lsms_library/', '', 1))
        except SyntaxError as e:
            print('SYNTAX', f, e)
    print(f'{rev}: {len(fs)} files, {len(hits)} hit(s)')
    for h in hits:
        print('   ', h)
