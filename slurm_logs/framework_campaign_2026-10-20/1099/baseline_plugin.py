"""Run current regression tests against the pre-fix aggregation method only.

No worktree file is replaced: the baseline method is compiled from git in the
private pytest process; the installed module and all IO helpers are unchanged.
"""
import ast,subprocess

def pytest_configure(config):
    import lsms_library.country as C
    source=subprocess.check_output(['git','show','976fc840d:lsms_library/country.py'],text=True)
    tree=ast.parse(source)
    country=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Country')
    method=next(n for n in country.body if isinstance(n,ast.FunctionDef) and n.name=='_aggregate_wave_data')
    method.decorator_list=[]
    compiled=ast.fix_missing_locations(ast.Module(body=[method],type_ignores=[]))
    namespace=dict(C.__dict__)
    exec(compile(compiled,'<git-976fc840d-country.py>','exec'),namespace)
    original=namespace['_aggregate_wave_data'];original.__qualname__='Country._aggregate_wave_data'
    C.Country._aggregate_wave_data=original
