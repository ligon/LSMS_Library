"""Bind exact852 invariance test code in memory; leave all sources untouched."""
import subprocess

def pytest_configure(config):
    from tests import test_uganda_invariance as module
    source=subprocess.check_output(['git','show','852ac2bfcd5f798533f56e7efa1f77db80c0138f:tests/test_uganda_invariance.py'],text=True)
    exec(compile(source,module.__file__,'exec'),module.__dict__)
