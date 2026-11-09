"""The actual registration node is safe on platforms without os.fork support."""
import ast
import os
from pathlib import Path

import pytest
import lsms_library.local_tools as LT


def _production_registration_node():
    source = Path(LT.__file__).read_text()
    module = ast.parse(source)
    matches = [node for node in module.body if isinstance(node, ast.If)
               and any(isinstance(call, ast.Call)
                       and isinstance(call.func, ast.Attribute)
                       and isinstance(call.func.value, ast.Name)
                       and call.func.value.id == 'os'
                       and call.func.attr == 'register_at_fork'
                       for call in ast.walk(node))]
    assert len(matches) == 1
    return ast.Module(body=matches, type_ignores=[])


@pytest.mark.parametrize('available', [False, True])
def test_actual_registration_node_respects_platform_capability(monkeypatch, available):
    calls = []

    def record_registration(**kwargs):
        calls.append(kwargs)

    # Keep the actual stdlib module; qualify only this platform capability.
    with monkeypatch.context() as capability:
        if available:
            capability.setattr(os, 'register_at_fork', record_registration, raising=False)
        else:
            capability.delattr(os, 'register_at_fork', raising=False)
        namespace = {'os': os,
                     '_fast_connect_after_fork': LT._fast_connect_after_fork}
        exec(compile(_production_registration_node(), LT.__file__, 'exec'), namespace)
        assert hasattr(os, 'register_at_fork') is available

    assert calls == ([{'after_in_child': LT._fast_connect_after_fork}]
                     if available else [])
