"""Post-snapshot publication is retained, not selected before validation.

Real publish/select transitions and one deterministic two-thread interleaving;
only route/scope metadata leaves are invented. No backend/SDK/network/data IO.
The independently owned parent counterexample is separate from this cohort.
"""
import threading
from types import SimpleNamespace as NS

import pytest

from lsms_library import local_tools as LT


@pytest.mark.parametrize('invalidate_after_publish', [True, False],
                         ids=['late-invalid', 'late-valid-next-call'])
def test_matching_late_publication_requires_own_selection_validation(
        monkeypatch, request, invalidate_after_publish):
    LT._reset_fast_connect_windows()
    state = LT._FAST_CONNECT_STATE
    owner = (object(), object(), 'invented-root', 'invented-cache', ())
    # Direct publication is already qualified; preserve the real owner check.
    state.owner = owner
    route = (*[object() for _ in range(7)],
             ('https', 'invented.invalid', 443), False, False)
    old, late, lookup = [NS(label=x, route=route)
                         for x in ['snapshot', 'late', 'lookup']]
    entered, published = threading.Event(), threading.Event()
    errors, observed_routes, saved_late = [], [], []
    armed = False

    def scope(prefix):
        return (owner, 'invented', 'invented-bucket/' + prefix,
                'invented-kind', (), 'invented-bucket', prefix, 'path')

    def scope_leaf(remote, backend):
        assert not state.lock.locked()
        return remote.scope

    def route_leaf(backend, bucket):
        assert not state.lock.locked(), 'metadata inspection must stay outside mutex'
        assert bucket == 'invented-bucket'
        observed_routes.append(backend.label)
        if armed and backend is old:
            entered.set()
            assert published.wait(10), 'late publisher must finish before selection'
        return backend.route

    monkeypatch.setattr(LT, '_fast_scope', scope_leaf)
    monkeypatch.setattr(LT, '_fast_route', route_leaf)
    monkeypatch.setattr(LT.time, 'monotonic', lambda: 0.0)

    def publish(prefix, backend):
        remote = NS(scope=scope(prefix))
        outcome = LT._FastProbeOutcome()
        outcome.scope, outcome.fs, outcome.remote = remote.scope, backend, remote
        outcome.route, outcome.errors, outcome.epoch = route, 2, state.epoch
        LT._fast_window_publish(outcome)

    publish('other', old)
    assert len(state.windows) == 1

    def publisher():
        try:
            assert entered.wait(10), 'selector must snapshot first'
            publish('target', late)
            saved_late.extend(e for e in state.windows if e[3] is late)
            assert len(saved_late) == 1
            # This occurs AFTER publication validation and BEFORE selection.
            if invalidate_after_publish:
                late.route = None
        except BaseException as exc:
            errors.append(type(exc).__name__)
        finally:
            published.set()

    thread = threading.Thread(target=publisher, name='invented-late-publisher')
    try:
        armed = True
        thread.start()
        first = LT._FastProbeOutcome()
        selected = LT._fast_window_select(first, NS(scope=scope('target')), lookup)
        thread.join(10)
        assert not thread.is_alive() and not errors
        assert selected is lookup and first.fs is lookup
        assert first.suppressed is False
        assert first.scope == scope('target') and first.epoch == state.epoch
        assert len(saved_late) == 1
        assert any(e is saved_late[0] for e in state.windows)
        assert saved_late[0][0] == 5.0, 'retention must not slide expiry'
        # Only the publisher, not this snapshot's validation, inspected late.
        assert observed_routes.count('late') == 1
        assert not state.lock.locked()

        armed = False
        next_lookup = NS(label='next-lookup', route=route)
        second = LT._FastProbeOutcome()
        next_selected = LT._fast_window_select(
            second, NS(scope=scope('target')), next_lookup)
        assert observed_routes.count('late') == 2
        if invalidate_after_publish:
            assert next_selected is next_lookup and second.fs is next_lookup
            assert second.suppressed is False
            assert not any(e is saved_late[0] for e in state.windows)
        else:
            assert next_selected is late and second.fs is late
            assert second.suppressed is True
            assert any(e is saved_late[0] for e in state.windows)
            assert saved_late[0][0] == 5.0
        request.node.user_properties.append(('late_window',
            'first=unsuppressed+retained; next=' +
            ('invalid+pruned' if invalidate_after_publish else 'validated+selected')))
    finally:
        armed = False
        entered.set()
        thread.join(10)
        LT._reset_fast_connect_windows()
        assert not state.lock.locked()
