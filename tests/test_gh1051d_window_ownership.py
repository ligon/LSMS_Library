"""R1 ownership transitions: invented metadata leaves, real window helpers.

The independent failed asset remains immutable. Its deterministic two-thread
schedule is preserved below, with ROOT's stronger retained-until-reset oracle.
The fixture binds its prevalidated owner before direct publication.
"""
import threading
from types import SimpleNamespace as NS
import weakref

import pytest

from lsms_library import local_tools as LT


def test_publication_during_selection_releases_ownership_outside_lock(monkeypatch, request):
    LT._reset_fast_connect_windows()
    state = LT._FAST_CONNECT_STATE
    events = []
    entered, finished = threading.Event(), threading.Event()
    owner = (object(), object(), 'invented-root', 'invented-cache', ())
    # Prevalidated owner metadata, matching the direct-publication fixture.
    state.owner = owner
    def scope(prefix):
        return (owner, 'invented', 'invented-bucket/' + prefix,
                'invented-backend-kind', (), 'invented-bucket', prefix, 'path')
    route = (*[object() for _ in range(7)], ('https', 'invented.invalid', 443), False, False)
    class Backend:
        def __init__(self, label):
            self.label = label
        def __del__(self):
            if self.label == 'newly-published':
                events.append(state.lock.locked())
    def publish(prefix, backend):
        remote = NS(scope=scope(prefix))
        outcome = LT._FastProbeOutcome()
        outcome.scope, outcome.fs, outcome.remote = remote.scope, backend, remote
        outcome.route, outcome.errors, outcome.epoch = route, 2, state.epoch
        LT._fast_window_publish(outcome)
    monkeypatch.setattr(LT, '_fast_scope', lambda remote, fs: remote.scope)
    armed = False
    old = Backend('old-snapshot')
    def route_leaf(backend, bucket):
        if armed and backend is old:
            entered.set()
            assert finished.wait(10), 'publisher must finish before selector pruning'
        return route
    monkeypatch.setattr(LT, '_fast_route', route_leaf)
    publish('old', old)
    assert len(state.windows) == 1
    published_ref = []
    errors = []
    def publisher():
        try:
            assert entered.wait(10), 'selector must take its snapshot first'
            backend = Backend('newly-published')
            published_ref.append(weakref.ref(backend))
            publish('new', backend)
            assert len(state.windows) == 2
            del backend  # after return, the new window is the sole owner
        except BaseException as exc:
            errors.append(type(exc).__name__)
        finally:
            finished.set()
    thread = threading.Thread(target=publisher, name='independent-publisher')
    try:
        armed = True
        thread.start()
        # This executes the real snapshot/validate/prune/select transitions.
        LT._fast_window_select(LT._FastProbeOutcome(), NS(scope=scope('old')), Backend('lookup'))
        thread.join(10)
        assert not thread.is_alive() and not errors
        assert published_ref and published_ref[0]() is not None
        assert any(e[3] is published_ref[0]() for e in state.windows)
        assert events == [], 'a concurrent valid publication must retain its backend'
        assert not state.lock.locked()
        LT._reset_fast_connect_windows()  # explicit release OUTSIDE the mutex
        assert published_ref[0]() is None
        assert events == [False], 'last backend owner must be released outside transition lock'
        request.node.user_properties.append(('R1_lifetime',
            'after_select=retained,alive,events[]; after_outside_reset=dead,events[False]'))
    finally:
        armed = False
        thread.join(10)
        LT._reset_fast_connect_windows()


@pytest.fixture
def window(monkeypatch):
    LT._reset_fast_connect_windows()
    state = LT._FAST_CONNECT_STATE
    events = []
    route = (*[object() for _ in range(7)], ('https', 'invented.invalid', 443), False, False)
    owner = (object(), object(), 'invented-root', 'invented-cache', ())
    h = NS(now=0.0, events=events, state=state, owner=owner, route=route)
    class Backend:
        def __init__(self, label):
            self.label, self.route = label, route
        def __del__(self):
            events.append((self.label, state.lock.locked()))
        def close(self):
            pytest.fail('window transitions must never close an in-flight backend')
    h.backend = Backend
    def scope(prefix, *, owner=owner, options=()):
        return (owner, 'invented', 'invented-bucket/' + prefix,
                'invented-backend-kind', options, 'invented-bucket', prefix, 'path')
    h.scope = scope
    monkeypatch.setattr(LT.time, 'monotonic', lambda: h.now)
    monkeypatch.setattr(LT, '_fast_scope', lambda remote, fs: remote.scope)
    def route_leaf(backend, bucket):
        assert not state.lock.locked(), 'route inspection must stay outside mutex'
        return backend.route
    monkeypatch.setattr(LT, '_fast_route', route_leaf)
    def outcome(prefix, *, options=()):
        backend = Backend(prefix)
        remote = NS(scope=scope(prefix, options=options))
        result = LT._FastProbeOutcome()
        assert LT._fast_window_select(result, remote, backend) is backend
        result.route, result.errors = route, 2
        return result
    h.outcome = outcome
    def publish(prefix):
        result = outcome(prefix)
        ref = weakref.ref(result.fs)
        LT._fast_window_publish(result)
        return ref
    h.publish = publish
    try:
        yield h
    finally:
        try:
            assert not state.lock.locked()
            assert all(held is False for _, held in events)
        finally:
            LT._reset_fast_connect_windows()


@pytest.mark.parametrize('transition', [
    'reset', 'clear', 'select-expiry', 'select-invalid-route',
    'select-owner', 'select-options', 'publish-expiry', 'rejected-publish-expiry',
])
def test_last_window_owner_released_outside_mutex(window, transition):
    h = window
    # Prepare a later publisher before seeding the sole-owned old entry.
    incoming = h.outcome('incoming') if 'publish' in transition else None
    old = h.publish('old')
    assert old() is not None and h.events == []
    if transition == 'reset':
        LT._reset_fast_connect_windows()
    elif transition == 'clear':
        LT._FastProbeOutcome().clear()
    elif 'publish' in transition:
        h.now = 5.0
        if transition == 'rejected-publish-expiry':
            incoming.epoch -= 1
        LT._fast_window_publish(incoming)
        assert len(h.state.windows) == (transition == 'publish-expiry')
    else:
        if transition == 'select-expiry':
            h.now = 5.0
        elif transition == 'select-invalid-route':
            old().route = None
        owner = (object(), object(), 'other-root', 'other-cache', ()) if transition == 'select-owner' else h.owner
        options = ('changed',) if transition == 'select-options' else ()
        prefix = 'old' if transition == 'select-options' else 'lookup'
        lookup = h.backend('lookup')
        result = LT._FastProbeOutcome()
        assert LT._fast_window_select(result, NS(scope=h.scope(prefix, owner=owner, options=options)), lookup) is lookup
        assert not result.suppressed
    assert old() is None
    assert h.events == [('old', False)]


@pytest.mark.parametrize('transition', ['owner', 'options'])
def test_concurrent_live_entry_evicted_outside_mutex(window, monkeypatch, transition):
    h = window
    old = h.publish('old')
    entered, finished = threading.Event(), threading.Event()
    published, errors = [], []
    def route_leaf(backend, bucket):
        assert not h.state.lock.locked()
        if backend is old():
            entered.set()
            assert finished.wait(10), 'publisher must finish before selector pruning'
        return backend.route
    monkeypatch.setattr(LT, '_fast_route', route_leaf)
    def publisher():
        try:
            assert entered.wait(10)
            # Prevalidated metadata publication, exactly as the independent peer.
            backend = h.backend('new')
            result = LT._FastProbeOutcome()
            result.scope, result.fs = h.scope('new'), backend
            result.remote = NS(scope=result.scope)
            result.route, result.errors, result.epoch = h.route, 2, h.state.epoch
            published.append(weakref.ref(backend))
            LT._fast_window_publish(result)
            assert len(h.state.windows) == 2
        except BaseException as exc:
            errors.append(type(exc).__name__)
        finally:
            finished.set()
    thread = threading.Thread(target=publisher, name='live-entry-publisher')
    owner = (object(), object(), 'other-root', 'other-cache', ()) if transition == 'owner' else h.owner
    options = ('changed',) if transition == 'options' else ()
    try:
        thread.start()
        lookup = h.backend('lookup')
        LT._fast_window_select(LT._FastProbeOutcome(), NS(scope=h.scope('new', owner=owner, options=options)), lookup)
        thread.join(10)
        assert not thread.is_alive() and not errors
        assert published and published[0]() is None
        assert ('new', False) in h.events
        assert all(held is False for _, held in h.events)
    finally:
        thread.join(10)
        LT._reset_fast_connect_windows()


@pytest.mark.parametrize('transition', ['reset', 'select-owner'])
def test_old_owner_metadata_released_outside_mutex(window, transition):
    h = window
    marker = h.backend('owner-token')
    ref = weakref.ref(marker)
    h.state.owner = (marker, object(), 'old-root', 'old-cache', ())
    del marker
    if transition == 'reset':
        LT._reset_fast_connect_windows()
    else:
        lookup = h.backend('lookup')
        LT._fast_window_select(LT._FastProbeOutcome(), NS(scope=h.scope('new')), lookup)
    assert ref() is None and h.events == [('owner-token', False)]


def test_reset_preserves_inflight_owner_without_close(window):
    h = window
    ref = h.publish('inflight')
    inflight = ref()
    LT._reset_fast_connect_windows()
    assert ref() is inflight and h.events == []
    assert inflight.route is h.route
    del inflight
    assert ref() is None and h.events == [('inflight', False)]
