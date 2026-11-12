"""Pure worker/relay controls; no Country instance or source producer is used."""
import os
import pickle
import sys
import types
import warnings

import pytest

from lsms_library import _parallel_waves as pw
from lsms_library.country import GrainCollapseError
from lsms_library.errors import WaveWorkerError
from lsms_library.null_read_audit import NullReadError


def payload(error):
    return {'wave': 'failed', 'warnings': [], 'grain': [], 'null': [],
            'datetime': [], 'drops': [], 'has_table': False, 'result': None,
            'error': error}


def packed_error(exc):
    try:
        raise exc
    except Exception as caught:
        return pw._pack_exception(caught)


@pytest.fixture
def two_workers(monkeypatch):
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '2')
    monkeypatch.setattr(pw, 'visible_cpus', lambda env=None: 2)
    monkeypatch.setattr(pw, 'memory_worker_cap', lambda env=None: None)
    assert pw._fork_available(), 'these controls require actual fork workers'


@pytest.mark.parametrize('parent_has_module', [False, True], ids=['parent-missing-module', 'parent-missing-class'])
def test_child_only_class_keeps_original_failure(two_workers, monkeypatch, parent_has_module):
    module_name = f'_gh1591_child_only_{os.getpid()}_{parent_has_module}'
    if parent_has_module:
        monkeypatch.setitem(sys.modules, module_name, types.ModuleType(module_name))
    else:
        monkeypatch.delitem(sys.modules, module_name, raising=False)
    parent_pid = os.getpid()

    def build(wave):
        assert pw._IN_WORKER and os.getpid() != parent_pid
        if wave == 'healthy':
            return True, {'pid': os.getpid(), 'wave': wave}
        module = sys.modules.get(module_name) or types.ModuleType(module_name)
        exec('class WorkerOnlyError(Exception): pass', module.__dict__)
        sys.modules[module_name] = module
        raise module.WorkerOnlyError('child-only refusal')

    outcomes = pw.prebuild(build, ['failed', 'healthy'], None, country='Invented', table='toy')
    with pytest.raises(WaveWorkerError) as caught:
        pw.relay(outcomes['failed'], country='Invented', table='toy')
    assert caught.value.original_type == 'WorkerOnlyError'
    assert str(caught.value) == 'WorkerOnlyError: child-only refusal'
    note = '\n'.join(caught.value.__notes__)
    assert 'Invented/failed/toy' in note and 'Traceback' in note
    assert 'raise module.WorkerOnlyError' in note
    assert isinstance(outcomes['failed']['error'][0], bytes)
    has, result = pw.relay(outcomes['healthy'], country='Invented', table='toy')
    assert has and result['wave'] == 'healthy' and result['pid'] != parent_pid
    assert pw._GROUP_BUILDER is None
    if parent_has_module:
        assert not hasattr(sys.modules[module_name], 'WorkerOnlyError')
    else:
        assert module_name not in sys.modules


def test_packing_does_not_claim_to_load_in_child(monkeypatch):
    calls = []

    def forbidden_load(*args, **kwargs):
        calls.append(True)
        raise AssertionError('packing must not load')

    monkeypatch.setattr(pw.pickle, 'loads', forbidden_load)
    packed = packed_error(ValueError('original'))
    assert isinstance(packed[0], bytes)
    assert packed[1:3] == ('ValueError', 'original') and 'Traceback' in packed[3]
    assert calls == []


@pytest.mark.parametrize('kind', [ValueError, KeyError, GrainCollapseError, NullReadError])
def test_original_type_payload_attributes_and_notes(kind):
    original = kind('strict refusal')
    original.evidence = {'wave': 'invented', 'counts': [1, 2]}
    original.add_note('existing note')
    packed = packed_error(original)
    assert isinstance(packed[0], bytes)
    with pytest.raises(kind) as caught:
        pw.relay(payload(packed), country='Invented', table='toy')
    assert type(caught.value) is kind and caught.value.args == original.args
    assert caught.value.evidence == original.evidence
    assert caught.value.__notes__[0] == 'existing note'
    assert 'Invented/failed/toy' in caught.value.__notes__[-1]
    assert 'Traceback' in caught.value.__notes__[-1]
    assert original.__notes__ == ['existing note']


class NeedsTwoArguments(Exception):
    def __init__(self, first, second):
        super().__init__(f'{first} {second}')


class RemappedException(Exception):
    def __reduce__(self):
        return Exception, (f'RemappedException: {self}',)


class NotAnException(Exception):
    def __reduce__(self):
        return str, ('decoded string is not an exception',)


@pytest.mark.parametrize('mode', ['bad-constructor', 'nonexception', 'corrupt-bytes'])
def test_parent_decode_failure_preserves_original_evidence(mode):
    if mode == 'bad-constructor':
        packed = packed_error(NeedsTwoArguments('two', 'arguments'))
    elif mode == 'nonexception':
        packed = packed_error(NotAnException('native refusal'))
    else:
        packed = (b'not a pickle', 'OriginalGuard', 'native refusal', 'original worker traceback')
    assert isinstance(packed[0], bytes)
    with pytest.raises(WaveWorkerError) as caught:
        pw.relay(payload(packed), country='Invented', table='toy')
    assert caught.value.original_type == packed[1]
    assert str(caught.value) == f'{packed[1]}: {packed[2]}'
    assert packed[3] in caught.value.__notes__[-1]
    assert 'Invented/failed/toy' in caught.value.__notes__[-1]
    assert not isinstance(caught.value, (RuntimeError, ValueError, KeyError, OSError))


def test_supported_baseexception_remapping_is_preserved():
    packed = packed_error(RemappedException('refusal'))
    with pytest.raises(Exception) as caught:
        pw.relay(payload(packed), country='Invented', table='toy')
    assert type(caught.value) is Exception
    assert str(caught.value) == 'RemappedException: refusal'
    assert 'Invented/failed/toy' in caught.value.__notes__[-1]


def test_a_dump_failure_uses_existing_text_fallback():
    class LocalGuard(Exception):
        pass
    packed = packed_error(LocalGuard('cannot serialize'))
    assert packed[0] is None
    with pytest.raises(WaveWorkerError) as caught:
        pw.relay(payload(packed), country='Invented', table='toy')
    assert caught.value.original_type == 'LocalGuard'
    assert str(caught.value) == 'LocalGuard: cannot serialize'
    assert 'Traceback' in caught.value.__notes__[-1]


def test_failure_relay_files_all_ledgers_and_warning_first(two_workers, monkeypatch):
    from lsms_library import country as country_module
    from lsms_library import null_read_audit, datetime_parse
    monkeypatch.setattr(country_module, '_GRAIN_LEDGER', {})
    monkeypatch.setattr(null_read_audit, '_NULL_READ_LEDGER', {})
    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})
    monkeypatch.setattr(country_module, '_BUILD_DROP_LEDGER', {})
    key = ('Invented', 'toy')
    module_name = f'_gh1591_ledger_only_{os.getpid()}'
    monkeypatch.delitem(sys.modules, module_name, raising=False)

    def build(wave):
        if wave == 'healthy':
            return True, 'healthy'
        for name, ledger in zip(['grain', 'null', 'datetime', 'drops'],
                                [*pw._ledgers(), pw._datetime_ledger(), pw._drop_ledger()]):
            ledger[key] = [{'wave': wave, 'marker': name}]
        warnings.warn('invented warning before refusal', UserWarning)
        module = types.ModuleType(module_name)
        exec('class LedgerGuard(Exception): pass', module.__dict__)
        sys.modules[module_name] = module
        raise module.LedgerGuard('refused after reports')

    outcomes = pw.prebuild(build, ['failed', 'healthy'], None, country='Invented', table='toy')
    for ledger in [*pw._ledgers(), pw._datetime_ledger(), pw._drop_ledger()]:
        assert key not in ledger
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter('always')
        with pytest.raises(WaveWorkerError) as caught:
            pw.relay(outcomes['failed'], country='Invented', table='toy')
    assert caught.value.original_type == 'LedgerGuard'
    assert [(m.category, str(m.message)) for m in caught_warnings] == [
        (UserWarning, 'invented warning before refusal')]
    for name, ledger in zip(['grain', 'null', 'datetime', 'drops'],
                            [*pw._ledgers(), pw._datetime_ledger(), pw._drop_ledger()]):
        assert ledger[key] == [{'wave': 'failed', 'marker': name}]


def test_actual_dead_child_stays_distinct(two_workers):
    def build(wave):
        if wave == 'dead':
            os._exit(19)
        return True, 'healthy'

    outcomes = pw.prebuild(build, ['dead', 'healthy'], None, country='Invented', table='toy')
    with pytest.raises(WaveWorkerError, match='worker process died') as caught:
        pw.relay(outcomes['dead'], country='Invented', table='toy')
    assert caught.value.original_type == 'BrokenProcessPool'
    assert 'Invented/dead/toy' in caught.value.__notes__[-1]
    assert pw._GROUP_BUILDER is None


def test_module_imported_only_after_fork_uses_original_error(two_workers, tmp_path):
    import importlib
    name = f'_gh1591_import_after_fork_{os.getpid()}'
    (tmp_path / (name + '.py')).write_text('class ImportedChildError(Exception): pass\n')
    assert name not in sys.modules and str(tmp_path) not in sys.path

    def build(wave):
        assert pw._IN_WORKER
        if wave == 'healthy':
            return True, 'healthy'
        sys.path.insert(0, str(tmp_path))
        module = importlib.import_module(name)
        raise module.ImportedChildError('imported only in the child')

    outcomes = pw.prebuild(build, ['failed', 'healthy'], None, country='Invented', table='toy')
    with pytest.raises(WaveWorkerError) as caught:
        pw.relay(outcomes['failed'], country='Invented', table='toy')
    assert caught.value.original_type == 'ImportedChildError'
    assert str(caught.value) == 'ImportedChildError: imported only in the child'
    assert 'raise module.ImportedChildError' in caught.value.__notes__[-1]
    assert 'Traceback' in caught.value.__notes__[-1]
    assert name not in sys.modules and str(tmp_path) not in sys.path
    assert pw.relay(outcomes['healthy'], country='Invented', table='toy') == (True, 'healthy')


class AdvertisedExceptionProxy:
    class_reads = 0

    @property
    def __class__(self):
        type(self).class_reads += 1
        return ValueError


class RaisingClassProxy:
    class_reads = 0

    @property
    def __class__(self):
        type(self).class_reads += 1
        raise RuntimeError('proxy class lookup is not the worker error')


class ReducesToProxy(Exception):
    def __init__(self, proxy_type):
        self.proxy_type = proxy_type
        super().__init__('original worker refusal')

    def __reduce__(self):
        return self.proxy_type, ()


@pytest.mark.parametrize('proxy_type', [AdvertisedExceptionProxy, RaisingClassProxy])
def test_decoded_proxy_class_cannot_replace_native_failure(proxy_type):
    proxy_type.class_reads = 0
    packed = packed_error(ReducesToProxy(proxy_type))
    assert isinstance(packed[0], bytes)
    with pytest.raises(WaveWorkerError) as caught:
        pw.relay(payload(packed), country='Invented', table='toy')
    assert caught.value.original_type == 'ReducesToProxy'
    assert str(caught.value) == 'ReducesToProxy: original worker refusal'
    assert packed[3] in caught.value.__notes__[-1]
    assert 'Invented/failed/toy' in caught.value.__notes__[-1]
    assert proxy_type.class_reads == 0


class FalseValuedError(Exception):
    def __bool__(self):
        return False


def test_false_valued_actual_exception_still_preserves_payload():
    original = FalseValuedError('actual exception despite false truth value')
    original.evidence = {'kind': 'native', 'missing': None}
    original.add_note('existing false-valued note')
    packed = packed_error(original)
    with pytest.raises(FalseValuedError) as caught:
        pw.relay(payload(packed), country='Invented', table='toy')
    assert type(caught.value) is FalseValuedError and not caught.value
    assert caught.value.args == original.args and caught.value.evidence == original.evidence
    assert caught.value.__notes__[0] == 'existing false-valued note'
    assert 'Invented/failed/toy' in caught.value.__notes__[-1]
