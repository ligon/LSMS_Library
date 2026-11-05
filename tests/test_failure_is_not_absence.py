"""A failure must not read as an empty or incomplete table (GH #1546, #1516, #1524, #1532).

Data-free: real registries, invented frames, private data roots.  Each test states a rule
about which failure may be read as what:

* a derived table falls through to the legacy path only if that path can build it, and only
  for a bare builtin: a subclass is somebody's statement (#1516, #1532);
* a by-path class or a dead worker reaches the parent as ``WaveWorkerError``, which nothing
  reads as absence (#1524);
* the v-join and the market lookup degrade only for a source that is not there (#1546).
"""
import importlib.util
import pickle

import pytest

from lsms_library import _parallel_waves as pw
from lsms_library.country import GrainCollapseError
from lsms_library.errors import WaveWorkerError

GENERIC = [FileNotFoundError, KeyError, ValueError, RuntimeError]


def by_path_guard(tmp_path, base='Exception'):
    """A guard class as a by-path ``mapping.py`` has one: importable by nobody."""
    path = tmp_path / 'mapping.py'
    path.write_text(f'class Guard({base}):\n    """A guard."""\n')
    spec = importlib.util.spec_from_file_location('mapping', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Guard



class PicklesButDoesNotLoad(Exception):
    def __init__(self, first, second):
        super().__init__(f'{first} {second}')



class TestWorkerRelay:
    def test_a_by_path_guard_arrives_as_wave_worker_error(self, tmp_path):
        Guard = by_path_guard(tmp_path)
        try:
            raise Guard('the two files disagree')
        except Guard as exc:
            packed = pw._pack_exception(exc)
        assert packed[0] is None and packed[1] == 'Guard'
        payload = {'wave': 'w', 'warnings': [], 'grain': [], 'null': [], 'error': packed}
        with pytest.raises(WaveWorkerError, match='Guard: the two files disagree') as caught:
            pw.relay(payload, country='X', table='t')
        assert caught.value.original_type == 'Guard'
        assert 'X/w/t' in caught.value.__notes__[0]
        assert 'Traceback' in caught.value.__notes__[0]

    @pytest.mark.parametrize('base', ['Exception', 'ValueError', 'RuntimeError'])
    def test_its_base_class_does_not_survive_to_be_caught(self, tmp_path, base):
        """A by-path ``ValueError`` is no more a build failure than an ``Exception``."""
        Guard = by_path_guard(tmp_path, base)
        packed = pw._pack_exception(Guard('refused'))
        payload = {'wave': 'w', 'warnings': [], 'grain': [], 'null': [], 'error': packed}
        with pytest.raises(WaveWorkerError) as caught:
            pw.relay(payload, country='X', table='t')
        assert not isinstance(caught.value, (ValueError, KeyError, RuntimeError, OSError))

    def test_a_class_that_pickles_but_will_not_load_does_not_break_the_pool(self):
        """``dumps`` passes, the parent's load fails, and the pool calls the worker dead."""
        exc = PicklesButDoesNotLoad('two', 'arguments')
        pickle.dumps(exc)
        assert pw._pack_exception(exc)[0] is None

    def test_an_importable_class_still_travels_whole(self):
        exc = GrainCollapseError('strict')
        assert pw._pack_exception(exc)[0] is exc

    def test_a_dead_worker_is_not_a_build_failure(self):
        payload = {'wave': 'w', 'warnings': [], 'grain': [], 'null': [],
                   'error': (None, 'BrokenProcessPool', 'worker process died', '')}
        with pytest.raises(WaveWorkerError, match='worker process died'):
            pw.relay(payload, country='X', table='t')

    def test_the_error_itself_pickles(self):
        back = pickle.loads(pickle.dumps(WaveWorkerError('Guard: x', original_type='Guard')))
        assert str(back) == 'Guard: x' and back.original_type == 'Guard'

    def test_it_is_not_a_class_a_derived_handler_catches(self):
        assert not issubclass(WaveWorkerError, tuple(GENERIC + [AttributeError, OSError]))
