import os
import socket
import sys
from pathlib import Path

P = Path('/local/job40255479/control/isa-backlog-20261021/nullable-float-comparator-root')
assert sys.executable == '/local/job40255479/venv_img/bin/python'
assert [x.split(':')[0].strip() for x in Path('/proc/net/dev').read_text().splitlines()[2:]] == ['lo']

def audit(event, args):
    if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
        mode, flags = args[1] or '', args[2]
        if any(c in mode for c in 'wax+') or flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            q = Path(os.fsdecode(args[0])).absolute()
            if not q.is_relative_to(P) or not q.resolve().is_relative_to(P):
                raise PermissionError('Synthetic comparator proof writes only its new private root')
    if event in {'os.remove', 'os.rmdir', 'os.mkdir', 'os.chmod', 'os.utime', 'os.truncate', 'os.rename', 'os.link', 'os.symlink'}:
        raise PermissionError('Synthetic comparator proof creates only new files through open')

sys.addaudithook(audit)

def offline(*args, **kwargs):
    raise OSError('Synthetic comparator proof is offline')

socket.socket.connect = offline
socket.socket.connect_ex = offline

import datetime
import hashlib
import importlib.metadata
import json
import numpy as np
import pandas as pd

assert all(os.environ[x] == '1' for x in ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS'))

def accepted(callback):
    try:
        callback()
    except AssertionError:
        return False
    return True

rows = []
for name, delta in [('tiny', 1e-12), ('nextafter', np.nextafter(1., 2.) - 1.)]:
    reference = pd.DataFrame({'x': pd.array([1., 2.], dtype='Float64')})
    mutant = reference.copy(deep=True)
    mutant.loc[0, 'x'] += delta
    a, b = reference.x.to_numpy(), mutant.x.to_numpy()
    outcomes = dict(
        frame_accepts_difference=accepted(lambda: pd.testing.assert_frame_equal(reference, mutant, check_exact=True, rtol=0, atol=0)),
        extension_accepts_difference=accepted(lambda: pd.testing.assert_extension_array_equal(reference.x.array, mutant.x.array, check_exact=True, rtol=0, atol=0)),
        numpy_accepts_difference=accepted(lambda: np.testing.assert_array_equal(a, b, strict=True)),
    )
    assert outcomes == dict(frame_accepts_difference=True, extension_accepts_difference=False, numpy_accepts_difference=False)
    for label, value in [('reference', reference), ('mutant', mutant)]:
        value.to_pickle(P / (name + '-' + label + '-private.pkl'))
    rows.append(dict(case=name, rows=2, dtype='Float64',
                     actual_max_abs_difference=float(np.max(np.abs(a - b))), **outcomes))

result = dict(recorded_at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              status='INDEPENDENT_SYNTHETIC_COMPARATOR_ESCAPE_REPRODUCED',
              packages={n: importlib.metadata.version(n) for n in ('pandas', 'numpy', 'pyarrow')},
              cases=rows, actual_country_product_mismatch_measured=False,
              source_decode_Country_Feature_API_or_existing_test_replay=0,
              existing_frozen_evidence_modified=False, signed='Sue')
with (P / 'ROOT-SYNTHETIC-PROOF.json').open('x') as stream:
    json.dump(result, stream, indent=2)
    stream.write('\n')
print(json.dumps(result))
