# Published 0.16.0 artifact verification

PASS for the actual [PyPI 0.16.0 release](https://pypi.org/project/LSMS-Library/0.16.0/) at annotated tag `v0.16.0`, commit `097283459f80016b0f00a51b814a0cb964a42789`.
The reviewed verifier executed once, followed by the parent's published-requirements checker; both exited zero with Python assertions enabled.
The source checkout and tag were checked before and after verification and remained unchanged.

| Download | Bytes | SHA256 |
| --- | ---: | --- |
| `lsms_library-0.16.0-py3-none-any.whl` | 9078781 | `a28fd5714355bc3bb9d72385c9f37278385d19aebf76c73577f62114d787eb1a` |
| `lsms_library-0.16.0.tar.gz` | 5526745 | `056ebe62fd7f6e10d581fcccde01c9205c0a6553cfffd7dbd36c80fe5a1579fd` |

Both sizes and SHA256s match the exact-version PyPI JSON. Both archives contain exactly all
11,753 tracked package files beneath `lsms_library/`, with no missing, extra or changed package
file relative to the tagged Git objects. No `.dta`, `.sav`, `.parquet` or `.pkl` payload is present.
Wheel and sdist metadata both report version 0.16.0; the wheel console entrypoint is
`lsms-library = lsms_library.cli:main`.

The independently checked published requirements in both archives match the tagged project:
Python `>=3.11,<4.0`, `fooddatacentral >=1.0.13,<2.0.0`, and pandas `>=3.0`.

The downloaded wheel installed offline with `--no-index --no-deps --no-compile --target` into a
new private target, reusing only the existing extracted image dependencies. The smoke ran outside
all checkouts with only that installation on PYTHONPATH, installed country configuration selected
without an override, and a new private data root. Package/config/data identities and installed
metadata/version agreement pass. With network and survey-build APIs refused, configuration discovery
finds 38 registered countries, eight Uganda waves, 28 Uganda table declarations and 37 roster-declaring
countries. No Country/Feature data method was called and no parquet was produced.

Execution used `/local/job40255479/venv_img/bin/python`, CPUs 16-19, BLAS/OMP/MKL/build/make
settings of one, `PYTHONOPTIMIZE` unset, no `-O`, `PYTHONDONTWRITEBYTECODE=1` and
`PYTHONNOUSERSITE=1`. The existing reviewed controls were
`verification-prep-seed1519/verify_published.py` and `installed_smoke.py`, followed by
`check_published_requirements.py`; the wait/controller was `run_published_once.py`.
No source, Git refs, tag, GitHub, original cache or existing environment was mutated.

Limits: this verifies actual published archive bytes, metadata and installed configuration discovery.
It does not resolve a clean dependency environment, test all optional imports or installed microdata,
or rebuild the downloaded sdist into a wheel. The archives were independently compared to the tag.
The publisher workflow conclusion and local/remote test-suite evidence are parent-owned separate records.

Copy only the files in COPYLIST.txt. Archives, full PyPI response, installed target and raw logs remain
private. All verification, installation, smoke, metadata-read and report processes are stopped.

Sue
