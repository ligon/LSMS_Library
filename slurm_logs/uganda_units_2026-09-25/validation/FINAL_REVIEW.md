SCOPE DEVIATIONS: None. Independent read-only review of the Uganda decoder/canonicalizer and `tests/test_962_unit_sentinel.py` against ledger sections 2-5. The reviewer authored the catalog/docs and does not claim independent review of them. No repository edits, staging, commits, fixture changes, or cache purges.

No remaining blocking findings. The reported P2 categorical-unit regression is resolved: the canonicalizer avoids assigning `Value` when no rows require it, and converts the unit column to object when categorical rows do require retyping. Ordinary categorical Kg rows preserve their values and dtype; categorical Unknown expenditure-only rows become Value with Quantity=Expenditure and missing Price.

Reviewed source at commit `72f8bce34f50159d0f0eb608d0b6578bfceac810`:

- `uganda.py` blob: `fcc7babacda7806b48d97328e8691bfa272bb3d8`.
- Test blob: `6b77cff42bab9806c159b60db876af44cbaad032`.

Independent rechecks:

- Original base/candidate categorical reproduction: both previously failing candidate cases now pass. Known Kg retains Q=2, E=20, Price=100. Unknown without quantity becomes Value, Q=E=20, Price missing.
- Scalar oracle: 1,728 input combinations with unique keys produce 2,862 matching canonical rows; the same inputs with duplicate keys produce 202 matching rows. Both runs preserve their input frames.
- Numeric dtypes float64, Float64, Int64, and object pass; categorical unit-index probe passes.
- Focused submitted suite: **28 passed, 2 warnings in 13.39s**. The warnings disclose expected unpriceable rows in runtime derivation tests.

Portable aggregate evidence prepared for copying:

- `probe_portable.py`: accepts `--output PATH`, uses repository cwd, asserts imported package identity, contains no job-specific paths, and writes only aggregate results.
- `final_probe_summary.json`: case counts and dtype outcomes.
- `categorical_repro.py` and `final_categorical_repro.log`: original reported regression, compared with base.
- `final_probe.log`, `final_pytest.log`: execution evidence.

Commands ran from the Uganda worktree with this prefix:

```sh
env -u PYTHONSAFEPATH \
  PATH=/local/job39263225/venv_img/bin:$PATH \
  PYTHONPATH=/local/job39263225/worktrees/uganda-food-units \
  LSMS_COUNTRIES_ROOT=/local/job39263225/worktrees/uganda-food-units/lsms_library/countries \
  LSMS_DATA_DIR=/local/job39263225/uganda-units/catalog/review-data LSMS_SKIP_AUTH=1 \
  OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 \
  taskset -c 4-7 /local/job39263225/venv_img/bin/python
```

Append one of:

```sh
/local/job39263225/uganda-units/catalog/code-review/probe_portable.py --output /local/job39263225/uganda-units/catalog/code-review/final_probe_summary.json
/local/job39263225/uganda-units/catalog/code-review/categorical_repro.py
-m pytest -q tests/test_962_unit_sentinel.py --no-purge -n 2 --dist=loadfile
```

This review verifies source partition, zero/negative/cancelling quantities, Value price semantics, physical/residual preservation, duplicate aggregation, and input mutation/dtype boundaries. Corpus rebuilding, nutrition attribution, and cache scope remain separate verification work; this report makes no claim about them.

-- Sue
