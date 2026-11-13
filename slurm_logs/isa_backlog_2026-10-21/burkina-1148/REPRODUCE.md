# Reproduce the read-only diagnosis

Use the release baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33` plus
this documentation/evidence change. The scripts emit aggregates only. Raw
copies must remain in a private directory outside the checkout. Do not copy
the shared L2 cache, create shared-cache symlinks, or fetch absent L1 blobs.

On the original allocation:

```sh
wt=/local/job40255479/worktrees/isa-Burkina_Faso
py=/local/job40255479/venv_img/bin/python
diag_out=/local/job40255479/control/isa-backlog-20261021/Burkina_Faso/1148/reproduced
umask 077
cd "$wt"
export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$wt"
export LSMS_COUNTRIES_ROOT="$wt/lsms_library/countries"
export LSMS_DATA_DIR="$diag_out/data"
export LSMS_ISSUES_LOG="$diag_out/issues.log"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
mkdir -p "$diag_out"
"$py" slurm_logs/isa_backlog_2026-10-21/burkina-1148/reproduce.py \
  --checkout "$wt" \
  --l1 /global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache \
  --out "$diag_out" \
  --seed /local/job40255479/release-v016-seed-data \
  --seed-manifest /local/job40255479/control/release-v016/verification-prep-seed1519/copy-manifest-private.json \
  > "$diag_out/census.log" 2>&1
```

The L2 copy is optional for the status census; omit both seed arguments when
the old private release manifest is not available. Each held raw blob is MD5
verified before copying; missing blobs are reported without acquisition. The
script checks cwd/import/config/data-root identity, blocks source networking,
uses sanctioned readers on private files, and never computes exposure or rates.

The original private record-level analysis is at
`/local/job40255479/control/isa-backlog-20261021/Burkina_Faso/1148`.
`source_manifest.json`, `status_summary.json`, `cross_status_summary.json`,
`dates_summary.json`, and `private-l2-manifest.json` retain the original
verification records. `private-*` files contain household records and must not
be exported. Exploratory conditional rate arithmetic is not an estimate and
was excluded from this committed diagnosis.

For the held PDF evidence, the dispatcher provisioned pypdf 5.0.1 outside the
project at `/local/job40255479/control/isa-backlog-20261021/pdf-tools/pypdf-5.0.1`.
With that directory appended to `PYTHONPATH`, use `PdfReader(held_path)`:
questionnaire `reader.pages[0].extract_text()` is A13, and report
`reader.pages[21].extract_text()` is table 1-2. No source upload is involved.
No project dependency or venv-image change is required.

Existing checks, from the same worktree with the environment above:

```sh
"$py" -m pytest -q \
  -o cache_dir="$diag_out/pytest-cache" --basetemp="$diag_out/pytest-temp" \
  tests/test_recall_record.py -k 'not Delivered' \
  tests/test_burkina_faso_2014_passages.py::test_visit_is_declared_in_the_country_index \
  tests/test_burkina_faso_2014_passages.py::test_every_passage_file_stamps_its_own_visit_number \
  tests/test_burkina_faso_2014_passages.py::test_the_stamp_functions_return_their_passage_number \
  tests/test_burkina_faso_2014_passages.py::test_the_hook_refuses_a_frame_with_no_passage_marker \
  > "$diag_out/pytest.log" 2>&1
```

Original result: `35 passed, 2 deselected in 2.52s`, serial; no data build.
Read-only API identity probe at the release baseline found no
`RecallRecord.exposure_days` or `recall.to_period`. Passing `period='week'` to
either generated food-flow method raises `TypeError` before data access.

Inspect the pending option-A code without changing HEAD:

```sh
git show cf117d101187799b9f985c0686a567adb2177937:lsms_library/recall.py
git show cf117d101187799b9f985c0686a567adb2177937:tests/test_recall_period.py
gh -R ligon/LSMS_Library issue view 1148 --json number,title,body,comments,state,url
gh -R ligon/LSMS_Library issue view 851 --json number,title,body,comments,state,url
```

Sue.
