# Malawi 2016 plot_features restricted reader: prior art

## Task and baseline
Source-only preparation on development 1864a9bd. Malawi CONTENTS, including TODO/WAITING and the six D source-half/original-position caveats, read first. The 2016 cross-section uses cs-17- while 2019 uses bare case_id. Plot_features identifies plots by gardenid_plotid; 2016 ag_d02 means respondent, not tenure, so Tenure remains absent. The restricted column read avoids a bad unused free-text byte. No field, namespace, reducer, eligibility or scientific interpretation changes.

## Reuse
PR #1351 and tests/test_malawi_plot_inputs_cached_reader.py establish the exact held-byte fast path: _ensure_dvc_pulled, then _dvc_cache_path; stream cached bytes when present, otherwise retain DVCFS.open under the countries cwd. The original tempfile/pyreadstat usecols/apply_value_formats=False and finally-unlink stay unchanged. Use Git/ripgrep/AST as the source-search floor.

Current plot_features unconditionally uses DVCFS.open after ensure. This repeats the source route implicated in the earlier plot_inputs repository-index overhead; no new timeout or data result is inferred here. Port only the established fast path, parameterize the existing reader regression for both actual source helpers, and execute only the four new plot_features cases. Original plot_inputs cases are not replayed.

## Invariants and boundaries
Bytes, columns, restricted numeric decoding, fallback and temporary cleanup must be identical. No acquisition, broad DVC index, raw parse or data actor is performed during preparation. Real seven-cell compiler/runtime planning is separate, including whether already-closed D frames actually contain these fields. The isolated source diff goes to independent review before publication or any data run.

## Source and invented validation
The executable helper AST (excluding its docstring) exactly matches the accepted plot_inputs fast path. All other plot_features driver AST, including its nine selected columns and all scientific calls, is unchanged except importing _dvc_cache_path. The shared test carrier retains every original assertion. Only four new plot_features cases ran: cached/fallback crossed with decoder success/failure, 4 passed and 4 original plot_inputs cases deselected. Runtime 0.10 seconds (outer 5.3654 seconds), core 38, one CPU, 8 GiB, threads one, network namespace, 120+5 bound. Source hashes remained unchanged. No survey data, real parser, native script, API or performance measurement occurred.
