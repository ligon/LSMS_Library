SCOPE DEVIATIONS

None. Read-only review of prepared release scripts and saved aggregate evidence. No Git/GitHub mutation, table API call, cache read or cache write. The separate installed-data smoke and launcher were prepared and syntax-compiled at the coordinator's request; neither was executed.

APPROVE the six prepared artifact-validation scripts at the hashes in review.json. Three initial gaps are resolved: both archives now scan forbidden credential/parquet filenames with a properly bounded assertion; the sdist build scrubs credential prefixes and uses new empty config/data plus nonexistent AWS credential paths; all packaged library paths and Git blob contents now equal the exact tagged source, rather than only eleven selected members.

The validation chain is sound: fetch checks the coordinator-pinned tag commit, PyPI version and file size/SHA-256; install_published obtains the pinned public PyPI version in a clean environment and checks pip's actual wheel URL/SHA against the fetched metadata; the installed smoke asserts package/config identity outside a checkout with no overrides; the no-Git source archive builds a wheel; verify_artifacts requires exact wheel member/byte equality and every installed package file to match. No-Git means outside a repository, not absence of the Git executable. Current ambient poetry/setuptools version override variables are absent. These are prepared assertions, not claims that the eventual published artifacts have already passed.

The empty-cache smoke scrubs AWS/LSMS/MICRODATA environment settings, overrides config and data roots, disables EC2 lookup and redirects AWS files to nonexistent private paths. Country/Feature calls in that smoke inspect declarations only; its quantity transform is synthetic. The later data smoke is explicitly separate: one ordinary warm Uganda assets call against the already prepared 172-file physical copy, with only immutable L1 available. Its gate checks the known fresh/full country cache hash before calling the API, and all private parquet hashes/metadata and installed package files must remain unchanged afterward.

Existing aggregate evidence supplies a full served assets expectation: 116,808 rows, index (i,t,j), float64 Value, fingerprint ef853b421deca593f6e5286a7925317689561515a48bb629234f94202ebdf714. This is the existing corpus fingerprint algorithm: SHA-256 of the order-sensitive pandas row/index hashes, with shape/columns/dtypes checked separately. The normalized Windows probe digest is 9202ed542a14 and uses a different algorithm. The 2019-20 served subset has 9,491 rows, but no independent served-subset fingerprint was saved. Its raw wave digest cab502dee8a55c0e19968b5fb6e11cd89c72c3fa11acac4ab58d7c1814bdad54 has index (i,j) and must not be confused with the post-read Country result. The prepared script therefore checks the complete existing served fingerprint instead of manufacturing a new subset baseline.

Uganda's relevant idiosyncrasy is the distinction between canonical raw assets and post-read categorical labels. The raw corrected script has zero duplicate keys, while the unchanged served frame has 4,237 duplicates after labels. The smoke checks that recorded behavior explicitly, not a false uniqueness assumption. CONTENTS records competing country-script/canonical builders; the earlier "assets still disagrees with its own script" is marked historical after the reviewed repair. No further country correction is inferred.

Prepared launch command, only after the coordinator supplies the verified installation identities:

```bash
/local/job39919092/venv_img/bin/python \
  /local/job39919092/release-work/control/runtime/published-data-smoke/run_installed_data_smoke.py \
  --expected-version 0.15.0 \
  --expected-sha VERIFIED_TAG_COMMIT \
  --expected-wheel-sha256 VERIFIED_PYPI_WHEEL_SHA256
```

The launcher uses the published-install-env interpreter, CPUs 0–7, one BLAS/build/make worker, an empty private config and the prepared data-installed-release-smoke root. The script checks the complete installed package against the pinned downloaded wheel and the recorded all-package Git identity gate. Original corpus, final promotion root and shared L2 are never API targets. Result JSON contains aggregates; any warning details stay in the private log. LSMS_SKIP_AUTH skips setup only and is not presented as credential-free source-build proof.
