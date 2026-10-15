SCOPE DEVIATIONS

None. Read-only review of frozen branch fix/1003-helper-root-isolation at 57eb12de5ef2e6d3400fb51a1bc0436c50ffd93a. No source/git/GitHub mutation, shared cache write, survey read or rebuild. All runtime checks used disposable synthetic sentinels and private data/config/pytest roots under this control directory, CPUs 8–15, BLAS/OMP/MKL and build workers set to 1.

Verdict: APPROVE.

The fixture is explicitly requested only by the two synthetic helper tests; it is neither autouse nor module-scoped. It sets its private LSMS_DATA_DIR, clears every data_root argument-cache entry before use, and clears again in finally. Its own MonkeyPatch context restores the prior environment before the caller resumes. countries_root is unchanged. No production cache policy or skip behavior changed.

Independently compared the complete Tier-2 delivered section against base 1973242b4: byte-identical. Therefore delivered tests keep their intended existing roots and assertions. This review did not run those destructive delivered-cache scenarios or claim new credentialed delivered coverage.

Independent before/after probe used the actual original helper and actual new fixture/functions, with asserted worktree import/config identity:
- Original helper with prewarmed B, called under A: deleted the synthetic B/Uganda/var/livestock.parquet sentinel. This demonstrates an unintended-root deletion, not merely an inspection of fixture spelling.
- Original helper with cold root cache: environment restored to B but data_root('Uganda') remained A/Uganda.
- Fixed fixture after both normal and exceptional body exit: B sentinel survived, environment returned to B, and both data_root() and data_root('Uganda') resolved B correctly. The JSON helper's private sentinel also survived.
- Independent pytest selection of both helpers and both new normal/exception regression cases: 4 passed, 0 skipped, 8.28 s, -n 6 --dist=loadfile. No survey builds.

Also read the committed failing-before controls: omitting only entry clearing gives two failures on actual B-sentinel deletion; omitting only finally clearing gives two failures on the leaked A root. The author's broader targeted run is 10 passed / 10 explicit absent-warm-cache skips; those ten skips are transparently the unchanged delivered tests, not claimed passes. The new regression exercises the real absent helper and the real fixture lifecycle, so it discriminates the two cache-state defects.

Evidence: 1003-independent-probe.json and 1003-independent-pytest.log. Reviewed committed report/ledger, discovery-probe.json, both negative logs and validation.json. Worktree stayed clean. This repair does not establish the historical #941 failure's cause.
