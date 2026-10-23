#!/bin/bash
# GH849 exact historical commands and a READ-ONLY sealed-proof checker.
# Do not replay any build/test command into the frozen author evidence root.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/Uganda/849-fix
R=/local/job40255479/worktrees/isa-Uganda-849-fix
# With READY present, this default command verifies all sealed bytes/modes/
# mtimes, literal Git symlinks, source HEADs/clean status and safe allowlist.
# It decodes no source/frame, invokes no table API/test/CLI and writes no file.
cd "$R"
unshare -rn env PYTHONDONTWRITEBYTECODE=1 GIT_OPTIONAL_LOCKS=0   PYTHONPATH="$R:$P" LSMS_COUNTRIES_ROOT="$R/lsms_library/countries"   OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1   LSMS_BUILD_WORKERS=1 MAKE_JOBS=1   /local/job40255479/venv_img/bin/python "$P/read-only-recheck.py" "$P"

# Historical executed sequence, pinned as checkers.json; a different reviewer
# must prepare a NEW physical private root, its own strict guard/metadata links
# and immutable source checkouts before reusing/adapting these checkers.
# All calls used bash "$P/run.sh" <base|candidate> <dataset> <absolute-script>.
# 1. candidate/default prepare.py; confirm-source-fields.py (terminal gates)
# 2. separate base/default and candidate/default hash-inventory-v2.py;
#    candidate/default hash-scope.py BEFORE any Country table build.
# 3. new-tests.py on each exact source; new file24pass, actual-parent20fail4pass.
# 4. build-cold.py cold on base/candidate x default/make, distinct roots.
# 5. candidate/default source-factor-oracle-v2.py (literal row/slot oracle).
# 6. warm.py on base/candidate x default/make, fresh processes/new objects.
# 7. candidate/default derived-v2.py; no cache/source bypass.
# 8. features.py initial then warm, CLI cli-check.py, scanner.py on each
#    default root sequentially; candidate/default feature-compare.py.
# 9. candidate/default prepare-tests.py; candidate/tests focused-tests.py:
#    pytest -q -n2 --dist=loadfile, 426cases,416pass10data skips.
# 10. compare-stages.py; populated subsets-v2.py on each root/backend.
#     Initial base/default subsets.py supported call stopped on typed attrs;
#     continuation skips that completed prefix and saves separate expected
#     reconstruction, preserving all actual/warning/cache endpoints.
# 11. warning-account.py; finish-artifacts.py; verify-protected.py.
# 12. package-safe-v2.py (original empty-directory stop retained); final static
#     source/cache proof, attributed docs/evidence commit, seal-author.py.
# No whole corpus, broad suite, source acquisition, manual stamps, cache clear,
# NO_CACHE/assume-fresh, DVC CLI, shared mutation or FDC call was used.
