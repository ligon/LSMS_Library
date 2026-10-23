#!/bin/bash
# Review command map only. Existing immutable outputs must not be replayed/overwritten.
# To reproduce from scratch, prepare a NEW private root and adapt exact root identities.
# Image: /local/job40255479/venv_img/bin/python; all executions use run.sh's
# exact cwd/PYTHONPATH/countries/config/data, socket/write guards, unshare-rn,
# numerical/build/Make threads1, max2 loadfile pytest workers.
# 1 bootstrap.py -> initial tracked literal-link snapshots / author READY gates
# 2 run.sh base|candidate default hash-inventory.py -> separate prebuild inventories
# 3 hash-scope.py ->549API/39framework/1362declared-wave/2139API-wave comparisons
# 4 prepare.py -> physical source MD5/SHA/size/mode/mtime/inode/nlink gates TERMINAL
# 5 bash run.sh base|candidate default|make build-v2.py cold
# 6 bash run.sh candidate make source-oracle.py
# 7 bash run.sh base|candidate default build-v2.py warm (strict support dtype stops)
# 8 bash run.sh base|candidate default|make finish-warm.py (Make strict mode stops)
# 9 extend-source-oracle.py; derived-v2.py; prepare-tests.py
# 10 bash run.sh candidate tests focused-tests.py
# 11 bash run.sh base negative negative-tests.py (expected36fail/3pass)
# 12 bash run.sh base|candidate default features.py initial|warm
# 13 feature-compare.py; scanner.py; scanner-account.py; cli-check.py
# 14 saved-frame-account-v2.py; identity-source-metadata.py; saved-date-tally.py
# 15 negative-account.py; lineage-diagnostics.py; warning-multisets.py
# 16 final-protection-v2.py; package-review.py; seal-review.py
# Exact original command arguments, scripts, XMLs, complete frames/endpoints and logs
# remain PRIVATE under this review root. Safe validation contains stages/counts/pins.
