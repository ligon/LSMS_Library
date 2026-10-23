#!/bin/bash
# Artifact-only command transcript. Never overwrite frozen roots; a reviewer
# needs a NEW owner-only private root and must adapt all launcher/guard paths.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/Uganda/849-exact-addendum
if test "${NEW_ARTIFACT_ROOT_ADAPTED:-}" != yes; then
  echo 'Frozen artifact evidence: adapt a new private root before replay.' >&2
  exit 2
fi
# The original policy is copied with only the private-root literal changed;
# its metadata exception must remain wholly inside the NEW private root.
# Bootstrap runs before any record import/unpickle. No old script is run.
bash "$P/run.sh" "$P/protect.py" before
bash "$P/run.sh" "$P/inventory.py"
bash "$P/run.sh" "$P/exact_addendum.py"
# Preserve initial checker outputs before the four expected-dtype corrections:
# initial-validation-with-native-dtype-stops.json and
# initial-four-source-dtype-stops-private.json are immutable actual receipts.
# The final bounded follow-up repeats ONLY the four 2011 operands, using the
# frozen oracle's native source to_numpy operation, not observed-result dtype.
bash "$P/run.sh" "$P/verify_four_operands.py"
bash "$P/run.sh" "$P/protect.py" after
# Read validation.json and complete comparator/checker sources. No pytest,
# Country/Feature API, helper/finalizer/ladder, source decode or acquisition.
# Actual terminal original receipts:344Uganda+22safe,exports all347files;
# all14880sourcecontents/modes/index/HEAD/clean;1309Mali receipts/all1311files.
# At final release, freeze.py verifies input receipts/quiescence and seals
# only the NEW root. Never rerun original Uganda/849 or Mali/1091-fix scripts.
