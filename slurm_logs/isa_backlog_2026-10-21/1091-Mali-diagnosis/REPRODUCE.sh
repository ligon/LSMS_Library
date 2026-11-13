#!/bin/bash
# Historical command forms for READ-ONLY diagnosis at aba2badfe.
# Do not rerun preparation or overwrite these frozen results. New reviewers
# need new physical private roots and separately approved metadata targets;
# adapt every pinned launcher/identity/guard path consistently.
set -eu
p=/local/job40255479/control/isa-backlog-20261021/Mali/1091
python_image=/local/job40255479/venv_img/bin/python
bash "$p/run.sh" prep "$p/protect.py" before
bash "$p/run.sh" prep "$p/prepare.py"
# Terminal complete26-copy MD5/SHA/mode/mtime/inode gate BEFORE decoding/API.
bash "$p/run.sh" prep "$p/static.py"
bash "$p/run.sh" default-all "$p/probe.py" cold
bash "$p/run.sh" default-all "$p/probe.py" warm
# Original Make cold call completed the normal API and saved its frame, then
# the original strict full-attrs equality stopped. Preserve the log/script.
set +e
bash "$p/run.sh" make-all "$p/probe-initial-attrs-stop.py" cold
attrs_status=$?
set -e
test "$attrs_status" -eq 1
# Expected nonzero: sole absent id_converted attr, not a value/type mismatch.
# The actual follow-up reads that completed public frame and raw caches;
# it does NOT relaunch a cold call over the populated root or stamp anything.
bash "$p/run.sh" make-all "$p/probe.py" recovercold
bash "$p/run.sh" make-all "$p/probe.py" warm
bash "$p/run.sh" default-valid "$p/probe.py" cold
bash "$p/run.sh" default-valid "$p/probe.py" warm
bash "$p/matrix.sh" default 2017 2018 2021 unsupported
bash "$p/matrix.sh" make valid 2017
bash "$p/matrix.sh" make 2018 2021 unsupported
bash "$p/run.sh" default-unsupported "$p/probe.py" warm
bash "$p/run.sh" make-unsupported "$p/probe.py" warm
bash "$p/run.sh" default-all "$p/extras.py" feature
bash "$p/run.sh" make-all "$p/extras.py" feature
bash "$p/run.sh" default-valid "$p/extras.py" cli
bash "$p/run.sh" make-2018 "$p/extras.py" cli
bash "$p/run.sh" prep scripts/api_surface_audit.py --strict
bash "$p/run.sh" prep "$p/analyze.py"
bash "$p/run.sh" prep "$p/protect.py" after
"$python_image" "$p/write_report.py"
# After all data/API/protection consumers have exited:
"$python_image" "$p/release.py"
# Every actual run used corresponding private stdout/stderr logs. Independent
# roots may parallelize after successful preparation; one consumer/root.
# No candidate implementation/tests or postchange549/39 measurement occurred.
