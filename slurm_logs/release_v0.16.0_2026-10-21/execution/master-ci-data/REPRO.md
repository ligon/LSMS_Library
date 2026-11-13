# Read-only later terminal proof

The stopped observer did not cancel or finish the GitHub job. The published
release does not wait for this duplicate run. Inspect its later state via the
linked job or use the following read-only commands in a NEW private directory.
Copy `observe.py`, `reconcile.py` and `progress.py` there first. Keep raw outputs
private. Use a repository cwd containing git object 097283459f80016b0f00a51b814a0cb964a42789
for the pinned-source `git show` checks; do not import or execute project code.

```sh
gh api repos/ligon/LSMS_Library/actions/runs/39604418333 > run-private.json
gh api 'repos/ligon/LSMS_Library/actions/runs/39604418333/jobs?per_page=100' > jobs-private.json
gh api repos/ligon/LSMS_Library/actions/jobs/119236827295 > data-job-private.json
# Only after the job is terminal:
gh api --allow-escape-sequences repos/ligon/LSMS_Library/actions/jobs/119236827295/logs > data-job-private.log
chmod 600 data-job-private.log
```

`reconcile.py` derives paths from its own directory. After terminal snapshots and
log are present there, run `/local/job40255479/venv_img/bin/python /absolute/private/path/reconcile.py`
with assertions enabled, from the repository cwd. The interpreter path is local
to this allocation; use a normal Python 3 interpreter if fetching later elsewhere.
The controller checks exact run/checkout identity and expected cohort counts;
any mismatch is a proof problem, not automatically a library defect.
It does not enumerate every skip reason or observe the BLAS kernel.
No dispatch, rerun, cancellation, test or data API is required.
