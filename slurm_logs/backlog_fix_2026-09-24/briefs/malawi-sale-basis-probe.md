# Diagnosis element `malawi-sale-basis-probe` -- can PRICE decide a sale's shelled/unshelled basis where the survey did not record it?

You are a worker agent in a Slurm job with your OWN clone of `fix/malawi-bundle` (your cwd, on a new branch `fix/malawi-sale-basis-probe`), your OWN mounted venv, and a PRIVATE `LSMS_DATA_DIR` seeded with Malawi's warm parquets (built from `development`, i.e. BEFORE the bundle; the bundle's `crop_production` rebuild is cheap and in scope in your root). `dvc-cache` is a read-only symlink to the blob store.

## Guardrails
**Out of scope -- STOP and report if the task would require these**:
- Modifying any file under `tests/fixtures/`, `.coder/coverage/`, or any file whose name contains `baseline`, `golden`, `expected`, `snapshot` or `blessed`
- Modifying `pyproject.toml`, `poetry.lock`, `requirements*.txt`, `dvc.lock`, any `.dvc` sidecar, or any dependency pin
- Weakening, skipping, or xfailing any existing test assertion (moving a pinned baseline the fix legitimately moves is allowed ONLY with the before/after numbers in the commit message)
- Running any "regenerate baseline"/"update snapshot"/coverage-matrix script, or `lsms-library cache clear` against any root other than your private `LSMS_DATA_DIR`
- Any `gh` WRITE (issue close/comment/label, pr create/merge). `gh` READS are fine (`GH_REPO` is set).
- Any git push to a branch other than your own `fix/<key>` on `origin` (the shared mirror); never push to `development`; never force-push; never run checkout/reset/stash/clean on any checkout but your own clone
- Touching files outside the paths the task names, and any DVC CLI invocation (read via `get_dataframe` / `get_data_file` only)
If any of these become necessary, STOP, leave the clone in a committable state, and put the conflict at the top of your report.

## Environment facts
- `PYTHONPATH` is your clone. For `python -c` / `-m` runs add `PYTHONSAFEPATH=1` so the clone is the imported package; do NOT export `PYTHONSAFEPATH` into `make`/wave-script subprocesses (338 wave scripts import a sibling by bare name; commit 6df53a5c6). Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`.
- Cores: `$SLURM_CPUS_ON_NODE` (8). Test recipe, verbatim: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest <targets> -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > <logfile> 2>&1` -- write pytest output to a file, never through a pipe. Never `-n auto`.
- A rebuild of one of your countries' tables is allowed and lands only in your private root. `find lsms_library/countries -name '*.parquet'` must stay empty (GH #803).
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/malawi-sale-basis-probe` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.

## Ruling from @ligon (2026-09-24), which this task serves
"Malawi: shelled vs unshelled is really a missing data problem, decipherable by looking at prices perhaps."  So the `shell_not_applicable`-vs-S/U class that `fix/malawi-bundle` suppresses as contradictions (464 sales, 40.8M MWK, mostly Tobacco and Cotton; see `slurm_logs/backlog_fix_2026-09-24/verdicts/malawi-bundle.md` section 5.1 and `reports/malawi-bundle.md`) is NOT to be adjudicated by a rule (wildcard vs contradiction).  A sale with no recorded basis has a MISSING basis, and the question is whether its unit price -- `Value_sold / Quantity_sold` per native unit -- identifies the basis, because a shelled unit of a crop sells at a predictable premium over an unshelled unit of the same crop in the same unit and wave.

## Your task: MEASURE, do not implement
This is a read-only diagnosis (Tier B, diagnose-then-fix).  Change no library code.  Your only commits are a measurement note and the probe scripts under `slurm_logs/backlog_fix_2026-09-24/malawi_sale_basis/` (README.org + scripts + outputs), pushed on your branch.
1. Read `lsms_library/countries/Malawi/_/CONTENTS.org` (prose AND LOGBOOK entries; especially lines ~201-217 and the bundle's new entries) and `malawi.py`'s `_sale_block` / the harvest-sale attachment code the bundle wrote.  Name the idiosyncrasies in your report.  Read `.coder/ledger/STANDING.md` and `docs/guide/derivations.md`: any inference you propose must be a REGISTERED derivation (`countries/Malawi/_/derivations.yml`, `Derivation` column) -- see CLAUDE.md "Derived Values".
2. Build the per-sale frame from the RAW modules through the bundle's own reader (Module I and Q for all four waves), one row per sale with: wave, hid, crop, unit, quantity, value, sale basis (S/U/NA/unknown), the harvest-side basis of the matching harvest row(s), and unit price = value/quantity.
3. For each (wave, crop, unit) cell with >= 10 sales on EACH known basis: the median unit price shelled vs unshelled, the ratio, the interquartile ranges, and whether the two distributions overlap (report the fraction of shelled prices above the unshelled median and vice versa).  Then the same with the unit dropped (wave, crop) and with the wave dropped (crop, unit), so we know at what grain the separation exists.  Groundnut, Maize, Rice, Soyabean are the crops with genuine S/U contradictions; Tobacco and Cotton dominate the NA class -- treat them separately and say whether the notion of shelling even applies (for cotton "shelled" may be ginned; say what the instrument's option means).
4. Classify every sale whose basis is missing (NA / not applicable / unknown on the sale side) by nearest-basis price: which basis its unit price is closer to (in log terms), the margin, and how many are unambiguous (say, closer by more than half the log-gap between the two medians).  Tabulate per wave and crop: count, MWK, share unambiguous.
5. Do the same for the 642 genuine contradictions: does the PRICE side with the harvest row's basis or the sale row's stated basis?  If price sides with one consistently, that is evidence about which field is the reliable one.
6. Report what the harvest side looks like for Tobacco/Cotton (does the harvest row carry a basis at all?), and how many of the 464 NA sales would attach under (a) price-nearest basis, (b) unambiguous only.
7. Propose, in the README, the derivation: key, rule, inputs, raw_variables, assumptions with a `basis` each, the threshold you would use and why, what stays suppressed, and the expected per-wave counts and MWK.  State the cache cost of implementing it.
Every number with the command that produced it; scripts committed so the next agent can re-run them.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
