# Common rules for every worker in the 2026-11-13 ISA batch

You are a worker for Sue, the coordinator (Slurm job 41201840 on n0035.savio4).
Read this whole file before doing anything.  Your stream brief says what to do;
this file says how.  If the two disagree, stop and report.

## Where things are
- Base for every stream: development `f138a3d61c203f892148592675a7ef72e160a322`
  (the v0.17.0 tree; master `cf09ea6b4` has the same tree).
- Your worktree and branch are named in your brief.  Edit and commit ONLY there.
  A post-commit hook publishes each commit to the shared mirror
  (`/global/home/users/ligon/mirrors/LSMS_Library`); check that the commit
  output says `SUCODER: published`.  If it says REJECTED, stop and report.
- Report directory: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/<stream>/`.
  Write `REPORT.md` there (and `PR_BODY.md` if your brief asks).  Put job
  scripts in `scripts/`, logs in `logs/`, outputs in `out/`.
- Rulings: `git show docs/isa-sweep-20261103:.sucoder/isa-decisions.org` (newest
  first).  Quote the ruling you implement; do not widen it.
- Repo rules: the repo's `CLAUDE.md`.  Country rules: see "Country files" below.

## Hard prohibitions
- Do not spawn sub-agents, forks or workflows.
- Git: no `checkout`/`switch`/`reset`/`stash`/`clean`/`restore`/`rebase`/
  `branch -D`/`merge` anywhere except inside your own worktree (and there, no
  `reset --hard`, no history rewrite).  Never touch the mirror's working tree;
  never run any git command inside the mirror directory (not even `fetch`).
  Never push to GitHub; Sue does.  Never force-push.
- Never invoke the `dvc` CLI.  Never delete a DVC lock.
- On n0035 (this node) run NO python, pytest, make, or data processing.
  Reading and editing source with Read / Edit / Grep / git diff is fine.
  Everything that imports `lsms_library` runs in a burst job.
- No shared caches: never use `/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library`
  or `~/.local/share/lsms_library`, and never rebuild or clear a shared cache.
- No production-weight or panel-ID change unless your brief cites the ruling
  that authorizes it.
- Do not regenerate shared generated files: recall records, the coverage
  snapshot (`.coder/coverage/latest.csv`), `PARTIALLY_WIRED_SAMPLE` and similar.
  If a test needs one regenerated, say so in the report; Sue does it once at
  integration.
- The data principle (@ligon 2026-11-12): "Without a very good reason we don't
  want to serve up someone else's undocumented imputations."

## Burst jobs (all Python)
- Submit with
  `sbatch -A co_carleton -p savio4_htc -q carleton_htc4_normal -N 1 -n 1 -c <4-16> --mem=<GB>G -t <h:mm:ss> --exclude=n0035.savio4,n0034.savio4,n0046.savio4 -o <report>/logs/%x-%j.out <script>`.
  Keep jobs small and short; many small jobs beat one fat one.  co_carleton is
  pre-emptible (a job may requeue): make scripts restartable.  Cancel your own
  jobs when done; never touch anyone else's.
- In the job script: `source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh <stream> <cand-sha> [<base-sha> ...]`.
  It sets private roots BEFORE the venv mount and any import, clones each SHA
  from the mirror into the job's own `/local`, mounts the venv, and asserts
  import identity and a private data root.  A burst job cannot see n0035's
  `/local`; commit first so the mirror has your SHA.
- pytest: `-n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider`,
  output to a file.  Never `-n auto`.  BLAS pins and `LSMS_BUILD_WORKERS=1` are
  set by burst-env.sh.
- Verification is base-vs-candidate in separate private empty roots (cold).
  The WB API key config is read from `~/.config/lsms_library/config.yml`; that
  is the only credential path, so leave `XDG_CONFIG_HOME` alone.

## Country files
- Before editing anything for a country, read `lsms_library/countries/{C}/_/CONTENTS.org`
  in full, including LOGBOOK / TODO / WAITING entries, and the docstring of any
  script you edit.  In your report, NAME the idiosyncrasies you found that bear
  on your change ("I checked" is not an answer).
- If a finding contradicts CONTENTS.org, report it and quote the passage; do
  not assume the file is stale.  Add any genuine idiosyncrasy you discover.

## Derivations and disclosure
- A served construction is a registered derivation (`countries/{C}/_/derivations.yml`,
  `Derivation` stamped after any collapse, an `inputs` callable that joins to
  the served rows, `rows: 0` entries for deletions).  See CLAUDE.md
  "Derived Values".  `derivations.BASES` order: questionnaire > variable-label >
  config-comment > derived-from-data > filename > modelling-choice.

## What every report must contain
1. Commits (full SHAs) and the branch head.
2. Idiosyncrasies named, per country.
3. Measurements, with the exact commands / job ids and where outputs are.
4. A hash census, base `f138a3d61` -> your head (`tools/hash_census.py` +
   `tools/hashdiff.py`), with counts moved per kind and per country.  Anything
   moving outside your countries, or a `build_transforms_fingerprint` move, is
   a FINDING to explain, not a footnote.
5. Tests run: files, command, pass/fail/skip counts.  Tests not run, and why.
6. For each issue: closes / partial.  For a partial, a drafted residual
   (title, body, tier) -- you cannot write to GitHub.
7. Questions for the user that the rulings do not answer.  Do not guess a
   ruling; stop at the question and report it.
An independent red team reviews your branch afterwards; do not self-certify.

The harness refuses REPORT.md / PR_BODY.md writes from sub-agents.  Return
both, in full, as your final message (Sue saves them); do not work around it.
Scripts, logs and `out/` files are fine.
