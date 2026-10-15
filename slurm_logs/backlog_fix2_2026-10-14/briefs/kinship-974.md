# Backlog fix element `kinship-974` -- issues #974 #945 #931

You are a worker agent in a Slurm job with your OWN clone of `development` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the warm parquets of your countries; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else; the live cache must not be touched.

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
- `PYTHONPATH` is your clone. For `python -c` / `-m` runs add `PYTHONSAFEPATH=1`; do NOT export it into `make`/wave-script subprocesses. Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`.
- Cores: `$SLURM_CPUS_ON_NODE` (a whole node). Test recipe, verbatim: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest <targets> -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > <logfile> 2>&1` -- output to a file, never a pipe. Never `-n auto`.
- `find lsms_library/countries -name '*.parquet'` must stay empty (GH #803).
- Commit as you go; end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/kinship-974` (origin is the shared mirror; no GitHub push, no PR from here).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org`.
- Read `CLAUDE.md` section headings and the sections your files touch; read `.claude/skills/backlog-workflow/SKILL.md` for the rule that an open issue must describe exactly the remaining work: if your fix resolves PART of an issue, DRAFT the residual (title + body + tier) in your report.

## Scope note from the scrum master
#974 is the mechanical application of `slurm_logs/2026-09-18_kinship_813/ADJUDICATION.org` to kinship.yml -- read it in full first; every surviving key's [Generation, Distance, Affinity] tuple stays byte-identical (prove it with a before/after diff of the decoded tuples). #945 is CLOSED into #974; its remaining auto item (the duplicate-key-line text test) and #931's docstring items ride along. Cache cost nil (read-path); verify with bench/kinship_reach.py and tests/test_kinship_yml_hygiene.py.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #974 -- kinship.yml: apply the #813/#945 adjudication -- canonicalise the reachable spelling variants
```json
{
 "tier": "A",
 "type": "framework",
 "tractability": "auto",
 "dedup_status": "open (filed 2026-10-14 as the residual of #813/#945)",
 "priority": 3,
 "summary": "Residual of #813 and #945, which are closed; both left the same half undone and this is it.\n\n**Done so far**\n- PR #954 (379b9e07b): the two pipeline defects fixed at source (`''` from EthiopiaRHS, `'Nan'` from Senegal).\n- PR #951 (b0e61fa23): the 70 keys the lookup can never reach removed, with a test that keeps that count at zero.\n- 717258b68: the per-key adjudication, `slurm_logs/2026-09-18_kinship_813/ADJUDICATION.org` -- 473 keys, 343 reached, 130 unreached (not 131); the EHCVM \"translated before kinship\" hypothesis refuted (EHCVM countries hand kinship the raw French/Portuguese label; the keys are dead for an orthographic reason: instruments write comma plus both genders, the dictionary guessed bare words and slashes); the three EthiopiaRHS keys dead rather than a live mis-decode.\n\n**Remaining (this issue)**\nApply the adjudication to `lsms_library/categorical_mapping/kinship.yml`: for each reachable spelling-variant group, pick the canonical key per ADJUDICATION.org and fold the variants onto it; delete the keys it rules dead for the orthographic reason; keep the `[Generation, Distance, Affinity]` tuple of every surviving key byte-identical (a canonicalisation must not change a decomposition -- that is #698/#804's business, not this one's). Mechanical given the adjudication; no human decision expected.\n\n**Verify**\n`bench/kinship_reach.py` before/after (reached count must not fall; unreached must fall to the adjudicated set); `tests/test_kinship_yml_hygiene.py` stays green; a `Feature('household_roster')` warm read emits no new unrecognised-label warning. Cache cost: nil -- kinship expansion runs in `_finalize_result` on every read (CLAUDE.md, \"Kinship decomposition\").\n\nTier A, auto-dispatchable.",
 "evidence": "slurm_logs/2026-09-18_kinship_813/ADJUDICATION.org; PR #951, PR #954",
 "approach": "apply ADJUDICATION.org to lsms_library/categorical_mapping/kinship.yml (see body)",
 "verify": "bench/kinship_reach.py before/after; tests/test_kinship_yml_hygiene.py; cache cost nil (read path)",
 "contents_org": "n/a (framework)",
 "blocked_by": "none",
 "decision_for_human": "none"
}
```

### #945 -- kinship.yml has no canonicalisation rule: 157 of 473 keys are spelling variants and 70 are unreachable by construction
```json
{
 "tier": "C",
 "type": "test",
 "tractability": "diagnose-then-auto",
 "dedup_status": "partial (PR #951, merge b0e61fa235: 70 unreachable keys removed, tests/test_kinship_yml_hygiene.py keeps that at zero; Sue's partial comment posted 2026-10-14)",
 "priority": 4,
 "summary": "Re-measured today on a7cf04f2d: 403 entries, 331 distinct modulo case/whitespace/punctuation, 72 pure spelling variants (was 157), 0 keys shadowed by a .title() twin (was 70). The structural half is done and pinned (test_no_key_is_shadowed_by_a_title_cased_twin). Still open: the rule for the 72 variants (option 1 'a test' needs a canonical form; the #813 adjudication warns .title() mangles apostrophes -- Lodger'S -- so .title() cannot be the canonical form). One free, unpinned hygiene item from the #813 adjudication: the file has 418 key lines for 403 keys -- 13 keys written 2-3x (Father/Mother x3, Sister/Brother x3, Niece, Cousin, Stepchild, Lodger, ...); yaml.safe_load keeps the last silently. All agree today.",
 "evidence": "python: yaml.safe_load + regex over lsms_library/categorical_mapping/kinship.yml (entries 403, variants 72, unreachable 0, duplicated keys 13); tests/test_kinship_yml_hygiene.py:32-43; slurm_logs/2026-09-18_kinship_813/ADJUDICATION.org (717258b68).",
 "approach": "Auto now: add test_no_key_line_is_duplicated to tests/test_kinship_yml_hygiene.py (read the file as TEXT, since safe_load cannot see duplicates) and collapse the 15 redundant lines (all 13 agree on their tuple -- verify in the same test before deleting). Then the rule: assert no two keys are equal under the lookup's own normalisation (strip + casefold + collapse whitespace/punctuation) -- NOT .title() -- after a one-time fold of the 72 variants that keeps the spelling a survey actually emits (bench/kinship_reach.py on a warm cache tells you which).",
 "verify": "pytest tests/test_kinship_yml_hygiene.py tests/test_kinship_other_relative.py; bench/kinship_reach.py on the shared warm cache (read-only) to confirm reached-set unchanged. CACHE COST: zero -- kinship.yml is read at _finalize_result (CLAUDE.md 'Cache vs. API'), not at build.",
 "contents_org": "n/a (framework)",
 "blocked_by": "none for the duplicate-line test; the variant rule needs the canonical-form choice",
 "decision_for_human": "Canonical form for kinship keys: the spelling the corpus actually emits (per bench/kinship_reach.py), with a casefold-equality test -- yes/no?"
}
```

### #931 -- harvest_kg_factors is blind to declared `unit_kg`, and that asymmetry is load-bearing but undocumented
```json
{
 "tier": "C",
 "type": "docs",
 "tractability": "diagnose-then-auto",
 "dedup_status": "open (filed-not-fixed in docs/releases/v0.14.0.md:265; nothing landed)",
 "priority": 4,
 "summary": "Claim holds: transformations.py harvest_kg_factors (:3768) still calls _seeded_kg_factors(df, volume_as_mass=volume_as_mass) with unit_kg defaulted (:3984) and takes no unit_kg parameter; the word unit_kg does not appear inside it. The nearby comment (:3975-3990) explains the #929/#838 metric-layer asymmetry and names the Ethiopia quintal check, but does not say the crop ladder ignores declared u_kg rows or that a test depends on it. No test pins the asymmetry (grep '931' tests/ -> none). tests/test_ethiopia_shipped_factors.py:397 test_quintal_is_exactly_100kg_on_every_newly_served_row is the check that would go red.",
 "evidence": "grep -n 'def harvest_kg_factors|def _seeded_kg_factors' lsms_library/transformations.py; grep unit_kg within the function body (none); grep -rn 931 tests/ (none); docs/releases/v0.14.0.md:264-267; Ethiopia/_/CONTENTS.org:1591.",
 "approach": "Auto (items 1+3): a docstring/comment block on harvest_kg_factors and on _seeded_kg_factors's unit_kg parameter stating the asymmetry, why (Ethiopia's external check), and the test that depends on it; a test that calls harvest_kg_factors on a frame with a u_kg-declared unit and asserts the factor is NOT applied, with a message naming #931 and the quintal check. Item 2 (should the crop ladder read u_kg?) is a human decision and must be landed together with a re-expressed external check.",
 "verify": "pytest tests/test_ethiopia_shipped_factors.py tests/test_gh919_country_qualified_units.py plus the new pin. CACHE COST: zero -- no transformations.py symbol reaches any build fingerprint (tests/test_build_transform_hash.py::test_no_transformations_symbol_reaches_any_fingerprint).",
 "contents_org": "Ethiopia CONTENTS.org:1591: the real external check on the WB shipped factor table is Quintal == 100.0 on 13,924 of 43,384 newly served rows; it is meaningful only while the library holds no independent quintal opinion on the crop ladder.",
 "blocked_by": "none for items 1+3",
 "decision_for_human": "Should the crop ladder read declared u_kg (and the Ethiopia external check be re-expressed in the same PR) -- yes/no? (Docs + pin can land first either way.)"
}
```

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
