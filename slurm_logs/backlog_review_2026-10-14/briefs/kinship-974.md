# RED-TEAM review of branch `fix/kinship-974` @ 1d77c6ff2 (element `kinship-974`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/kinship-974` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Senegal,Guinea-Bissau,EthiopiaRHS; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/kinship-974/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

## Mandate: REFUTE
Your job is to find the reason this branch must NOT be merged. Default to `verdict: fail` where you are uncertain. A fix agent wrote this branch and its report; the report is a self-report and its "SCOPE DEVIATIONS: none" is a claim, not a proof. Check, by running, not by reading:
1. **Does it fix the issue?** Reproduce the issue's symptom on `origin/development` code (a second clone: `git worktree add /local/job$SLURM_JOB_ID/base origin/development` is allowed -- that is your own scratch, not the shared mirror) and show it gone on the branch, in your private root, cold where the change is a build-path change (`.venv/bin/python -m lsms_library.cli cache clear --country X` against YOUR root; NEVER against the live one).
2. **Do the claimed numbers reproduce?** Every before/after figure in the report and commit messages: re-measure and tabulate claimed vs measured. A number you cannot reproduce is a finding.
3. **Data loss / laundering.** Row counts, index uniqueness, `grain_reports()`, `null_index_reports()`, `null_read_reports()` on every touched table before vs after; any `find lsms_library/countries -name '*.parquet'` hit; any write outside the private root.
4. **Healthy countries intact.** For a framework change: the full suite under the verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest tests -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > /local/job$SLURM_JOB_ID/pytest.log 2>&1`, then read the file), and `bench/feature_audit/scan.py --features <touched feature> --countries <2-3 warm ones>` where a feature is touched. For a country change: that country's tests plus `tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py`.
5. **Scope and stop-list.** `git diff --stat origin/development..HEAD` versus the task's named files; any baseline/golden/blessed/lockfile/dependency edit; any weakened, skipped or xfailed assertion (a moved pin is acceptable ONLY with before/after numbers in the commit message -- check they are there and that they match your measurement).
6. **Are the new tests meaningful?** Would each new test fail on `origin/development`? Run them there (the worktree) and say which do not.
7. **CONTENTS.org**: did the agent name the idiosyncrasy, and is what it wrote true? Anything it claims contradicts CONTENTS.org -- verify against the raw data.
8. **Cache cost claim**: is the stated re-warm scope right? (A `_/*.org`, `_/*.py`, `data_scheme.yml`, wave-module edit moves that country; `get_dataframe`/`_normalize_dataframe_index`-reachable framework edits move the corpus.)

Environment: `PYTHONPATH` is your clone; use `PYTHONSAFEPATH=1` for `-c`/`-m` runs only. Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`. `GH_REPO` is set; `gh` reads only, no writes. Read `lsms_library/countries/<C>/_/CONTENTS.org` for each country concerned.

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- You commit NOTHING and push NOTHING: this is a review. `git status --short` must be empty at the end (delete any scratch you made inside the clone).
- Write the report to `$STAGE/VERDICT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.


## Verdict format (write to `$STAGE/VERDICT.md` AND repeat as your final message)
Begin with one line: `VERDICT: pass | pass-with-fixes | fail | needs-human` and one sentence why. Then:
1. **Refutations** -- each a claim from the report/commit, what you ran, what you got (a claim that survives is listed as "held", with the command).
2. **Claimed vs measured** table.
3. **Regression** -- suite/scan summary lines verbatim.
4. **Scope findings**.
5. **Residual risks and what a human must decide** (the branch may be right and still need a decision).
6. **Recommended follow-up commits** (concrete, with paths), if `pass-with-fixes`.

## Rules the fix agent was under (2026-10-14): an open issue must describe exactly the remaining work -- if the branch resolves part of an issue, the report must DRAFT the residual; check that it did.

## The task the fix agent was given
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


## The branch: diffstat (vs development)
```
 lsms_library/categorical_mapping/kinship.yml      |  63 ++++++------
 lsms_library/countries/EthiopiaRHS/_/CONTENTS.org |   5 +-
 lsms_library/transformations.py                   |  35 +++++++
 tests/test_crop_kg_factor.py                      |  52 ++++++++++
 tests/test_kinship_yml_hygiene.py                 | 120 ++++++++++++++++++++++
 5 files changed, 241 insertions(+), 34 deletions(-)
```
## The branch: commit messages
--- 1d77c6ff2 EthiopiaRHS CONTENTS.org: record that the three dead kinship.yml typo keys are now deleted (#974)
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 809233da2 harvest_kg_factors: document and pin that the crop ladder is blind to declared u_kg (#931 items 1+3)
Docstring paragraph on harvest_kg_factors (under volume_as_mass) and a
comment beside _seeded_kg_factors's unit_kg loop stating the asymmetry:
only the food callers pass unit_kg; harvest_kg_factors seeds its metric
layer with unit_kg defaulted and _kg_factor_series cannot pass it, so a
country's declared u_kg rows never reach the crop ladder.  Both name why
it is load-bearing (Ethiopia's Quintal == 100.0 external check on the WB
shipped table, test_quintal_is_exactly_100kg_on_every_newly_served_row)
and that item 2 -- whether the crop ladder should read u_kg -- is a human
decision to land with a re-expressed check.

tests/test_crop_kg_factor.py::test_crop_ladder_is_blind_to_declared_unit_kg
pins it: harvest_kg_factors has no unit_kg parameter, and a `quintal` row
(kept out of KNOWN_METRIC and the parser by GH #919) is served `none`
while _seeded_kg_factors(df, unit_kg={'quintal': 100}) resolves it.

No behaviour change.  Cache cost: nil -- no transformations.py symbol
reaches any build fingerprint.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- fb369fcea kinship.yml: apply the #813 adjudication -- delete 15 dead keys, collapse 15 duplicate lines (#974, #945)
Mechanical application of slurm_logs/2026-09-18_kinship_813/ADJUDICATION.org
to lsms_library/categorical_mapping/kinship.yml:

- the 15 keys the adjudication rules dead for an orthographic reason are
  deleted (rows 3-9 of its table: ASCII/spacing twins of emitted EHCVM
  labels, the three EthiopiaRHS 1999 value-label typos whose decode path no
  longer exists, `Nurse, Assistant Girl`, the two Tajikistan short forms,
  `Son/Daughter Of Head Or Spouse`, the bare `Autres Parents Du Cm`, and
  both bare `Personne Non Apparent(é)e Au Cm` forms);
- the 15 redundant key lines (13 keys written 2-3x; yaml.safe_load kept the
  last silently) are collapsed to their first occurrence;
- the orphaned EthiopiaRHS comment and the French-block comment are
  reworded; the header records the two new rules.

Measured: 403 -> 388 entries, 418 -> 388 key lines (388 distinct); removed
set == the 15 adjudicated keys exactly; 0 surviving tuples changed
(before/after yaml.safe_load diff).  bench/kinship_reach.py on the three
locally warm countries (EthiopiaRHS, Guinea-Bissau, Senegal): reached 38
before and after, 0 labels seen but not in kinship.yml before and after.

NOT done, deliberately: folding REACHED spelling variants onto a canonical
key.  The adjudication names no canonical form for any variant group, and
the lookup is exact-match (strip + title, then raw), so deleting a reached
variant would make a survey label unknown.  That is #945's open
canonical-form decision.

tests/test_kinship_yml_hygiene.py gains: a text-level duplicate-key-line
test (the parser cannot see duplicates), a check that the text reader and
the parser agree, a guard that spelling variants of one word agree on
their tuple, and a pin that the 15 adjudicated keys stay out.

Cache cost: nil -- kinship.yml is read in _finalize_result on every read,
not at build (Country._table_cache_hash for household_roster byte-identical
on all three local countries).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


