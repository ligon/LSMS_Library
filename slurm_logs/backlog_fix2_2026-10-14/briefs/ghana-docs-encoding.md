# Backlog fix element `ghana-docs-encoding` -- issues #926 #880 #812

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
- Commit as you go; end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/ghana-docs-encoding` (origin is the shared mirror; no GitHub push, no PR from here).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org`.
- Read `CLAUDE.md` section headings and the sections your files touch; read `.claude/skills/backlog-workflow/SKILL.md` for the rule that an open issue must describe exactly the remaining work: if your fix resolves PART of an issue, DRAFT the residual (title + body + tier) in your report.

## Scope note from the scrum master
#926 is a CONFIG repair in GhanaLSS/_/categorical_mapping.org (replace the committed C1 code points with the intended characters; the #805 reader fix will not clear them) -- an `_/*.org` edit moves every GhanaLSS table hash: state the GhanaLSS re-warm, verify nonfood_expenditures `j` has 0 C1 code points on a cold private build. #880 and #812 are CONTENTS.org corrections only (quote the stale text you replace).

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #926 -- GhanaLSS nonfood_expenditures serves 25,585 rows with cp1252-as-Latin1 C1 controls in j; the bad code points are committed in curated config, so the #805 reader fix will not clear them
```json
{
 "tier": "B",
 "type": "bug",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 3,
 "summary": "Claim holds: warm GhanaLSS/var/nonfood_expenditures.parquet has 25,585 rows / 27 distinct j with C1 controls, all 2016-17; the country org file has 28 C1 cells (nonfood_items Preferred Label + Label_2016-17), and GhanaLSS/2016-17/_/categorical_mapping.org has 3 more in harmonize_price_item (lines 915/932/954) which reach community_prices.j AND .Description (927 rows each, 2016-17) -- an instance the issue did not count. PR #949 (merged 2026-09-18) explicitly 'reported, not touched' #926 and listed the other countries' C1 cells. The MECHANISM is slightly different from the issue's: g7sec9a.dta is format 118 (<stata_dta> header), i.e. UTF-8-declared, and its STRING DATA carries raw cp1252 bytes -- 41,803 'Women\\x92s' beside 97,531 clean \"Women's\" in the same file -- so pandas' per-string latin-1 FALLBACK (not the pre-118 default rule) produced U+0092 -- measured: pd.read_stata on the blob raises NO UnicodeWarning on pandas 3.0.6 and 390,140 of 7,077,839 s9aname values carry a C1 control. Consequence: #805's 'cp1252-first for format<118' would not repair a re-derivation of this crosswalk either; the fix is the config repair the issue asks for. Corpus sweep answer to the issue's last paragraph: C1 in curated org config also exists in Mali/_/categorical_mapping.org harmonize_food Code (rows 362/374), Niger/_/categorical_mapping.org harmonize_food Original Label (rows 921/922), Burkina_Faso harmonize_food Original Label (296/303/304) -- all KEY columns (load-bearing, #794's strategy B), and Uganda/_/categorical_mapping.org u table wave-label columns (325-329; u is keyed on Code, so inert). GhanaLSS is the only served-Preferred-Label instance. community_prices.j and .Description are both built from the org table (community_prices.py:65-69 code.map(items['Preferred Label']) / items['Label']), so the 927-row instance is config-side too, not a read-time artefact.",
 "evidence": "pyarrow read of GhanaLSS/var/nonfood_expenditures.parquet (25,585 rows, 27 distinct, t=2016-17); $STAGE/served_mojibake_scan.txt (community_prices j/Description 927); grep -c -P '[\\x{0080}-\\x{009F}]' on GhanaLSS/_/categorical_mapping.org (28) and 2016-17/_/categorical_mapping.org (3); raw blob of GhanaLSS/2016-17/Data/g7sec9a.dta: header b'<stata_dta>', count(b'Women\\x92s')=41,803, count(b\"Women's\")=97,531; GhanaLSS/2016-17/_/nonfood_expenditures.py:99 decodes codes through nonfood_items Preferred Label; gh pr view 949 body §'GH #926 -- reported, not touched'.",
 "approach": "Config repair, one PR: in lsms_library/countries/GhanaLSS/_/categorical_mapping.org replace U+0092 -> \"'\", U+0096 -> '-', U+0094 -> '\"' in the 28 nonfood_items cells (Preferred Label and Label_2016-17 consistently, keeping Preferred Labels injective -- the script asserts it at nonfood_expenditures.py:108), and the 3 harmonize_price_item cells in GhanaLSS/2016-17/_/categorical_mapping.org. Add a test that no curated .org under countries/ or categorical_mapping/ carries a codepoint in U+0080-U+009F (fails today on GhanaLSS, Mali, Niger, Burkina_Faso, Uganda -- so either scope it to Preferred Label columns or land the other re-keys with #794/#805). Record the mixed-encoding-in-one-file fact in GhanaLSS/_/CONTENTS.org.",
 "verify": "Cold build of GhanaLSS nonfood_expenditures and community_prices in a private root; assert 0 j values match [\\x80-\\x9f]; row counts unchanged (977,193 / 660,168); tests/test_ghanalss_* pass. CACHE COST: _/categorical_mapping.org is a build-time *.org input in every GhanaLSS table's hash (CLAUDE.md v0.8.0 staleness list) -> full GhanaLSS re-warm (its food_acquired alone is a 167 s warm read; cold is minutes), no other country.",
 "contents_org": "GhanaLSS/_/CONTENTS.org has NO entry for this; the nearest is line 3453 (2012-13 code 262's Aggregate 'Cooked Rice and Stew' + two mojibake Ê, a Mac-Roman NBSP read as latin-1 -- a different artefact). The idiosyncrasy to add: g7sec9a.dta is a format-118 file whose item-description strings mix cp1252 bytes with clean ASCII for the same words.",
 "blocked_by": "none",
 "decision_for_human": ""
}
```

### #880 -- GhanaSPS/_/CONTENTS.org still documents wave-1 MonthsAway as 'not wired' and 'filling 0 would fabricate data', contradicting the live config
```json
{
 "tier": "C",
 "type": "docs",
 "tractability": "auto",
 "dedup_status": "open (GhanaSPS/_/CONTENTS.org last touched 2026-09-03, 82e78e863 / PR #765, before the issue was filed; §2 text at :594-650 is verbatim the quoted stale text)",
 "priority": 3,
 "summary": "The claim holds exactly. CONTENTS.org:594 still reads '** 2. Wave 1 has NO usable MonthsAway ...' and :598 'It is nonetheless *not wired*'; :645-646 still says the 15-month value 'is moot while the column is undeclared'. The live side is unchanged too: 2009-10/_/data_info.yml:63 '# MonthsAway: s1d_27, with NaN read as 0' and :142 'MonthsAway: s1d_27'; the df_edit hook in 2009-10/_/mapping.py fills NaN->0 and clips 15->12. The reconciliation is a docs rewrite of §2 that keeps the overturned reasoning as history, carries the two residuals (9 members with move data but no s1d_27; hhno 107268086 vanishing), and re-measures the wave-1 household_characteristics count before restating it (5,008 was measured under the fallback path; probably 5,007 now).",
 "evidence": "lsms_library/countries/GhanaSPS/_/CONTENTS.org:594-598,613-615,620-650; GhanaSPS/2009-10/_/data_info.yml:63,142; GhanaSPS/_/data_scheme.yml:43-50; git log -- GhanaSPS/_/CONTENTS.org -> 82e78e863 2026-09-02 latest; SkunkWorks/derived_values.org:408-411 (adjudication).",
 "approach": "Rewrite GhanaSPS/_/CONTENTS.org §2 (:594-650) per the issue's 'What right looks like': retitle to 'wave 1 MonthsAway IS wired, NaN -> 0', one-line evidence pointing at 2009-10/_/data_info.yml:63-133, re-frame the measured-cost paragraph as the cost of the rejected alternative, replace the 'moot' sentence with the clip-to-12 decision (mapping.py:74), add the two residuals as open items, and restate the wave-1 household_characteristics count only after re-measuring it (Country('GhanaSPS').household_characteristics(waves=['2009-10']) in a private LSMS_DATA_DIR -- a cold build of GhanaSPS household_roster; budget ~minutes).",
 "verify": "Docs-only; re-measured count quoted with the command. Cache cost: zero -- CONTENTS.org is excluded from every build hash by name (CLAUDE.md: 'CONTENTS.org excluded as docs').",
 "contents_org": "The idiosyncrasy IS the issue: GhanaSPS wave 1's Q27-Q33 is a skip block recorded per EA (blank = not away, answer rate varies 6%-96% by EA), which CONTENTS.org:594-650 misreads as household-level missingness; plus the one 15-month value in a 12-month window (hhno 105144071) now clipped.",
 "blocked_by": "",
 "decision_for_human": ""
}
```

### #812 -- Guinea-Bissau: CONTENTS.org calls the design 'a single two-stage national draw', but the weights show deliberate region-level allocation (76.8% of log-weight variance between regions; 3.2x over-sampling in Bolama-Bijagos)
```json
{
 "tier": "C",
 "type": "docs",
 "tractability": "auto",
 "dedup_status": "open (PR #971 edits Guinea-Bissau/_/CONTENTS.org for #811 Region spellings; its diff has 0 mentions of stratification/allocation)",
 "priority": "3 -- documentation that steers inference (SEs without strata); no served number changes",
 "summary": "Claim holds and reproduces exactly on the warm `Guinea-Bissau/var/sample.parquet` (raw weights; the within-wave normalisation is a constant and cancels): 450 clusters, weight constant within every cluster, 76.8% of log cluster-weight variance between the 9 `strata` values, over/under-sampling 0.65x (sab) to 3.23x (BOLAMA_BIJAGOS). `Guinea-Bissau/_/CONTENTS.org:178-190` still reads 'The design as stated is a single two-stage national draw' with the verbatim DDI/BID quote and no measured counterpart. The sweep the issue asks for, same script on the warm sample parquets: Togo 2018 66.5% between 6 strata (0.59-1.82x, Savanes); Benin 2018-19 31.4% between 12 (0.65-1.77x, Donga); CotedIvoire 2018-19 36.5% between 33 (0.33-6.16x, Folon); Senegal/Mali 2018-19/2021-22, Niger 2018-19/2021-22 and Burkina 2018-19/2021-22 declare NO `strata` key in their `data_info.yml` at all (`grep -c strata` = 0 in all eight files -- absent from config, not declared-and-empty), so the check cannot run there (a gap of its own). For comparison the ECVMA/EMC/EACI waves with strata are plainly stratified (Niger 2011-12 84.6%, Burkina 2014 79.7%, Mali 2017-18 46.5%). PR #971 (open, in flight) touches the same CONTENTS.org file -- land this after it to avoid a textual conflict.",
 "evidence": "pd.read_parquet warm Guinea-Bissau/var/sample.parquet + the issue's script (reproduced numbers above); same script over Benin/Togo/CotedIvoire/Senegal/Mali/Niger/Burkina_Faso var/sample.parquet; Guinea-Bissau/_/CONTENTS.org:176-195; Guinea-Bissau/2018-19/_/data_info.yml:16 (strata: s00q01); gh pr view 971 --json files; gh pr diff 971 | grep -ic 'stratif|allocation' = 0",
 "approach": "Docs only: in `Guinea-Bissau/_/CONTENTS.org` keep the verbatim quote at :183-190 and add beneath it the measured allocation table from the issue (reproduced here), the sentence 'treat the design as region-stratified for inference; no source consulted states the allocation rule', and a pointer that `sample.strata` is the library slot name wired from the cover-page region code (`data_info.yml:16`), not evidence of declared strata. Add the one-paragraph twin to Togo/_/CONTENTS.org (66.5%) and Benin/_/CONTENTS.org (31.4%); note in CotedIvoire (36.5%, 33 strata). Separately file (or fold into this) that Senegal/Mali/Niger/Burkina EHCVM waves wire no `strata` at all.",
 "verify": "Re-run the issue's script against `Country('Guinea-Bissau').sample()` (warm read) and compare to the numbers written; no test. CACHE COST: zero -- CONTENTS.org is excluded from every build hash (CLAUDE.md: `CONTENTS.org excluded as docs`).",
 "contents_org": "Guinea-Bissau/_/CONTENTS.org:178-195 -- no oversample/booster/refresh declared; 'single two-stage national draw' (DDI p.4 / BID p.10 verbatim: 450 EAs, 12 households each); AND the 2018-19 directory holds BOTH of the survey's seasonal passes (a seasonal split a reader pooling the country must know about). The measured allocation CONTRADICTS the gloss, which is the issue.",
 "blocked_by": "PR #971 (same file; sequence after it)",
 "decision_for_human": "none (docs); optionally: should the missing `strata` wiring in Senegal/Mali/Niger/Burkina EHCVM waves get its own issue? yes/no"
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
