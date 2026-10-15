# RED-TEAM review of branch `fix/ghana-docs-encoding` @ 3973e9094 (element `ghana-docs-encoding`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/ghana-docs-encoding` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of GhanaLSS,GhanaSPS,Guinea-Bissau; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/ghana-docs-encoding/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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


## The branch: diffstat (vs development)
```
 lsms_library/countries/Benin/_/CONTENTS.org        |  12 ++
 lsms_library/countries/CotedIvoire/_/CONTENTS.org  |  12 ++
 .../GhanaLSS/2016-17/_/categorical_mapping.org     |   6 +-
 lsms_library/countries/GhanaLSS/_/CONTENTS.org     |  45 +++++-
 .../countries/GhanaLSS/_/categorical_mapping.org   |  56 ++++----
 lsms_library/countries/GhanaSPS/_/CONTENTS.org     | 157 ++++++++++++++-------
 .../countries/Guinea-Bissau/_/CONTENTS.org         |  42 ++++++
 lsms_library/countries/Togo/_/CONTENTS.org         |  13 ++
 tests/test_no_c1_controls_in_curated_labels.py     | 131 +++++++++++++++++
 9 files changed, 389 insertions(+), 85 deletions(-)
```
## The branch: commit messages
--- 3973e9094 ghana-docs-encoding: measured reader numbers, Guinea-Bissau outline fix, test non-vacuity (GH #926/#812)
- GhanaLSS/_/CONTENTS.org: the reader-side figures (390,140 of 7,077,839
  s9aname values with a C1 control; no warning of any class) are now stated
  as measured here through get_dataframe(convert_categoricals=False) on
  pandas 3.0.6, not carried over from the triage record.
- Guinea-Bissau/_/CONTENTS.org: the new '*** The weights say otherwise'
  subsection had been inserted before the seasonal-split prose, making the
  two DDI quotes and the wave-1/wave-2 table its children in the outline.
  Moved to after them, immediately before '** 2018-19 -- where the
  statement lives'.  Still clear of PR #971's hunk (@@ -442).
- tests/test_no_c1_controls_in_curated_labels.py: the scanner sanity test
  also asserts a harmonize_price_item Preferred Label from the 2016-17 file
  is yielded, so assertion 1 is non-vacuous for both GhanaLSS files.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- af637df9a EHCVM: record the measured region-level allocation beneath the 'single two-stage draw' quotes (GH #812)
Guinea-Bissau/_/CONTENTS.org still glossed the design as 'a single two-stage
national draw'.  Measured on Country('Guinea-Bissau').sample(waves=['2018-19']):
450 clusters, weight constant within cluster, 76.8% of log cluster-weight
variance between the 9 strata, sample/population share 0.65x (SAB) to 3.24x
(Bolama-Bijagos).  Added the allocation table, the instruction to treat the
design as region-stratified for inference (no source states the allocation
rule), and the note that sample.strata is wired from the cover-page region
code (data_info.yml:16), not a declared stratum.  Twin paragraphs, each
re-measured here via the API on a cold private root: Togo 66.5% / 6 strata
(0.54x-1.90x), Benin 31.4% / 12 (0.65x-1.77x, placed under its existing
'disproportionate by design' heading), CotedIvoire 36.5% / 33 (0.33x-6.17x).
Senegal / Mali / Niger / Burkina_Faso EHCVM waves wire no strata key, noted
as a gap.  Does not overlap PR #971's hunk (@@ -442) in the same file.

Cache cost: none (CONTENTS.org is excluded from every build hash).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 8c7d61c58 GhanaSPS: CONTENTS.org §2 -- wave-1 MonthsAway IS wired (NaN -> 0), superseding 'not wired' (GH #880)
The section still read '** 2. Wave 1 has NO usable MonthsAway ...', 'It is
nonetheless *not wired*' and 'It is moot while the column is undeclared',
contradicting 2009-10/_/data_info.yml:142 (MonthsAway: s1d_27) and the
df_edit hook in 2009-10/_/mapping.py (fillna(0).clip(upper=12)).  Rewritten:
the live reading (skip block, per-EA recording convention), the two
residuals (9 members with move data but no s1d_27; hhno 107268086 vanishing
because all three members report 12 months away), the clip-to-12 decision
for the one 15-month value, and the overturned reasoning kept as a dated
'Superseded' subsection with the measured cost re-framed as the cost of the
rejected alternative.

Re-measured on a cold private root, Country('GhanaSPS').household_characteristics():
wave-1 households 5,008 -> 5,007 (both 107254027 and 107268086 absent),
mean household size 3.771 -> 3.769; no residence-fallback UserWarning fires.

Cache cost: none (CONTENTS.org is excluded from every build hash).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 21a8e9e07 GhanaLSS: replace committed cp1252-as-latin-1 C1 controls in curated labels (GH #926)
The nonfood_items crosswalk in GhanaLSS/_/categorical_mapping.org (28 cells,
Preferred Label + Label_2016-17) and the 2016-17 harmonize_price_item table
(3 cells, codes 309/326/348) carried U+0092/U+0096/U+0094 -- cp1252 bytes
decoded as latin-1 -- so nonfood_expenditures().j served 25,585 rows (27
distinct j, all 2016-17) and community_prices().j / .Description 927 rows
with a C1 control embedded.  Replaced with ASCII ' / - / " (a decision,
recorded in CONTENTS.org).  Preferred Labels remain injective per wave
(250 / 251 / 508 entries).

The source g7sec9a.dta is format 118 (UTF-8-declared) and MIXES raw cp1252
bytes with clean ASCII for the same words (41,803 Women\x92s beside 97,531
Women's), so pandas' per-string latin-1 fallback produced the controls with
no warning; #805's format<118 reader rule would not have prevented them and
will not protect a re-derivation.  Recorded in GhanaLSS/_/CONTENTS.org.

New tests/test_no_c1_controls_in_curated_labels.py: every Preferred Label
column of every curated org table corpus-wide is C1-free, and the two
GhanaLSS files are C1-free anywhere.  Key-column C1 cells in Mali / Niger /
Burkina_Faso (load-bearing until GH #794/#805 re-key them) and Uganda's
inert u wave-label cells are named in the docstring, not allowlisted.

Cache cost: _/categorical_mapping.org is a build-time *.org input in every
GhanaLSS table's hash -> full GhanaLSS re-warm; no other country.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


