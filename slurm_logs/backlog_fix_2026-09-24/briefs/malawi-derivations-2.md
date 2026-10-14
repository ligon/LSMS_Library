# Fix element `malawi-derivations-2` -- apply the review's fixes and the three rulings (on `fix/malawi-derivations`)

You are a worker agent in a Slurm job with your OWN clone of `fix/malawi-derivations` (your cwd, on a new branch `fix/malawi-derivations-2`), your OWN mounted venv, and a PRIVATE `LSMS_DATA_DIR` seeded with Malawi's warm parquets (built from `development`). `dvc-cache` is a read-only symlink. A Malawi cold rebuild in your root is the verification.

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/malawi-derivations-2` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.

## Read first, in this order
1. `docs/guide/derivations.md`, then the module docstring of `lsms_library/derivations.py` (field contract, the loader's refusals, `BASES`), then `SkunkWorks/derived_values.org` sections "The precept" and "What the corpus already does". The fit rule you are implementing: a served value that reaches a row through a BRANCH SELECTION is a derivation, ONE KEY PER BRANCH so the path taken is countable; a deletion is registered with `rows: 0`; exact survey answers carry no key.
2. `lsms_library/countries/GhanaLSS/_/derivations.yml` (the template) and `tests/test_ghanalss_12b.py` + `tests/test_derivations.py` (the tests to mirror; note the drift test: every `derive_*` in a country module must be registered, and the wave-script uniqueness assert / post-collapse stamp rule).
3. `lsms_library/countries/Malawi/_/CONTENTS.org` (prose AND LOGBOOK; the bundle's new entries and the probe's new section) and `malawi.py`'s `_sale_block` / harvest-sale attachment as the bundle left it (commits 1a040c61f, 13da05b11).
4. The probe on branch `fix/malawi-sale-basis-probe` (`git fetch origin fix/malawi-sale-basis-probe`; read `slurm_logs/backlog_fix_2026-09-24/malawi_sale_basis/README.org` and reuse its scripts, especially `build_sales.py`, `separation.py`, `classify.py`). Its measured facts: price separates the basis for Groundnut only (shelled 1.6-2.2x unshelled per 50 kg bag, P(S>U) 0.78-0.94; leave-one-out recall ~80%, 87% unambiguous); Maize/Rice/Soyabean/Tobacco/Cotton do not separate (P ~0.5); the instrument asks S/U "for all applicable crops" with no crop list; Tobacco/Cotton harvest rows are 86-100% not-applicable; the 465 NA-vs-S/U class is 307 (sale S/U, harvest NA) + 158 (reverse); the 647 contradictions side with the SALE field 379:207 and 319 / 10.63M MWK are gated-re-attachable (217 sale, 102 harvest).
5. `slurm_logs/backlog_fix_2026-09-24/verdicts/malawi-bundle.md` sections 5-6 (the reviewer's follow-ups: two wave-script docstrings, the "Checked, not assumed" CONTENTS paragraph) -- do those too.

## The rulings (@ligon, 2026-09-24)
- "shelled vs unshelled is really a missing data problem, decipherable by looking at prices" -- and "this would be a derived value, see docs".
- "the 647 contradictions could also sometimes produce a derived value, based on price".
- Not-applicable IS the basis where shelling does not apply (the instrument's own wording) -- confirmed as part (b) of the three-part call.

## The task: implement the ladder, register every branch
Attachment of a sale (Module I / Q) to a harvest row currently takes one of these paths; make each a countable branch with a registry key in a NEW `lsms_library/countries/Malawi/_/derivations.yml` (sibling of population.yml; a .yml moves no hash), stamped on the `Derivation` column of exactly the harvest rows whose `Value_sold`/`Quantity_sold` arrived through that branch (post-collapse, in `assemble_crop_production` or wherever the bundle attaches; never in `_finalize_result`):
0. exact basis match -> NO key (a survey answer, not a construction).
1. `Malawi::crop_production::sale-basis-unknown-wildcard` -- the bundle's existing branch: either side `unknown_condition` (Module Q has no S/U column before 2016-17; `basis: questionnaire`, evidence the IHS3 Module Q page header per the probe; plus `modelling-choice` for treating unknown as compatible).
2. `Malawi::crop_production::sale-basis-from-price` -- one side not-applicable/missing, the reference cell SEPARATES (the probe's gates: >= 10 agreeing sales per basis in the within-wave `(crop, u)` cell, P outside (0.25, 0.75), margin > half the log-gap), and the price picks a basis; attach with that basis. Assumptions with bases as the probe drafted; `validated_against` the leave-one-out recall. No crop allowlist: the gate is the data.
3. `Malawi::crop_production::sale-basis-not-applicable` -- one side not-applicable, the reference cell does NOT separate (the basis is not priced, or no reference exists): attach, not-applicable being the basis. `basis: questionnaire` ("for all applicable crops"; harvest side 86-100% NA for Tobacco/Cotton) AND `modelling-choice` (where the market does not price the basis, the basis does not discriminate the sale). Measure and state per crop what this branch carries; if Maize/Rice/Soyabean rows land here in numbers, say so -- their physical basis exists even though price ignores it, and the key is what makes that visible.
4. `Malawi::crop_production::contradiction-price-sale` and `Malawi::crop_production::contradiction-price-harvest` -- both sides stated and DIFFERENT; the same gated classifier sides with the sale's stated basis / with the harvest row's; attach under the chosen basis. Two keys, so 217 vs 102 (probe) is countable.
5. Remainder: unresolved contradictions and undecidable NA cases stay SUPPRESSED (the bundle's `sale_basis_mismatch` attrs), and are registered as a deletion entry with `rows: 0` and an `inputs` callable returning the suppressed sale lines, so `ll.derivations()` and `attrs['derivations']` show them.
Requirements: `inputs` callables return the raw Module I/Q lines at the input grain with original variable names, keyed to join the served `(t, i, plot_id, j, u, condition, season)` rows (reuse the probe's frame builder); `raw_variables` name the Stata columns per wave; the wave-script uniqueness assert holds; `Derivation` stays `NA` on exact matches; the derived food tables must not carry the column; `tests/test_derivations.py` drift test green; a Malawi instance test mirroring `tests/test_ghanalss_12b.py` (stamp on every branch row and on no exact-match row; per-branch counts pinned with the before/after numbers in the commit message). Do the reviewer's three prose follow-ups in the same branch. If a design point is genuinely undecidable from the docs and rulings above, choose, NAME it as a decision, and keep going.

## Verify (cold, in your private root, foreground with timeout)
`cache clear --country Malawi` against YOUR root; rebuild `crop_production` and `food_acquired`; report per wave and per key: rows stamped, MWK attached; the mismatch tallies before (303 / 59 / 247 / 503 sales; 64.1M MWK) and after; total `Value_sold` before/after; `grain_reports()` / `null_index_reports()` unchanged; `ll.Country('Malawi').derivations()` lists every key; `find lsms_library/countries -name '*.parquet'` empty. Then the verbatim pytest recipe on `tests/test_malawi_shipped_factors.py tests/test_derivations.py tests/test_schema_consistency.py tests/test_build_transform_hash.py tests/test_feature_canonical_index.py` plus your new test, output to a file, summary line pasted.

## NEVER force-push. The previous agent did (`--force-with-lease`, its own branch); it was disclosed and it was still a breach. Amend nothing that is pushed; add a commit.

## Read first
`docs/guide/derivations.md`; `lsms_library/countries/Malawi/_/derivations.yml` and `malawi.py`'s ladder (`derive_sale_basis`, `_attach_sales`, the tally loop) as the branch left them; `Malawi/_/CONTENTS.org` (the LADDER section and "The rule"); `tests/test_malawi_sale_basis.py`; the reviewer's verdict sections 5-6 below; `slurm_logs/backlog_fix_2026-09-24/reports/malawi-derivations.md` for the per-key table you must re-baseline.

## Rulings (@ligon, 2026-09-25), which override the previous agent's decisions where they conflict
1. **ADD a key for the S/U-sale-vs-NA-harvest side where price CONTRADICTS the sale's recorded S/U** (the previous agent's decision 4 refused those: 11 sales / 707,400 MWK, 4 with an unambiguous contrary price). Price decides on that side too, exactly as the contradiction rungs do: attach under the price-chosen basis, with a key of its own (suggested `Malawi::crop_production::sale-basis-from-price-overrule`; one key per branch so it is countable), `basis: modelling-choice` for overruling a recorded answer, the same gates (separating cell, margin > 0.5 x gap). Sales the price cannot decide stay unresolved.
2. **Rung 3 splits into two keys, exposed PER ROW**: `sale-basis-not-applicable` keeps only the sales whose in-wave reference cell was MEASURED non-separating (114 sales / 25.4M MWK per the review); a new `Malawi::crop_production::sale-basis-no-reference` carries the sales admitted because NO reference cell exists (296 / 13.3M, incl. Tobacco 37, Cotton 57, Soyabean 16 in 2019-20). Its assumption is honest: admitted on absence of evidence, `basis: questionnaire` for "all applicable crops" plus `modelling-choice`; `validated_against` says what could and could not be measured.
3. The margin gate stays as validated (`margin > 0.5 x gap`, the outer quarter between the medians counts as unambiguous); only the PROSE is wrong -- fix it (review item 1) and add the measured 77 / 357 count.

## The reviewer's follow-ups, all of them (items 1-7 below), plus:
- Re-baseline the per-key table (rows and MWK per wave per key; the new keys; unresolved before -> after) with the numbers in the commit message and in `tests/test_malawi_sale_basis.py`'s pins; total Value_sold before -> after.
- Do NOT fix the pre-existing wave-script `first()` variety collapse (27 sales / 373,600 MWK); stop attributing the shortfall to #869, state the two causes with counts, and write the issue text for it into `slurm_logs/backlog_fix_2026-09-24/issues_to_file.org` under a heading (no `gh` write).
- `derivations.yml` must still load; `tests/test_derivations.py` drift tests green; the Malawi instance test covers the two new keys (stamp on exactly their rows, none elsewhere).

## 5. Residual risks and what a human must decide

1. **Decision 4 (corroboration only on the S/U-sale-vs-NA-harvest side).** The ruling says price decides; the branch applies that fully on the NA-sale side (price names the basis and the row) but on the NA-harvest side only ADMITS a sale whose price agrees with its own answer, leaving 11 sales / 707,400 MWK unresolved, 4 of them with an unambiguous price that says the sale's S/U was misrecorded. Consistent with "price decides"? Arguably no: a price that contradicts the sale's answer is exactly the case the contradiction rungs re-base. It is registered as a `modelling-choice` assumption, so it is disclosed; @ligon should say whether re-basing on that side (a sixth key, `from-price` with the sale's answer overruled) is wanted.
2. **Rung 3 admits on absence of evidence.** 296 of its 410 sales (13.3M MWK, incl. Tobacco 37, Cotton 57, Soyabean 16 in 2019-20) have no in-wave reference cell at all; only 114 (25.4M MWK) are measured non-separating. The entry's rule text discloses both grounds, but its `validated_against` and CONTENTS' "the market does not price the basis" only support the second. Decide whether "no reference" should be its own key (countable) or at least be reported in `attrs['sale_basis_ladder']` (`rungs` already gives fine/mid; the no-reference count is the complement and is not printed).
3. **The 58 Maize/Rice/Soyabean rows** are disclosed on the entry and in CONTENTS with the right counts, but for Soyabean 2019-20 (16 rows) the "P 0.42-0.54" justification is the probe's pooled measurement, not the in-wave gate; in-wave there was nothing to measure.
4. **Pre-existing, not this delta:** the wave-script defensive collapse (`_grain`, `Value_sold: 'first'`) silently discards 27 sales / 373,600 MWK where two same-label varieties of one crop on one plot were both sold, serving a composite row (Quantity summed across varieties, one variety's sale) -- the GH #871 "compose vs select" pattern inside a wave script. Identical on the bundle. Worth its own issue; the delta's accounting should stop attributing the shortfall to #869.
5. **The margin rule.** `margin > 0.5 x gap` is a sound gate and is what the probe validated (87% on that subset); only the prose is wrong. But the human should confirm that "outer quarter between the medians counts as unambiguous" is the intended tolerance, since 22% of the price-rung attachments live there.

## 6. Recommended follow-up commits

1. `lsms_library/countries/Malawi/_/derivations.yml` (`sale-basis-from-price.rule`, last sentence) and `CONTENTS.org` "Decisions taken here" bullet 2: replace "sits BETWEEN the two medians" with "sits in the middle half between the two medians (margin <= 0.5 x gap)", and add the measured 77 / 357 admitted-between count.
2. `tests/test_malawi_sale_basis.py` ROWS comment and `CONTENTS.org` "What each rung carries" first bullet: attribute the sales -> rows shortfall to BOTH the #869 drop (13, all 2013-14) and the defensive collapse (2/3/1/5); state the collapse's total loss (27 sales / 373,600 MWK, pre-existing) and open an issue for it (the collapse composes a row; `most_complete_row`-style selection or keying the collapse on `crop_variety` are the candidates).
3. `derivations.yml` `sale-basis-not-applicable`: add the no-reference vs non-separating split (296 / 114 sales; 13.3M / 25.4M MWK) to `validated_against`, and note that the 2019-20 Soyabean 16 are no-reference; optionally emit `no_reference` in `attrs['sale_basis_ladder'][key]` beside `rungs` (one line in `_attach_sales`' tally loop).
4. `derivations.yml` `sale-basis-from-price` (decision-4 assumption) and `CONTENTS.org` "from-price is 38, not 17": add the refused complement (11 sales / 707,400 MWK on the S/U-sale-vs-NA-harvest side, 4 price-contradicted; 6 / 267,800 on the NA-sale side).
5. Commit message / CONTENTS cache-cost sentence: "all eleven Malawi tables" -> "all 21 registered Malawi tables" (measured with `_table_cache_hash`).
6. `malawi.py` `assemble_crop_production`, the unresolved-class `SaleAttachmentWarning` text: it still says the sale "CONTRADICTS the condition of every harvest row", but the class now also holds NA pairs in separating cells the price could not decide and S/U sales the price contradicted; name the three grounds as the registry entry does.
7. `CONTENTS.org` l.276-278 ("The rule"): add an inline "[reversed the same day -- see the LADDER section]" marker so the stale sentence cannot be read alone.


## Verify (cold, private root, foreground with timeout)
`cache clear --country Malawi` against YOUR root; rebuild `crop_production`; `ll.Country('Malawi').derivations()` lists eight keys (six + two new); per-key table reproduced and re-baselined; `grain_reports()` / `null_index_reports()` unchanged; `food_expenditures()` carries no `Derivation`; `find lsms_library/countries -name '*.parquet'` empty; verbatim pytest recipe on `tests/test_malawi_sale_basis.py tests/test_malawi_shipped_factors.py tests/test_derivations.py tests/test_schema_consistency.py tests/test_build_transform_hash.py`, output to a file, summary line pasted.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
