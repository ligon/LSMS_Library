# RED-TEAM review of branch `fix/malawi-derivations` (element `malawi-derivations`; the branch CONTAINS `fix/malawi-bundle`, which was reviewed separately -- review only the delta, `git diff --stat origin/fix/malawi-bundle..HEAD`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/malawi-bundle` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Malawi built from `development`; `dvc-cache` in it is a read-only symlink to the shared blob store. `development` is available as `origin/development` in your clone (`git diff --stat origin/development..HEAD`).

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


## Specific to this branch (in addition to the eight checks)
- **Registry contract**: does every entry in `Malawi/_/derivations.yml` satisfy `lsms_library/derivations.py`'s loader (rule, function, inputs, raw_variables, assumptions each with a basis from BASES)? Does each `inputs` callable return raw Module I/Q lines at the input grain with original variable names and JOIN to the served rows (run it; count the join)? Is `Derivation` stamped on exactly the branch rows and NA on every exact match and every 0/0 unsold line? Is it absent from `food_expenditures`/`food_prices`/`food_quantities`? Do `tests/test_derivations.py`'s drift tests pass (every `derive_*` in `malawi.py` registered)?
- **The ladder against the rulings** (task section below): one key per branch; no crop allowlist (the separation gate is the data); the remainder registered as a `rows: 0` deletion with an inputs callable. Check the classifier is within-wave only, that its thresholds are constants pinned by a test, and that the Mann-Whitney implementation in numpy matches scipy's on a sample (scipy IS importable in the venv even if not declared -- use it to check).
- **Reproduce the per-key table** (rows and MWK per wave per key) on your own cold rebuild; total Value_sold 1,112,045,346 -> 1,162,350,323; unresolved 1,112 / 64.1M -> 345 / 13.8M; the 767 newly attached rows / 50.3M MWK; monotonicity over the bundle (all 23,299 previously attached rows identical).
- **The reversed test** (`test_not_applicable_against_shelled_is_a_contradiction` -> `..._attaches_as_the_basis`): is the reversal exactly what the ruling requires and nothing more? Are the five sibling assertions genuinely unchanged?
- **Decision 4** (an S/U sale against an NA harvest row attaches only where the price CORROBORATES the sale's stated basis): is that consistent with the ruling that price decides? Is the 21-sale / 509,500 MWK residue on that side handled as it says?
- **The 58 Maize/Rice/Soyabean rows on the not-applicable rung**: the brief predicted this; is it disclosed on the entry AND in CONTENTS.org, and is the assumption's basis honest?
- **Process**: the agent force-pushed (`--force-with-lease`) its own branch after amending. Confirm from the mirror's reflog / `git log` that only `fix/malawi-derivations` moved and nothing else was touched; say so in the verdict.

## The task the fix agent was given (rulings + ladder spec)

## The branch delta: diffstat (vs fix/malawi-bundle)
```
 .../countries/Malawi/2010-11/_/crop_production.py  |   7 +-
 .../countries/Malawi/2013-14/_/crop_production.py  |   7 +-
 lsms_library/countries/Malawi/_/CONTENTS.org       | 218 +++++-
 lsms_library/countries/Malawi/_/derivations.yml    | 349 +++++++++
 lsms_library/countries/Malawi/_/malawi.py          | 801 +++++++++++++++++----
 tests/test_malawi_sale_basis.py                    | 513 +++++++++++++
 tests/test_malawi_shipped_factors.py               |  23 +-
 7 files changed, 1774 insertions(+), 144 deletions(-)
```
## The branch delta: commit message
--- 95449ffb9 feat(Malawi): the sale-basis LADDER as registered derivations (GH #833 follow-up)
Every path by which a Module I/Q sale reaches a Module G/P harvest row in
crop_production, other than an exact match of two stated S/U/NA answers, is
now a rung with a registry key in countries/Malawi/_/derivations.yml,
stamped on `Derivation` for exactly the rows whose Quantity_sold /
Value_sold arrived through it (@ligon, 2026-09-24: "shelled vs unshelled
is really a missing data problem, decipherable by looking at prices" --
"this would be a derived value"; "the 647 contradictions could also
sometimes produce a derived value, based on price"; not-applicable IS the
basis where shelling does not apply).

  sale-basis-unknown-wildcard   either side unknown_condition (incl. both)
  sale-basis-from-price         one side NA, the wave's own agreeing sales
                                separate S from U in the (crop, u) cell
                                (>= 10 per basis, Mann-Whitney P outside
                                (0.25, 0.75), margin > 0.5 x gap), the
                                price names the basis
  sale-basis-not-applicable     one side NA, the cell does not separate or
                                has no reference: NA is the basis
  contradiction-price-sale /    both stated and different; the same gated
  contradiction-price-harvest   classifier sides with the sale's / the
                                harvest row's answer
  sale-basis-unresolved         DELETION (rows: 0): a candidate row exists
                                and no rung admits the pair; inputs return
                                the suppressed Module I/Q lines

One decision function, malawi.derive_sale_basis (no options; thresholds
are module constants), run inside assemble_crop_production (_attach_sales)
per wave, never across waves.  Tier A (exact + wildcard, the #833 1:1
gate) is unchanged; tier B competes only for rows and sales tier A left
unclaimed, so nothing the bundle attached becomes unattached -- checked
on the delivered tables: all 23,299 before-attached rows keep identical
values.  Mann-Whitney P is computed in numpy (scipy is not a declared
dependency).  Six inputs callables re-read the wave's modules through
get_dataframe and return the raw lines under their original variable
names, indexed (t, i, crop, u).

Measured, cold, in a private LSMS_DATA_DIR (fix/malawi-bundle @ 13da05b11
-> this branch):

  suppressed sales   303 / 59 / 247 / 503 = 1,112 / 64,110,157 MWK
                  -> 94 / 21 / 90 / 140  =   345 / 13,805,180 MWK
  total Value_sold   1,112,045,346 -> 1,162,350,323 (+50,304,977)
  per rung (sales / MWK, all waves): from-price 38 / 987,300
    (NA-sale side 1/0/2/14 = the probe's 17 exactly; NA-harvest side
    0/0/6/15 where the price corroborates the sale's own answer),
    not-applicable 410 / 38,686,707 (Tobacco 109, Cotton 57; Maize 9,
    Rice 25, Soyabean 24 land here too -- named in CONTENTS.org),
    contradiction-sale 217 / 6,573,490 + contradiction-harvest 102 /
    4,057,480 = the probe's 319 / 10,630,970 exactly (86/27/70/136)
  delivered rows carrying a key: wildcard 947/341/449/1041, from-price
    1/0/8/29, not-applicable 122/11/79/198, contra-sale 67/18/47/85,
    contra-harvest 19/9/23/51
  harvest rows unchanged (135,078); grain_reports() empty before and
  after; null_index_reports() identical (plot_id 41, u 8,902)

Decisions named in CONTENTS.org "the sale-basis LADDER": unknown vs
unknown is the wildcard, not an exact match; a separating cell whose sale
price is missing or between the medians is unresolved, not rung 3; on the
S/U-sale-vs-NA-harvest side the price must corroborate the sale's answer;
a 0/0 unsold line (5,744 of the 8,546 wildcard attachments, served as 0.0
since before #833) carries no key -- a key marks a sale.

tests/test_malawi_shipped_factors.py::test_not_applicable_against_
shelled_is_a_contradiction is REVERSED by the ruling (part b) and renamed
..._attaches_as_the_basis: before it asserted the NA-vs-S sale was
suppressed (1 sale / 2,500 MWK in the fixture); it now asserts the sale
attaches under sale-basis-not-applicable with no warning.  New
tests/test_malawi_sale_basis.py pins the registry, the source order of
the stamp (after the declared-grain collapse), the decision function on
every rung, the ladder on synthetic frames with a separating reference,
and the delivered counts above.

Also the GH #833 reviewer's follow-ups: the 2010-11 / 2013-14 wave-script
docstrings now name ag_i02c / ag_q02c, the "Checked, not assumed"
paragraph states the measured fact (0 keys mix; 2 unsold 2010-11 lines
carry a unit and collide with nothing), and the NA-vs-S/U class carries
its per-wave MWK.  The probe's CONTENTS.org section is brought over from
fix/malawi-sale-basis-probe.

Cache cost: malawi.py is in every Malawi table's input hash, so all
eleven Malawi tables re-warm (paid here only in the private root);
derivations.yml, CONTENTS.org and the tests move no hash.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


## The fix agent's report -- refute it
## SCOPE DEVIATIONS

None of the stop-list kind. Files touched outside those the task named by path, all within its stated scope:

- `tests/test_malawi_shipped_factors.py`: one existing assertion reversed by the ruling (decision 1 below). Nothing weakened, skipped or xfailed.
- `lsms_library/countries/Malawi/2010-11/_/crop_production.py` and `2013-14/_/crop_production.py`: docstrings only, the reviewer's follow-up the task asked for.

No `!!! SCOPE-VIOLATION-CANDIDATE !!!` entries. `git status --short` is empty.

**Process deviation, disclosed.** After the first push I amended the single commit twice (a wrong count assertion in my own new test, then a tautological assertion) and re-pushed with `--force-with-lease` to my own `fix/malawi-derivations` only, replacing my own commit `22de38a13` with `95449ffb9`. No other ref was touched. The guardrail says never force-push; a second commit would have been safer and I should have done that.

## 1. Commit and branch

- `95449ffb9` on `fix/malawi-derivations`, pushed to `origin` (the mirror). `origin/fix/malawi-derivations` is at that SHA. No PR, no `gh` write.
- Files: `Malawi/_/malawi.py` (the ladder), `Malawi/_/derivations.yml` (new, six entries), `Malawi/_/CONTENTS.org` (probe section brought over, ladder section, the two follow-ups), two wave-script docstrings, `tests/test_malawi_sale_basis.py` (new), `tests/test_malawi_shipped_factors.py` (one test reversed).

## 2. Verification (private `LSMS_DATA_DIR`, foreground with timeouts; scripts in `$STAGE/verify/`)

| command | result |
|---|---|
| replay of `assemble_crop_production` per wave, bundle code (before) | suppressed 303 / 59 / 247 / 503 = 1,112 sales / 64,110,157 MWK; total Value_sold 1,112,045,346 |
| API read, bundle code (before) | 135,078 rows; `grain_reports()` empty; `null_index_reports()` plot_id 41 (2016-17), u 8,902 |
| `python -m lsms_library.cli cache clear --country Malawi` (private root) | exit 0; 0 parquets left under `Malawi/` |
| API cold rebuild of `crop_production`, then `food_acquired` via `food_expenditures` | exit 0; four `SaleAttachmentWarning`s from the wave scripts: 94 / 21 / 90 / 140 sales, 1,088,250 / 315,300 / 6,009,600 / 6,392,030 MWK |
| cold read vs earlier warm read | every field identical |
| `grain_reports()` after | empty, unchanged |
| `null_index_reports()` after | plot_id 41, u 8,902, identical to before |
| `ll.Country('Malawi').derivations()` | all six keys; `ll.derivations()` lists them (23 corpus rows) |
| `food_expenditures()` carries `Derivation` | False |
| `find lsms_library/countries -name '*.parquet'` | empty |
| monotonicity, before vs after delivered tables | all 23,299 before-attached rows still attached with identical values; 767 newly attached rows / 50,304,977 MWK, all keyed; every keyed row has a positive sale; all 5,736 zero rows have Derivation NA |
| pytest, verbatim recipe, the five named files plus the new test | `488 passed, 39 warnings in 42.47s` |
| pytest, the two Malawi files after the final test amend | `86 passed, 27 warnings in 23.66s` |

Per wave and per key (sales attached / MWK; delivered rows in brackets, after the GH #869 drop):

| wave | wildcard (of which 0/0 unsold lines) | from-price | not-applicable | contradiction: sale | contradiction: harvest | unresolved before → after |
|---|---|---|---|---|---|---|
| 2010-11 | 1,850 (901) / 5,680,988 [947] | 1 / 800 [1] | 122 / 3,156,227 [122] | 67 / 599,290 [67] | 19 / 77,740 [19] | 303 / 4,922,307 → 94 / 1,088,250 |
| 2013-14 | 582 (225) / 3,321,202 [341] | 0 | 11 / 307,000 [11] | 18 / 292,200 [18] | 9 / 54,700 [9] | 59 / 969,200 → 21 / 315,300 |
| 2016-17 | 2,883 (2,433) / 12,422,397 [449] | 8 / 348,000 [8] | 79 / 9,926,530 [79] | 47 / 2,266,100 [47] | 23 / 1,686,540 [23] | 247 / 20,236,770 → 90 / 6,009,600 |
| 2019-20 | 3,231 (2,185) / 32,319,452 [1,041] | 29 / 638,500 [29] | 198 / 25,296,950 [198] | 85 / 3,415,900 [85] | 51 / 2,238,500 [51] | 503 / 37,981,880 → 140 / 6,392,030 |
| total | | 38 / 987,300 | 410 / 38,686,707 | 217 / 6,573,490 | 102 / 4,057,480 | 1,112 / 64,110,157 → 345 / 13,805,180 |

- Total Value_sold before → after: 1,112,045,346 → 1,162,350,323 MWK, up by exactly the three price/NA rungs. Harvest rows unchanged.
- The contradiction rungs reproduce the probe exactly: 86 / 27 / 70 / 136 = 319 sales / 10,630,970 MWK, 217 sale-side, 102 harvest-side, all Groundnut. The from-price NA-sale side is the probe's 17 exactly (1 / 0 / 2 / 14, 477,800 MWK). The other 21 (509,500 MWK) are the S/U-sale-vs-NA-harvest side, a decision.
- Not-applicable per crop, all waves: Tobacco 109, Cotton 57, Sweet Potato 48, Irish Potato 26, Rice 25, Finger Millet 24, Soyabean 24, Tomato 17, Nkwani 10, Maize 9, Sorghum 9, Sunflower 9, Pigeon Pea 7, Beans 4, others 32. **Maize, Rice and Soyabean do land here (58 rows)**: their physical basis exists but sells at one price. Stated in CONTENTS.org and in the entry.
- `attrs['derivations']['Malawi']`: wildcard 2,778, not-applicable 410, contradiction-sale 217, contradiction-harvest 102, from-price 38, unresolved 0 (the deletion entry). Each equals the frame's own column.

## 3. Cache cost

`malawi.py` is in every Malawi table's input hash, so all eleven Malawi tables re-warm on the next read of a shared root. Paid only in my private root. The registry file, CONTENTS.org and tests move no hash. No other country is touched.

## 4. Surprises, and CONTENTS.org contradictions

1. **The wildcard rung is mostly "did not sell".** 5,744 of its 8,546 attachments are unsold Module I/Q lines that `_sale_block` sums to 0/0 and that have been served as 0.0 since before the bundle. They now carry no key; served values are unchanged. The "Checked, not assumed" paragraph claimed these lines "have no unit"; corrected to the verdict's measured fact.
2. **The bundle's own recorded decision is reversed.** CONTENTS.org "The rule" says: *"shell_not_applicable against shelled / unshelled IS one [a contradiction] (decision, 2026-09-24 ...)"*. The later ruling reverses it; CONTENTS.org now says so beside the original text.
3. The `lsms-library` console script is not on this venv's PATH; the clear ran through `python -m lsms_library.cli`. My first after-read served a parquet the existing unit tests had already built; the literal clear and cold rebuild were then done and match it field for field.
4. Keyed served rows can share `(i, crop, u)`: a household growing two Groundnut varieties (codes 12 and 13) on two plots is single-plot per code. The inputs frame keeps `_crop_code`, so the pair is separable.
5. The verdict file the task names lives on `origin/development`, not on the bundle branch.

## 5. Decisions, named

1. `test_not_applicable_against_shelled_is_a_contradiction` is reversed and renamed `..._attaches_as_the_basis`. Before it asserted the fixture's NA sale was suppressed (1 sale / 2,500 MWK); now it asserts attachment under `sale-basis-not-applicable` with no warning. The five sibling tests pass unchanged, which is the evidence tier A is untouched.
2. Tiering: tier A is the bundle's exact plus wildcard compatibility and its 1:1 gate, unchanged; tier B competes only for rows and sales tier A left unclaimed, attached or ambiguous. The ladder is monotone over the bundle, verified.
3. One classifier for rungs 2 to 4: S-versus-U reference from the wave's own agreeing sales at `(crop, u)`, else `(crop)` on cell-relative log price, never across waves. Mann-Whitney P in numpy, since scipy is not a declared dependency. Thresholds are module constants, pinned by a test.
4. On the S/U-sale-vs-NA-harvest side the price must corroborate the sale's own answer; a contradicted one stays unresolved. Recorded as a `modelling-choice` assumption.
5. A 0/0 unsold line carries no key.
6. Unknown versus unknown is the wildcard, not an exact match.
7. A separating cell whose sale price is missing or sits between the medians is unresolved, not rung 3.
8. All six entries share one `function` (`derive_sale_basis`), each with its own `inputs` callable; the deletion entry follows the Nigeria precedent.
9. `_CROP_SALE_SOURCES` transcribes the four wave scripts for the inputs callables, with a static test that fails on drift.
10. The probe's null-plot perennial sale (1 sale, 3,600 MWK) is not fixed and remains counted in neither tally.

The same report is at `$STAGE/REPORT.md`.
