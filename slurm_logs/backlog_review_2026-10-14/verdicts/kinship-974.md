VERDICT: pass-with-fixes -- the kinship.yml edit and the #931 pin are exactly what the adjudication prescribes and change no served value or cache hash, but the branch is 13 commits behind development and conflicts textually in `tests/test_crop_kg_factor.py` with PR #972 (GH #848); it must be rebased/merged (keep both hunks) before merge.

Reviewed `fix/kinship-974` @ 1d77c6ff2 (merge-base 2ed85b49b) against origin/development c7377eca4, in a private `LSMS_DATA_DIR` (Senegal, EthiopiaRHS, Guinea-Bissau warm), own clone + three scratch worktrees (`base`=origin/development, `mb`=merge-base, `merged`=trial merge). GitNexus unavailable; call-site sweeps done with `rg`/`grep` (declared substitution).

## 1. Refutations (claim -> what I ran -> result)

- **"403 -> 388 entries; removed set == the 15 adjudicated keys; 0 surviving tuples changed"** -- HELD. `yaml.safe_load` diff of `origin/development:kinship.yml` vs branch (scratch `tuplediff.py`): entries 403 -> 388, removed exactly the 15 keys in ADJUDICATION.org's delete table, added 0, changed tuples `{}`.
- **"418 -> 388 key lines; 13 keys written 2-3x, all agreeing"** -- HELD. Text census: old 418 lines / 403 distinct, dup keys = {Brother/Sister-In-Law, Cousin, Cousin, Cousine, Father/Mother, Father/Mother-In-Law, Lodger, Neveu, Nièce, Niece, Other Not Related, Sister/Brother, Sister/Brother Of Head Or Spouse, Son/Daughter-In-Law, Stepchild} (13); new 388 / 388, no dups. Every duplicated key still has one surviving line (checked `Father/Mother`, `Sister/Brother`, `Sister/Brother Of Head Or Spouse` etc. by grep).
- **"None of the 15 deleted keys is emitted by any wave"** (the key claim; ADJUDICATION.org row 3 *prose* says `Frere, Soeur` is "emitted") -- HELD, corpus-wide. Read-only `pyarrow` scan of raw `Relationship` in every `var/household_roster.parquet` of the shared warm cache (35 countries, 392 distinct raw labels; no library import, no writes): **0 of the 15 deleted keys is hit by any raw label** (title-cased or raw). The only sibling forms in the corpus are accented: `Frère, Sœur` (Mali, Niger, Senegal), `Frère, soeur` (Niger), `Frère, sœur` (Benin, Burkina, CIV, Togo), `Frère, Soeur` (Mali), `Frère/sœur` (Burkina). The adjudication's table was right and its prose wrong. No country `categorical_mapping.org`/`mapping.py`/`data_info.yml` maps anything TO a deleted string (grep).
- **"bench/kinship_reach.py: 38 reached before and after; 0 labels seen but not in kinship.yml"** -- HELD. Branch run on the three private countries: `kinship.yml keys: 388; reached: 38; UNREACHED: 350`, `labels seen but NOT in kinship.yml: 0`.
- **"Predicted corpus-wide 388 / 273 / 115"** -- approximately held, by a different method: my raw-label scan gives reached **272** on BOTH old (403) and new (388) files, i.e. reached unchanged, unreached 131 -> 116. The ±1 vs 273 is methodology: the scan is pre-categorical-mapping, the bench harvests post-mapping (GhanaLSS/GhanaSPS translate 14 raw labels in their `categorical_mapping.org` before kinship sees them; all 14 are present in those files, and the set of "raw labels not in yml" is identical before/after). Not a refutation.
- **"0 unknown-label warnings; Senegal 129,650 / EthiopiaRHS 51,574 / Guinea-Bissau 42,839 rows"** -- HELD, and extended: served `(Generation, Distance, Affinity)` value-count digests are byte-identical base vs branch on all three (Senegal 822f1646…, EthiopiaRHS d6cf6089…, Guinea-Bissau 78ecb601…); index unique; Generation/Affinity NA counts identical (16,927/16,927; 3,204/2,475; 9,132/9,132). `grain_reports`/`null_read_reports`/`null_index_reports` for household_roster: 0/0/0 each.
- **"Cache cost nil; `_table_cache_hash('household_roster')` byte-identical"** -- HELD against the merge-base (Senegal 28924d40…, EthiopiaRHS 5bca4d18…, Guinea-Bissau 9f3ddd5d… identical in `mb` and branch). NOTE: against origin/development the Senegal and Guinea-Bissau hashes DIFFER -- that movement is development's own PR #971 (#811 Region edits), not this branch; do not charge the post-merge re-warm to #974.
- **#931 docstring claims** -- HELD by grep: `harvest_kg_factors` (transformations.py:3779) and `_kg_factor_series` (:3249) take no `unit_kg`; `_seeded_kg_factors` (:1578) and `food_kg_factors` (:1795) do; `Country._unit_kg_factors` (country.py:2053) is passed only for `food_prices`/`food_quantities` (country.py:5246); `test_quintal_is_exactly_100kg_on_every_newly_served_row` exists (test_ethiopia_shipped_factors.py:397); "13,924 of the 43,384" is at Ethiopia/_/CONTENTS.org:1591. Commit says "no behaviour change": the diff to transformations.py is comments/docstring only.
- **"SCOPE DEVIATIONS: none beyond the two named"** -- HELD vs merge-base: `git diff --stat 2ed85b49b..HEAD` = exactly the 5 files in the brief; no blessed/baseline/lockfile/dependency edits; no assertion weakened, skipped or xfailed; `find lsms_library/countries -name '*.parquet'` empty; no writes outside the private root.
- **EthiopiaRHS CONTENTS.org addition** ("51,574 rows, no unknown-label warning") -- HELD (measured above).
- **Residual drafts present** -- yes: #974 wording, #945 canonical-form decision, #931 item 2 are all drafted in REPORT.md. The fix agent's pushback that ADJUDICATION.org names no canonical form for any variant group is correct (I read it in full: delete list + duplicate finding + `.title()` warning only).

## 2. Claimed vs measured

| claim | claimed | measured |
|---|---|---|
| kinship.yml entries | 403 -> 388 | 403 -> 388 |
| removed / added / tuples changed | 15 / 0 / 0 | 15 (same set) / 0 / 0 |
| key lines (text) | 418 -> 388 | 418 (403 distinct, 13 dup keys) -> 388 (388 distinct) |
| reach, 3 countries | 388 / 38 / 350, unknown 0 | 388 / 38 / 350, unknown 0 |
| reach, corpus (predicted) | 388 / 273 / 115 | raw-label scan: 272 reached both before and after (see methodology note) |
| deleted keys emitted anywhere | 0 | 0 (35 countries, raw labels) |
| rows Senegal / EthiopiaRHS / Guinea-Bissau | 129,650 / 51,574 / 42,839 | same; tuple digests identical to base |
| household_roster cache hash | byte-identical | byte-identical vs merge-base (differs vs development because of #971, not #974) |
| new tests pass on branch | 25 passed (kinship set) | hygiene+crop pin+other_relative on merged tree: 21 passed |
| merges cleanly onto development | (not claimed; assumed) | **NO** -- conflict in tests/test_crop_kg_factor.py |

## 3. Regression
- Full suite on the branch, Makefile recipe (`OPENBLAS/OMP/MKL=1 LSMS_BUILD_WORKERS=1 pytest tests -n 32 --dist=loadfile -q`), private root: `4697 passed, 197 skipped, 3 xfailed, 1 xpassed, 834 warnings in 1043.13s (0:17:23)`, exit 0, no worker lost (skips are the usual cache-gated tests for countries not warm in the private root).
- New tests against origin/development code (branch test files copied into `base`): `2 failed, 6 passed` -- `test_no_key_line_is_duplicated` and `test_adjudicated_dead_keys_stay_out` FAIL on development (meaningful); `test_the_text_reader_sees_every_loaded_key`, `test_spelling_variants_of_one_key_agree_on_their_tuple` and `test_crop_ladder_is_blind_to_declared_unit_kg` PASS on development -- they are pins of invariants that already hold (the crop pin is by design, under a "no behaviour change" commit). Acceptable, but 3 of 5 new tests do not distinguish the branch.
- Trial merge (`merged` worktree, conflict resolved keep-both): hygiene + crop pin + kinship_other_relative `21 passed in 8.67s`; merged baseline keeps #848's `133_331`.
- `bench/feature_audit/scan.py` not run: the touched feature's served frame is byte-identical to base on the three warm countries (digests above), which is the stronger check for a read-path label edit.

## 4. Scope findings
- Exactly the five files; the two deviations the report names (CONTENTS.org sentence, the test placed in `test_crop_kg_factor.py`) are reasonable.
- **Branch is behind development (13 commits) and conflicts** with PR #972's `#848` comment block in `tests/test_crop_kg_factor.py:586-658`. Resolution is trivial (keep development's comment and numbers, then the new test) -- verified.
- The `_fold` equivalence in `test_spelling_variants_of_one_key_agree_on_their_tuple` is explicitly documented as NOT the lookup normalisation and not a canonical form; it does not pre-empt #945's decision.

## 5. Residual risks / human decisions
- **#945 canonical form** (the live decision, correctly NOT taken here): (a) make `_expand_kinship` normalise (casefold + whitespace/punctuation collapse, not `.title()`) and keep one key per group, re-spelling the Peru/Guatemala `CompaÑero`/`CuÑado` keys (live data change, ~5,680 people); or (b) keep every emitted spelling, forbid unemitted ones (the status quo, now written into the file header).
- **#931 item 2**: should the crop ladder read declared `u_kg`? Must land with a re-expressed Ethiopia quintal check; the new pin is written to go red on exactly that change.
- **#974 closure wording**: its body's "fold reachable variants onto a canonical key" sentence is not decided by ADJUDICATION.org; either close #974 with the fold re-homed to #945, or trim #974 to that sentence.
- Stale docstrings outside the branch: `Senegal/2018-19/_/mapping.py:128` and `Niger/2018-19/_/mapping.py:167` say `_fix_relationship` yields `Frere, Soeur`, "the label the sibling EHCVM-1 waves serve"; the data shows the waves serve the accented `Frère, Sœur`, and `Frere, Soeur` is now a deleted key. Misleading, not wrong behaviour.
- The corpus-wide reach count was not re-run through the library (would need all 35 countries warm in a private root); the raw-parquet scan is read-only and covers the deletion question completely but is ±1 on the reached total.

## 6. Recommended follow-up commits
1. Merge/rebase `fix/kinship-974` onto development; resolve `tests/test_crop_kg_factor.py` by keeping development's `#848` comment + numbers and appending `test_crop_ladder_is_blind_to_declared_unit_kg` (verified resolution).
2. Fix the two docstrings: `lsms_library/countries/Senegal/2018-19/_/mapping.py` (`_fix_relationship`) and `lsms_library/countries/Niger/2018-19/_/mapping.py` -- replace "`Frere, Soeur`, the label the sibling EHCVM-1 waves serve" with the accented `Frère, Sœur` and note the ASCII key was deleted under #974.
3. Optionally correct ADJUDICATION.org row 3 prose ("both emitted") to match its own table, citing the 35-country scan.
