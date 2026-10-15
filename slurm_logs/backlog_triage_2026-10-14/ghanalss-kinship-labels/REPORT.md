# Triage element `ghanalss-kinship-labels` — 14 issues, READ-ONLY (2026-10-14)

Base: `development` @ `a7cf04f2d`. No fix was made, no branch created, no `gh` write, no cold build. The only `Country()` calls were config-only (`categorical_mapping`, `_relabel_j` on a synthetic frame) in the private `LSMS_DATA_DIR`. One external fetch: the Tajikistan 1999 BID (WB catalog 279 / 11687) was downloaded to `$STAGE/tajbid/` as evidence for #804 (pypdf installed under `$STAGE/pylib` to read it). GitNexus was down; `rg`/grep substitutes used throughout. In `records.json` the extra key `priority_why` carries the brief's "one sentence of why" beside the integer `priority`; the other 15 fields are exactly the brief's list. Nothing was committed or pushed: the clone is unchanged (`git status --short` empty, no in-tree parquets) and every write went under `$STAGE`.

## 1. Table

| # | tier | type | tractability | dedup_status | prio | root_cluster | approach |
|---|---|---|---|---|---|---|---|
| 948 | B | bug | auto | **resolved** PR #952 `171502fbb4` (+`6efb96437`), CLOSED today | 3 | countries-root-override | nothing |
| 936 | B | bug | auto | **resolved** PR #947 `ff1eb3f658`, CLOSED today | 3 | ghanalss-food-valuation | nothing; CONTENTS.org:3406 follow-up (22 dropna'd rows) has no issue |
| 783 | C | framework | auto | **resolved** PR #789 `f72818d0fe` (+#955 `2c27352758` pins the recorded decision) — still OPEN | 4 | categorical-mapping-resolution | close it |
| 913 | B | bug | human | partial — PR #916 `f959ff6444` valued 4/5 waves; 1991-92 excluded by recorded decision | 2 | ghanalss-food-valuation | A/B/C for 1991-92; basis='total' coverage warning dispatchable alone |
| 787 | B | bug | diagnose-then-auto | partial — finding 9 fixed `733a4499a` (#925); 1, 2, 4 open; 5 now a no-op by accident | 2 | categorical-mapping-resolution | `_relabel_j`: ' Label' normalisation, unmatched-j signal, doc accepted values; yes/no on refusing 1→many |
| 956 | C | data-gap | human | open (recount: 940 joined / 385 disagree) | 3 | categorical-mapping-resolution | adjudicate DISAGREEMENTS.org, then drop the 7 wave columns (batch with PR #969) |
| 698 | C | design | human | partial — item 1 landed `3069ef80`; policy (ii) open, 36,632 people | 3 | kinship-vocabulary-hygiene | yes/no on `[G, D, null]` for the of-head-or-spouse classes |
| 804 | B | bug | diagnose-then-auto | open — **blocker dissolved**: BID fetched, contains NO code list | 3 | kinship-vocabulary-hygiene | yes/no: remap `14.0: lodger` → `Other` (13 people) |
| 953 | C | design | human | partial — PR #955 docs+tests only | 4 | categorical-mapping-resolution | A (retire cascade rung, merge owns inheritance) / B (directory rung) |
| 945 | C | test | diagnose-then-auto | partial — PR #951 `b0e61fa235`; 72 variants left (was 157), 0 unreachable | 4 | kinship-vocabulary-hygiene | duplicate-key-line test now; casefold rule after canonical-form choice |
| 813 | C | data-gap | diagnose-then-auto | partial — PRs #954/#951; 15-key delete list (`717258b68`) unapplied | 4 | kinship-vocabulary-hygiene | apply the adjudication on a yes |
| 781 | C | design | **blocked** (guardrail: `.coder/coverage/`) | open; motivating set shrank 6→3 waves after #916 | 4 | coverage-verdict-vocabulary | loader vocabulary (auto) + 3 verdict rows (human) |
| 928 | C | design | human | partial — EthiopiaRHS item landed `1fcdf1e5a`; P1–P4 unbuilt; 2 of 4 sites consolidated | 4 | categorical-mapping-resolution | yes/no on P1+P2 first |
| 931 | C | docs | diagnose-then-auto | open | 4 | kg-ladder-asymmetry | doc note + pinning test now; yes/no on crop ladder reading `u_kg` |

## 2. Reconcile

**Close now (verified by reading the diff/tree):**
- **#783** → PR #789 `f72818d0fe` (2026-09-08). All six countries ship `#+name: harmonize_food` in `_/categorical_mapping.org`, `food_items.*` gone, `nutrition.py` repointed, dead `harmonized_food_labels` pairs deleted; `Country(c).categorical_mapping` resolves for all six (config-only check). Scope item 3 (whole-table wave override) is on the record in the ledger §6 and pinned by #955's `test_wave_rung_of_the_read_time_merge_is_whole_table_even_for_allow_listed_names`. No reconcile comment exists yet.
- **#948** and **#936** were closed today by the scrum master; my independent read of the seven scripts and of `1998-99/_/food_acquired.py:127-222` confirms both.

**Already carry a partial comment from today (verified, nothing to add):** #953, #945, #813.

**Need a partial comment:**
- **#913** → PR #916 `f959ff6444`: produced `Expenditure` non-null 1998-99 98,370 / 2005-06 170,291 / 2012-13 197,924 / 2016-17 138,755 (warm parquet read directly); 1991-92 still 0 of 287,712 by `derivations.yml:160-211`'s recorded assumption. Sue's comment already says this but no state change was recorded.
- **#787** → `733a4499a` (#925) fixed the blank→`''` collapse (finding 9); #789 shrank the raise-on-everything set from 8 to 2 (Nepal, Peru). Findings 1, 2, 4 reproduce on today's code.
- **#698** → `3069ef80` landed item 1; the body's two contrasted tuples no longer exist.
- **#928** → `1fcdf1e5a` landed migration item 1 (EthiopiaRHS).
- **#804** → the gating BID is obtainable and silent (below).

## 3. Dispatchable now (auto, no decision needed; tier in brackets)

1. **[B] #787 (a)–(c)** in `Country._relabel_j` (`country.py:3071-3192`): strip a trailing `' Label'` before the no-op test; count unmatched `j` and attach `attrs['labels_unmatched']` + warn; document the accepted-value surface. Tests in `tests/test_label_selection.py`. Cache: expected zero (post-load read path) but `_relabel_j` is not in `_EXCLUDED_CALLABLES` — probe `_table_cache_hash` before landing.
2. **[C, auto half] #945 duplicate-key-line test** (`tests/test_kinship_yml_hygiene.py`, read as text): 418 lines / 403 keys, 13 keys written 2–3×, all agreeing today. Cache: zero (kinship.yml is read-path).
3. **[C, auto half] #931 items 1+3**: asymmetry note on `harvest_kg_factors` / `_seeded_kg_factors(unit_kg=)` and a pinning test naming the Ethiopia quintal check. Cache: zero (no `transformations.py` symbol in any fingerprint).
4. **[B] #913 warning half**: `food_expenditures(basis='total')` disclosing a wave where a requested source contributes no value (transformations.py, zero cache cost). Closes the silence regardless of the 1991-92 decision.
5. **[B] #936 follow-up** (needs its own issue first): replace `dropna(subset=['u'])` on 22 undecodable produced rows with the `Unknown` sentinel. Cache: one GhanaLSS re-warm — batch with #956's drop and PR #969 (both already force it).

Everything else above is one yes/no away: #804, #813, #698, #956, #787(d), #953, #928, #781.

## 4. Escalations (one line each)

- **#913**: 1991-92 own production — A) value the 28% under 8h-farmgate, B) new key for `u='All'` whole-harvest, or C) keep NA + coverage warning?
- **#956**: adjudicate `DISAGREEMENTS.org` with "country table wins unless argued per row" — yes/no? (then the wave-column drop is auto)
- **#953**: cross-country inheritance — A) retire the cascade's third rung, read-time per-row merge owns it; B) directory rung with first-hit semantics?
- **#945**: canonical key form = the spelling the corpus emits, casefold-equality test (not `.title()`, which mangles `Lodger's`) — yes/no?
- **#813**: apply ADJUDICATION.org (delete 15 unreached keys, keep 115) — yes/no?
- **#698**: `[G, D, null]` for every "of head or spouse" / "X or X-in-law" label (36,632 people, Affinity slot only) — yes/no?
- **#804**: BID is silent; accept the questionnaire and remap code 14 `lodger` → `Other` (13 people) — yes/no?
- **#787(d)**: refuse (raise) when one Preferred Label maps to >1 target value instead of last-row-wins — yes/no?
- **#781**: adopt `correctly-empty` (closing, evidence-required) / `empty-pending-machinery` for dropped cells in `absent_verdicts.csv`, and may an agent write the three GhanaLSS rows — yes/no?
- **#928**: P1 (`#+key:`) + P2 (one resolver) first, P3/P4 deferred — yes/no?
- **#931 item 2**: should the crop ladder read `u_kg`, with the Ethiopia check re-expressed in the same PR — yes/no?

## 5. Contradictions with CONTENTS.org / CLAUDE.md

- **Tajikistan CONTENTS.org:519-525** says the 1999 BID is "the document that would settle it". I fetched and read it (57 pages): it has no per-question code list, states "There are no predefined missing codes" (p. 9), and only mentions lodgers in interviewer instructions. It cannot settle #804. The entry (and #804's own gate) should be reworded.
- **GhanaLSS CONTENTS.org:3460** says the country `harmonize_food` carries an Aggregate Label "for every one of its 215 Preferred Labels"; the table now has 219 rows (0 blank Aggregate, so the invariant holds, the count is stale).
- **Peru and Guatemala CONTENTS.org** record nothing about #698 (`Otro Pariente` null by the ENNIV manual; `Distance` constant 0 is the instrument). The ruling lives only in `kinship.yml:442-464` — a gap, not a contradiction.
- **#928's "four places"** is stale: `country.py:393` `_categorical_key_column` now serves `:410-411` and `:2109`; only `:2983` and `:3326` remain positional inline.
- **`.coder/coverage/latest.csv`** still grades six GhanaLSS `food_prices` cells `dropped`; since #916 + `67e9a841e` four of those waves are served, so the "correctly empty" set for #781 is three waves, not six.
- Nothing contradicts CLAUDE.md; its "Categorical mappings resolve wave → country → global" and "Derived Values" sections match the code read here.

## In-flight PR overlap
None touches these issues directly. #969 edits `GhanaLSS/_/Makefile` (forces a GhanaLSS re-warm → batch #956/#936 edits with it); #967 edits Uganda's and #971 three countries' `categorical_mapping.org` (the #928 surface; batch any migration there).
