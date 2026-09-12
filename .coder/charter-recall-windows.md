# Workshop Charter — Phase I: surface what aggregation needs; defer which aggregation

> Phase-1 artifact of the `workshop-problem` skill. §2 is the oracle phases 2–6
> loop against. **SIGNED OFF 2026-09-09 by @ligon; Phase I closed.**  Rewritten 2026-09-09 after four adversarial reviews
> largely refuted the first draft, which was then dropped (its premises and their
> refutations are recorded in §0; the reviews' findings in §6).

## Context (why)

`food_expenditures` returns a number whose reference period is recorded nowhere.
Most of the corpus is **7 days asked once**; GhanaLSS delivers **30 days** of
coverage, Burkina Faso 2014 **28**, Guatemala and Panama **15**. Nothing in the
API distinguishes them, so pooling GhanaLSS with Ethiopia compares a 30-day
figure with a 7-day one. Nine cells across five countries have no single window
at all. GH #851.

## §0 Question, restated

@ligon, 2026-09-09:

> Phase I should just make sure we're surfacing whatever information is needed
> to do a sensible aggregation, while deferring the question of *what*
> aggregation one should use. Acknowledging also that some of the data that
> seems to be needed is simply missing, in which case we will have to fall back
> on claims about the visit schedule.

So: **ship facts, not transformations.** Make the reference period legible, make
the measured/declared boundary explicit at the point of use, and make the gaps
visible as gaps. Decide nothing about how to aggregate.

**Premises challenged, and their resolution:**

- *"Measure the interval from the survey's own dates"* (@ligon, and the first
  draft's core) — **refuted empirically.** Reported spend does not respond to the
  realised interval: within household on GLSS4, doubling the gap from 5 to 10
  days moves reported spend +1.2% where proportionality predicts +100%
  (elasticity 0.018, identical item counts). Dividing by measured days injects
  fieldwork tempo — bin GLSS4 by exposure and total spend is flat while the
  implied daily rate spans 2.7×. Scott and Amenuvegbe (1990) predicted the
  mechanism: respondents switch to *normative* reporting ("what I usually buy").
  **Resolved:** record dates as provenance, never as a denominator.
- *"The R1/R2 partition is mechanically checkable"* (first draft) — **falsified.**
  Running that test over the corpus returns 14 R1 cells whose measured gaps are
  0.06–1.05 days (enumerator sittings) or 101 days (panel rounds); and it
  classifies the two cells it was built for as R2. 14/14 false positives, 2/2
  false negatives. **Resolved:** dropped.
- *"`population.yml` is the precedent"* (first draft) — **half right.** Its
  `attrs` + provenance shape transfers; its cardinality does not, and #603 built
  no transformation. **Resolved:** take the attach-and-record half, leave the
  rest; the evidence ladder comes from `capability.py`, whose problem (nobody has
  read the questionnaire yet) is the one actually binding.
- *"Item-list length is a gap we cannot close"* (Sue, 2026-09-09) — **wrong, and
  @ligon caught it.** Beegle et al. (2012) find list length costs ~21% against
  recall length's ~7%, and the list length is `groupby('t').j.nunique()` — it is
  already in the data. **Resolved:** compute it; do not record it.

## §1 Scope & non-goals

**In scope (Phase I):**
- Surface, per `(country, wave)`, the four facts an aggregator needs: the length
  of one ask, the number of asks, whether the window varies within the wave and
  on what axis, and the evidence rung — including `not-recorded`.
- Attach that record to `df.attrs['recall']` and expose `ll.recall()`.
- **Distinguish measured from declared at the point of use**, as a field.
- **Compute the item-list count** from the delivered `j` level.
- **Make the ask count observable where the data can support it** — which
  requires fixing Burkina Faso 2014 (below) and normalising GhanaLSS's `visit`
  dtype.

**Out of scope / non-goals:**
- `period=`, scaling, annualisation — *any* transformation. Deferred to Phase II,
  whose gates are **coverage and a missing-data contract** (§6), not a research
  question.
- A per-column or per-row window carrier (`recall_days:` on `Columns:`,
  extending `RecallWindow`). The right grain, and a Phase II decision.
- The date-measurement pipeline: `melt_visit_intervals` wiring, exposure joins,
  plausibility screens.
- The pooling warning. It would now fire meaningfully, but what to *tell* people
  is a judgement; Phase I is about having something true to tell them.
- Non-food, `crop_production`, `livestock`, `plot_labor`.
- Countries beyond what the committed table already covers.

## §2 Definition of done — the loop oracle

- [x] **2.1** All 67 cells resolve to a record with an explicit rung, including
      `not-recorded`. *Decided by:* a test enumerating the table against the
      `sane` `food_acquired` cells of `.coder/coverage/latest.csv`.
- [x] **2.2** `df.attrs['recall']` is present and correctly keyed for a
      `Country(...)` call and survives a `Feature(...)` assembly. *Decided by:*
      tests on both paths, including the `attrs`-disagreement case that #603
      pinned.
- [x] **2.3** The record distinguishes **measured** from **declared** ask counts
      in a field, not a comment. GhanaLSS 1991-92 / 1998-99 read theirs from
      data; every other cell is declared. *Decided by:* a test asserting the
      field's vocabulary and that no cell is silently one or the other.
- [x] **2.4** `n_items` is computed from the delivered `j` level per
      `(country, wave)` and carried in the record, labelled as *delivered
      distinct items* — not instrument list length. *Decided by:* a test that it
      matches `groupby('t').j.nunique()` on a built table.
- [x] **2.5** The within-wave-variation cells report their axis rather than a
      number.  Five of them, not the nine first counted: Guatemala and Panama
      were reclassified once the promoter refused a cell carrying both an axis
      and a value — their instruments mix periods but their builds resolve it,
      so the DELIVERED window is single-valued. *Decided by:* a test asserting numeric fields are null wherever
      `within_wave_variation` is set.
- [x] **2.6** **No returned number changes** for any country except Burkina Faso
      2014, whose change is the point (see 2.7). *Decided by:* before/after
      comparison across the built food tables.
- [x] **2.7** Burkina Faso 2014's four 7-day passages are no longer summed into
      one unmarked figure; `visit` is on the index and the ask count is
      observable. *Decided by:* a test that the wave's `food_acquired` carries
      `visit ∈ {1..4}` and that per-passage sums are recoverable.
- [x] **2.8** GhanaLSS's `visit` level is a single dtype across all seven waves,
      with no collision between "intake" and "the single recall". *Decided by:* a
      test on the delivered level's dtype and value set.
- [x] **2.9** No cache hash moves for any `(country, table)` not otherwise
      touched. *Decided by:* the `_table_cache_hash` probe used for
      `population.yml`.
- [x] **2.10** Reviewed and accepted by **@ligon** — signed off 2026-09-09.

## Phase I closed

All ten criteria met.  Verified rather than asserted, in the cases where that
mattered: no cache hash moved (8/8 probes byte-identical for the record layer,
27/30 for the Burkina Faso fix with only its own three moving); the GhanaLSS
`visit` normalisation conserved the table bit-for-bit (5,317,365 rows, index
identical, three measure deltas exactly 0.0); the record attaches on a real
325,920-row build with the population record undisturbed.

Two findings during implementation were larger than the criteria that surfaced
them, and both are recorded where a future reader will meet them rather than
only here:

- **Burkina Faso 2014's rows were being DELETED, not summed** — 460,438 of them,
  82.5% of the wave, on a NaN `u` key.  Delivered expenditure went 80,286,096 ->
  342,110,515 CFA.  This is the cell `CLAUDE.md` §3b already named as the
  corpus's worst such loss; both the red-team and Sue diagnosed a summation
  instead, because `_ADDITIVE_MEASURE_COLUMNS` made that story plausible.  The
  documentation was right and the diagnosis was wrong.
- **The mixed-type `visit` level was a cold-build-only symptom.**  A warm read
  came back uniformly string, because pyarrow launders a mixed object level on
  the parquet write — so the same call returned differently typed data depending
  on cache warmth, and a sweep of 193 warm parquets found zero mixed levels.
  GH #323's signature: the bug hiding behind the cache it poisons.

Phase II remains unstarted.  Its gates are coverage and a missing-data contract
(§6) — both decisions, neither research.  Follow-up filed as GH #864
(`food_prices` misattributes Burkina Faso's now-visible unpriceable share).

## §3 Constraints & assumptions

- **Must not break:** any existing test; any returned value except 2.7; D1
  (`visit` preserved on `food_acquired`, summed in derived tables).
- **Attach point:** `_finalize_result`, which is in
  `_build_registry._EXCLUDED_CALLABLES` — the only insertion point that costs no
  invalidation. Do **not** attach from `_aggregate_wave_data` or `Wave.grab_data`.
- **No cross-table join.** The record is read from a CSV, not from
  `interview_date`; a join there recurses (demonstrated) and breaks the four
  countries that have no `interview_date`.
- **Reported values only.** Nothing is scaled.

## §4 Links

- **Evidence base:** `.coder/coverage/food_recall.csv` + `.org` (committed
  `889967056`)
- **Prior-art ledger:** `.coder/ledger/ghanalss-visit-offset.md`
- **Issue:** GH #851
- **Literature:** `GhanaLSS/_/CONTENTS.org` §"The literature" — Scott and
  Amenuvegbe (1990); Beegle et al. (2012)

## §5 Constraints inherited from the reviews

- `n_visits` counts **asks**, not interviewer visits, corpus-wide. GLSS7 is 6
  asks over 7 visits; `recall_days × n_visits` is arithmetic only on asks.
- Only **8 of 34** stated windows are questionnaire-grade. The record must not
  present a `config-comment` assertion as a fact.
- **Blank beats plausible.** GhanaLSS 2005-06 keeps every numeric field empty.

## §6 Open questions & risks

- **The ask-order bias does NOT gate Phase II.**  Recorded here because an
  earlier draft of this charter said it did, and @ligon corrected it
  (2026-09-09): *"I don't see how the ask-order bias is relevant to our narrow
  issue.  It's another general issue."*  He is right.  `period=` divides a total
  by a known exposure, and that arithmetic is correct GIVEN the total.  Whether
  the total estimates true consumption is a measurement question — ask-order
  bias, recall decay, list length, diary vs recall — and every one of those is a
  general property of how these surveys elicit consumption.  Withholding a
  correct conversion because its input carries measurement error is not a
  coherent policy: by that standard `food_expenditures` could not be reported
  either, since it carries the identical error.  The same mistake as importing
  the list-length problem into scope, made twice in one session.

  What survives is a DOCUMENTATION note, not a hold: a single-ask 7-day survey
  is *entirely* first-ask, whereas GhanaLSS's 30 days is one first-ask premium
  spread over six asks (1.84× unaided / 1.17× diary-assisted, measured at
  identical interval lengths).  The premium therefore enters the two at
  different weights and dividing by days does not equalise it.  Say so beside
  `period=`; do not withhold it.  Note the illusion of comparability already
  exists — callers compare `food_expenditures` totals across countries today
  without knowing the windows differ at all.

- **Phase II's actual gates, both decisions rather than research:**
  1. *Coverage.*  `total_exposure_days` is known for **28 of 67 cells**.
     `period=` is undefined for the other 39 — 33 `not-recorded`, 5 varying
     within the wave, and GLSS5 whose spacing is unestablished.  What share is
     enough to ship on is a judgement.
  2. *A missing-data contract.*  The red-team argued **Contract B** — a typed
     raise from `Country(...)`, structured degrade plus one aggregated warning
     from `Feature(...)`, by analogy with `LabelUnavailableError` — and against
     NaN, which propagates into `sum()` as `0.0`.  GH #828 is the live
     precedent for why silent-empty is the wrong answer.
  3. *`food_prices` must REJECT `period=`.*  A price is not a flow, and someone
     will add it for symmetry unless it is refused explicitly.
- **`n_items` from delivered `j` is a proxy** for instrument list length —
  households report only what they consumed, so the wave-level union
  approximates the printed list but does not equal it. Label it as delivered.
- **Half the corpus is `not-recorded`** (33 of 67 cells, ~3.1M rows). Phase I
  makes that visible; it does not fill it. Per `capability.py` that is a bounded
  human step per series — one RA, one questionnaire.
