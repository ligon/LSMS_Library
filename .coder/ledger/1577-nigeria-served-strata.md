# Prior-Art Ledger — #1577 Nigeria served strata

**Search tier:** git, ripgrep, Nigeria country notes, and exact projected
Stata source comparisons at household-visit grain.

## §1 Task, restated

Record the released labels served by Nigeria's first eight planting and
harvest visits. Verify Wave 5's separate code-to-label lookup, without
calling those labels native or changing the existing sample.

## §2 Existing machinery

| Machinery | Source | Decision |
|-----------|--------|----------|
| `served_strata` | `lsms_library/sampling.py` | Reuse the `native-label` record and attrs carrier. |
| `ROUND_COVERS`, `round_covers`, `sample_for_wave` | Nigeria `_/nigeria.py` | Preserve distinct PP and PH covers and logical wave keys in each physical folder. |
| `_cover_frame`, `_roster_locator` | Nigeria `_/nigeria.py` | Preserve cover, supplement then roster precedence; W3 PH roster uses the own PH cover's rendering of the same code. |
| `_W5_STRATA`, PH location repair | Nigeria `_/nigeria.py` | Retain the lookup, and keep PH location repair confined to `v`. |
| Routing and native-path tests | `tests/test_gh1120_nigeria_round_covers.py`, `tests/test_sampling.py` | Keep the existing YAML guard; verify the script-backed source route on invented data. |

## §3 Definitions & conventions in force

Use #1577's existing registry-and-tag ruling and `STANDING.md` §§3–4.
`CONTENTS.org` documents that W1–W3 serve geopolitical zones and
have no separately named stratum field. The record describes what
`sample().strata` serves, not a reconstructed design or comparability
across visits. PP and PH do not necessarily include the same households.

## §4 Invariants & assumptions

- Read only the own visit's cover, optional agriculture supplement and
  roster. Do not read geovariables, infer strata from `v`, fill a
  missing cover answer, or introduce a new label.
- W2 PP may serve its own agriculture cover label on supplement-only
  households. W3 PH's retained roster codes use the same visit's
  released cover label when their code occurs there.
- W5's unlabelled source and library lookup are evidence of a
  vocabulary gap, not an absent column, native label, or registered
  construction.
- Leave all returned sample rows, types, warnings and cache hashes
  unchanged. The first visit receives no panel boundary.

## §5 Reuse decision

Apply eight existing native-label records and leave the two lookup
visits untagged. No runtime loader or vocabulary change.

## §6 Open questions for the human

The two Wave 5 code-to-label visits share the open classification
question for Burkina Faso's corresponding lookup. Do not decide that
question by inventing a tag or stamping a derivation in this patch.

---
### Phase 3 — verification

- **OK (§§2–4).** Twenty-one exact held covers and rosters
  (106,964,789 bytes) matched the tracked sizes and digests. An
  independent projection matched every pre-finalize label at
  household-visit grain. Each of the first eight covers has six native
  label bindings, whereas both Wave 5 covers lack a binding and occupy
  the six codes decoded by the existing lookup. Own supplement labels
  contribute 21 households in 2012Q3, and 2016Q1 uses two roster-only
  codes with the matching cover spellings.
- **OK (§§2–5).** The native-path guard exercises the actual PP/PH
  producer with invented operands. Other operands carry different
  labels, and a shared invented household has distinct planting and
  harvest labels. The Wave 5 test requires the lookup-specific
  refusal. Existing Nigeria cover routing and same-location tests
  remain in the focused check.
- **OK (§4).** Ten strict cold/warm Country and Feature comparisons
  keep all 49,089 rows, keys, dtypes, included warnings and non-target
  attrs exact. Fresh-process warm calls do not open a native source or
  rewrite the sample cache. The nine existing panel records are exact,
  and all 264 Nigeria cache fingerprints are unchanged. The focused
  source-free suite passed 415 tests, with one skip.
