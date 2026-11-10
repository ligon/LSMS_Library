# Prior-Art Ledger — GH #1687 Nigeria interviewed-link test oracle

**Base:** d47ff849eb0e7f2c7e9d4d77a7852b61e9240e61.
**Scope:** ROOT-approved test-only repair; isolated dispatch 92c1ce90.
**Search tier:** git + ripgrep floor (GitNexus unavailable).
Inherits [STANDING.md](STANDING.md), CLAUDE.md and canonical data_info.yml.

## §1 Task, restated

Correct only Nigeria's operands in TestTheReproduction's curated-size check.
Historical GH #1448 quantities count committed GH #1264 interviewed links,
whereas the current test intersects all served household IDs. Keep the exact
N/D/K assertions, curated records, public sample and every non-Nigeria branch.

## §2 Existing machinery

| Symbol/source | Locator | Role / coverage | Decision |
|---|---|---|---|
| `_links` | tests/test_gh1448_nigeria_sampling_notes.py:25 | Parses committed round/i endpoints; its existing data test compares their v | reuse operand definition, leave existing helper untouched |
| target reproduction | tests/test_sampling.py:1447 | Exact N, D, optional K on paired Series | extend only operand selection |
| committed crosswalk | Nigeria/_/panel_ids.json | Reviewed source, not a producer to run | read UTF-8; never regenerate |
| sampling curation | Nigeria/_/sampling.yml:27–31,117–129,164–180 | Dated crosswalk measurements | preserve verbatim |
| Country cache gates | country.py; STANDING §2 | Normal constructor and warm sample | unchanged |
| normal test fixtures | tests/conftest.py:167–184,199–211; root conftest.py | Root-cache clearing and real requires_s3 credential gate | retain in actual phase |

Closed diagnosis packet: Nigeria-Sampling1687/3ad1de52098640f9980e179f944f133d,
final10b8696003568acf5c03db4e45a248e1b1f80cd5. REPORT/DECISIONS/proposal/pure
plan read; no old data/cohort campaign will be replayed. Original actual target
failures at N (1493 versus1454;5019 versus4736) and old global-runtime placement
qualification remain history. No new scientific interpretation follows.

## §3 Definitions & conventions in force

Nigeria CONTENTS personally read in full, including WAITING/TODO/LOGBOOK:
4142 lines, SHA25632247c0c6aa6863c7c16a6c35436c39878864077ed7cf7a4bac890279805bb14.
PP/PH are distinct t values and own covers. v is state/lga/ea; moved households
have singleton keys. Whole-EA LGA recoding can change v without physical movement.
CONTENTS:894–925: status gates links, not sample/roster admission; sampling.yml
"counts the links of the committed crosswalk". Stable hhid across a missed
interview does not create an adjacent link. No contradiction with this repair.
Retain the documented contradiction: superseded "The same households are
visited in both rounds" versus "The per-round source counts below disprove
identical membership" (691–697). Sample skill's generic `ea` row is qualified
by country documentation: "ea ALONE IS NOT THE CLUSTER ID" (country heading).

## §4 Invariants & assumptions

Inherit STANDING §4 and CLAUDE sample/cache invariants. Only this ledger and
tests/test_sampling.py may change. No country-note appendix is needed.
Helper requires a nonempty declared current round, declared predecessor,
unique endpoints, present nonnull v and explicit pair alignment. Left Series
receives the RIGHT typed Index, preserving its name/dtype. Input immutable.
All old numerical assertions, fixtures, functions and non-Nigeria logic stay.
No product, ID/map, weighting, science, warning, source or raw acquisition edits.
Pure cohort is finite and synthetic, independent of measured Nigeria counts.
Source/helper/control freeze precedes tests. Ordinary -n2 --dist=loadfile,
thread1/build1, direct logs/XML and source-bound private observer required.
After pure pass only two original Nigeria parametrizations are granted, with
normal conftests/real reader credential file and authenticated full d47 cache.
No rebuild/source/native/network/provider/writer fallback; unexpected cold stops.
Runtime MAIN image identity and all state/cache/tmp under OWN node-local root
must be set BEFORE first package import, including mount helper import check.

## §5 Reuse decision

| Quantity/behavior | Decision | Reason |
|---|---|---|
| Nigeria link population | extend test with parser/alignment helper | same committed relation as GH #1448; extra validation needed |
| N/D/K | reuse exact target body | no expected-count change; K split/merge controls exercise original body |
| other countries | reuse exact intersection branch | no claim or behavior change |
| controls | new, synthetic | tests adversarial membership/pairing/refusal, not real counts |
| runtime observer | reuse closed operator with explicit bounded adaptations | own state roots and normal pytest are newly required |

## §6 Open questions for coordinator

None blocking under explicit implementation/pure/two-actual grant. Root final
review/CI and whole issue/country/scientific acceptance remain pending.

### Phase 3 — verification

Pending. Source inverse must recover original module byte-for-byte after
removing only new helper/control class and replacing Nigeria selection branch
with original two lines. Record finite pure and exact two actual outcomes;
failure/skip/guard is never PASS. Ledger final update is a separate commit.
