# Prior-Art Ledger — GH #1439 / #1526 demand source index

**Search tier:** ripgrep + git; no GitNexus tools are exposed. Base
`c780562b299bb100cdd006037ba424c8ad3264e9`. Inherits `STANDING.md`.

## §1 Task, restated

Repair the in-repository demand adapter and its skill recipe: the Country
food-expenditure API intentionally retains acquisition source `s`, whereas
installed CFE accepts log expenditures on exactly `(i,t,m,j)`. Aggregate
monetary levels explicitly at that consumer boundary, preserve the Country
API, and refuse identities that cannot be represented by a single-country
demand system. Validate with invented frames, without sources or fitting.

## §2 Existing machinery

| symbol | path:line (base) | behavior / tests | decision |
|---|---|---|---|
| `main` | `lsms_library/demands.py:31` | Country getters -> logs -> Regression; no dedicated tests found | extend |
| `food_expenditures_from_acquired` | `lsms_library/transformations.py:3306` | purchased default; total opt-in; deliberately retains s; doc explicitly permits consumer sum across s; existing food-basis tests | reuse, unchanged |
| `_add_market_index` | `lsms_library/country.py:3420` (search symbol) | market index supersedes v, keeps unlabeled markets; market tests | reuse, unchanged |
| `_prepare_inputs` | installed `cfe/regression.py:896` | exact y/d axes, sums duplicate log records in levels; no sample selection | use actual input check in synthetic tests, never edit dependency |
| `prepare_data` / `Regression` | installed `cfe/regression.py:936,1147` | fitting selection after input check | leave unchanged; fitting outside scope |
| aggregation evaluator | `SkunkWorks/aggregation/evaluation.py:72` | wide item partitions, fills missing with zero | not reusable: different axis and missingness contract |
| CLI | `lsms_library/Makefile:78` | invokes `python -m lsms_library.demands` | preserve route |

`git log` shows no merged demand-index fix after `c2b837355` (Country adapter).
Frozen task-context records both issues OPEN with zero comments; #1526 is the
recipe manifestation of #1439, not a separate estimator task. No existing
task ledger for either issue was found. Shared #1602/#1603 and #1480 are excluded.

## §3 Definitions & conventions in force

- `s` is acquisition source, canonical `lsms_library/data_info.yml:92`.
- `STANDING.md` §3/§4 and `CLAUDE.md` Derived Tables: Country food API is
  runtime-derived and retains native source detail. Monetary values share a
  unit within a country/period; this adapter neither converts currency nor
  pools countries.
- `food_expenditures_from_acquired` docstring: purchased means cash outlay;
  total means all recorded acquisition value, without implicit valuation.
  Its Notes explicitly describe summing across `s` at the consumer.
- `.claude/skills/add-feature/food-acquired/aggregate-labels/SKILL.md`:
  Aggregate changes item labels, not source identity. Missing curation raises
  at Country; Feature degrades loudly. Tanzania has no Aggregate curation.
- Uganda `_/CONTENTS.org:1239-1253`: Kgs is dropped before canonical API;
  `:1407` onward: UBOS price versus unit-value comparison is mostly an identity.
  No price or quantity conversion belongs in this monetary adapter.
- Uganda `_/CONTENTS.org:539-579,601-676`: household Region avoids the mover
  loss, but missing markets remain; District vocabulary differs across waves.
  `:4124` onward: nonfood has a different window/shape; do not add it here.

## §4 Invariants & assumptions

- Sum only supplied acquisition-source rows in monetary levels before logs;
  do not drop s by renaming, select one source, or sum logarithms.
- `sum(min_count=1)` retains all-missing groups. Partial missing groups yield
  the observed subtotal; no unreported value is imputed. Native zeros and
  negative adjustments enter the sum. Zero totals have no log; negative or
  infinite totals need an explicit refusal.
- One household-period has one market. x/d may be in different level orders
  but cannot disagree on market for a shared household-period. Null identities,
  repeated native keys and unrecognized axes (including country) refuse.
- A pooled Feature frame must first be explicitly sliced to one country on
  BOTH inputs; never erase country and merge household IDs across countries.
- Preserve caller frames and attrs; adapter does not build or write caches.
- Runtime verification suspended by root steering: prepare a deterministic
  source-free operator and request root execution; no nested unshare here.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| expenditure source sum | new consumer helper in demands.py | existing API deliberately preserves s; evaluator's fill-zero item reduction is unsuitable |
| log / Regression / fit | reuse | existing demand behavior, normalize axes first |
| expenditure basis | extend main argument, retain purchased default | explicit cash/recorded-total choice, no new scientific default |
| recipe | extend | call same adapter to avoid a drifting duplicate |
| Country getters, units, labels, caches, baselines | reuse unchanged | outside adapter boundary |

## §6 Open questions / remaining gates

No scientific decision is needed for this bounded source aggregation; the
existing transform documents it. Real-country estimation, sample adequacy,
market comparability and numerical fitting are outside this fix. Root owns
runtime verification and review/publication. No source-stage grant is needed.

### Phase 3 — verification

Pending frozen source-free tests and root runtime execution. No runtime pass
is claimed at ledger creation.
