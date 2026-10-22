# Prior-Art Ledger - GH218 Malawi derived-food registrations

**Search tier:** ripgrep + git floor. Inherits `STANDING.md`; the advertised
shared skill paths were unavailable in this allocation, so repository guidance
and the existing per-task ledger convention are used directly.

## §1 Task, restated

Remove only the two null `food_expenditures` and `food_prices` registrations
from `countries/Malawi/_/data_scheme.yml`. Retain all three public food-derived
methods, Country discovery/order, Feature discovery, and their existing
derivation from `food_acquired`. This does not retire the products.

Root explicitly approved this independent Malawi-only item2 implementation on
release `f8d236694c26fe9e709c5548f02c70f74ea72c33`. It does not depend on
unmerged Malawi1109/1111. The separate actual-f8 parent, all earlier Tanzania/
Uganda proofs and other country lanes remain immutable. Live GH218 was read:
open, updated2026-10-18, zero comments; item2 names these exact two keys.

## §2 Existing machinery

| Symbol / path | Existing behavior | Reuse decision |
|---------------|-------------------|----------------|
| `Country.data_scheme`, country.py:2927 | appends `_FOOD_DERIVED` names in existing order when `food_acquired` exists | reuse unchanged |
| `Country._FOOD_DERIVED`, country.py:6069 | maps expenditures/prices/quantities to existing transformations | reuse unchanged |
| `Country._materialization_entry`, country.py:2464 | returns `{}` for null and absent entries | reuse unchanged |
| derived branch, country.py:6240 | derives from food_acquired before legacy materialization fallback | reuse unchanged |
| `Feature._DERIVED_SOURCE` / `_discover_countries_for_table`, feature.py:751/759 | discovers derived names through their source declaration | reuse unchanged |
| `Malawi/_/CONTENTS.org`:1653ff | old derived-food script and Make rule already retired2026-05-05 | retain historical decision |
| `get_dataframe` / `to_parquet` / existing hashes and manifests | sanctioned IO, natural invalidation and requested-wave coverage | reuse per STANDING§2/4 |
| existing food_prices_units_kwarg / food_expenditures_basis / food_prices_dtype / feature_vocabulary tests | existing unit, basis, dtype and discovery contracts | read and select focused controls before execution |

No alternative registry, transform, reducer, marker, selector, framework flag or
Make fallback will be added. A narrow raw-registration policy test is allowed
only after the actual loader-based census establishes the current offenders.

## §3 Definitions and country grounding

Per `STANDING.md§3/4`, `CLAUDE.md` Derived Tables and
`lsms_library/data_info.yml`: derived tables are runtime products, not raw Data
Scheme registrations. The current raw null keys contradict that registration
policy while the country note already says they "now derive from food_acquired".
The methods themselves are intentionally preserved.

The complete current3063-line Malawi CONTENTS was read, including tasks and
LOGBOOK entries. Its food-relevant idiosyncrasies are retained:

- 2013-14 is panel-only; 2016-17 and2019-20 combine cross-section and tracking
  panel populations with different universes. Preserve joins, cs-17 ID handling
  and baseline-vs-current EA geography; tracked movers are not broken keys.
- IHS2 has no survey conversion table: its factor comes from metric magnitude
  in the unit label. IHS3+ use region-keyed conversion tables. The old Known
  gaps phrase "lacks any unit-to-kg conversion factors entirely" is contrasted
  with the later explicit label-magnitude exception, not silently discarded.
- Missing-unit OTHER/N/A text means `Unknown`; existing shipped factors may
  outrank that sentinel. No unit spelling, factor, valuation or eligibility
  policy changes are authorized here.
- Historical crop comments saying labels "JOIN food_acquired.j" are explicitly
  contradicted by the later measured overlap; no vocabulary harmonization is
  part of these two registration deletions.
- Default price basis is `kgvalue`; native `unitvalue` is distinct. Malawi's
  recorded-price modes may raise `UnitsUnavailableError` because `Price` is
  absent. An exception is not an empty payload or a reason to change units.

The sale-basis morning decision and its same-day reversal, deferred FIES
instrument evidence, TODO116 and the later unit-code rulings remain intact.

## §4 Invariants and acceptance boundary

The production allowlist is exactly two YAML null-key deletions. Keep all
remaining YAML bytes and every helper, framework, Makefile, mapping, factor,
unit, source predicate, schema, marker, dependency, fixture and golden unchanged.
Additional scope is this ledger, meaningful focused tests, an additive dated
country note and source-safe reports.

Measure actual549API/39framework and relevant-wave hashes before any source
API rebuild, retaining Country/Feature names and order. Do not transfer any
previous Malawi footprint prediction or suppress an unexpected delta.
Choose the smallest representative source/table/test cohort after static
prerequisite inspection; no optional-argument Cartesian product or broad suite.

Guard exact mounted interpreter/cwd/PYTHONPATH/package/countries/config/data
identity before imports, records or API calls. Physically private held sources
must pass terminal MD5/rawSHA/size/mode/mtime/inode/nlink/layout gates before
dependent decoding/builds. No acquisition, DVC CLI, shared mutation, manual
stamp, bypass, assume-fresh or in-tree payload. Threads/build/Make one, at most
two explicit loadfile test workers, one consumer per data root.

Preserve complete frame values, axes/classes/unused levels/codes/dtypes/masks/
order and recursive typed attrs at check_exact=True,rtol=atol=0. Match API args
and serialization stages. Persist endpoints before assertions; classify actual
warm writes and known dtype/unsupported-unit limitations rather than normalizing
them away. Structured scanner occurrences and warning capture scope are explicit.

## §5 Reuse decision

| Quantity / behavior | Decision | Ground |
|---------------------|----------|--------|
| expenditures/prices/quantities | reuse | §2; STANDING default |
| discovery/materialization | reuse | §2; null/absent metadata both `{}` |
| IO, cache hashes and coverage | reuse | §2/4 |
| factor/valuation/unit/label science | unchanged | §3/4; not registration work |
| raw-registration regression | narrow new policy assertion if census supports | no second registry or missing-script global guard |

## §6 Open questions

No new scientific decision is needed for the two keys. Report real missing held
prerequisites, unexpected footprint/order changes or source/API contradictions
before expanding the cohort. GH218 stays open for Togo/CotedIvoire/Afghanistan
rules, country.py/docs/gitignore and remaining phase3 countries. No GitHub write,
public merge or closure; freeze for another agent's independent review.

## Phase3 verification

Pending implementation and measured bounded controls. No source output,
fingerprint scope, baseline-negative test count or author PASS is claimed yet.

Sue (GPT-6).
