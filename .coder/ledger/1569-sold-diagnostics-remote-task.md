# Prior-Art Ledger — #1569 sold diagnostics

Search tier: git + ripgrep + AST floor; no GitNexus tools available.
Base: 9514be02e06d360dd4418db67426c2555ddff5be. Inherits [STANDING.md](STANDING.md).

## §1 Task, restated

Diagnose Site Q coverage of crop_production sold quantities and sale prices,
including Farm-level rows, using only invented Alpha/Beta operands. Then make
the bounded report-only extension in quantity_audit.py authorised by ROOT's
superseding charter. No producer, conversion, sentinel, scientific default,
metadata or real-country data change. Existing tests/oracles stay untouched.
Issue #1569 was read publicly 2026-11-09; open, zero comments. Issue examples
are historical motivation, not evidence measured in this task.

## §2 Existing machinery

| Symbol | Source at base | Role / test | Decision |
|---|---|---|---|
| check_quantities / _SCREENED_COLUMNS | quantity_audit.py:1055 / :437 | Only Quantity/Quantity_kg for crops; reports and returns same object. test_quantity_screen.py, test_gh1493_near_zero_screen.py | extend dispatch for sold diagnostics |
| audit_quantities | quantity_audit.py:757 | upper or lower tail; keys harvest u, t, j; no sold routing | extend sold-column path only |
| _cell_references / _group_quantiles | quantity_audit.py:675 / :631 | positive finite support >=30, p90, floor1, K100; optional explicit rungs (#1482/#1485); test_group_quantile_matches_pandas | reuse without arithmetic changes |
| _reports_for / _record_quantity_report | quantity_audit.py:850 / :1001 | row identity, per-country/wave denominator, warning/strict ledger | reuse with minimal sold metadata augmentation |
| _axis / _numeric | quantity_audit.py:540 / :593 | index-or-column/categorical/nullable handling | reuse |
| Country._finalize_result | country.py:4477 | sole product caller of check_quantities | unchanged |
| _build_registry exclusions | _build_registry.py:262 | existing reporting entry points excluded | unchanged; no cache-cost claim without measurement |

Call-site sweep found audit entry points in framework finalizer, tests and
historical bench scripts; no data mutation callers. Blast radius: all crop
reads can emit additional reports, while other tables and harvest references
must be identical. Report-only change is low data risk; warning/strict/report
consumers require regression coverage.

## §3 Definitions & conventions in force

Per STANDING §3/§4, CLAUDE.md Site Q, quantity_audit module rule and ledgers
857/1259/1482-1485. Native p90 policy is a stated tolerance, not a physical law.
Canonical lsms_library/data_info.yml:1196–1282 states Unit_sold labels the
SALE while u labels HARVEST; agreeing units identify Value_sold/Quantity_sold
per that native unit, disagreement is a quality flag, not proof of either.
:1283–1301 says Condition_sold is a separate answer: fresh/dry amounts and
prices are not commensurable. Unit_sold/Condition_sold may be absent/NA.
Consumer harvest fallback at :1241 is specific to median_price_valuation;
this charter explicitly forbids transferring that fallback into the audit.
No country owned: own CONTENTS.org requirement is NONE, including LOGBOOK /
WAITING / TODO. No real-country metadata/fixture is used as an operand.
Concrete framework idiosyncrasy: sold units are ordinary NA-bearing columns,
whereas harvest u can be an index sentinel; Farm-level sales need not have a
harvest unit. No contradiction to country CONTENTS is claimed.

## §4 Invariants & assumptions

- Same input object, index order/domains, payload values/missingness/dtypes and
  nested attrs survive every warning and strict failure.
- Existing harvest outputs, default ladder, thresholds, near-zero policy and
  warnings stay stable. No clipping, conversion, factor inference, densities,
  source builds, sentinel ruling, aggregation or new producer eligibility.
- Sold comparisons use explicit Unit_sold, never harvest u. Keep supplied
  country/t/v/crop/source and sold condition boundaries; Farm-level is a row
  identity, not an exclusion. Absent sold labels must be visible as unsupported,
  not interpreted through harvest labels. Disagreements need explicit evidence.
- Reuse positive finite support, tolerance and report machinery; thin cells
  cannot borrow another unit/condition/country's scale. Missing/zero/negative/
  nonfinite quantity/value and ratio eligibility get independent controls.
- Runtime: prepared PROJECT_PYTHON, exact cwd/PYTHONPATH/COUNTRIES_ROOT and
  identity assertion; private config/data/DVC site, absent credentials,
  BLAS/OMP/MKL=1, build workers1, max pytest2 --dist=loadfile, log/JUnit.
- No actual Country/Feature/getter/source/Make-data/DVC/cache/acquisition;
  no dependency install, HOME/XDG/CODEX_HOME mutation, subagents or GitHub writes.

## §5 Reuse decision and proposed shape

Reuse _cell_references and _reports_for for sold quantity and native sale-price
ratio with explicit sold keys. Extend audit_quantities for crop Quantity_sold
routing and add a private crop-sale helper if needed; check_quantities invokes
it without writing any column into the input. A diagnostic ratio may be
Value_sold/Quantity_sold ONLY with a stated sold-unit denominator; no inferred
price denominator and no physical kg assertion. Positive finite quantity and
value are necessary for that ratio. Preserve ordinary harvest dispatch.
Actionable unsupported-unit/thin-support diagnostics should disclose rows not
judged, plus unit disagreements, without presenting them as outliers.
No estimator duplication and no factor machinery fork is needed for native
screening. Exact field routing/report shape is surfaced after synthetic
baseline controls and before production edits.

## §6 Open questions / gates

UNRUN: synthetic baseline, exact proposed diff, implementation, regression,
source conservation. Canonical native price-per-explicit-sold-unit contract
exists; disagreeing units remain unreliable labels, not resolved truth. Whether
sale-price screening needs any other denominator cannot be assumed; this task
will implement only the documented native ratio and disclose limitations.
Independent frozen-head review/current integration/CI/GitHub/merge belongs ROOT.

### Phase 3 verification

UNRUN at ledger-only checkpoint. make api-audit STRICT=1 failed exit2 because
Poetry is absent (recipe127); actual recipe via prepared interpreter is running.
