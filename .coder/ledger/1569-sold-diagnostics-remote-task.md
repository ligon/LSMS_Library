# Prior-Art Ledger — #1569 sold diagnostics

Search tier: git + ripgrep + AST floor; no GitNexus tools available.
Base: 9514be02e06d360dd4418db67426c2555ddff5be. Inherits [STANDING.md](STANDING.md).
Code/test freeze: 519217902cb22488561b584a778531b96d01610a.

## §1 Task, restated

Diagnose Site Q coverage of crop_production sold quantities and sale prices,
including Farm-level rows, using invented Alpha/Beta operands, then implement
the bounded report-only fix authorised by ROOT. No producer, conversion,
sentinel, scientific default, source or metadata changes. Issue #1569 was
read publicly 2026-11-09: open, zero comments. Its historical examples were
not replayed and are not fresh evidence. ROOT's mutable-channel correction
preserves the existing silent/no-reference contract: no new coverage warning,
coverage API, zero-flag report or strict behavior. The initial proposal and
its superseded controls remain in private evidence and the ledger's history.

## §2 Existing machinery

| Symbol | Source at frozen code | Role / tests | Decision |
|---|---|---|---|
| check_quantities / _SCREENED_COLUMNS | quantity_audit.py:1192 / :442 | table dispatch, warnings and same-object return; test_quantity_screen.py | extend crop registry only |
| audit_quantities | quantity_audit.py:852 | existing high/low measurement; same public signature | route crop sold columns to private helper |
| _cell_references / _group_quantiles | quantity_audit.py:690 / :646 | finite positive support >=30, p90, floor1, K100, explicit rungs; test_group_quantile_matches_pandas | reuse unchanged |
| _axis / _numeric | quantity_audit.py:555 / :608 | index/column aliases, categorical and nullable numeric support | reuse unchanged |
| _reports_for / _format_offender | quantity_audit.py:952 / :1046 | original per-country/wave report and row naming | sold-only identity augmentation |
| _record_quantity_report / _emit_quantity_report | quantity_audit.py reporting section | existing ledger, warning and strict lever | unchanged |
| _unit_sentinel_mask | transformations.py:4177 | existing absent/Unknown/manquant unit predicate; no conversion | reuse via private Unit_sold-as-u operand |
| Country._finalize_result / exclusions | country.py:4477 / _build_registry.py:262 | product caller and excluded audit entry points | unchanged |

Call-site sweep: framework finalizer, tests and historical bench scripts;
no producer call site changed. All crop reads may emit new sold findings;
other tables and harvest reports retain their existing paths. No GitNexus
available: used declared rg/git substitutes, not an index install.

## §3 Definitions & conventions in force

Cite STANDING §3/§4, CLAUDE.md Site Q, quantity_audit's module rule, and ledgers
857/1259/1482-1485. Canonical data_info.yml:1196–1282 states Unit_sold is the
SALE unit while u is HARVEST, and :1283–1301 makes Condition_sold independent
of harvest condition. Native Value_sold/Quantity_sold is explicitly defined;
unit disagreement is a flag, not identification of physical truth.
Consumer fallback at :1241 is specific to median_price_valuation and is NOT
transferred into this task's no-fallback audit. Canonical crop grain is
:153 (including season and production_obs); the old audit's fine-to-coarse
shape supplies the crop-to-unit pooling convention, not a new broad key policy.
No country owned: full own CONTENTS.org / LOGBOOK / WAITING / TODO requirement
is NONE. No real-country metadata/fixture was an operand. Concrete framework
idiosyncrasy: NA-bearing sold units are columns while harvest u may be an index
sentinel; a Farm-level sale can lack harvest quantity/u. No contradiction to
any country CONTENTS is claimed.

## §4 Invariants & assumptions

- Same object, full index/row order, values, missingness, dtypes and nested
  attrs survive warnings and strict failures. No clipping/conversion/default,
  source builds, sentinel ruling, reducers or producer eligibility changes.
- Existing harvest ladder/high+low reports and constants stay unchanged.
  Sold columns get only the existing upper screen by default.
- Sold references retain supplied country,t,season,Unit_sold,Condition_sold;
  try crop j, then those same fixed boundaries. Never drop sold unit/state,
  borrow harvest u/condition, or pool countries/waves. Missing condition stays
  its own unknown group when supplied, not an inferred harvest state.
- v/source/production_obs/plot name rows; they are not invented support gates.
  Findings retain full native index, source/state fields and positional identity.
- Sold quantity support is finite positive quantities independent of sale
  value. Price requires finite positive quantity/value and finite positive
  quotient with explicit nonmissing/nonsentinel/nonblank sold unit. Unknown
  unit, missing fields or thin support returns [] silently, as instructed.
- Prepared PROJECT_PYTHON; exact cwd/PYTHONPATH/COUNTRIES_ROOT/import assertion;
  private config/data/DVC site and absent credentials; unchanged HOME/XDG/
  CODEX_HOME; BLAS/OMP/MKL=1, build1, pytest2 --dist=loadfile, logs/JUnit.
- No actual Country/Feature/getter/source/Make-data/DVC/cache/acquisition,
  dependency installs, subagents or GitHub/source-repository writes.

## §5 Reuse decision and implementation

Private _audit_crop_sales(df, column, *, country, tails=('high',)) is the only
new function. Public audit_quantities signature stays fixed; crop Quantity_sold
and virtual Sale_price route to it. check_quantities's registry invokes both
without modifying the frame. Reuses _cell_references, _reports_for and unit
sentinel handling; no factor estimator is forked. Report-only native price
states definition='Value_sold / Quantity_sold', unit_column='Unit_sold',
unit_semantics='as-reported', plus source amounts and both unit labels.
No coverage warning/API or zero-flag report remains. ROOT's superseding scope
correction is retained verbatim in external steering evidence; no existing
assertion/fixture/oracle/skip/tolerance was modified.

## §6 Open questions / unexecuted gates

No unresolved denominator is assumed for the defined native ratio. Physical
truth of disagreeing unit labels, richer coverage diagnostics, and any 9,999,999
sentinel ruling remain separate. Silent unassessable cells are not acceptance.
Independent frozen-head review, current integration, full CI, real-data/source/
producer/cache validation, performance and actual cache-hash comparison are
UNRUN and belong to ROOT or a separately authorised stage.

### Phase 3 verification

- _audit_crop_sales — OK (§2/§3/§4/§5): existing p90/support/floor/K reused;
  explicit sold denominator, crop ladder, sold-state separation, no fallback.
- audit_quantities/registry — OK (§4/§5): new upper-only sold dispatch, exact
  literal old harvest report and old high/low behavior preserved.
- _reports_for/formatters — OK (§4): sold-only metadata names original rows;
  input object, nested attrs and categorical/nullable dtypes remain unchanged.
- No REINVENTION found: formula/call-site search found canonical native ratio
  and conversion estimator, but no existing native sold screen. Conversion
  machinery is intentionally not invoked for native-unit diagnostics.
- Corrected final selection: 67 passed, 0 failed/errors/skips, exit0 (64 own
  synthetic controls +3 existing arithmetic/exclusion tests). JUnit/logs retained.
- Regression evidence on actual base audit: two new sale warning tests FAILED
  as expected (no warnings); unchanged harvest oracle PASSED, exit1. The tests'
  assertions are unchanged in the final corrected file. Not relabelled green.
- Superseded coverage proposal runs: 69 passed, then 82 passed; archived, NOT
  final acceptance for the narrowed contract. No existing tests were weakened.
- make api-audit STRICT=1 failed exit2 (Poetry missing, recipe127); actual
  prepared-interpreter recipe scripts/api_surface_audit.py --strict passed
  initially and after final code, exit0, 15 skills listed; no install.
- git diff --check passed. Source conservation proof excludes only the exact
  authorised audit module, own new test and this ledger; no full-source runtime,
  corpus firing-rate, performance, hash-cost, CI or independent-review claim.
