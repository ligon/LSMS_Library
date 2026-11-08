# GH1603 — diagnostic for an unqualified shipped table on pooled countries

Search tier: git/rg/AST floor. Fresh GitHub development fetch resolves
72197c8eeccde679673ef3561204d39666c5dfea (1535merge after8505). New isolated
branchfix/1603-pooled-shipped-warning-20261108; no other tree or frozen actor
is touched. Inherit STANDING.md§§3–4, CLAUDE IO/DerivedValues and canonical schema.
Issue/body/comments and exact current lookup hash are frozen in
B/reports/1603-shipped-factor-warning-20261108, where
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103.

## §1 Task

Emit the documented ShippedFactorWarning when a nonempty caller-supplied
shipped-factor table has no country key and is offered to rows with more than
one explicit country label. Preserve the correct explicit-table5/5 result and
qualified5/20 result, lookup grain/normalization, source/rank/counters, ambiguity
refusals and all existing diagnostics. This is generic synthetic framework
diagnosis, not a survey/country conversion task or a new factor policy.

## §2 Existing machinery

| Symbol/evidence | Existing role/test | Reuse decision |
|---|---|---|
| ShippedFactorWarning,transformations.py:94 |Dedicated filterable/error-promotable class; documents zero-match/dropped-key diagnostics | reuse; describe additional already-promised pooled case |
| _shipped_factor_lookup:4393 |Caller-loaded table only; country/t/j/crop/variety/u/condition/region overlap; strict duplicate refusal; existing dropped-key and zero-match warnings | minimally extend diagnostic after validation, no value/lookup rewrite |
| _level_or_column:4243 and _shipped_table_key |Index-before-column extraction on frame/table | reuse for explicit country identity; do not infer from attrs |
| _one_country:5599 |One supplied label else attrs; None conflates pooled with unsupplied | reuse identity precedence, but not this fallback to detect pooling |
| _normalise_join_key / _valid_factor / _screen_reported_factors |Existing key/NA normalization, finite-positive screening and quality disclosures | unchanged |
| harvest_kg_factors:4813 / harvest_kg |Existing reported/shipped/survey/metric/inferred precedence, source arrays and matched counts | unchanged; tests/test_shipped_factors.py,test_crop_kg_factor.py |
| GH1475ledger / tests/test_gh1475_country_crop_transforms.py |Row country retained; caller-qualified shipped factor semantics already correct | preserve; no inference/identity change |
| review-1475-country-crop-20261107 residual/probe |Both c964 and2269return5/5 without warning; qualified5/20 passes; lookupaaf704...identical | immutable baseline evidence; current72197lookup remains byte-identical |

## §3 Definitions and applicability

The actual lookup docstring says a table without country applied to a pooled
Feature frame is broadcast to all countries and “it now warns.” The current
code warns only when a TABLE key is absent from the frame, the reverse direction
of this missing qualification. This is a real documented diagnostic gap, not
permission to change the caller's deliberately supplied factors.

Pooled here means more than one distinct nonmissing value in the explicit
country index level or column, extracted with existing index precedence. A
singleton label, a country-less frame or a scalar attrs country is not evidence
of pooling. A declared country table key remains qualification even if no rows
match; existing zero-match/refusal diagnostics handle that case, without guessing
its country. No new normalization or missing-identity policy is introduced.

Prior-art, add-feature and cross-country skills were read. This task neither
adds a country feature nor calls survey readers/Feature builders; their country
source-acquisition workflow is outside the explicit source-free charter. All
new country labels are invented Alpha/Beta; no country-specific conclusions or
configuration examples are needed and no survey CONTENTS is newly scoped.

## §4 Invariants

- No discovery, inferred table country, averaging, first-pick duplicate,
  new keys/aliases, default factor, rank, quantity/unit or scientific policy.
- Keep table validation/duplicate refusals before new warning, so a diagnostic
  promoted to an error cannot replace an existing ambiguity ValueError.
- Proposed warning is one per call for a nonempty unqualified table and pooled
  explicit frame; source lookup remains positional and arithmetic unchanged.
  It describes offered scope even if reported data outrank the offer.
- Empty tables add no new pooled warning. Existing dropped-key/zero-match
  messages are neither suppressed nor replaced; combined conditions may emit
  their independent diagnostics. Empty-row behavior/refusals stay as before.
- Index country outranks redundant column/attrs. No attrs-based inference and
  no change to NA keys, screening, Source values, matched/rung counts or masks.
- No survey/raw/harmonized read, acquisition, credentials, shared caches,
  DVC CLI, dependencies or GitHub writes. Main mounted runtime; exactcwd/
  PYTHONPATH/config/import, private empty roots, false credential gate,
  unshare, BLAS/build1,pytest2loadfile. Other frozen lanes are untouched.

## §5 Approved design and reuse

Use existing _level_or_column(df,'country') and the existing table_keys. After
table validation/duplicate refusal (alongside existing diagnostics), add only a
guard for nonempty sf, absent table country and >1distinct nonmissing explicit
row labels. Emit existing ShippedFactorWarning with a factual broadcast-scope
message and stacklevel3. Do not infer whether the caller intended a country-
specific table. Clarify the class/lookup docstrings without removing the promise.

Source-free controls will cross index/column frame identity and table key form,
pooled/singleton/country-less frames, qualified/unqualified tables, empty inputs/
tables, stale attrs/redundant columns, qualified precedence, ambiguity and joint
dropped-key/zero-match diagnostics. Preserve exact values/masks/source/counters
and existing tests. Retain an untouched-base warning negative. Reuse existing
strict API audit and metadata hash operator to measure footprint rather than
assuming this analyst-callable diagnostic invalidates caches.

## §6 Coordinator gate

Root reviewed/approved the ledger/design BEFORE product edits. The authorized
nonempty-unqualified-table/explicit-pooled-country warning is implemented at
frozenab8e6fdfc3b5b2e51d457ab2c1ae198c5d1f35f9. All author/reviewer/deputy
actors are closed. No scientific ruling is needed for this diagnostic. Root
owns final-head configured GitHub checks/publication/merge. No source campaign
or shared rewarm is authorized or claimed.

### Phase3 — anchored verification

- **OK (§§2–5):** frozenimplementationab8e6fdfc3b5b2e51d457ab2c1ae198c5d1f35f9 adds only the specified warning guard beside existing diagnostics, after table validation/duplicate refusal. Existing index-before-column country extraction is reused. Nonempty table, no tablecountry key and >1distinct nonmissing explicit rowcountry labels are all required. No attrs inference, parser, factor discovery, lookup keys, source/rank/counters, factor policy or value arithmetic changed.
- **OK (§§3–4):** explicit caller-table unqualified5/5 and country-qualified5/20 values remain; singleton/countryless/empty cases add no new pooled warning. Existing dropped-key and zero-match diagnostics can coemit. Ambiguity ValueError still precedes warning-as-error. Public-NA behavior remains the existing caller contract, not a new identity policy.
- **OK (§§2–5):** exactfrozenauthor focus201passed, including38new warning/boundary/actual-caller controls; untouched72197parent external38case selection13expectedwarningfail/25pass. Separate3existing positive hash controls pass. StrictAPIaudit0. All use explicitprivateemptyroots, falsecredentialgate, mountedMAIN/package/configidentity, unshare, BLAS/build1 andpytest2/loadfile; focused selection `-m 'not requires_s3'` excludes source-marked tests. No fixture/baseline weakening or source replay.
- **OK (§§3–4):** root independent correctedv3 review45passed/0fail (8.56s pytest,10.37s wrapper), exactab8. The originalv1 result37pass/8fail had public-NA assumptions; v2result44pass/1fail lacked a harnessfixture. Both failures/logs are preserved. Only the independent harness was corrected; no product or public-output oracle was weakened. These review controls overlap author coverage and are not an additional45disjoint scientific cases.
- **OK (§§2,4):** closed guarded metadata census base72197→ab8 enumerates38registeredcountries/450declaredcountrytable/1895wave/35build/186import fingerprints; ALLunchanged,0errors/denials.18None wave cells unchanged; 11unregisteredcountry and3noncountry entries explicit. Public/runtime553country-method/2313wave-method/39method surface is separate, not extra persisted caches. The existing audited1510v3operator is reused; no hash-exclusion/schema-policy change and no sharedrewarm. Positive hash controls establish versioned build machinery remains live.
- **OK (§4), ordinary composition CI:** deputycompositionff8a29edfc60c720a928e003a0fb4cc5eac375c3 combines frozenab8 with currentupstream1ce4f525b6f6042eadfe785c3bfd2818e34e1808, commonancestor72197; no pathoverlap/conflict, allowned/incoming objects exact. Freejob41018203 COMPLETED0:0/13m39, privateemptydata/config/nonsharedL1/ownmountedimage/falsecredentials/unshare. FULL `python -m pytest -n4 --dist=loadfile --timeout=600 --timeout-method=thread --durations=25 -q --junitxml=<durableunit-tests.xml> tests`, no marker-selection exclusion:11702passed/5652skipped/4xfailed,0failed/errors,17358JUnitcases,711.81s pytest. JUnit skipmessages classify5589credential/S3-related and63other; source-skipped cases were NOT executed or verified. This is configured OFFLINEcomposition evidence, not a source campaign or final-head GitHub result.
- **Finaldocs-only freeze (§4):** only this ledger is updated after all actorsclosed. Alllibrary/config/tests bytes remain EXACTab8; no new build inputs. Original author/reviewer/deputy failures and reports remain untouched. External final report/PRdraft/head/byteproof receipts document this distinction. Root must complete final-head configured GitHub checks beforemerge; no additional source/full-local rerun is warranted for this ledger-only delta.

Evidence: `B/reports/1603-shipped-factor-warning-20261108/`, independent
`review-1603-pooled-shipped-warning-20261108/root-v3.*`, and deputy
`verification-deputy-three-offline-20261108/1603/`. This is the existing
diagnostic promise being fulfilled, not a new scientific factor/default.
