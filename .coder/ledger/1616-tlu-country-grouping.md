# GH1616 — preserve supplied country in TLU household identity

Search tier: git/ripgrep/AST; inherits STANDING§§3–4 and CLAUDE. New isolated
fix/1616-tlu-country-grouping-20261108 at current merged development
c2d98b7d90d0829de563c54158701cb0667c3951. Original1588author/verification/hash
worktrees and all prior1485/other trees are immutable. This ledger precedes code.

## §1 Task and authority

Root authorizes country identity repair after concrete prior-art/contract
reading, with no factor/default/scientific choice. Fresh GH1616 body and all
comments (0) read. Its exact Chicken10/20 same(t,i) example currently pools
0.1+0.2 into one country-less row; separate households must remain distinct.
Only tlu grouping/identity changes; prior1485coverage disclosure is preserved.
No other livestock transform, source, native table, factor or default changes.

## §2 Reuse machinery

- transformations.tlu: current numeric head coercion, species-label lowercase
  lookup, factor mapping, contribution/dropna/groupby.sum and coverage/warning.
- _country_index_for_transform (GH1475/1517 shared core): explicit country index
  or column by presence; non-null refusal before filtering; index precedence;
  column promotion before losing row identity; scalar attrs never invent an axis.
- _level_or_column: index-before-column value accessor used by that helper.
- _plot_input_country_index: wrapper's plot-specific local-key guard must NOT be
  imposed on household TLU. Reuse its common core only.
- 1159-pooled-reducers,1160-pooled-rate,1475-country-crop-transforms ledgers:
  established country-first/singleton/empty/null/precedence and unchanged-local-
  key contracts. No paired-frame rule is needed for this single-frame reducer.
- tests/test_gh1485_tlu_coverage.py (32controls), test_863_transforms.py and
  test_gh1159_pooled_reducers.py: reuse factor/missingness/warning/grouping controls.

## §3 Definitions, grounding and resolved choices

Country is an explicit Feature-prepended row identity, independent of its
cardinality; it is not inferred from household strings or attrs['country'].
Keep country first in output, then EXISTING available t/i group keys. Current
TLU permits t-only or i-only; no new requirement for both is authorized. With
neither key it raises via groupby([]); country alone must not legalize that sum.
Present-null country rejects even on rows later filtered for unsupported/missing
heads. An index label outranks a contradictory/null column; null index cannot be
repaired from a valid column or scalar attr. No new label vocabulary/case cleanup.

Authenticated prior personal FULLMalawi5417-line read including LOGBOOK/WAITING/
TODO is byte-exact currentc2d/029860, SHA25632e40d5744d65b9c7c8646b0c0d0f5b105f9798f483fe154dc5a795e93117729.
Relevant idiosyncrasies: native labels span two namespaces; Cow/Goat/localhen are
not automatically default factors. HeadCount is owned NOW; HeadCount12MonthsAgo
is a separate stock, HeadAcquired/HeadSold are flows. RaisedOrOwned can retain a
row with all numeric answers blank; blank heads are not zero. The older note
says a cow/goat/localhen “adds nothing and nothing warns”; the appended1485entry
explicitly supersedes ONLYthe warning statement, preserving historical totals.
Current normal Malawi Country input lacks a country axis and must remain exact.
This shared identity defect is not a newly discovered country idiosyncrasy.

## §4 Invariants

_TLU_FACTORS, TLUCoverageWarning and _tlu_species_coverage bodies remain exact.
Keep all factor kwargs/head_col overrides, lowercase-only lookup, numeric
coercion, negative/zero/missing/nonfinite heads/factors, unsupported-only absence,
warning text/category/stack and all-input coverage metadata. No per-country factor
inference, new species mapping, zero/NA herd fabrication or clipping. The existing
all-input coverage tally remains aggregate, not a new country-specific schema.

Country-less returned rows/typed values/attrs/errors remain exact. Supplied-country
pooled payloads must equal separate-country calls with identical arguments; do
not compare aggregate coverage attrs to per-country tallies as if identical.
Input values/index/order/masks/attrs must remain unmodified. Promote country before
filtering; no post-filter .loc reattachment over colliding indexes.

No Country/source/API/native/raw acquisition/decoding, DVC/dependencies/GitHub or
shared-cache writes/rewarm. Tests use explicit private data/config/cache, false
credentials, unshare, exact MAINvenv/cwd/PYTHONPATH/import/countries root and
BLAS/OMP/MKL/build1, pytest2/loadfile and file logs. Metadata native hashes are
unmeasured until a separately guarded comparison; no0/N assumption.

## §5 Minimal implementation and evidence plan

Reuse _country_index_for_transform in tlu, preserve old arithmetic/groupby call,
and prepend country ONLYwhen an original local t/i key exists. That preserves
existing no-local-key refusal, t-only/i-only and country-less paths. Read the
chosen head column before country promotion (a valid custom head_col can itself
be named country) and align its unchanged numeric Series positionally to the
promoted same-row/order index; labels/factors then use the normalized frame.
No new scientific choice is necessary under the already-ruled identity contract.

First prove currentbase collision using invented actual tlu calls. Then index/
column/reordered axes, same/distinct local keys, multiple/reversed/singleton/
empty/all-filtered countries, partial/all-null labels and index-vs-column/attrs
precedence. Include absent labels with stale attrs, categorical unused labels,
custom head_col including country, native animal index/column, local null/partial/
missing keys, unsupported-only/no-local-key boundary, supportedzero/default/custom
factors, missing/nonfinite/sign head categories, warning/coverage preservation,
exact Country-less baseline/randomized parity and actual synthetic Feature
livestock assembly. Reuse existing coverage/helper/transform controls. Preserve
failed attempts, freeze before root independent review/fullCI, no source campaign.

## §6 Decisions and gates

No genuine scientific/contract conflict found: row-country presence, non-null
refusal, precedence and ordinary local-key rules already exist. Root explicitly
authorizes implementation once grounded; surface this ledger then proceed.
No paired-operand, fertilizer, food, country-label-inference or factor expansion.
Source/hash/fullCI/independent review/current-base composition/publication gates
remain separate and must be reported truthfully. Root owns GitHub and rulings.
