# Private prior-art ledger DRAFT -- GH849 Uganda reported harvest factors

Search tier: Git, ripgrep, direct source and existing tests. Read-only diagnosis at f8d236694c26fe9e709c5548f02c70f74ea72c33; no tracked ledger/code edit is authorized.

## 1. Task

Measure actual source q6d fields and a private factor-only colmap scenario for 2010-11, 2011-12, 2013-14, 2015-16 and 2019-20. Retain 2018-19 already-wired healthy control and all old payload. Produce a concrete proposal before any wiring. PR967 and PR972 are merged and present; the issue's blocker is stale, not an outstanding dependency.

## 2. Existing machinery

- Uganda `crop_production_for_wave(t,df5a,df5b,df4a,colmap,df4b=None)`, uganda.py:1230: pure colmap input; existing factor extraction keeps positive reported numbers, zero/nonpositive -> NA. Reuse with deep copied local parameter only, never module/global mutation.
- `CROP_COLMAPS`, uganda.py:1810: current per-season/condition wiring, three 2018-19 factor entries, none on the five requested waves. Source absence raises `CropColmapError`; do not invent fallback fields.
- `_harvest_unit_map` / `_harvest_condition_map`, uganda.py:1178/1182: existing category dictionaries. Source vocabulary, not variable-label guesses, establishes units/conditions.
- `harvest_kg_factors` / `harvest_kg`, transformations.py:4282/4698: existing reported > shipped > survey_median > metric/inferred preference, diagnostics and summed plot-crop output. Reuse unchanged, including Unknown exclusion from median pooling and cross-condition aggregation warning.
- `_screen_reported_factors`, transformations.py:3674: reuses finite-positive validity and unchanged cap250, kilogram tolerance1%, calendar-year rejection; count rejected factors, never repair/clip/rescale them.
- `Country.crop_production`, finalization, content hash/coverage, sanctioned `get_dataframe`: standing machinery, genuine normal empty-L2 baseline and fresh-process warm. No new cache mechanism.
- Existing `test_crop_kg_factor.py`, `test_shipped_factors.py`, `test_gh842_uganda_harvest_unit.py`, `test_uganda_crop_condition.py`, source tests and prior ledgers842/crop-production-kgfactor/harvest-kg-shipped-factors establish contracts. Their historical row/sum pins precede later merged work; current source determines current totals.

## 3. Definitions

Inherit STANDING.md sections3/4, CLAUDE.md, canonical crop schema and the source ledgers rather than redefining them. KgFactor is reported kg per ONE native unit on that row, never a total or constructed unit weight. No reported_total layer. Agreement uses finite positive independent label/read factors and screened reported reference, relative error abs(other-reported)/reported, with exact denominators and 10%/20% thresholds; disclose unscreened sensitivity separately. Harvest sums are analyst transforms that may combine conditions; factor promotion does not fix that existing design.

## 4. Invariants/caveats

Keep every non-factor column, source identity/key/grain/multiplicity/order/dtype/axis/full attr unchanged in the helper scenario. Explicitly audit existing coalescences (first factor vs summed quantities); do not hide them. 2015-16 Value_sold x100 correction and its existing defect guard must be applied identically to baseline/scenario, never extended to factors or quantities. AGSEC4A/B are separate season rosters. 2018-19 A has no distributed unit; 2013-14 A variable labels swap unit/condition; 2018 mixed naming; wide slots are conditions, not additional seasons. Source descriptions and older totals are quoted with their historical scopes rather than silently overwritten.

All sources must be held, MD5/SHA/size/mode/mtime-gated and physically copied before decoding. Ordinary baseline caches only; counterfactual frames stay private and are never promoted to API caches. Exact cwd/import/config/data identities; private socket/write guards plus unshare-rn; threads/build/Make1; at most2loadfileworkers. No acquisition, DVC CLI, FDC/nutrition, shared writes, freshness bypass, source edits or Git/ref/GH mutations. All prior1078 roots remain frozen.

## 5. Reuse decisions

Reuse all loaders/build/factor/screen machinery. New private analysis only: source-slot inventory/oracle, paired factor-only colmap scenario, independent denominators/unit-condition dispersion and safe aggregate report. A future small colmap/comment/doc patch is a proposal, not implementation or a changed-source/hash proof.

## 6. Open measurements

Source carrier/labels/nonmissing per slot; current baseline vs proposed harvest kg/coverage per wave; reported/reference disagreement and within-unit-condition dispersion; factor==Quantity and >MAX counts; source/coalescence ambiguity; exact possible country rewarm scope. No wave promotion is approved before root reviews those results.

## Anchored verification and resulting design boundary

- OK (sections2/3): all43 held inputs and129 physical copies pass complete terminal receipts before decode; original source/config/Git state remains f8. Data values use get_dataframe; metadata-only StataReader supplies variable labels. Original failed private launches remain preserved.
- OK (sections2/4): deep-copied12-key helper scenario leaves133331rows and every non-factor helper/raw/public value/key/order/dtype/null/axis/full attr exact. Existing ID walk and finalizer reproduce the real ordinary baseline strictly.2015Value_sold defect guard/x100 is identical on both sides, not applied to factors or Quantity.
- MEASUREMENT (sections3/6): ordinary baseline harvest78571rows,29093322.407893993kg; private scalar-carrier scenario42035114.892979845kg. Seven-wave agreement/coverage/provenance, all15source slots, raw/canonical cap/equality counts and unit-condition dispersion are fully reported.2018healthy control stays exact;2009remains outside factor scope.
- CONTRADICTION (sections3/4): the current sum-Quantity/first-nonmissing-factor carrier cannot preserve each reported factor's own Quantity. Requested waves have115raw positive-factor conflict groups,113surviving screened conflicts,112complete/comparable source groups, all with different carrier versus source-product sums (net+10710.9000002kg; absolute26602.0999998kg).2groups have rejected operands and1has Unknown unit; no fabricated supported total is assigned.2018's6unchanged groups are a pre-existing residue, not permission to replicate the loss.
- OK (sections2/5, reuse): no alternative screen, reported_total, weighted scalar, average, silent factor NA, new units/layer, reducer special case or source-axis implementation is introduced. The correct follow-up is native-record grain preservation for ALL distinct records, including equal-factor/partial groups, before factor mapping.
- SOURCE IDENTITY (sections4/6): actual productionID/production2ID (2011) and Production_ID/Production2_ID (2013/2015) distinguish all eligible nonnull-key records; wide slot identity does likewise in2018/2019.2010has no such source ID; private row-position locators are explicitly constructed, MD5-pinned and not an approved API axis. All-record key audit and concrete Country/Feature/harvest contracts are in PROPOSAL.md; this is a new/split carrier site rather than a falsely resolved637tail.
- OK (sections2/4, validation):81existing pure factor/shipped-factor cases pass with no skips/failures; a separate real-data pytest case genuinely skips at the unchanged credential collection gate. Full normal held-source Country cold, strict fresh warm and default-constructor public/harvest frames run independently and leave all60cache files unchanged on warm reads. No bypass or test monkeypatch is used.
- LIMIT (section6): no actual patched source/API/hash proof exists. Current28country/224API-wave hashes remain f8; uganda.py-only changes would affect all those Uganda caches by existing dependency membership. A future grain/Feature design must measure its actual scope independently. No implementation/public axis is approved until root reviews the concrete follow-up; no vague new human decision is presumed.
