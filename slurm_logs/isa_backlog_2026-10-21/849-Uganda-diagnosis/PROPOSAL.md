# GH849 follow-up: retain native production observations before wiring factors

The source factor fields are real. The measured 12-key colmap scenario is **not a safe promotion**: the current `(t,i,plot_id,j,u,condition,season)` reducer first sums quantities and independently takes the first nonmissing factor. Different source records then lose their own quantity-factor pairing. File this as a **new/split Uganda carrier site**, linked as the genuine blocker to849. It is not an uncompleted tail of637, and the old Uganda condition review did not resolve it. No implementation or public axis is approved by this diagnosis.

The follow-up should preserve **all distinct native production observations**, including equal-factor and partial-report groups. A branch that special-cases only the112 comparable conflicts would make identity depend on a measured factor value and would leave other records collapsed.

## What the source supports

| Wave | Actual source identity available | Current keys | Keys retaining audited identity | Existing null-key source rows, separate |
|---|---|---:|---:|---:|
|2010-11|No native production-ID field; held-file row position is only a locator|20970|21051 private positional bound|0|
|2011-12|`productionID` A / `production2ID` B|20152|20200|43|
|2013-14|`Production_ID` A / `Production2_ID` B|17585|17646|0|
|2015-16|`Production_ID` A / `Production2_ID` B|19576|19639|0|
|2018-19 control|Actual `_1` / `_2` condition-slot suffix|15616|15630|0|
|2019-20|Actual `_1` / `_2` condition-slot suffix|17024|17046|0|

Each audited native identity is nonmissing and makes every eligible nonnull-key observation unique in2011/2013/2015; the actual slot does likewise in2018/2019. These counts include equal-factor groups. Within the current coalesced groups, the equal-or-missing-factor counts are40/28/41/40/8/10 in the table's wave order. These are not grounds to discard observations merely because their rates agree.

The2010 positional bound does not establish an instrument production ID. A truthful unkeyed-record axis would identify an observation in a specific held source file and row order, with its construction declared. It would need source-relative path/release MD5 plus an ordinal, not a claim that the survey supplied an ID; an out-of-band file reorder changes that locator. Full-row duplicate classification must distinguish an identical duplicated record from separate observations before a lossless de-dup is allowed.2009 is outside the requested factor promotion but has the same unkeyed issue (22408 current keys versus22549 positional bound, with360 existing null-key source rows separately excluded).

Private locators already retain file-relative path, held MD5, season, zero-based row position and actual slot; actual production IDs are retained privately where present. Raw keys/values remain private in `scenario-v3/source-identity-locators-private.pkl` and per-wave collision diagnostics. These diagnostic coordinates are not a new API contract.

## Preferred engineering direction for review

Preserve the native observation coordinate through the Uganda builder and Country output, **before** any rate is attached to a combined quantity. Reuse actual production IDs where they exist and actual form-slot identity for the wide vintage. For2010, explicitly document and test a constructed source-observation coordinate if no stronger native key is found; do not fabricate a reported identifier. A common observation-within-production-key axis is one design candidate, with a provenance declaration distinguishing reported ID, slot and source-order construction. Its spelling/type/sentinel policy must be reviewed with the schema/Feature contract, not slipped into the colmap patch.

This is a source-grain repair across all production records, not a different scalar reducer. Keep each source Quantity, KgFactor, sale unit/condition/value, harvest month and crop-stand attribute together. A first nonmissing rate, weighted rate or average rate does not preserve those observations. Do not blank conflicting reported factors, read a factor as a total, invent a conversion or add a new core conversion layer. The existing validity/screen rules continue to apply separately to each observation at analysis time.

The existing API machinery makes the integration work concrete:

- `CLAUDE.md:491/515` treats disagreeing duplicate keys as missing identity/grain, not permission to declare a reducer. Uganda's earlier condition repair (`323-uganda-crop-condition.md`) preserved an actual survey dimension; it did not license collapsing other source production IDs.
- `SkunkWorks/grain_aggregation_policy.org:15` records GhanaLSS native visit preservation and analyst-invoked aggregation. `data_info.yml:152/166` declares community-price `variety/visit/obs`; its missing-level policy at262 and Feature's `_align_to_canonical_levels` provide existing observation/sentinel precedents. They are patterns to reuse, not permission to reuse an axis with a false meaning.
- Country preserves its declared native index and joins `v` after cache reads. The new coordinate must therefore be declared in the country schema and carried through normal ID walk/finalization, with no ID rewriting or null-key invention.
- Feature currently drops undeclared extra levels (`feature.py:703/737`) and then collapses. A country-only new axis would lose the rows again. The reviewed design must either declare the observation coordinate in the canonical crop index and use the existing honest missing-level sentinel mechanism for countries that lack it, or supply an equally explicit lossless assembly design. Do not silently invoke the current reducer or assume a constant means a source reports only one observation.
- `harvest_kg` is an analyst transformation at `(t,i,plot_id,j)`. It may sum **after** each native Quantity is multiplied by its own screened factor; it already ignores additional index levels. Preserving observations also changes survey-median report counts and weights, so whole-frame derived effects must be measured, including equal-factor groups and the min_reports threshold. No new-grain derived API prototype was promoted in this diagnosis.

This is a concrete engineering follow-up for root review and assignment, not a vague requirement for a new human decision. No production/API change should begin before that review.

## Measured carrier consequence and source-quality exceptions

The requested waves have115 raw positive-factor conflict groups,113 with multiple surviving screened factors,112 with all source Quantities finite/nonnegative, every factor screened usable and a common known unit/condition. Their current first-factor carrier differs from the direct source product sum in all112: net+10710.9000002kg, absolute26602.0999998kg. Per-wave net is+3484.5/+5077.4000002/+2327/+587/-765kg for2010/2011/2013/2015/2019. Two2013 groups contain rejected reports; one2019 group has Unknown unit and cannot establish comparable native quantities. They are not assigned an invented supported total. Exact per-key evidence is private.

The2018 control's six groups are unchanged by the scenario but already have a net-2090kg/absolute2130kg carrier difference. That is a separate pre-existing manifestation to cover in an all-record repair, not justification to copy it. Partial-factor coalescences and existing null-key losses are separately counted; no automatic repair is applied to either. Raw Factor==Quantity and >250 counts are diagnostic only; no `reported_total` layer follows from them. The2015 sales-value x100 correction remains isolated from factors/quantities, and the measured factor screen does not support a whole-wave2015 exception.

## Factor mapping after native-grain preservation

The rejected scalar-carrier scenario used exactly these existing source fields, with deep-copied parameter maps only:

|Wave|A slot1|A slot2|B slot1|B slot2|
|---|---|---|---|---|
|2010-11|a5aq6d|not a wide slot|a5bq6d|not a wide slot|
|2011-12|a5aq6d|not a wide slot|a5bq6d|not a wide slot|
|2013-14|a5aq6d|not a wide slot|a5bq6d|not a wide slot|
|2015-16|a5aq6d|not a wide slot|a5bq6d|not a wide slot|
|2019-20|s5aq06d_1|s5aq06d_2|s5bq06d_1|s5bq06d_2|

Only after the source-observation carrier is sound should a later patch add these12 `kg_factor` entries, correct the deliberately-unwired comments in Uganda's module/wave docstrings and add the measured country note. No unit/factor dictionary curation is supported merely by the reported-vs-nominal container dispersion.

## Meaningful acceptance tests for the follow-up

1. Two native production IDs with the same current key survive independently even when their factors agree; source-ID and complete payload equality, not merely an equal kg sum, is the assertion. Different valid factors must stay paired with their own quantities. The current sum/first implementation must fail the negative control.
2. Wide slot1/2 with the same condition/unit survive as separate native observations, including complementary missing factors, Unknown unit and same-value reports. No per-column composite may appear.
3. Native-ID absence/missingness and actual full-row duplicates are explicit cases. A constructed2010 locator is reproducible under the same held MD5 and is labelled constructed; it is not presented as an instrument ID. No automatic selection, averaging or factor blanking is accepted for a disagreeing duplicate identity.
4. Source invalid/zero/year/over-cap/Kg-not-one factors keep their raw reported carrier and hit the unchanged screen at conversion. Quantity is never fabricated. Reported-factor eligibility/counts and survey-median behavior are verified at native observation grain.
5. Ordinary cold Country plus fresh-process warm and Feature default/currency-matched calls preserve every source observation/value/attr through finalization and assembly. A mutation that drops the new observation coordinate must fail shape/key/provenance assertions. No manual cache stamps or assumed freshness.
6. Measure whole-wave/all-frame harvest sums, coverage, source-layer/median changes and downstream yield behavior against a direct held-source oracle. Update any baseline only after the exact movements are explained, with an independent healthy comparator and full scanner accounting.

For a uganda.py-only patch, existing hash inputs imply all28 Uganda country API tables and224 API-wave hashes rewarm. Current f8 hashes remain unchanged because nothing is implemented. A native-grain/Feature/schema follow-up may have a wider operational footprint; measure its actual hashes in separate exact-source processes before rebuilding, rather than importing the narrow28-table expectation blindly.

Sue
