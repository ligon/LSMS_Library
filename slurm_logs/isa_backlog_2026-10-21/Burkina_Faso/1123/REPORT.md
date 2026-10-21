# #1123 Burkina Faso native input identities

Implementation `69ed4d2028a6f989a8ac45d117a5cffc60a831fd`, based on
`f8d236694c26fe9e709c5548f02c70f74ea72c33`. The ledger was committed first
in `d780445e8`. Both commits were published by the ordinary post-commit hook.
Independent full review passed at
`3848778d0e8676c798883c11ea3e33fc88e8a591`; that source/runtime evidence
remains pinned to this head and the runtime commit above. The implementation
is reviewed but unmerged. This documentation amendment records the review's
separate gift/purchase finding; its narrow review is pending.

The actual fresh held-source Country build now retains **22,877 input lines**,
up from **22,694**: all **183** previously lost native lines return with full
payloads. This includes **163** extras in **150** conflicting-payload groups
and **20** equal-payload extras from distinct native input slots. The historical
163-row grain stamp measures the conflicting subset, not the entire loss.
Payload conservation preserves the existing API columns and their bindings;
`Purchased` and `Quantity_purchased` are names, not verified purchase semantics.

## Exact change

The native key `(grappe, menage, s16bq01)` is unique on all 22,877 applied
2018-19 source rows. The declared country key remains `(t,i,input,crop,u)`;
this is household x input grain with no plot/parcel axis. The source maps
one-for-one into the wave frame, with zero native/declared null keys and no
merge fanout. A non-injective crop map pooled distinct slots onto 170 keys
(157 pairs, 13 triples), all `Seed / Autre crop / Kilogramme`.

Exactly three Preferred Label cells change:

| Original Label | Before | After |
|---|---|---|
| Semences d'autres céréales | Autre crop | Autre céréale |
| Plants/boutures de tubercules | Autre crop | Autre tubercule |
| 21 | Autre crop | Unknown [Burkina_Faso 2018-19 s16bq01=21] |

The two named categories reuse the existing human-authored Benin/Togo fix
`36170a57b04317e55716919eb5eb1054b5b43102`. Copying those two splits alone
leaves **98 duplicate extras** in Burkina and is rejected by a regression.
The source-qualified third target is approved as an **identity fallback, not
a crop classification**. Tanzania's `crop_` native-code fallback and existing
visible unresolved-code conventions supply the preservation precedent; the
exact qualified spelling was explicitly approved for this case.

The existing `input='Seed'` binding for code 21 remains **unverified legacy
curation**. The held 2018 dictionary binds code 20 to `Autres semences` and
has no entry for code 21, which occurs on 602 applied records. The old comment
claiming `'21' is Burkina's trailing 'Autres semences' slot` contradicted that
metadata and is corrected. The held 2021 dictionary gives code 21 a different
meaning (`Plants d'hévéa, de teck`), which is not borrowed. Four relevant 2018
instrument sidecars lack held blobs; none was acquired.

All other mapping cells, including the input map and bare 20, are unchanged.
No identifier, input classification, quantity, unit, schema, axis or shared
selector/reducer changes. The 2018 builder now checks declared-index uniqueness
immediately before `to_parquet`; its error exposes scope/count and no source
keys. Equal-payload future duplicates also fail before writing.

## Actual cold and separate-process warm evidence

Two physically absent Burkina L2 roots were prepared with physical copies of
eight held, MD5-verified source blobs (6,887,434 bytes): the input roster plus
all sample/cluster prerequisites. No Burkina L2 artifact was seeded or
restamped. Baseline and candidate rebuilt through the normal Country route,
including all-wave sample and cluster tables.

| Check | Baseline | Candidate |
|---|---:|---:|
| Native 2018-19 wave lines | 22,877 | 22,877 |
| Public Country plot_inputs | 22,694 | 22,877 |
| Public sample | 20,648 | 20,648 |
| Public cluster_features | 2,035 | 2,035 |
| plot_inputs subset 2014 / 2021-22 | 0 / 0 | 0 / 0 |

The entire native wave frames compare exactly after removing only crop, with
877 changed crop labels. Every originally selected line and all 183 lost
lines retain their other keys, complete payload, units, dtypes and NA patterns.
The nine new tests independently reconstruct native source identities from the
served labels and compare every real source measurement and unit.

All recovered lines are reported in `Kilogramme` and total **2,409.58 reported
kg**. Every recovered existing API `Purchased` flag is False and existing API
`Quantity_purchased` is missing; 18 of those source records actually report
purchases and positive purchase quantity. The identity repair preserves these
bindings, rather than certifying their purchase interpretation.
The amount includes 931.60 kg on unlabelled code 21; this is a reported amount,
not a certification that the legacy Seed classification is physically correct.
The existing public `Kilogramme -> Kg` spelling is unchanged.

Independent source review found a separate contradiction: the builder comments
call `s16bq05` "purchased?" and `s16bq07a` "quantity purchased", but held q05
asks "Avez-vous obtenu [INTRANT] sous forme de cadeau ou de don?". Q07a is
gift/donation quantity with unit q07b. Purchase flag/quantity are q08/q09a,
with purchase unit q09b; the current API row unit is used quantity's q03b.
On all 22,877 applied rows, gift yes is 802 versus purchase yes 7,689, with
8,297 flag differences. Gift quantity is positive/nonmissing on 801 rows,
purchase quantity on 7,689; values or missingness differ on 8,371 rows.
Gift/used units differ on 23 rows and purchase/used units on 193, among rows
with both relevant units known. [#1188](https://github.com/ligon/LSMS_Library/issues/1188)
owns the separate field/unit routing diagnosis. This patch changes no binding,
unit, eligibility or grain; full retained payload means the current binding.

Complete sample and cluster public frames, pre-final country frames and all
six raw wave frames compare exactly across revisions, including indexes,
dtypes and **full logical attrs**. Parquet hashes/coverage match the current
expected inputs after the actual builds. Separate fresh processes repeat all
three tables, zero-row subsets and a new Country instance: outputs match the
first cold snapshots exactly and every warm parquet byte hash and nanosecond
mtime remains unchanged.

**Pre-existing documentation discrepancy, left unchanged:** CONTENTS's sample
notes state `10,224 households`, `6,961 households` and `3,218 households` for
the three waves. Both current fresh f8 and candidate builds serve
10,411 / 7,010 / 3,227 sample rows. This is recorded without calling the notes
stale or changing sample policy. Source and program conventions also remain:
2014 is EMC; 2021-22 is an EHCVM half-panel, restricted to original EAs with
distinct panel/cross-sectional weights. The existing EHCVM household formatter
stays injective, with zero input-household sample orphans.

Only the 2018-19 script currently reads `harmonize_seed_crop`; no other current
wave key receives the 2018-qualified token. Held input counterparts exist for
2014 (138,242 rows; 36 rows on repeated native keys) and 2021-22 (70,994 rows,
12,223 applied, unique native key, changed dictionary), but neither has a
current plot_inputs builder. Adding them is separate source/grain work.

## Comparators, Feature and fingerprint scope

Benin and Togo use physically copied, independently hash/coverage-verified
current cache artifacts. Their complete Country and Feature frames and logical
attrs remain exact at 10,605 / 13,730 rows, and all 12 comparator cache files
remain byte-identical. Togo's three hollow source rows are still removed by
the existing public all-payload-missing filter.

`Feature('plot_inputs')(['Burkina_Faso','Benin','Togo'], currency=None)` now
returns **47,212 rows**, versus 47,029 at f8. Each country frame exactly matches
its normal Country counterpart after the existing canonical alias/sentinel
alignment. There is zero additional Feature row loss and no warm cache write.

The existing harness was rerun for those countries and phases 1/2. Plot_inputs
has no phase-2 kwarg grid; no unrun variants are claimed. Findings change from
**8 warnings to 6 warnings**, with zero failure/error/skip at either revision.
Exactly the two Burkina input-collapse warnings disappear. The full normalized
multiset of all other **six records is exact**; only checkout/data paths were
normalized. Full diagnostics and warning examples remain private.

Actual all-country inventory: **24 Burkina hashes move; 525 other country
hashes and all 39 tagged framework fingerprints are exact**. The Org file is
a whole-country build input; no dependency is hidden or excluded. There are
exactly the three mapping-cell changes above, and the complete legacy input
map is exact. No comparator-country or shared-runtime file changed.

## Validation and residual work

- New focused regressions: **9 passed, zero skipped**.
- Final regression file on immutable f8: **8 failed, 1 passed**, expected.
- Scoped existing Benin/Togo tests: **4 passed, zero skipped** (4 unrelated
  parameter cases deliberately deselected).
- `make api-audit`: passes using an explicit mounted-image Python adapter.
- Ordinary publishing hook used; source data and full logs stay private.

The separate old Burkina/#1148 worktree remains clean at
`a63d26d7f7cdc5204931c1fbe334c1fcc6cea8fb`; no old tracked file or frozen ref
was changed. No fixture, golden, dependency, lock, acquisition, DVC CLI,
shared-cache mutation, cache promotion or assumed-fresh route was used.

This reviewed but unmerged implementation partially addresses #1123; no
production resolution is claimed. The dispatcher narrows its remaining
Niger/Senegal/CotedIvoire scope during the same PR readiness phase, with that
status explicit. `DRAFT-ISSUES.md` records that wording and the separate
residuals: [#1187](https://github.com/ligon/LSMS_Library/issues/1187) for unlabelled
code 21's meaning and unverified legacy Seed classification, and #1188 for
gift/purchase routing. The old unsupported code-21 comment is already corrected;
this does not establish its physical meaning. #1119's named-crop vocabulary
mismatch remains separate. No GitHub writes were made by this author; the
dispatcher filed #1187 and #1188.

Commands and private evidence locations are in `REPRODUCE.md`; source-safe
assertions and source MD5s are in `validation.json`.

Sue
