# Draft residual issue updates — no GitHub writes

Runtime `69ed4d2028a6f989a8ac45d117a5cffc60a831fd` passed independent full review
at `3848778d0e8676c798883c11ea3e33fc88e8a591`. The Burkina implementation is
reviewed but unmerged; no production resolution is claimed. The dispatcher
narrows #1123 and files separate residuals during the same PR readiness phase,
with this status explicit. The dispatcher has filed #1187 and #1188 below;
this author has made no GitHub write. This documentation amendment awaits
narrow review.

## Existing #1123 residual

Title: `#569 residual F: diagnose remaining Country-level plot_inputs collapses in Niger, Senegal and CotedIvoire`

Tier: B for the remaining demonstrated source-identity defects; current reproduction still required per country.

Body:

> Reviewed but unmerged: Burkina Faso 2018–19 source-identity repair, PR `<fill during PR readiness>`, runtime commit `69ed4d2028a6f989a8ac45d117a5cffc60a831fd`, independent full-review head `3848778d0e8676c798883c11ea3e33fc88e8a591`. At immutable f8, the held 22,877-row source loses 183 native line-items: 163 extras from 150 conflicting-payload groups plus 20 equal-payload extras from distinct native slots. The reviewed implementation preserves all 183 using the source-specific mapping and pre-write uniqueness guard, without changing the core reducer. This is not a claim that production is resolved.
>
> Remaining countries: Niger, Senegal and CotedIvoire. Original #1123 grain stamps reported Niger 51 / 35 / 6 / 3, Senegal 10 and CotedIvoire 1. These are historical observations, not current raw-source measurements from the Burkina diagnosis. Reproduce each using native household/input/crop/unit/round identity, quantify exact/complementary/conflicting cases and fanout before choosing a fix.
>
> Niger’s EXTENSION household-ID fix is already merged; CONTENTS still separately records harmonization collisions in 2011–12/2014–15 and relates them to #614/#637. Do not re-fix the ID namespace or silently include this work in historical reducer censuses. Benin/Togo’s injective-map fix is already present and current strict builds remain healthy; blindly copying only their two named-category splits leaves 98 BF collisions and is not a general solution.
>
> Burkina's unlabelled code-21 authority (#1187), gift/purchase field/unit routing (#1188) and #1119's named-crop vocabulary mismatches are separate residuals. The identity repair preserves the existing API bindings; Purchased/Quantity_purchased do not establish purchase semantics. No acquisition or physical crop inference is required merely to preserve a reported source identity.

## Separate Burkina source-authority issue: [#1187](https://github.com/ligon/LSMS_Library/issues/1187)

Title: `Burkina Faso 2018–19: establish source meaning of unlabelled agricultural input code 21`

Tier: D — source authority / curation decision.

Body:

> The held `s16b_me_bfa2018.dta` has 602 applied records with native `s16bq01=21`, but its dictionary ends at 20, bound to `Autres semences`. The retained `input='Seed'` classification is unverified legacy curation. The former comment calling 21 another Autres-semences slot has been corrected in the reviewed identity patch; that correction does not establish a physical meaning.
>
> The reviewed but unmerged identity patch in PR `<fill during PR readiness>` retains these records under the approved source-qualified unknown crop token. This preserves source rows and existing API values; it does not certify that input=Seed is correct or claim a production correction. Establish or revise that classification only from an authoritative 2018 instrument/producer record, with a separate before/after and tests.
>
> Do not borrow 2021’s numeric meaning: that wave binds code 21 to `Plants d'hévéa, de teck` and code 22 to `Autres semences`; it has no current plot_inputs builder. The 2018 household questionnaire XLS files and nomenclature PDFs have sidecars but no held blobs (see the exact four MD5s in the private diagnosis source-gap manifest). No files were acquired. Obtaining source authority is separate from preserving the existing native identity.

## Separate purchase/gift routing issue: [#1188](https://github.com/ligon/LSMS_Library/issues/1188)

Title: `Burkina Faso 2018-19 plot_inputs: gift/donation fields are served as purchase fields`

Tier: B -- source wiring with quantity/unit routing diagnosis before implementation.

> The builder currently serves gift flag q05 as `Purchased` and gift quantity q07a as `Quantity_purchased`; actual purchases are q08/q09a. Gift and purchase quantities have their own native units q07b/q09b, while the API row uses q03b, the used-quantity unit. The held 22,877 applied rows have 802 gift yes versus 7,689 purchase yes, with 8,297 flag differences. Quantities are positive/nonmissing on 801 versus 7,689 rows, with 8,371 value/missingness differences. Gift/used units differ on 23 rows and purchase/used units on 193, among rows with both units known.
>
> Eighteen of the 183 recovered lines actually report purchases and positive purchase quantity, although all recovered existing API Purchased values are False and Quantity_purchased missing. Source-identity conservation preserves these existing bindings; it does not certify the names' purchase semantics. The reviewed identity patch makes no rebind, physical-unit or grain change. Diagnose coherent gift/purchase/used quantity routing separately; do not substitute q09a blindly under q03b or discard reported gifts.

The complete source-safe reviewer draft and reproduction instructions remain in
the private independent review's `PURCHASE-RESIDUAL.md`; the full review remains
pinned to 3848778d and runtime 69ed4d, without rerunning source APIs for this
documentation amendment.

Sue
