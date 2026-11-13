# Draft residual issue updates — no GitHub writes

These drafts apply **after** an approved Burkina implementation is merged. No fix has been implemented during this diagnosis.

## Existing #1123 residual

Title: `#569 residual F: diagnose remaining Country-level plot_inputs collapses in Niger, Senegal and CotedIvoire`

Tier: B for the remaining demonstrated source-identity defects; current reproduction still required per country.

Body:

> Resolved so far: Burkina Faso 2018–19 source-identity collision, PR `<to fill after merge>`, commit `<to fill>`. The held 22,877-row source loses 183 native line-items on current Country: 163 extras from 150 conflicting-payload groups plus 20 equal-payload extras from distinct native slots. The approved patch must preserve all 183 using the source-specific mapping and pre-write uniqueness guard, without changing the core reducer.
>
> Remaining countries: Niger, Senegal and CotedIvoire. Original #1123 grain stamps reported Niger 51 / 35 / 6 / 3, Senegal 10 and CotedIvoire 1. These are historical observations, not current raw-source measurements from the Burkina diagnosis. Reproduce each using native household/input/crop/unit/round identity, quantify exact/complementary/conflicting cases and fanout before choosing a fix.
>
> Niger’s EXTENSION household-ID fix is already merged; CONTENTS still separately records harmonization collisions in 2011–12/2014–15 and relates them to #614/#637. Do not re-fix the ID namespace or silently include this work in historical reducer censuses. Benin/Togo’s injective-map fix is already present and current strict builds remain healthy; blindly copying only their two named-category splits leaves 98 BF collisions and is not a general solution.
>
> Burkina’s source-authority question for unlabelled code 21 and #1119’s named-crop vocabulary mismatches are separate residuals. No acquisition or physical crop inference is required merely to preserve a reported source identity.

## Separate Burkina source-authority issue

Title: `Burkina Faso 2018–19: establish source meaning of unlabelled agricultural input code 21`

Tier: D — source authority / curation decision.

Body:

> The held `s16b_me_bfa2018.dta` has 602 applied records with native `s16bq01=21`, but its dictionary ends at 20, bound to `Autres semences`. The current country table classifies literal `21` as `input='Seed'` and calls it another Autres-semences slot; that is legacy curation, not corroborated by the held 2018 metadata.
>
> After the identity fix in PR `<fill after merge>`, retain these records under the approved source-qualified unknown crop token rather than inventing a physical crop meaning. This preserves source rows and values; it does not certify that input=Seed is correct. Establish or revise that classification only from an authoritative 2018 instrument/producer record, with a separate before/after and tests.
>
> Do not borrow 2021’s numeric meaning: that wave binds code 21 to `Plants d'hévéa, de teck` and code 22 to `Autres semences`; it has no current plot_inputs builder. The 2018 household questionnaire XLS files and nomenclature PDFs have sidecars but no held blobs (see the exact four MD5s in the private diagnosis source-gap manifest). No files were acquired. Obtaining source authority is separate from preserving the existing native identity.

Sue
