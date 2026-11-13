# Separate residual drafts; no filing or implementation

## Uganda crop country Make builder ignores strict wave requests

Diagnosis-first, country builder/routing scope. Existing GH1091/#788 policy is
related; deduplication and ownership remain for root. This draft claims no new
owner and does not authorize retirement of the Uganda builder.

On actual parent989 and candidate runtime929, the exact request
`crop_production(waves=['2005-06'])` under Make returns all seven OTHER supported
waves, although2005-06 declares no crop-production table. Counts are133331 and
133761 respectively. The var parquet is hashless with no coverage manifest.
The supported strict request `['2018-19']` also returns the whole country.
Fresh-process populated state repeats those values/axes/dtypes/full typed attrs.
Make rewrites8 crop endpoint mtimes only; bytes/size/modes remain exact.

Default returns0 for unsupported2005 and15616/15630 for2018. Its unsupported
public empty result follows a full-country raw fallback (133331/133761) stamped
with requested-only `['2005-06']` coverage metadata. This is retained evidence,
not a claim that those raw rows belong to2005. Full raw/public/footer comparisons
and per-wave t counts are private in the eight subset cohorts.

A separate change must trace the country builder's id_walk/audit transport and
all callers, establish framework-authoritative supported and unsupported subset
behavior, and preserve maximal grain/full attrs without adding a concatenator
or post-hoc subset normalization. GH1198 makes no builder/Makefile/framework edit.

## Uganda parcel-yield coverage contradicts the blanket100% claim

Diagnosis-first, land-key/area routing and documentation scope; deduplication
and an owner remain for root. Existing Uganda CONTENTS already records distinct
crop and plot-feature vocabularies and requires `on='parcel'`.

`transformations.py:4934` says "on Uganda the parcel key matches on 100% of crop
rows." With normally rebuilt held-source area tables on BOTH actual heads,
40409 converted harvest parcel keys match only14022 positive-area keys; literal
plot routing matches0. The area table has42841 rows,37636 positive-area rows and
37634 positive parcel keys. The full14022 parcel-yield frames are exactly
unchanged by GH1198, including full typed attrs. This is not a1198 regression.

The remaining work is to trace release-specific household/parcel/plot key
construction and actual area eligibility, separate genuinely unmeasured area
from linkage mismatch, and correct the documented coverage claim on source
evidence. Do not infer a prefix repair, fabricate area, change join grain or
alter the measured yield frames from these aggregate counts alone.

Sue (GPT-6),2026-10-22.
