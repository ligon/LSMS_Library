# Malawi organic inputs: source-backed amendment

Frozen candidate `c08abb3f1` mechanically preserves rows and missingness, but
its claim that Module D records no organic quantity is false. Independent
review read all seven held source headers and the six sources actually
loaded by the builder (the 2010 Panel source is redundant here).

The sources label `ag_d36` as organic fertilizer use, `ag_d37a` as the quantity
applied and `ag_d37b` as its unit. The 11,075 positive-use records include
11,068 positive amounts, seven missing amounts and 11,067 observed units.
Every source's unit dictionary agrees: 2 Kilogram, 10 Bucket, 11 Wheelbarrow,
12 Ox cart, 13 Other (Specify), with case differences only.
Reviewer evidence is private under `review-1109/source-semantics.json` and
`source-amounts.json` in the campaign control directory.

Root chooses to restore the recorded organic amounts/units within this
country fix, rather than merely correct prose and defer available data.

1. Extend `_organic_block` to read `ag_d37a` and `ag_d37b` when present. Keep
   the original positive `ag_d36` gate and carry `Applied=True` from it.
2. Read quantities numerically using existing country helpers. Preserve
   observed zero/numeric values; keep missing values missing. Do not impute,
   rescale or infer any organic amount.
3. Decode these five organic-unit codes using their own evidenced dictionary,
   with existing canonical spellings. Do not borrow food/acquired/applied
   inorganic codebooks. Keep an unlisted recorded code as a normalized raw
   code, and fill only truly missing identity with existing `U_UNKNOWN` per
   the signed #1041 contract. Do not infer container weights or organic N.
4. Retain the organic-only `sum(min_count=1)` overlay and optional Applied
   carrier. Preserve all 437 unadjudicated nonorganic records and every
   pre-existing nonorganic field. Other Malawi missing-unit work remains
   #1118; no broader codebook repair is included.
5. Correct ledger, tests, country notes and reports with dated source
   evidence. Quote the superseded no-quantity claim as a contradiction;
   distinguish unmaterialized from unasked. Remove no scientific caveat.

First fail regressions on `c08abb3f1` for all five codes, positive/missing/zero
quantity, missing/unlisted identity, no-use gates and absent optional source
fields. Verify direct helper and finalized public-API rows. Cold baseline
and amended builds must account for every organic row's quantity/unit against
the actual inputs, preserve the whole row count and other fields modulo
the declared unit rekeying, and retain missing-amount use records. Compare
nitrogen exactly and explain any actual change before claiming success.
Re-run relevant Feature checks with the verified healthy comparator.

Source free-text `other` descriptions are a distinct inventory; do not invent
an interpretation while wiring the recorded amount/unit fields.

Sue

## Public finalization amendment

The candidate wave outputs kept six unlabelled organic-unit codes as literal
`1` (five rows) and `3` (one row). The ordinary Country finalizer interpreted
them through the unrelated food-unit map as Kilogramme and 90kg Bag. Root
approved source-qualified `Organic unit code 1` / `Organic unit code 3`
fallbacks, superseding the literal numeric-string fallback above. This retains
the observed raw identity without supplying an unrecorded unit meaning. It
leaves the eight genuinely missing units distinct and changes no shared
mapping or framework policy. Add actual finalizer regressions and repeat
per-source/public row accounting before freeze.
