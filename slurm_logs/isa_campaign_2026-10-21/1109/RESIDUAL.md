# Draft remaining scope - not filed

Proposed title: **Malawi plot_inputs: determine reported use for 437 nonorganic rows whose missing quantities become zero**

Tier B, diagnosis first. This is separate from the source-backed organic-input correction in #1109 (amended 2026-10-21).

A private offline cold build at the v0.16.0 runtime yields 437 nonorganic records that disappear when the existing Quantity sum is replaced globally by `sum(min_count=1)`. Their other payload columns are all missing. They comprise 390 Seed, 24 NPK Fertilizer, 11 Urea Fertilizer, 6 DAP Fertilizer, 4 Herbicide, 1 Fungicide and 1 Insecticide. Of these, 419 also have a missing unit and 18 carry a unit. A further 76 nonorganic records have zero as their only payload but survive `min_count=1`; those are outside this 437-record missing-quantity finding.

Remaining work:

1. Trace the 437 groups back to each wave's source records, preserving the complete household, plot, input, crop and unit key. Determine whether the instrument records actual use, a crop roster entry without a seed-use answer, an omitted quantity question or missing response. Read the applicable Module D/G questions and gates; do not infer `Applied=True` merely from an item type or crop code.
2. Where use is positively reported, carry that answer separately using the existing optional `Applied` convention, and preserve missing Quantity. Where no input use is reported, choose and document the appropriate retained record or drop rule.
3. Rebuild privately and account for every row/value change, with observed zero controls. Keep #1111's disputed fertilizer-unit codebook and #1106's product composition outside this change.

Deduplication:

- **#905** is the remaining vectorized min_count reducer sweep, including Malawi nonfood sites. This finding is related and can be attached there if the issue explicitly includes *correctness and source adjudication* before vectorization. Replacing a reducer mechanically loses these rows; an optimization/equivalence task alone does not settle their meaning.
- **#1118** concerns missing unit identity and the existing `U_UNKNOWN` ruling. Filling a unit cannot distinguish a missing amount from measured zero or establish application. The 419 overlapping unit-null rows remain within that unit-normalization issue; their missing-quantity semantics are this separate question. After the source-backed #1109 amendment, 17,649 nonorganic null-unit rows remain (all with the existing nonmissing Quantity); the 11,075 organic records now carry their recorded units or Unknown for eight missing units, and only seven have missing Quantity. Six recorded but unlabelled organic codes retain source-qualified identities; their physical meaning is not inferred. The original 28,724/all-organic-missing denominator described c08 and is superseded. #1118 remains partially open.
- **#637** also lists the Malawi assembly helper for key-soundness review, but the 437 finding is not proof of an identifier collision. Preserve its separate diagnosis rather than renaming this a key defect.

#1109 keeps the existing nonorganic sums unchanged. No GitHub issue or comment was created.

- Sue
