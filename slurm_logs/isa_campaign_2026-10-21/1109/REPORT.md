# Malawi organic input quantities - GH #1109

Malawi's organic-use records now retain missing Quantity and carry the raw affirmative Module D answer as optional nullable `Applied=True`. This preserves the fact that fertilizer was used without inventing an amount. Existing nonorganic quantities and the framework's all-null-row policy are unchanged.

The shared assembly keeps its original reducers. An organic-only `sum(min_count=1)` is aligned on the complete existing key, including null crop/unit components; numeric and observed-zero organic inputs are preserved. `Applied` follows Ethiopia's existing convention and is reduced with `max`.

The first proposed global min_count replacement failed actual cold validation: it deleted all 11,075 organic records plus 437 nonorganic records at Country finalization. The accepted extension carries the explicit organic-use answer and confines the quantity correction to organic groups. The rejected result is recorded separately; no such lost-row result is accepted.

## Verification

Frozen baseline runtime: `f8d236694c26fe9e709c5548f02c70f74ea72c33`. Both source builds ran offline with exact checkout/config/data identity, threads limited to one, existing image Python, physical private L1 copies and fresh private Malawi L2. No DVC CLI, new source acquisition, shared cache mutation/promotion, dependency edit or fixture regeneration occurred.

- **36 targeted tests passed**, including the new 11-case actual-helper/finalizer regression and the private warm Malawi nitrogen check. The Ethiopia warm test was explicitly excluded from this Malawi seed.
- **21 existing plot declaration, harmonization and permutation checks passed.**
- The untouched base produces **six regression failures and five compatibility passes**.
- Cold and fresh warm Malawi retain **180,184 rows**, exact original index labels/order/dtypes, original column order and all preexisting measures except the intended **11,075 organic Quantity values: 0 -> NA**. `Applied` is boolean, true on those rows and missing on all 169,109 others.
- Organic counts per wave are **1,991 / 822 / 3,617 / 4,645** for 2010-11 / 2013-14 / 2016-17 / 2019-20.
- Nitrogen output is **exactly frame-equal**: 33,466 plots, total **840,235.7795800001 kg N**; the organic refusal warning remains.
- `Feature('plot_inputs')(['Malawi','Uganda'], currency=None)` retains both countries and all **264,400 rows** (180,184 + 84,216). Original pooled index/order/dtypes and all preexisting measures are exact except the intended organic Quantity correction. Uganda is unchanged.
- The relevant scanner has **15 warnings, zero failures and zero errors** on both sources. All check identities and warning records are unchanged except the expected constant-column detail now also names `Applied`: its nonmissing values are affirmative. Existing null crop/unit and scheme-order warnings remain outside this patch.
- Measured hash impact: **22/22 Malawi table hashes change, 0/24 Uganda hashes change**. A Malawi country rewarm is required; no original cache was promoted or cleared.

The initial comparator copy from the older shared cache was stale and caused a rejected offline scanner run. Final comparison uses physically copied, footer-verified current-release Uganda plot_inputs/sample parquets from the validated v0.16 seed; original files are unchanged.

## Remaining scope

`RESIDUAL.md` drafts source diagnosis for the 437 nonorganic records exposed by the rejected global reducer. They retain existing values here; their reported-use semantics are not inferred. The draft reconciles #905's reducer sweep, #1118's unit issue and #637's key audit. No new issue or comment was posted.

Malawi still has 28,724 native null-unit rows. After this correction, 17,649 have nonmissing Quantity and 11,075 are organic-use records with missing Quantity and Applied true. The old #1118 classification counted fabricated organic zero as an amount. Unit codebook #1111, product composition #1106 and native marker issues #1118/#1119 remain unchanged.

The ledger verification is **OK against sections 2-4**: existing use carrier and min_count machinery are reused, reported zero is preserved, full native grain and other reducers are unchanged, and no core policy is bypassed. Independent review remains the dispatcher's next step.

- Sue
