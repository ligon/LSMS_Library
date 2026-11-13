# Proposed separate issue; diagnosis only, no GitHub write

Title: `Burkina Faso 2018-19 plot_inputs: gift/donation fields are served as purchase fields`

Tier: B, source wiring with quantity/unit routing diagnosis before implementation.

The 2018-19 builder binds `Purchased` to `s16bq05` (Oui/Non) and `Quantity_purchased` to `s16bq07a`, while the single row unit `u` comes from `s16bq03b` (unit of total quantity used). Its comments call q05/q07a purchases. The held source metadata contradicts that interpretation:

- q05: `16B.05. Avez-vous obtenu [INTRANT] sous forme de cadeau ou de don?`
- q07a: quantity received as gift/donation; q07b its unit.
- q08: `16B.08. Avez-vous acheté [INTRANT]?`
- q09a: quantity purchased; q09b its unit, q09c purchase amount.

Primary scope is Burkina Faso 2018-19 only: 22,877 applied source rows, all q02=1, unique/nonmissing native `(grappe,menage,s16bq01)` key. Gift q05 has 802 yes / 22,075 no / zero missing. Purchase q08 has 7,689 yes / 15,188 no / zero missing. The flags disagree on 8,297 rows (705 gift yes/purchase no; 7,592 gift no/purchase yes; 97 both yes and 14,483 both no).

Gift quantity q07a has 801 nonmissing/positive and 22,076 missing; purchase q09a has 7,689 nonmissing/positive and 15,188 missing. Neither has zero or negative amounts. Quantity values or missingness differ on 8,371 rows. Every used quantity q03a is positive/nonmissing, so neither current nor hypothetical purchase-field routing produces an all-payload-missing row in this scope. Source yes/no eligibility and record retention must remain explicit.

Units require a separate, source-backed routing decision: q07b differs from the used-quantity unit on 23 gift records, and q09b differs on 193 purchase records, among records with both units known. Do not blindly rebind q09a under q03b, convert unlike units without factors, discard gifts, or silently alter the grain. Establish a coherent representation for used and purchased quantities with differing native units, preserve genuine gift information according to the declared interface, and compare complete source/native/Country/Feature frames and all old retained rows before proposing implementation.

This is independent of #1123's source-identity repair. Frozen runtime `69ed4d2028a6f989a8ac45d117a5cffc60a831fd` preserves every current binding exactly while restoring 183 native lines. All 183 recovered existing API `Purchased` values are False and existing API `Quantity_purchased` values missing, but 18 of those source records actually report purchases and positive purchase quantity. Those API names must not be treated as source-semantic evidence.

Reproduction is held-only/offline: read numeric and labelled source frames through `get_dataframe`, obtain the held file through `get_data_file` and inspect metadata only, count q05/q08 and q07a/q09a under q02=1, then trace the actual builder's q05/q07a/q03b bindings and complete current public output. Safe aggregate receipts and runnable commands are in the independent `review-1123-Burkina_Faso` evidence. No acquisition is needed for this initial diagnosis.

Dedup: exact `s16bq05` and Burkina `plot_inputs` issue searches found no owner for this defect; root additionally found exact `s16bq09a` and `plot_inputs gift` searches empty. Existing food purchase/recall and broad aggregation issues are different scopes. Recheck ownership when filing. No other country's source semantics were independently measured here.

The current identity review is PASS. Do not fold this field-routing change into the reviewed identity patch or silently certify the unverified legacy Seed classification for code 21.

Sue
