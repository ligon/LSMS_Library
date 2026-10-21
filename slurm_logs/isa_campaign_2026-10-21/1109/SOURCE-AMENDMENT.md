# Source-backed amendment, 2026-10-21

The initial patch inherited the false CONTENTS.org claim "Module D records no organic quantity".
All seven held headers label ag_d37a as quantity applied and ag_d37b as unit.
Six sources loaded by the current builder contain 11,075 affirmative records, 11,068 positive amounts, seven missing amounts and 11,067 observed units.
The no-quantity premise is withdrawn: these fields were unmaterialized, not unasked.

Root-approved design: retain affirmative ag_d36 gate and Applied carrier; read numeric ag_d37a and decode ag_d37b using its own dictionary (2 Kilogram, 10 Bucket, 11 Wheelbarrow, 12 Ox cart, 13 Other (Specify)), with canonical Kilogramme/Ox-Cart spellings.
Missing units use existing U_UNKNOWN; unlisted recorded codes remain normalized raw codes.
Keep optional-field compatibility, organic-only min_count overlay and all nonorganic behavior.
Add both fields to 2016 Cross_Sectional whitelist.
No container weight, organic N share, imputation, rescaling or free-text interpretation.

Evidence: campaign control review-1109/{source-semantics.json,source-amounts.json,source-organic-unit-counts.json} and campaign lane slurm_logs/isa_backlog_2026-10-21/1109-source-amendment.md.
Earlier JSONs in this directory describe historical f8/c08 behavior, not the amended result.
Final source-backed results and runnable commands will be added separately.

Verification: first fail source-backed tests against c08, then targeted tests (max four loadfile workers); physically private cold Malawi build; each organic quantity/unit checked against six actual sources; exact nonorganic/nitrogen and Malawi/Uganda Feature comparisons modulo organic unit rekeying.
Update #1118 denominators in report only.

Sue
