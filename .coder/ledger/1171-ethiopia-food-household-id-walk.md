# Ethiopia #1171: country food household ID walk

## Scope and evidence

Ledger first on public development `cb2cf55453f9e20393a76909e66f8d2ccdea5bc5`. The food compiler, country framework and local_tools files are byte-identical to the source used by the closed diagnostic. No raw survey or cover parse is needed for this repair. Root owns public issue reconciliation; #1171 remains broader than this food identity defect and #771 remains the independent nutrient/FCT reproducibility issue.

The unchanged legacy compiler and current pre-final framework each returned 326,456 rows, but strict parity failed on 75,367 observed household `i` positions (2013-14:30,463; 2015-16:44,904). Quantity/Expenditure payloads were exactly equal by original row position, not by household key; `t/j/u/s` and their unused dictionaries matched. Attrs differed. The failed seven-input map-guard setup, failed framework parity and all private outputs remain preserved at recovery/Ethiopia/1171-framework-only-result-v1. Do not relabel these failures as passes.

## Prior art and definition in force

- Country._compute_panel_ids and its production JSON read route load committed panel_ids/updated_ids; do not duplicate that route or cast the maps.
- Country._aggregate_wave_data stage 2 walks each native contribution through local_tools.id_walk with state='raw' when household `i` exists, id_converted is absent/false and maps exist. This is the exact target contract.
- Ethiopia livestock/food_coping already reuse local_tools.id_walk rather than the obsolete local j walker. Their older 'idempotent' docstrings do not supersede GH1245's exactly-once chained-map semantics, defined/tested in local_tools.py and tests/test_id_walk.py.
- Framework authority is settled by #788 comments6244122374/6259387154. Inherit .coder/ledger/STANDING.md, CLAUDE.md and canonical data_info; no new crosswalk or scientific reducer.

## Country caveat and invariants

The old local change_id treats `j` as household, contradicting canonical food `(t,i,j,u,s)` where `i` is household and `j` is food. Correct only this identity operation using the established shared walker. Preserve actual country/wave declarations, folder deduplication, existing squeeze, concat, food-label rename, writer, row eligibility, quantities, expenditure, units, source channels and recall. Native frames already marked id_converted must not be walked again. Source raw-state ambiguity uses the same state='raw' contract as the framework; do not follow chains transitively or invent unknown household IDs. #1357's exclusion of anonymous household-keyed rows remains unchanged.

## Verification

Update the existing real Country/Wave metadata selector carrier: its two old tests deliberately assert the wrong j walk/cover read and must instead test household i, unchanged food j and explicit absence of cover reads. Retain discovery/error/retirement/future/folder cases and source immutability. Add meaningful chained raw, already-walked and map-failure/no-partial-write tests, using invented maps only. No actual/data verification is inferred from synthetic tests. Proposed actual verification reuses five closed native frames plus the saved framework RETURN; no legacy or raw-cover replay. Preserve matching CALL/RETURN/storage and failed predicates independently.
