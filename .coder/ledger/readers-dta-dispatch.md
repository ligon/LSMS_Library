# Prior-Art Ledger — reader dispatch landing follow-up

**Search tier:** ripgrep + git; inherits STANDING.md.

## §1 Task
Make a .dta suffix/magic disagreement loud, and prevent a truncated DTA whose
header passes sniffing from becoming a CSV frame under latin-1 decoding.

## §2 Existing machinery
`local_tools.get_dataframe.read_file` already sniffs `_is_stata_dta`, tries
`from_dta`, then pyreadstat before the text fallback. Extend these gates.
`tests/test_reader_sniff.py` has real synthetic DTA writers and corruption
cases. Extend them across encodings and assert the mismatch message.

## §3 Definitions
Per STANDING.md §2/§4, get_dataframe is the sanctioned reader. A .dta suffix
claims Stata content; its header and decoding must support that claim.
Per Peru CONTENTS.org 1990 entry, .SSP here is SAS XPORT, not SPSS/PC+;
`pandas.read_sas` is required to preserve all columns.

## §4 Invariants
Retain content-sniff dispatch and supported Stata decoder fallback. Do not
widen parser exception catches or let a claimed DTA reach text decoding.
Preserve XPORT, DAT/DCT, CSV and other suffix paths.

## §5 Reuse decision
Extend the existing sniff and Stata fallback gates. No new reader or parser.

## §6 Open questions
The general unreadable-binary/text-tail and uncaught path-retry SPSS failure
remain separate residuals. This approved follow-up closes only the DTA path.

## Phase 3 verification
OK (§2–§5): existing readers and exception classes retained. All 89 focused
tests pass; the separate credential-gated country-data guard skips. The
Peru EXPEND.SSP blob test ran (standalone reader suite: 37 passed). The four
new corruption cases give 3 failures/1 pass on the pre-fix tree, including
the discriminating truncated latin-1 case.
