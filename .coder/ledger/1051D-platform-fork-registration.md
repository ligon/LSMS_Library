# Prior-Art Ledger — GH1051D platform fork-registration correction

## §1 Task
Correct actual Windows CI42687513487/windows129398181310 import failure: os.register_at_fork is unavailable on no-fork platforms. Parent80c7 validated-membership correction remains frozen.

## §2 Existing machinery
Reuse local_tools._fast_connect_after_fork and _reset_fast_connect_windows unchanged; existing D reset/owner/fork controls already test fresh child lock/reset. Actual source Windows traceback at reports/ci-admission-1673-20261109/windows-original-failed.log:331 is preserved. Git+rg prior-art floor; inherit STANDING.md/CLAUDE metadata-only, no new source capabilities.

## §3 Contract
Only `if hasattr(os, 'register_at_fork'):` wraps the original registration. Linux keeps identical callback/function; platforms without fork skip registration and import normally. No fallback callback, catch-all, helper/client/locking/selection/ref-release change. Actual Windows CI remains final full import proof.

## §4 Verification
New two pure absent/present capability cases execute the ACTUAL parsed production guard node with real stdlib os, altering only that one capability via monkeypatch; actual callback identity/one registration preserved. Relevant original fork/reset/owner cases only, no12/23/114bulk. One final existing metadata collector versus closedD map, no baseline replay/exclusions/schema changes/raw/API/provider/nativeData/sharedwrites. Private empty config/data, false credentials, mountedMAIN/unshare/thread1/2loadfile and three import identities.

## §5 Reuse decision
Reuse closed Dfocused runner/identity plugin and dd7667 metadata/run-gate operators; context/selection only. No new harness.

## §6 Remaining gates
Linux pure controls cannot prove Windows full module import; ROOT owns exact new-head configuredWindowsCI/publication. No scientific decision. Phase1 precedes product edit.
