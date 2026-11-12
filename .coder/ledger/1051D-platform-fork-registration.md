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

### Phase3 — actual finite closure
OK (§§2–4): tested9931ce282fd0206ecfc90b9bc3ee925fc5e4fc83, focused4411 actualouter0/child0,7PASS0F/E/S (new2capability+existing5fork/reset/owner),7.54s pytest/13.959746s wrapper,3identities/privateconfig+data0/sourceclean. Solemetadata55758 actualouter0/child0/132.107320s: vsclosedD05f/headerpreserved,0/452country0/1903wave(18None)0/37BTF0/186FIF;all14nonhash/topkeysets exact/errors0/denials0/privateconfig+data0. Original Windows importfailure42687513487 preserved; actualnewWindowsCI stillfinalimportgate. No12/23/114bulk/baseline/raw/API replay. Guard removal reconstructs entireparentlocal_tools byteexact; callbacks/allhelperbodies/existingtests unchanged, no exclusions/schema policy. External reports/1051D-platform-fork-registration-20261109 bind fullsourcepatch/7JUnit/3identities/originalfailure/completecensuscomparison and actualhandles. This final ledger-only child changes no tested/censused byte. ROOT owns integration/GitHub/CI/release.
