# Narrow documentation descendant receipt

Frozen descendant `cfd0782766068da6dc7e7605334e957f988a4ab1` is clean and published. Narrow independent documentation review is pending.
The underlying full PASS remains pinned to `3848778d0e8676c798883c11ea3e33fc88e8a591` and runtime `69ed4d2028a6f989a8ac45d117a5cffc60a831fd`.

Only CONTENTS, the task ledger, REPORT.md and DRAFT-ISSUES.md change. They qualify conservation as preserving existing API Purchased/Quantity_purchased bindings, quote the gift/donation contradiction, record aggregate scope and link #1187/#1188. Burkina remains reviewed but unmerged. No binding, unit, grain, mapping, schema, driver, test or original validation.json change.

All 14882 other tracked Git blobs and modes are exact to the reviewed head, including 11656 country-tree entries excluding CONTENTS and all 249 test entries. The genuine independent 384 inventory was physically copied without altering its original receipt and agrees with the original author inventory. All 549 descendant country hashes and 39 framework fingerprints were recomputed from metadata in a fresh offline image process and compare exactly: additional changes are 0/0, with zero existing-source reads or network attempts. The standard fingerprint method checks absent local raw paths before reading their sidecars; those ENOENT checks are metadata, not source reads. CONTENTS is explicitly excluded by country.py's _ORG_HASH_SKIP. No source API or test cohort was rerun. Full frozen tracked SHA256 receipts are in frozen-tracked.json; old implementation/reviewer proofs remain untouched.

Private verifier mistakes are retained: an initial reader monkeypatch changed introspected country hashes, so its invalid inventory was excluded; an overbroad open guard then blocked absent raw-path checks and was corrected to reject existing-source reads while allowing ENOENT. A premature finalizer failed before the replacement inventory existed. These were private metadata-verifier errors, with no tracked edits or raw-source read. The final proof uses unmodified fingerprinted library objects and the independently reviewed original inventory.

Diffstat:

```
.coder/ledger/1123-burkina-input-identity.md       | 31 ++++++++++++---
 lsms_library/countries/Burkina_Faso/_/CONTENTS.org | 46 ++++++++++++++++++++--
 .../Burkina_Faso/1123/DRAFT-ISSUES.md              | 33 +++++++++++++---
 .../Burkina_Faso/1123/REPORT.md                    | 42 ++++++++++++++++----
 4 files changed, 131 insertions(+), 21 deletions(-)
```

Root review instructions: compare `3848778d0e8676c798883c11ea3e33fc88e8a591..cfd0782766068da6dc7e7605334e957f988a4ab1` and inspect the four authorized prose files, the precise gift/purchase qualification and the reviewed-but-unmerged residual wording. Check identical runtime/config/test/country-input blobs and zero additional 549/39 hash movement using validation.json and the two hash inventories. Carry forward the existing full source/runtime review; no data rerun is needed for this descendant.

Runnable metadata-only reproduction (384-hashes.json is the genuine independent review's original 384 inventory):

```sh
P=/local/job40255479/control/isa-backlog-20261021/Burkina_Faso/1123/doc-descendant
RUN=/local/job40255479/control/isa-backlog-20261021/Burkina_Faso/1123/implementation/run.sh
bash "$RUN" candidate "$P/hashes.py" "$P/descendant-hashes.json"
bash "$RUN" candidate "$P/finish.py"
```

No remaining task processes. No GitHub write. Sue.
