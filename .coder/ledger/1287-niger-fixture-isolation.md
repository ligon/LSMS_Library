# Niger integration carrier — fixture isolation (ledger first)

Source-only successor to258b67f9220ecb17654f054336024ff45759ef9d. Root reviewed
A1 lossless audit union and requested deliberate portable fixture isolation before
public normal CI; no new local50-case guard or test execution is authorized.
Reuse integration/logical-audit ledgers, STANDING.md and Nigeria1318 fixture pattern.

Current actor install calls Country.waves before execute's snapshot, so ordinary
CI may already have private registration pyc present. That happy-path warm-cache
side effect is not a deliberate bytecode policy. Setup before yield also occurs
outside the current final cache cleanup, so a setup error can leave private cached
path resolution after monkeypatch restores environment. Strict snapshots remain.

Repair only actor fixture: child monkeypatch.context owns fixture environment
LSMS_COUNTRIES_ROOT/LSMS_DATA_DIR and scoped sys.dont_write_bytecode=True. Enclose
complete setup and yield in that context; outer finally clears both paths caches
after context restores prior env/flag on success, setup failure, test failure or
snapshot refusal. Keep every existing assertion and all220 case parameterizations.

Deliberately retain ALL function/attribute/item patches on the SAME outer
monkeypatch owner used by tests. Tests later override Wave.grab_data,
local_tools.to_parquet, ledger globals etc. A child patch stack for fixture attrs
would undo first, then outer test undo could resurrect an actor stub. Child owns
only fixture env/bytecode; ordinary pytest outer undo retains existing correct
reverse ordering for actor/test attrs/items. Test-only LSMS_NO_CACHE stays on its
outer owner; actor does not own that variable, so no overlap inversion exists.

No product/helper/compiler/Make/CONTENTS/schema/source/science edit. Originalv3,
old195+new25 assertions/source tests and prior reports/receipts stay frozen in their
own packets. This successor changes only fixture setup/cleanup plus this ledger.
Prepare source AST/scope/hash checks; no pytest/collection/project imports/actors,
actual data/cache/provider/native/Make/API, jobs, push/PR/public writes. Root will
review full source then owns draft and normal configured CI. Real data jobs remain
skipped; valid scriptless Make/freshness1288/actual source/API/science stay held.
