# Corrected object-EA saved-artifact supplement for GH1206

The fast-v2 StringDtype optimization incorrectly required np.dtype before
typed object-scalar recursion. Pandas3.0.6 uses NumpyEADtype for object EAs,
so it accepted synthetic equal-valued objects of different types. The root's
independent synthetic proof is preserved. This was a checker defect; no real
country mismatch was inferred from the toys or hidden by normalization.

The NEW helper restores typed recursion for every kind-O dtype except
explicit pd.StringDtype. All42 controls reject: the original12 plus30 object
cases across value columns,row/column axes,nested Series/DataFrame and scalar
attrs. Helper SHA is68be90fadd95ef16d46a7911bee1b26e4d671662d669ccf681230b60a09c7e94.
EA exact checks and strict nonmissing NumPy checks remain,with exact masks,
dtypes,storage,NA policy,axes/unusedlevels/codes/order and typed attrs/fields.

Every actual fast-v2 positive saved comparison was reassessed here:196
integration17 producer groups,265 integration18 groups,603 integration19
groups and96 standalone PR1200 Niger groups,1160 total. All pass with no
finding. Eight groups retain their prior stage qualifications. These are
overlapping comparison families,not a unique-pair count. The unaffected252
integration17 prefix groups used the original slow helper and were not
replayed.39 actual NumpyEADtype object blocks were checked;32 cell pairs were
strings,the remaining object blocks empty. Numeric EA/scalar checks cover all
other encountered arrays; object recursion is not inferred from absence.

Before payload reads,all frozen prior release manifests and original6468,
1885,2842,1437 manifest entries were reverified,all four immutable-record
class Git blobs matched every cohort,then1154 selected saved harmonized
operands were copied physically into this NEW owner-only root,nonsymlink,
nlink1,distinctinode. Original/copy SHA,size,mode,mtime were checked before
every read. The completed checker ran under unshare-rn,active socket/write
guards before imports,exact f8 source/cwd/PYTHONPATH/countries/config/data,
private config/XDG/tmp/DVC and threads1. No raw decode,Country/Feature/CLI
call,pytest,build/cache operation,acquisition or external job was replayed.

All prior releases and helpers remain unchanged.38 original in-memory empty
expectations remain missing (18/11/9 by18/19/1200); actual saved empty frames
have no nested numeric-array values,which is not a recovered expected-frame
proof. Two additional integration18 marked single-country Feature comparison
executions lack exact saved memory operands. Original negative stage meaning,
StringDtype/JSON/id_converted/writer qualifications and fixture no-replay stay.
Integration17's13 suite failures,doc-mtime guard/attribution gap and eight
unrequested stale companions stay explicit. PR1200's author overwritten
aggregate,auxiliary delta and populated per-call endpoint gaps stay explicit.
No blanket all-campaign acceptance or source/test coverage is claimed.

New expected operands are only the already declared artifact-only saved
normalization concatenations and nine-zero reconstruction; no lost source/
API prefix or original memory was manufactured. Original fullbyte witnesses
remain protected by unchanged manifests and are unaffected by this helper
defect. Root owns independent acceptance and publication. GitHub unchanged.

Sue.
