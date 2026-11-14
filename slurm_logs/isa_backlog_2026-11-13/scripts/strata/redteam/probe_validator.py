"""Red-team probes of sampling._check_served_strata / from_config (candidate only).

Each probe is (label, block-or-yaml, expectation) where expectation is
'refuse' (the validator should refuse) or 'accept' (it should accept), and the
script prints what actually happened.  'GAP' marks a case that loads but is
wrong or unverifiable at load time.
"""
import sys
import json
import yaml
from lsms_library import sampling as S
from lsms_library.sampling import SamplingRecord

GOOD = {"holds": "native-label", "variable": "strate",
        "source_type": "derived-from-data", "confidence": "high",
        "locator": "measured"}


def blk(**kw):
    b = dict(GOOD)
    b.update(kw)
    return {k: v for k, v in b.items() if v is not None}


CONS = dict(holds="constructed", variable=None,
            derivation="Mali::sample::region-milieu-stratum")
probes = [
    ("padded key (parse_key strips; stored unstripped)",
     {"served_strata": blk(**{**CONS, "derivation": " Mali::sample::region-milieu-stratum "})}, "refuse"),
    ("key of this country's sample table that is NOT registered",
     {"served_strata": blk(**{**CONS, "derivation": "Mali::sample::no-such-entry"})}, "refuse?"),
    ("native-label naming a variable the wave never reads",
     {"served_strata": blk(variable="no_such_var (nofile.dta)")}, "GAP"),
    ("absent with source_type local-documentation",
     {"served_strata": blk(holds="absent", variable=None, source_type="local-documentation")}, "?"),
    ("absent with confidence low",
     {"served_strata": blk(holds="absent", variable=None, confidence="low")}, "accept"),
    ("holds with trailing space",
     {"served_strata": blk(holds="absent ", variable=None)}, "refuse"),
    ("empty served_strata mapping beside panel None",
     {"served_strata": {}, "panel": None}, "refuse"),
    ("served_strata + top-level notes (design field) without triple",
     {"served_strata": blk(), "notes": "x"}, "refuse"),
    ("served_strata + top-level locator without triple",
     {"served_strata": blk(), "locator": "x"}, "refuse"),
    ("constructed key with table slot 'Sample' (case)",
     {"served_strata": blk(**{**CONS, "derivation": "Mali::Sample::region-milieu-stratum"})}, "refuse"),
    ("constructed key with country 'mali' (case)",
     {"served_strata": blk(**{**CONS, "derivation": "mali::sample::region-milieu-stratum"})}, "refuse"),
    ("native-code with confidence low and a wb-catalog source",
     {"served_strata": blk(holds="native-code", source_type="wb-catalog", confidence="low")}, "accept"),
    ("note as a list",
     {"served_strata": blk(note=["a"])}, "refuse"),
    ("holds as YAML bool",
     {"served_strata": blk(holds=True)}, "refuse"),
]
print("== from_config probes (country 'Mali', wave '2018-19')")
for label, block, expect in probes:
    try:
        r = SamplingRecord.from_config("Mali", "2018-19", block)
        got = f"ACCEPTED (strata_holds={r.strata_holds!r}, derivation={dict(r.served_strata or {}).get('derivation')!r})"
    except Exception as e:
        got = f"REFUSED {type(e).__name__}: {str(e)[:220]}"
    print(f"- [{expect}] {label}\n    -> {got}")

print("\n== duplicate keys in YAML")
dup = """
Sampling:
  '2014-15':
    served_strata:
      holds: native-code
      variable: x
      source_type: derived-from-data
      confidence: high
      locator: a
    served_strata:
      holds: absent
      source_type: derived-from-data
      confidence: high
      locator: b
  '2014-15':
    served_strata:
      holds: absent
      source_type: derived-from-data
      confidence: high
      locator: c
"""
try:
    data = yaml.load(dup, Loader=S._SamplingLoader)
    print("loader ACCEPTED duplicates; result:", json.dumps(data))
except Exception as e:
    print("loader REFUSED duplicates:", type(e).__name__, str(e)[:200])

print("\n== vocab error message for an unknown holds value")
try:
    SamplingRecord.from_config("A", "1", {"served_strata": blk(holds="decoded")})
except Exception as e:
    print(str(e))

print("\n== unrecorded placeholder vs new served_strata-only record, attribute view")
u = SamplingRecord.unrecorded("Mali", "2014-15")
r = S.sampling_records("Mali")["2014-15"]
for name in ("design_tag", "source_type", "confidence", "documented", "strata_holds",
             "cluster_id_continuity", "notes"):
    print(f"  {name:22s} placeholder={getattr(u, name)!r:40.40s} new={getattr(r, name)!r:40.40s}")
print("  json ok:", bool(json.dumps(r)), "hash ok:", isinstance(hash(r), int))
