"""F4 mutation check: does tests/test_sampling.py::_native_config_problems bite?
Run with cwd = candidate clone, LSMS_COUNTRIES_ROOT = a MUTATED copy of its
countries tree (mutations applied by the job script).  Prints problems per case."""
import sys
import warnings

warnings.simplefilter("ignore")
sys.path.insert(0, ".")
from lsms_library.paths import countries_root  # noqa: E402
from tests.test_sampling import _native_config_problems  # noqa: E402

print("countries_root:", countries_root())
cases = [
    ("Niger", "2011-12", "strate (NER_2011_ECVMA_v01_M_Stata8/ecvmamen_p1_en.dta)",
     "a: wave mapping.py gains a relabelling `strata` formatter -> expect a problem"),
    ("Uganda", "2009-10", "stratum (GSEC1.dta)",
     "b: declaration gains a mapping -> expect 'not declared bare'"),
    ("Niger", "2014-15", "strate (NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2014_P1P2_Conso.dta)",
     "c: sub-df hook df_strate writes strata -> expect a hook problem"),
    ("Mali", "2017-18", "strate (EACI17_ECHANTILLON.dta)",
     "d: sample hook delegates the relabel to a helper -> shallow check, may MISS"),
    ("Uganda", "2010-11", "stratum (GSEC1.dta)",
     "e: country-module `strata` formatter (uganda.py) -> expect a problem"),
    ("Uganda", "2005-06", "stratum (GSEC1.dta)",
     "control: unmutated (but uganda.py mutated for e) -> expect the e problem too"),
]
for c, w, var, what in cases:
    try:
        p = _native_config_problems(c, w, var)
    except Exception as e:  # noqa: BLE001
        p = [f"RAISED {type(e).__name__}: {e}"]
    print(f"- {c} {w}: {what}\n    problems={p}")
