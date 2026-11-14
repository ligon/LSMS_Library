"""Niger 2014-15: served strata vs the Conso file's strate, per grappe; coverage."""
import subprocess
import warnings

import pandas as pd

warnings.simplefilter("ignore")
import lsms_library as ll  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402

print("head", subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip())
d = "Niger/2014-15/Data/NER_2014_ECVMA-II_v02_M_STATA8/"
raw = pd.read_stata(str(get_data_file(d + "ECVMA2014_P1P2_Conso.dta")),
                    columns=["GRAPPE", "MENAGE", "EXTENSION", "strate"])
raw["strate"] = raw["strate"].astype("string")
n = raw.groupby("GRAPPE")["strate"].nunique(dropna=True)
print("conso grappes:", n.size, "with !=1 strate:", int((n != 1).sum()),
      "rows with NA strate:", int(raw["strate"].isna().sum()))
g = raw.dropna(subset=["strate"]).groupby("GRAPPE")["strate"].first()
g.index = g.index.map(lambda x: str(int(x)))
s = ll.Country("Niger").sample(waves=["2014-15"]).reset_index()
got = s["v"].astype(str).map(g)
print("served rows:", len(s), "v found in conso:", int(got.notna().sum()),
      "served == conso strate:", int((got.astype("string") == s["strata"].astype("string")).sum()))
wel = pd.read_stata(str(get_data_file(d + "ECVMA2014welfare_T1c.dta")),
                    columns=["GRAPPE", "MENAGE", "EXTENSION"], convert_categoricals=False)


def keys(df):
    df = df.dropna(subset=["GRAPPE", "MENAGE", "EXTENSION"])
    return set(zip(df["GRAPPE"].astype(int), df["MENAGE"].astype(int), df["EXTENSION"].astype(int)))


kw, kc = keys(wel), keys(raw.assign(EXTENSION=pd.to_numeric(raw["EXTENSION"], errors="coerce")))
print("welfare households:", len(kw), "of which in conso:", len(kw & kc))
print("served value counts == raw grappe-weighted:",
      s["strata"].astype(str).value_counts().sort_index().to_dict() ==
      got.astype(str).value_counts().sort_index().to_dict())
