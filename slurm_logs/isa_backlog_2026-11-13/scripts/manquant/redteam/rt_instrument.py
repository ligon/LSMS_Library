"""Instrument Senegal/_/plot_features.py in a job-local clone.  usage: rt_instrument.py <clone>"""
import sys
from pathlib import Path
p = Path(sys.argv[1]) / 'lsms_library/countries/Senegal/_/plot_features.py'
s = p.read_text()
marker = ("import os as _o, json as _j, time as _t\n"
          "def _mark(**kw):\n"
          "    m = _o.environ.get('RT_MARKER')\n"
          "    if m:\n"
          "        kw.update(pid=_o.getpid(), cwd=_o.getcwd(), data=_o.environ.get('LSMS_DATA_DIR'),\n"
          "                  stack=_o.environ.get('LSMS_MAKE_BUILD_STACK'), backend=_o.environ.get('LSMS_BUILD_BACKEND'))\n"
          "        open(m, 'a').write(_j.dumps(kw) + '\\n')\n"
          "_mark(event='start')\n")
assert s.startswith('"""')
end = s.index('"""', 3) + 3
s = s[:end] + '\n' + marker + s[end:]
if "country = Country('Senegal')" in s:
    s = s.replace("country = Country('Senegal')",
                  "_t0 = _t.time()\ncountry = Country('Senegal')\n"
                  "_mark(event='country_init', sec=round(_t.time() - _t0, 3), "
                  "panel_attempted=getattr(country, '_panel_ids_attempted', None))", 1)
s = s.replace("to_parquet(p, '../var/plot_features.parquet')",
              "_mark(event='write', rows=len(p))\nto_parquet(p, '../var/plot_features.parquet')", 1)
p.write_text(s)
print('instrumented', p)
