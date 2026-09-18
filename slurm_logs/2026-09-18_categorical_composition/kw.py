import sys; sys.path.insert(0,'/local/job39002769/mirrors/LSMS_Library')
from lsms_library.local_tools import all_dfs_from_orgfile
import tempfile, pathlib
p=pathlib.Path(tempfile.mkdtemp())/'t.org'
p.write_text("""#+name: demo
#+key: Code
#+role: Code=key/code; Preferred Label=out/canonical
| Code | Preferred Label |
|    1 | Kg              |

#+name: demo2
#+begin_example
| A | B |
| 1 | 2 |
#+end_example
""")
d=all_dfs_from_orgfile(p)
print("parsed:", {k:(list(v.columns),len(v)) for k,v in d.items()})
