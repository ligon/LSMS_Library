import lsms_library, os, sys
top = os.environ['CHECKOUT']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.paths import data_root
print("OK import", lsms_library.__file__, "data_root", data_root())
