"""The plotting helpers are part of the top-level API (2026-09-07), and
importing the package must not drag in their optional heavy dependencies."""
import subprocess
import sys


def test_top_level_names_are_the_module_functions():
    import lsms_library as ll
    assert ll.coordinate_map is ll.visualizations.coordinate_map
    assert ll.population_pyramid is ll.visualizations.population_pyramid


def test_importing_the_package_does_not_import_matplotlib_or_folium():
    # A fresh interpreter: the current one may have them loaded by other tests.
    code = ("import sys, lsms_library; "
            "print(sorted(m for m in ('matplotlib', 'folium') if m in sys.modules))")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                         env={**__import__('os').environ, "LSMS_SKIP_AUTH": "1"})
    assert out.returncode == 0, out.stderr[-500:]
    assert out.stdout.strip() == "[]", out.stdout
