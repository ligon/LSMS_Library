# Visualizations

Two plotting helpers that take a `Country`, a country name, or the frame the
plot is drawn from. Both are exported at the package top level (and as
`ll.visualizations`):

```python
import lsms_library as ll

ll.population_pyramid('Uganda', wave='2013-14', ghost=False)        # weighted, unweighted outlined
ll.coordinate_map('Uganda', wave='2013-14', size='weight')           # clusters on a Leaflet basemap
ll.coordinate_map('Uganda', wave='2013-14', size='weight', interactive=False)   # static matplotlib
```

`matplotlib` and `folium` are ordinary dependencies (since v0.11.0); the
interactive map (`interactive=True`, the default) renders inline in Jupyter or
saves as standalone HTML, and `interactive=False` draws a static matplotlib
scatter. `folium` is imported lazily; `matplotlib` is loaded at package import
anyway (CFEDemands, a core dependency, imports `matplotlib.pyplot`).

::: lsms_library.visualizations
    options:
      members:
        - population_pyramid
        - coordinate_map
