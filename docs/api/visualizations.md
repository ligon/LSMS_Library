# Visualizations

Two plotting helpers that take a `Country`, a country name, or the frame the
plot is drawn from. Both are in `lsms_library.visualizations` (not exported at
the package top level):

```python
from lsms_library.visualizations import population_pyramid, coordinate_map

population_pyramid('Uganda', wave='2013-14', ghost=False)        # weighted, unweighted outlined
coordinate_map('Uganda', wave='2013-14', size='weight')           # clusters on a Leaflet basemap
coordinate_map('Uganda', wave='2013-14', size='weight', interactive=False)   # static matplotlib
```

`matplotlib` and `folium` are optional: they live in the `viz` dependency
group (`poetry install --with viz`). The static paths need only
`matplotlib`; the interactive map (`interactive=True`, the default) needs
`folium` and renders inline in Jupyter or saves as standalone HTML. A missing
dependency raises with the install hint rather than failing inside the plot.

::: lsms_library.visualizations
    options:
      members:
        - population_pyramid
        - coordinate_map
